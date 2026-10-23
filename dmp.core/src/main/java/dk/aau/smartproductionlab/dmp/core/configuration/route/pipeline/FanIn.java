package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataTransformerConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.Evaluation;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SourceEventAdapter;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;
import dk.aau.smartproductionlab.dmp.core.util.TransformerMeta;

/**
 * Fan-in: the mirror image of {@link FanOut} (SPEC §6).
 *
 * <p>Both halves of a mapping share one shape and one vocabulary:
 *
 * <pre>
 *   fan-in    N source contributions ─▶ ONE {@link KeyedSnapshot} keyed by SourceId
 *             (disassembled at the source, gated on completeness)
 *   transform ONE evaluation of the shared snapshot
 *   fan-out   the shared result      ─▶ ONE {@link KeyedSnapshot} keyed by SinkId
 *             (parsed once)            ─▶ N independent payloads
 * </pre>
 *
 * <p>So the two obey the same three rules: the shared value is materialised
 * exactly once, it is never mutated afterwards, and every per-entity payload
 * derives from that one immutable instance. A source can never observe a
 * sibling's payload, and a sink can never observe a sibling's output.
 *
 * <p>The difference is the direction of completion: fan-in waits for every
 * required source before it can emit (a partial sources map is never produced),
 * and a contribution only drives evaluation when its source's {@link Evaluation}
 * mode makes it a trigger. A trigger the readiness gate holds back stays pending
 * until an evaluation passes the gate.
 *
 * <p>Values are frozen ({@link JsonCodec#freeze}) once, when the cache accepts them;
 * every evaluation, the transformation and the sinks share them from then on.
 */
public final class FanIn implements ManagedRouteResource {

	private static final Logger LOGGER = LoggerFactory.getLogger(FanIn.class);
	/** Exchange property flagging a source that actually contributed a value. */
	public static final String CONTRIBUTION_PROPERTY = SourceEventAdapter.CONTRIBUTION_PROPERTY;
	/** When the first source of this message accepted its value ({@link System#nanoTime}). */
	private static final String ARRIVED_PROPERTY = FanIn.class.getName() + ".arrived";
	private static final String ACCEPTED_SNAPSHOT_PROPERTY = FanIn.class.getName() + ".acceptedSnapshot";
	private static final String CONTRIBUTING_SOURCE_IDS_PROPERTY = FanIn.class.getName() + ".contributingSourceIds";
	private static final String ACCEPTED_TIMES_PROPERTY = FanIn.class.getName() + ".acceptedTimes";
	/** The endpoints of this message whose value differs from their previous one. */
	private static final String CHANGED_PROPERTY = FanIn.class.getName() + ".changed";
	/** The SourceIds whose new value triggers this evaluation (Lua's {@code meta.sources.<id>.trigger}). */
	private static final String TRIGGERS_PROPERTY = FanIn.class.getName() + ".triggers";

	private final RoutesConfiguration routesConfiguration;
	private final String mappingId;
	private final Map<String, String> sourceIdByEndpoint = new LinkedHashMap<>();
	private final Map<String, Evaluation> evaluationByEndpoint = new LinkedHashMap<>();
	private final SourceCache cache = new SourceCache();
	private final Map<String, AffordanceValidation> validators = new LinkedHashMap<>();
	/** Lua's meta.sinks: each SinkId with the kind of connector it writes through. */
	private final Map<String, Object> sinkMeta = new LinkedHashMap<>();
	/** One evaluation of the mapping at a time, so a later one never writes older state. */
	private final java.util.concurrent.locks.ReentrantLock evaluation = new java.util.concurrent.locks.ReentrantLock(true);
	private volatile org.apache.camel.ProducerTemplate producer;
	private volatile org.apache.camel.Endpoint collect;

	@Override
	public void bind(org.apache.camel.CamelContext context) { producer = context.createProducerTemplate(); }

	@Override
	public void close() {
		cache.clear();
		if (producer != null) producer.stop();
	}

	public FanIn(RouteConfiguration routeConfig, RoutesConfiguration routesConfiguration) {
		this(routeConfig, routesConfiguration, AffordanceValidation.configuredMode());
	}

	FanIn(RouteConfiguration routeConfig, RoutesConfiguration routesConfiguration, AffordanceValidation.Mode mode) {
		this.routesConfiguration = routesConfiguration;
		this.mappingId = routeConfig.getRouteId();
		for (String endpointId : routeConfig.getDatasources()) {
			sourceIdByEndpoint.put(endpointId, resolveSourceId(routesConfiguration, endpointId));
			DataSourceConfiguration config = routesConfiguration.getDatasources().get(endpointId);
			evaluationByEndpoint.put(endpointId, config == null ? Evaluation.ON_UPDATE : config.getEvaluation());
			validators.put(endpointId, new AffordanceValidation(
					config == null ? null : config.getSchema(), "source " + endpointId, mode));
		}
		for (String endpointId : routeConfig.getDatasinks()) {
			DataSinkConfiguration sink = routesConfiguration.getDatasinks().get(endpointId);
			if (sink != null && sink.getSinkId() != null) sinkMeta.put(sink.getSinkId(), TransformerMeta.sink(sink.getConnectionURI()));
		}
	}

	/** Every SourceId this mapping requires before it may evaluate: all but those read at evaluation. */
	public Set<String> expectedSourceIds() {
		return sourceIdByEndpoint.entrySet().stream()
				.filter(entry -> evaluationByEndpoint.get(entry.getKey()) != Evaluation.READ_AT_EVALUATION)
				.map(Map.Entry::getValue).collect(java.util.stream.Collectors.toSet());
	}

	/**
	 * The raw SourceId an endpoint publishes under: its explicit
	 * {@code sourceId}, else the endpoint's own unique id.
	 */
	public static String resolveSourceId(RoutesConfiguration routesConfiguration, String endpointId) {
		DataSourceConfiguration config = routesConfiguration.getDatasources().get(endpointId);
		String sourceId = config == null ? null : config.getSourceId();
		return sourceId == null || sourceId.isBlank() ? endpointId : sourceId;
	}

	/**
	 * The per-source step: normalise the payload, apply this source's declared
	 * {@code valuePath} (fine granularity, ADR-020) at the DMP boundary, and
	 * record the value under its SourceId. The mirror of {@code FanOut}'s
	 * per-sink shaping — both are the only place an entity's payload is
	 * derived.
	 *
	 * <p>No contribution is recorded for an empty or failed read, so "never
	 * delivered" and "delivered nothing" are treated alike by the gate.
	 */
	public Processor disassemble(String dataSourceId) {
		String sourceId = sourceIdByEndpoint.get(dataSourceId);
		return exchange -> {
			exchange.setProperty(CONTRIBUTION_PROPERTY, false);
			Object body = exchange.getMessage().getBody();
			LOGGER.debug("source {}: raw body type={} value={}", sourceId,
					body == null ? "null" : body.getClass().getName(),
					body instanceof String ? ((String) body) : body);
			Object value = disassemble(exchange, dataSourceId);
			if (value == null) {
				return;
			}
			if (!validators.get(dataSourceId).incoming(
					value == JsonPointer.JSON_NULL ? null : value)) {
				return;
			}
			if (exchange.getProperty(ARRIVED_PROPERTY) == null) exchange.setProperty(ARRIVED_PROPERTY, System.nanoTime());
			SourceCache.Snapshot accepted = cache.accept(sourceId, value,
					evaluationByEndpoint.get(dataSourceId) == Evaluation.ON_CHANGE);
			exchange.setProperty(ACCEPTED_SNAPSHOT_PROPERTY, accepted.values());
			exchange.setProperty(ACCEPTED_TIMES_PROPERTY, accepted.times());
			exchange.setProperty(CONTRIBUTION_PROPERTY, true);
			if (accepted.changed()) changed(exchange).add(dataSourceId);
		};
	}

	/**
	 * One acquired payload may contribute independently to several authored sources. A
	 * source whose value cannot be read from it (an unresolved {@code valuePath}, a schema
	 * mismatch) contributes nothing without keeping the others from contributing; the
	 * payload fails only when no source could read it.
	 */
	public Processor disassemble(List<String> dataSourceIds) {
		List<Processor> disassemblers = dataSourceIds.stream().map(this::disassemble).toList();
		return exchange -> {
			bufferStream(exchange);
			List<String> contributing = new ArrayList<>();
			RuntimeException firstFailure = null;
			for (int i = 0; i < disassemblers.size(); i++) {
				try {
					disassemblers.get(i).process(exchange);
				} catch (RuntimeException e) {
					if (firstFailure == null) firstFailure = e;
					LOGGER.warn("source {} did not contribute: {}", dataSourceIds.get(i), e.getMessage());
					exchange.setProperty(CONTRIBUTION_PROPERTY, false);
				}
				if (contributed(exchange)) contributing.add(dataSourceIds.get(i));
			}
			if (contributing.isEmpty() && firstFailure != null) throw firstFailure;
			exchange.setProperty(CONTRIBUTING_SOURCE_IDS_PROPERTY, List.copyOf(contributing));
			exchange.setProperty(CONTRIBUTION_PROPERTY, !contributing.isEmpty());
		};
	}

	/**
	 * A value read at evaluation: decoded like a contribution, never cached. Fails when the message carries no
	 * value the source can use.
	 */
	public Object decode(String dataSourceId, Exchange exchange) {
		bufferStream(exchange);
		if (exchange.getMessage().getBody() == null) throw new IllegalStateException("the read returned no value");
		Object value = disassemble(exchange, dataSourceId);
		if (value == null) throw new IllegalStateException("the read returned no value");
		Object plain = value == JsonPointer.JSON_NULL ? null : value;
		if (!validators.get(dataSourceId).incoming(plain)) throw new IllegalStateException("the value does not match the source's schema");
		return JsonCodec.freeze(plain);
	}

	@SuppressWarnings("unchecked")
	private static Set<String> changed(Exchange exchange) {
		Set<String> changed = exchange.getProperty(CHANGED_PROPERTY, Set.class);
		if (changed == null) {
			changed = new java.util.LinkedHashSet<>();
			exchange.setProperty(CHANGED_PROPERTY, changed);
		}
		return changed;
	}

	/** Reads a one-shot stream body once, so every source of the group sees the payload. */
	private static void bufferStream(Exchange exchange) {
		Object body = exchange.getMessage().getBody();
		if (body instanceof java.io.InputStream stream && !(body instanceof org.apache.camel.StreamCache)) {
			try {
				exchange.getMessage().setBody(stream.readAllBytes());
			} catch (java.io.IOException e) {
				throw new IllegalArgumentException("could not read incoming source payload", e);
			}
		}
	}

	/** Route filter: only a contributing exchange is worth collecting. */
	public boolean contributed(Exchange exchange) {
		return Boolean.TRUE.equals(exchange.getProperty(CONTRIBUTION_PROPERTY));
	}

	/**
	 * True when this contribution triggers an evaluation: a contributing source evaluates {@code OnUpdate}, or
	 * {@code OnChange} and its value changed, or an earlier trigger is still held back by the readiness gate.
	 * The triggering SourceIds are kept for Lua's {@code meta}.
	 */
	public boolean selectedTrigger(Exchange exchange) {
		Set<String> changed = changed(exchange);
		Set<String> triggers = new java.util.LinkedHashSet<>();
		Object grouped = exchange.getProperty(CONTRIBUTING_SOURCE_IDS_PROPERTY);
		if (grouped instanceof List<?> ids) for (Object id : ids) {
			String endpointId = String.valueOf(id);
			Evaluation evaluation = evaluationByEndpoint.getOrDefault(endpointId, Evaluation.ON_UPDATE);
			if (evaluation == Evaluation.ON_UPDATE || evaluation == Evaluation.ON_CHANGE && changed.contains(endpointId))
				triggers.add(sourceIdByEndpoint.get(endpointId));
		}
		exchange.setProperty(TRIGGERS_PROPERTY, Set.copyOf(triggers));
		return !triggers.isEmpty() || cache.holdsTriggers();
	}

	/**
	 * Evaluates the mapping through its collector route, one evaluation at a time. The sources map
	 * is snapshotted inside the lock, so an evaluation starts from a cache at least as new as the
	 * one before it and its sinks never overwrite a newer evaluation's writes. Each source still
	 * accepts its value on its own thread; a source waits only while another evaluation runs.
	 *
	 * <p>The evaluation runs on an exchange of its own: no header or property of the source's message
	 * reaches the transformation or a sink. A failed evaluation fails the source's message. A trigger
	 * that arrives before every source has delivered is held until an evaluation passes the gate.
	 */
	public Processor evaluate(String collectUri) {
		return exchange -> {
			@SuppressWarnings("unchecked")
			Set<String> triggers = exchange.getProperty(TRIGGERS_PROPERTY, Set.class);
			Long arrived = exchange.getProperty(ARRIVED_PROPERTY, Long.class);
			evaluation.lock();
			try {
				long started = System.nanoTime();
				SourceCache.Snapshot current = cache.snapshot();
				if (!complete(current.values())) {
					cache.hold(triggers);
					LOGGER.debug("fan-in: waiting for sources {} (have {})", expectedSourceIds(), current.values().ids());
					return;
				}
				Set<String> evaluated = new java.util.LinkedHashSet<>(triggers == null ? Set.of() : triggers);
				evaluated.addAll(cache.releaseHeld());
				var event = new dk.aau.smartproductionlab.dmp.core.diagnostics.EvaluationEvent();
				event.begin();
				Exchange run = new org.apache.camel.support.DefaultExchange(exchange.getContext());
				run.setProperty(ACCEPTED_SNAPSHOT_PROPERTY, current.values());
				run.setProperty(ACCEPTED_TIMES_PROPERTY, current.times());
				run.setProperty(TRIGGERS_PROPERTY, Set.copyOf(evaluated));
				if (producer == null) producer = exchange.getContext().createProducerTemplate();
				if (collect == null) collect = exchange.getContext().getEndpoint(collectUri);
				producer.send(collect, run);
				if (run.getException() != null) exchange.setException(run.getException());
				if (event.shouldCommit()) {
					event.mapping = mappingId;
					event.triggers = String.join(",", evaluated);
					event.waitedNanos = arrived == null ? 0 : Math.max(0, started - arrived);
					event.succeeded = run.getException() == null;
					event.commit();
				}
			} finally {
				evaluation.unlock();
			}
		};
	}

	/** Whether every source the mapping waits for has delivered at least once. */
	private boolean complete(KeyedSnapshot accepted) {
		for (String id : expectedSourceIds()) if (!accepted.has(id)) return false;
		return true;
	}

	/**
	 * The completeness gate, as a Camel predicate for {@code filter(...)}.
	 * Evaluation proceeds only once EVERY source of the mapping has delivered
	 * at least once; a partial sources map is never emitted. A {@code false}
	 * result silently drops this evaluation — the next arrival (or poll tick)
	 * re-runs the gate.
	 */
	public org.apache.camel.Predicate gatePredicate() {
		Set<String> required = expectedSourceIds();
		return exchange -> {
			KeyedSnapshot accepted = exchange.getProperty(ACCEPTED_SNAPSHOT_PROPERTY, KeyedSnapshot.class);
			if (accepted == null) return false;
			List<String> missing = new ArrayList<>();
			for (String id : required) {
				if (!accepted.has(id)) {
					missing.add(id);
				}
			}
			if (missing.isEmpty()) {
				return true;
			}
			LOGGER.debug("fan-in: waiting for sources {} (have {})", missing, accepted.ids());
			return false;
		};
	}

	/**
	 * Materialises a decoded, keyed value tree for the transformer chain. Keys
	 * are AIMC SourceIds; Lua returns values keyed by SinkId.
	 */
	public Processor merge() {
		return exchange -> {
			KeyedSnapshot accepted = exchange.getProperty(ACCEPTED_SNAPSHOT_PROPERTY, KeyedSnapshot.class);
			if (accepted == null) throw new IllegalStateException("fan-in exchange has no accepted source snapshot");
			exchange.getMessage().setBody(accepted.copyValues());
			@SuppressWarnings("unchecked")
			Map<String, java.time.Instant> times = exchange.getProperty(ACCEPTED_TIMES_PROPERTY, Map.class);
			Map<String, Object> sources = new LinkedHashMap<>();
			@SuppressWarnings("unchecked")
			Set<String> triggers = exchange.getProperty(TRIGGERS_PROPERTY, Set.class);
			if (times != null) times.forEach((id, time) -> sources.put(id,
					TransformerMeta.source(time, triggers != null && triggers.contains(id))));
			exchange.setProperty(DataTransformerConfiguration.META, TransformerMeta.of(sources, sinkMeta,
					TransformerMeta.invocation(java.time.Instant.now())));
		};
	}

	/**
	 * Decodes incoming text/bytes only when content type or the declared JSON
	 * schema identifies a JSON payload, then applies the source's {@code valuePath}.
	 * An absent pointer keeps the whole typed value; a pointer on a missing path
	 * is a clear route error (SPEC §5.4).
	 */
	private Object disassemble(Exchange exchange, String dataSourceId) {
		DataSourceConfiguration config = routesConfiguration.getDatasources().get(dataSourceId);
		String pointer = config == null ? null : config.getValuePath();
		Object body = exchange.getMessage().getBody();
		if (body == JsonPointer.JSON_NULL) {
			return JsonPointer.JSON_NULL;
		}
		// Connector POJOs have no JSON meaning — run them through registered
		// Camel type converters, which yield the connector's canonical form.
		if (!Boolean.TRUE.equals(exchange.getProperty(SourceEventAdapter.NORMALIZED_VALUE_PROPERTY))
				&& !(body instanceof String || body instanceof Number || body instanceof Boolean
				|| body instanceof byte[] || body instanceof java.io.InputStream
				|| body instanceof org.apache.camel.StreamCache
				|| body instanceof java.util.Map || body instanceof java.util.List)) {
			Object converted = exchange.getContext().getTypeConverter().tryConvertTo(String.class, exchange, body);
			if (converted instanceof String s) {
				body = s;
			}
		}
		Object json = decodeIncoming(exchange, config, body);
		checkDeviceSchema(exchange, config, dataSourceId, pointer);
		if (pointer == null || pointer.isBlank()) {
			// Connector canonical forms may wrap the payload in a single-key
			// envelope keyed by the endpoint id. Strip it: the cached source
			// payload is the message value.
			json = unwrapCanonicalEnvelope(json, dataSourceId);
			return json == JsonPointer.JSON_NULL ? JsonPointer.JSON_NULL : json;
		}

		java.util.Optional<Object> leaf = JsonPointer.extract(json, pointer);
		if (leaf.isEmpty()) {
			throw new IllegalArgumentException("Source '" + dataSourceId + "': valuePath '" + pointer
					+ "' does not resolve in the incoming payload");
		}
		Object value = leaf.get();
		return value == JsonPointer.JSON_NULL ? JsonPointer.JSON_NULL : value;
	}

	/**
	 * The AID describes the device: what the source's AID schema says about its value must exist
	 * on the device with a compatible type, as the connector declares it. A source the AID
	 * misdescribes contributes nothing; each value fails with the differences.
	 */
	private static void checkDeviceSchema(Exchange exchange, DataSourceConfiguration config, String dataSourceId, String pointer) {
		Object device = exchange.getMessage().getHeader(SourceEventAdapter.DEVICE_SCHEMA_HEADER);
		if (device == null || config == null || config.getSchema() == null) return;
		Object member = pointer == null || pointer.isBlank() ? device
				: dk.aau.smartproductionlab.dmp.core.util.SchemaCompatibility.member(device, JsonPointer.parse(pointer).tokens());
		if (member == null)
			throw new IllegalArgumentException("Source '" + dataSourceId + "': the AID references " + pointer
					+ ", which the device's type does not have");
		java.util.List<String> differences = dk.aau.smartproductionlab.dmp.core.util.SchemaCompatibility.differences(config.getSchema(), member);
		if (!differences.isEmpty())
			throw new IllegalArgumentException("Source '" + dataSourceId + "': the AID schema"
					+ (pointer == null || pointer.isBlank() ? "" : " of " + pointer) + " does not match the device's type: "
					+ String.join("; ", differences));
	}

	/** Strips a single-key envelope whose key is the endpoint id itself. */
	private static Object unwrapCanonicalEnvelope(Object json, String dataSourceId) {
		if (json instanceof Map<?, ?> map && map.size() == 1
				&& map.containsKey(dataSourceId)) {
			return map.get(dataSourceId);
		}
		return json;
	}

	private Object decodeIncoming(Exchange exchange, DataSourceConfiguration config, Object body) {
		// The cache freezes what a source accepts (its one copy): a decoded value is passed on as it is.
		if (Boolean.TRUE.equals(exchange.getProperty(SourceEventAdapter.NORMALIZED_VALUE_PROPERTY))) {
			return body;
		}
		if (body instanceof Map<?, ?> || body instanceof List<?> || body == JsonPointer.JSON_NULL) {
			return body;
		}
		String text = null;
		if (body instanceof String value) {
			text = value;
		} else if (body instanceof org.apache.camel.StreamCache streamCache) {
			try {
				java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
				streamCache.reset();
				streamCache.writeTo(bytes);
				if (!jsonContent(exchange) && !jsonSchema(config.getSchema())) return bytes.toByteArray();
				text = bytes.toString(java.nio.charset.StandardCharsets.UTF_8);
			} catch (java.io.IOException e) {
				throw new IllegalArgumentException("could not read incoming source stream cache", e);
			}
		} else if (body instanceof byte[] bytes) {
			if (!jsonContent(exchange) && !jsonSchema(config.getSchema())) return bytes.clone();
			text = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
		} else if (body instanceof java.io.InputStream stream) {
			try {
				byte[] bytes = stream.readAllBytes();
				if (!jsonContent(exchange) && !jsonSchema(config.getSchema())) return bytes;
				text = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
			} catch (java.io.IOException e) {
				throw new IllegalArgumentException("could not read incoming source payload", e);
			}
		}
		if (text == null) {
			return body;
		}
		if (!jsonContent(exchange) && !jsonSchema(config.getSchema())) return text;
		Object decoded = JsonCodec.parseJson(text);
		return decoded == null ? JsonPointer.JSON_NULL : decoded;
	}

	private static boolean jsonContent(Exchange exchange) {
		String contentType = exchange.getMessage().getHeader(Exchange.CONTENT_TYPE, String.class);
		if (contentType == null) return false;
		String mediaType = contentType.split(";", 2)[0].trim().toLowerCase(java.util.Locale.ROOT);
		return "application/json".equals(mediaType) || mediaType.endsWith("+json");
	}

	/** Whether a source's schema makes its payload decode as JSON regardless of content type. */
	public static boolean jsonSchema(Object rawSchema) {
		if (!(rawSchema instanceof Map<?, ?> schema)) return false;
		Object type = schema.get("type");
		if ("object".equals(type) || "array".equals(type)) return true;
		return schema.containsKey("properties") || schema.containsKey("items");
	}

	/**
	 * Thread-safe latest-value cache of SourceId → payload. The fan-in
	 * counterpart of the fan-out sink snapshot: both accumulate keyed
	 * contributions and hand out exactly one immutable {@link KeyedSnapshot}.
	 */
	static final class SourceCache {
		private final Map<String, Object> latest = new LinkedHashMap<>();
		/** When each source's latest value was accepted. */
		private final Map<String, java.time.Instant> accepted = new LinkedHashMap<>();

		/** Triggers the readiness gate held back; the next evaluation that passes it releases them. */
		private final Set<String> held = new java.util.LinkedHashSet<>();

		/** The cached values with the times they were accepted, taken together; whether the accepted value changed. */
		record Snapshot(KeyedSnapshot values, Map<String, java.time.Instant> times, boolean changed) { }

		/**
		 * Freezes and caches {@code value}, the one copy it gets. Only an {@code OnChange} source
		 * ({@code compare}) compares it with its previous value; for the others {@code changed} is false.
		 */
		synchronized Snapshot accept(String sourceId, Object value, boolean compare) {
			Object next = value == null ? JsonPointer.JSON_NULL : JsonCodec.freeze(value);
			boolean changed = compare && (!latest.containsKey(sourceId) || !sameValue(latest.get(sourceId), next));
			latest.put(sourceId, next);
			accepted.put(sourceId, java.time.Instant.now());
			return snapshot(changed);
		}

		synchronized Snapshot snapshot() { return snapshot(false); }

		/** Shares the frozen values: only the SourceId map itself is new. */
		private Snapshot snapshot(boolean changed) {
			Map<String, Object> values = new LinkedHashMap<>();
			latest.forEach((key, cachedValue) -> values.put(key,
					cachedValue == JsonPointer.JSON_NULL ? null : cachedValue));
			return new Snapshot(KeyedSnapshot.of(values), Map.copyOf(accepted), changed);
		}

		synchronized void hold(Set<String> triggers) {
			if (triggers != null) held.addAll(triggers);
		}

		synchronized boolean holdsTriggers() { return !held.isEmpty(); }

		synchronized Set<String> releaseHeld() {
			Set<String> released = Set.copyOf(held);
			held.clear();
			return released;
		}

		/** Decoded values compared by content: maps, lists, byte arrays and numbers by their value. */
		private static boolean sameValue(Object left, Object right) {
			if (left instanceof byte[] a && right instanceof byte[] b) return java.util.Arrays.equals(a, b);
			if (left instanceof Number a && right instanceof Number b) {
				try { return new java.math.BigDecimal(a.toString()).compareTo(new java.math.BigDecimal(b.toString())) == 0; }
				catch (NumberFormatException notFinite) { return Double.compare(a.doubleValue(), b.doubleValue()) == 0; }
			}
			if (left instanceof Map<?, ?> a && right instanceof Map<?, ?> b) {
				if (a.size() != b.size()) return false;
				for (Map.Entry<?, ?> entry : a.entrySet())
					if (!b.containsKey(entry.getKey()) || !sameValue(entry.getValue(), b.get(entry.getKey()))) return false;
				return true;
			}
			if (left instanceof List<?> a && right instanceof List<?> b) {
				if (a.size() != b.size()) return false;
				for (int i = 0; i < a.size(); i++) if (!sameValue(a.get(i), b.get(i))) return false;
				return true;
			}
			return java.util.Objects.equals(left, right);
		}

		synchronized void clear() { latest.clear(); accepted.clear(); held.clear(); }
	}
}
