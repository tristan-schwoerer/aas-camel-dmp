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
import dk.aau.smartproductionlab.dmp.core.plan.Evaluation;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.core.execution.DmpExecutors;
import dk.aau.smartproductionlab.dmp.core.execution.SerialLane;
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
	public static final String CONTRIBUTION_PROPERTY = Source.CONTRIBUTION_PROPERTY;
	/** When the first source of this message accepted its value ({@link System#nanoTime}). */
	private static final String ARRIVED_PROPERTY = FanIn.class.getName() + ".arrived";
	private static final String ACCEPTED_SNAPSHOT_PROPERTY = FanIn.class.getName() + ".acceptedSnapshot";
	private static final String CONTRIBUTING_SOURCE_IDS_PROPERTY = FanIn.class.getName() + ".contributingSourceIds";
	private static final String ACCEPTED_TIMES_PROPERTY = FanIn.class.getName() + ".acceptedTimes";
	/** The endpoints of this message whose value differs from their previous one. */
	private static final String CHANGED_PROPERTY = FanIn.class.getName() + ".changed";
	/** The SourceIds whose new value triggers this evaluation (Lua's {@code meta.sources.<id>.trigger}). */
	private static final String TRIGGERS_PROPERTY = FanIn.class.getName() + ".triggers";

	private final String mappingId;
	/** The mapping's sources by {@link Source#id()}. */
	private final Map<String, Source> sourcesById = new LinkedHashMap<>();
	private final Map<String, String> sourceIdByEndpoint = new LinkedHashMap<>();
	private final Map<String, Evaluation> evaluationByEndpoint = new LinkedHashMap<>();
	private final SourceCache cache = new SourceCache();
	private final Map<String, AffordanceValidation> validators = new LinkedHashMap<>();
	/** Lua's meta.sinks: each SinkId with the kind of connector it writes through. */
	private final Map<String, Object> sinkMeta = new LinkedHashMap<>();
	/** Evaluations a mapping may have waiting; one more is merged into the newest (SerialLane). */
	static final int PENDING_EVALUATIONS = Integer.getInteger("dmp.pendingEvaluations", 1024);
	/** The mapping's evaluations, one at a time on the shared workers, so a later one never writes older state. */
	private SerialLane<Pending> lane;
	private volatile org.apache.camel.ProducerTemplate producer;
	private volatile org.apache.camel.Endpoint collect;

	/**
	 * An evaluation to run: its triggers, the sources as they were when it was admitted, and when the first of its
	 * triggers arrived ({@link System#nanoTime}). A merged one evaluates the later sources for both triggers.
	 */
	private record Pending(Set<String> triggers, SourceCache.Snapshot sources, Long arrived) {
		Pending merge(Pending later) {
			Set<String> both = new java.util.LinkedHashSet<>(triggers);
			both.addAll(later.triggers);
			return new Pending(Set.copyOf(both), later.sources, arrived != null ? arrived : later.arrived);
		}
	}

	@Override
	public void bind(org.apache.camel.CamelContext context) {
		producer = context.createProducerTemplate();
		lane(context);
	}

	private synchronized SerialLane<Pending> lane(org.apache.camel.CamelContext context) {
		if (lane == null) lane = new SerialLane<>("mapping " + mappingId, DmpExecutors.of(context).workers(),
				PENDING_EVALUATIONS, pending -> run(context, pending), Pending::merge);
		return lane;
	}

	/** Lets the queued evaluations finish; past 5/6 of {@code timeout} the rest is dropped. */
	@Override
	public void drain(java.time.Duration timeout) throws Exception {
		SerialLane<Pending> current;
		synchronized (this) { current = lane; }
		if (current == null) return;
		long deadline = System.nanoTime() + timeout.toNanos();
		if (current.drain(java.time.Duration.ofNanos(timeout.toNanos() * 5 / 6))) return;
		current.close();
		if (!current.drain(java.time.Duration.ofNanos(Math.max(0, deadline - System.nanoTime()))))
			throw new IllegalStateException("mapping " + mappingId + ": an evaluation did not end before bundle retirement");
	}

	@Override
	public void close() {
		synchronized (this) { if (lane != null) lane.close(); }
		cache.clear();
		if (producer != null) producer.stop();
	}

	@Override
	public Map<String, Long> metrics() {
		SerialLane<Pending> current;
		synchronized (this) { current = lane; }
		return current == null ? Map.of() : Map.of("pendingEvaluations", (long) current.pending(),
				"mergedEvaluations", current.merged());
	}

	/** The fan-in of {@code mappingId}'s {@code sources}; {@code sinkUris}, by SinkId, give Lua's {@code meta.sinks}. */
	public FanIn(String mappingId, List<Source> sources, Map<String, String> sinkUris) {
		this(mappingId, sources, sinkUris, AffordanceValidation.configuredMode());
	}

	FanIn(String mappingId, List<Source> sources, Map<String, String> sinkUris, AffordanceValidation.Mode mode) {
		this.mappingId = mappingId;
		for (Source source : sources) {
			String endpointId = source.id();
			sourcesById.put(endpointId, source);
			sourceIdByEndpoint.put(endpointId, source.sourceId());
			evaluationByEndpoint.put(endpointId, source.evaluation());
			validators.put(endpointId, new AffordanceValidation(source.schema(), "source " + endpointId, mode));
		}
		sinkUris.forEach((sinkId, uri) -> sinkMeta.put(sinkId, TransformerMeta.sink(uri)));
	}

	/** Every SourceId this mapping requires before it may evaluate: all but those read at evaluation. */
	public Set<String> expectedSourceIds() {
		return sourceIdByEndpoint.entrySet().stream()
				.filter(entry -> evaluationByEndpoint.get(entry.getKey()) != Evaluation.READ_AT_EVALUATION)
				.map(Map.Entry::getValue).collect(java.util.stream.Collectors.toSet());
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
		return contribute(dataSourceId, false);
	}

	/** One source's contribution; an initial read ({@code seed}) only fills a source that has no value yet. */
	private Processor contribute(String dataSourceId, boolean seed) {
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
			SourceCache.Snapshot accepted = seed ? cache.seed(sourceId, value) : cache.accept(sourceId, value,
					evaluationByEndpoint.get(dataSourceId) == Evaluation.ON_CHANGE);
			// An update that arrived before the initial read is newer than it.
			if (accepted == null) return;
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
		return contribute(dataSourceIds, false);
	}

	/**
	 * An initial read of {@code dataSourceIds} ({@link Source#initialRead()}): contributes like {@link #disassemble(List)},
	 * except that a source which already has a value keeps it, as an update that arrived first is the newer value.
	 */
	public Processor seed(List<String> dataSourceIds) {
		return contribute(dataSourceIds, true);
	}

	private Processor contribute(List<String> dataSourceIds, boolean seed) {
		List<Processor> disassemblers = dataSourceIds.stream().map(id -> contribute(id, seed)).toList();
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
	 * Admits an evaluation and returns: the source's thread only accepts its value, the evaluation runs later on
	 * the shared workers, one evaluation of the mapping at a time ({@link SerialLane}). Each evaluation sees the
	 * sources as they were when it was admitted, and evaluations run in the order they were admitted, so each
	 * update is evaluated with its own value and a later evaluation never writes older state.
	 *
	 * <p>The readiness gate is decided here, in the order the values were accepted: a trigger that arrives before
	 * every source has delivered is held until a later one passes the gate, and then evaluates with it. An
	 * evaluation runs on an exchange of its own, so no header or property of the source's message reaches the
	 * transformation or a sink; a failed evaluation is logged.
	 */
	public Processor evaluate(String collectUri) {
		return exchange -> {
			@SuppressWarnings("unchecked")
			Set<String> triggers = exchange.getProperty(TRIGGERS_PROPERTY, Set.class);
			org.apache.camel.CamelContext context = exchange.getContext();
			if (collect == null) collect = context.getEndpoint(collectUri);
			SerialLane<Pending> evaluations = lane(context);
			// Admission and queueing in one step: the lane's order is the order the sources were read in.
			synchronized (cache) {
				Set<String> admitted = cache.admit(triggers, expectedSourceIds());
				if (admitted == null) {
					LOGGER.debug("fan-in: waiting for sources {} (have {})", expectedSourceIds(), cache.snapshot().values().ids());
					return;
				}
				// A held trigger another source's evaluation took along leaves nothing to evaluate.
				if (admitted.isEmpty()) return;
				evaluations.submit(new Pending(admitted, cache.snapshot(), exchange.getProperty(ARRIVED_PROPERTY, Long.class)));
			}
		};
	}

	/** One evaluation, on a worker: the admitted sources through the collector route. */
	private void run(org.apache.camel.CamelContext context, Pending pending) {
		long started = System.nanoTime();
		SourceCache.Snapshot current = pending.sources();
		var event = new dk.aau.smartproductionlab.dmp.core.diagnostics.EvaluationEvent();
		event.begin();
		Exchange run = new org.apache.camel.support.DefaultExchange(context);
		run.setProperty(ACCEPTED_SNAPSHOT_PROPERTY, current.values());
		run.setProperty(ACCEPTED_TIMES_PROPERTY, current.times());
		run.setProperty(TRIGGERS_PROPERTY, pending.triggers());
		if (producer == null) producer = context.createProducerTemplate();
		producer.send(collect, run);
		if (run.getException() != null)
			LOGGER.warn("mapping {}: evaluation failed: {}", mappingId, run.getException().getMessage());
		if (event.shouldCommit()) {
			event.mapping = mappingId;
			event.triggers = String.join(",", pending.triggers());
			event.waitedNanos = pending.arrived() == null ? 0 : Math.max(0, started - pending.arrived());
			event.succeeded = run.getException() == null;
			event.commit();
		}
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
			exchange.setProperty(TransformerMeta.PROPERTY, TransformerMeta.of(sources, sinkMeta,
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
		Source config = sourcesById.get(dataSourceId);
		String pointer = config == null ? null : config.valuePath();
		Object body = exchange.getMessage().getBody();
		if (body == JsonPointer.JSON_NULL) {
			return JsonPointer.JSON_NULL;
		}
		// Connector POJOs have no JSON meaning — run them through registered
		// Camel type converters, which yield the connector's canonical form.
		if (!Boolean.TRUE.equals(exchange.getProperty(Source.NORMALIZED_VALUE_PROPERTY))
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
	private static void checkDeviceSchema(Exchange exchange, Source config, String dataSourceId, String pointer) {
		Object device = exchange.getMessage().getHeader(Source.DEVICE_SCHEMA_HEADER);
		if (device == null || config == null || config.schema() == null) return;
		Object member = pointer == null || pointer.isBlank() ? device
				: dk.aau.smartproductionlab.dmp.core.util.SchemaCompatibility.member(device, JsonPointer.parse(pointer).tokens());
		if (member == null)
			throw new IllegalArgumentException("Source '" + dataSourceId + "': the AID references " + pointer
					+ ", which the device's type does not have");
		java.util.List<String> differences = dk.aau.smartproductionlab.dmp.core.util.SchemaCompatibility.differences(config.schema(), member);
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

	private Object decodeIncoming(Exchange exchange, Source config, Object body) {
		// The cache freezes what a source accepts (its one copy): a decoded value is passed on as it is.
		if (Boolean.TRUE.equals(exchange.getProperty(Source.NORMALIZED_VALUE_PROPERTY))) {
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
				if (!jsonContent(exchange) && !jsonSchema(config.schema())) return bytes.toByteArray();
				text = bytes.toString(java.nio.charset.StandardCharsets.UTF_8);
			} catch (java.io.IOException e) {
				throw new IllegalArgumentException("could not read incoming source stream cache", e);
			}
		} else if (body instanceof byte[] bytes) {
			if (!jsonContent(exchange) && !jsonSchema(config.schema())) return bytes.clone();
			text = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
		} else if (body instanceof java.io.InputStream stream) {
			try {
				byte[] bytes = stream.readAllBytes();
				if (!jsonContent(exchange) && !jsonSchema(config.schema())) return bytes;
				text = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
			} catch (java.io.IOException e) {
				throw new IllegalArgumentException("could not read incoming source payload", e);
			}
		}
		if (text == null) {
			return body;
		}
		if (!jsonContent(exchange) && !jsonSchema(config.schema())) return text;
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

		/** Caches {@code value} only when the source has none yet; null when it kept the one it had. */
		synchronized Snapshot seed(String sourceId, Object value) {
			return latest.containsKey(sourceId) ? null : accept(sourceId, value, false);
		}

		synchronized Snapshot snapshot() { return snapshot(false); }

		/** Shares the frozen values: only the SourceId map itself is new. */
		private Snapshot snapshot(boolean changed) {
			Map<String, Object> values = new LinkedHashMap<>();
			latest.forEach((key, cachedValue) -> values.put(key,
					cachedValue == JsonPointer.JSON_NULL ? null : cachedValue));
			return new Snapshot(KeyedSnapshot.of(values), Map.copyOf(accepted), changed);
		}

		/**
		 * The triggers an evaluation runs for: these and every held one, once each {@code expected} source has
		 * delivered; until then null, and these are held.
		 */
		synchronized Set<String> admit(Set<String> triggers, Set<String> expected) {
			if (triggers != null) held.addAll(triggers);
			if (!latest.keySet().containsAll(expected)) return null;
			Set<String> admitted = Set.copyOf(held);
			held.clear();
			return admitted;
		}

		synchronized boolean holdsTriggers() { return !held.isEmpty(); }

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
