package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.model.ProcessorDefinition;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;
import dk.aau.smartproductionlab.dmp.core.util.ConnectorJsonPayload;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import org.apache.camel.spi.ThreadPoolProfile;
import org.apache.camel.util.concurrent.ThreadPoolRejectedPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Best-effort response writes: prepares a job per Lua-selected response value (device-affordance
 * members grouped into one payload) and contributes the route steps that write them. An
 * Operation's are detached (a bounded Wire Tap into a write route, one writer per mapping so
 * writes land in the order the calls finished), so the caller's reply never waits for them; an
 * ordinary mapping's are written in order before its next evaluation ({@link #ordered}).
 */
public final class ResponseSinkFanOutProcessor implements ManagedRouteResource {
	/** Header naming the endpoint of one detached response write. */
	public static final String WRITE_ENDPOINT_HEADER = "DmpResponseWriteEndpoint";
	private static final String ADMITTED = "dmp.response.admitted";
	/** One writer: a mapping's detached writes never overtake each other. */
	public static final int DEFAULT_WORKERS = 1;
	public static final int DEFAULT_QUEUE_CAPACITY = 128;
	private static final Logger LOGGER = LoggerFactory.getLogger(ResponseSinkFanOutProcessor.class);
	private final CamelContext camelContext;
	private final ExecutorService responseExecutor;
	private final AtomicLong rejectedWrites = new AtomicLong();
	private final AtomicLong failedWrites = new AtomicLong();
	private final int workerCount;
	private final int queueCapacity;
	private final String correlationId;
	private final List<Target> bindings;
	private final Map<String, AffordanceValidation> memberValidators;
	private final Map<String, AffordanceValidation> payloadValidators;
	private final Map<String, AffordanceValidation> singlePayloadValidators;
	private final Object lifecycleLock = new Object();
	private final AtomicInteger pendingWrites = new AtomicInteger();
	private volatile boolean acceptingWrites = true;
	private volatile boolean closed;

	public ResponseSinkFanOutProcessor(CamelContext context, String correlationId, List<Target> targets) {
		this(context, correlationId, targets, DEFAULT_WORKERS, DEFAULT_QUEUE_CAPACITY);
	}

	/** Response writes made in order within the evaluation ({@link #configureOrderedDispatch}); no executor. */
	public static ResponseSinkFanOutProcessor ordered(CamelContext context, String correlationId, List<Target> targets) {
		return new ResponseSinkFanOutProcessor(context, correlationId, targets, 0, 0);
	}

	ResponseSinkFanOutProcessor(CamelContext context, String correlationId,
			List<Target> targets, int workers, int queueCapacity) {
		this.camelContext = context;
		this.workerCount = workers;
		this.queueCapacity = queueCapacity;
		ThreadPoolProfile profile = new ThreadPoolProfile("response-sink-writes-" + correlationId);
		profile.setPoolSize(workers);
		profile.setMaxPoolSize(workers);
		profile.setKeepAliveTime(60L);
		profile.setTimeUnit(TimeUnit.SECONDS);
		profile.setMaxQueueSize(queueCapacity);
		profile.setRejectedPolicy(ThreadPoolRejectedPolicy.Abort);
		if (workers > 0) LOGGER.info("mapping {} response sink writes use bounded executor (workers={}, queueCapacity={}, rejection=Abort)",
				correlationId, workers, queueCapacity);
		this.correlationId = correlationId;
		this.memberValidators = new LinkedHashMap<>();
		this.payloadValidators = new LinkedHashMap<>();
		this.singlePayloadValidators = new LinkedHashMap<>();
		for (Target binding : targets) {
			if (binding.schema != null) {
				memberValidators.put(binding.sinkId,
						new AffordanceValidation(binding.schema, "response sink " + binding.sinkId));
			}
			if (binding.payloadGroup != null && binding.payloadSchema != null) {
				payloadValidators.putIfAbsent(binding.payloadGroup,
						new AffordanceValidation(binding.payloadSchema,
								"response payload group " + binding.payloadGroup));
			}
			if (binding.payloadGroup == null && binding.payloadSchema != null) {
				singlePayloadValidators.put(binding.sinkId,
						new AffordanceValidation(binding.payloadSchema,
								"response sink payload " + binding.sinkId));
			}
		}
		this.bindings = List.copyOf(targets);
		this.responseExecutor = workers <= 0 ? null : context.getExecutorServiceManager().newThreadPool(
				this, "response-sink-writes-" + correlationId, profile);
	}

	/**
	 * Steps that hand every job of the body (the {@link #prepareJobs} list) to a bounded Wire
	 * Tap into {@code writeUri}. A full queue or a drained processor rejects the job, counted,
	 * without blocking or failing the caller.
	 */
	public void configureDispatch(ProcessorDefinition<?> route, String writeUri) {
		var jobs = route.split(org.apache.camel.builder.Builder.body());
		var enqueue = jobs.doTry();
		enqueue.process(this::admit)
				.wireTap(writeUri)
				.onPrepare(ResponseSinkFanOutProcessor::prepareDetachedExchange)
				.executorService(responseExecutor);
		enqueue.doCatch(Exception.class)
				.process(this::recordRejection)
				.end();
		jobs.end();
	}

	/**
	 * Steps that write every job of the body through {@code writeUri}, one after another, before the route
	 * continues. A failed write is counted and logged by the write route; the evaluation goes on.
	 */
	public void configureOrderedDispatch(ProcessorDefinition<?> route, String writeUri) {
		var jobs = route.split(org.apache.camel.builder.Builder.body());
		var write = jobs.doTry();
		write.process(this::admit).to(writeUri);
		write.doCatch(Exception.class)
				.process(this::recordRejection)
				.end();
		jobs.end();
	}

	/**
	 * Steps of the write route: send the job's payload to its endpoint, chosen among the bindings' endpoints
	 * (resolved once, when the route starts); a failure is counted and logged.
	 */
	public void configureWrite(ProcessorDefinition<?> route) {
		var attempt = route.process(ResponseSinkFanOutProcessor::toWriteMessage).doTry();
		var endpoints = attempt.choice();
		for (String endpoint : bindings.stream().map(Target::endpoint).distinct().toList())
			endpoints.when(exchange -> endpoint.equals(exchange.getMessage().getHeader(WRITE_ENDPOINT_HEADER, String.class)))
					.to(endpoint);
		endpoints.otherwise().process(exchange -> {
			throw new IllegalStateException("response write references an unconfigured sink endpoint");
		});
		endpoints.end();
		attempt.doCatch(Exception.class)
					.process(this::recordFailure)
				.doFinally()
					.process(exchange -> pendingWrites.decrementAndGet())
				.end();
	}

	/** Admits one detached write, unless the processor is draining. */
	private void admit(Exchange exchange) {
		synchronized (lifecycleLock) {
			if (!acceptingWrites) throw new RejectedExecutionException("response writes are draining");
			pendingWrites.incrementAndGet();
			exchange.setProperty(ADMITTED, true);
		}
	}

	/** The job as a message: its payload (JSON text for HTTP and MQTT) addressed to its endpoint. */
	private static void toWriteMessage(Exchange exchange) {
		DispatchJob job = (DispatchJob) exchange.getMessage().getBody();
		Object body = job.body();
		if (job.jsonPayload() && (body instanceof String || body == null)) {
			body = new ConnectorJsonPayload(body);
		} else if (body == null) {
			// Preserve an intentional wire null, not an absent transport body.
			body = JsonPointer.JSON_NULL;
		}
		exchange.getMessage().setHeaders(new LinkedHashMap<>());
		if (job.jsonPayload()) exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
		exchange.getMessage().setHeader(WRITE_ENDPOINT_HEADER, job.endpoint());
		exchange.setProperty(SINK_LABEL, job.sinkLabel());
		exchange.getMessage().setBody(body);
	}

	private static final String SINK_LABEL = "dmp.response.sinkLabel";

	private void recordFailure(Exchange exchange) {
		long failures = failedWrites.incrementAndGet();
		Throwable cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
		LOGGER.warn("mapping {} response sink group {} failed; the call or evaluation is unchanged (failed={})",
				correlationId, exchange.getProperty(SINK_LABEL), failures, cause);
	}

	public List<DispatchJob> prepareJobs(Exchange exchange) {
		return prepareJobs(exchange, true);
	}

	/**
	 * The jobs of the body's SinkId values. {@code warnOmitted} is false for a failed call's response, which
	 * usually writes only its log sinks.
	 */
	public List<DispatchJob> prepareJobs(Exchange exchange, boolean warnOmitted) {
		Object parsed = exchange.getMessage().getBody();
		if (!(parsed instanceof Map<?, ?> rawValues)) {
			throw new IllegalArgumentException("response Lua returned no SinkId map");
		}
		Map<String, Object> values = new LinkedHashMap<>();
		rawValues.forEach((key, value) -> values.put(String.valueOf(key), value));
		Map<String, Group> groups = new LinkedHashMap<>();
		List<DispatchJob> jobs = new ArrayList<>();
		java.util.Set<String> incompleteGroups = new java.util.HashSet<>();
		for (Target binding : bindings) {
			if (!values.containsKey(binding.sinkId)) {
				if (warnOmitted) LOGGER.warn("mapping {} response Lua omitted SinkId {}; skipping response sink",
						correlationId, binding.sinkId);
				if (binding.payloadGroup != null) {
					incompleteGroups.add(binding.payloadGroup);
				}
				continue;
			}
			String groupKey = binding.payloadGroup != null
					? "payload:" + binding.payloadGroup : "sink:" + binding.sinkId;
			groups.computeIfAbsent(groupKey, ignored -> new Group(binding.payloadGroup, binding.jsonPayload))
					.add(binding, values.get(binding.sinkId));
		}
		for (Group group : groups.values()) {
			if (group.payloadGroup != null && incompleteGroups.contains(group.payloadGroup)) {
				LOGGER.warn("mapping {} response sink group {} is incomplete; skipping group",
					correlationId, group.payloadGroup);
				continue;
			}
			DispatchJob job = dispatch(group);
			if (job != null) jobs.add(job);
		}
		return List.copyOf(jobs);
	}

	private DispatchJob dispatch(Group group) {
		Target first = group.bindings.get(0);
		Object payload;
		boolean assembleMembers = group.bindings.size() > 1 || first.valuePath != null;
		try {
			if (assembleMembers) {
				Map<String, Object> keyed = new LinkedHashMap<>();
				Map<String, String> paths = new LinkedHashMap<>();
				for (Target binding : group.bindings) {
					keyed.put(binding.sinkId, group.values.get(binding.sinkId));
					if (binding.valuePath != null) paths.put(binding.sinkId, binding.valuePath);
					validateMember(binding, group.values.get(binding.sinkId));
				}
				payload = PayloadAssembler.assemble(keyed,
						group.bindings.stream().map(b -> b.sinkId).toList(), paths);
				AffordanceValidation validator = group.payloadGroup == null
						? null : payloadValidators.get(group.payloadGroup);
				if (validator != null) validator.outgoing(payload);
			} else {
				payload = group.values.get(first.sinkId);
				validateMember(first, payload);
				AffordanceValidation validator = first.payloadGroup == null
						? singlePayloadValidators.get(first.sinkId)
						: payloadValidators.get(first.payloadGroup);
				if (validator != null) validator.outgoing(payload);
			}
		} catch (RuntimeException e) {
			LOGGER.warn("mapping {} response sink group {} rejected before send: {}",
				correlationId, group.payloadGroup == null ? first.sinkId : group.payloadGroup, e.getMessage());
			return null;
		}

		String endpoint = first.endpoint;
		String sinkLabel = group.payloadGroup == null ? first.sinkId : group.payloadGroup;
		return new DispatchJob(endpoint, JsonCodec.toJsonCompatibleValue(payload), sinkLabel, group.jsonPayload);
	}

	/** Wire Tap's exchange copy is shallow; snapshot payload trees before scheduling. */
	public static void prepareDetachedExchange(Exchange exchange) {
		Object body = exchange.getMessage().getBody();
		exchange.getMessage().setBody(body instanceof DispatchJob job ? copyJob(job) : JsonCodec.copyValue(body));
		Map<String, Object> headers = new LinkedHashMap<>();
		exchange.getMessage().getHeaders().forEach((key, value) -> headers.put(key, JsonCodec.copyValue(value)));
		exchange.getMessage().setHeaders(headers);
		new LinkedHashMap<>(exchange.getProperties()).forEach((key, value) ->
				exchange.setProperty(key, JsonCodec.copyValue(value)));
	}

	private static DispatchJob copyJob(DispatchJob job) {
		return new DispatchJob(job.endpoint(), JsonCodec.copyValue(JsonCodec.toJsonCompatibleValue(job.body())),
				job.sinkLabel(), job.jsonPayload());
	}

	public void recordRejection(Exchange exchange) {
		if (Boolean.TRUE.equals(exchange.getProperty(ADMITTED))) pendingWrites.decrementAndGet();
		DispatchJob job = (DispatchJob) exchange.getMessage().getBody();
		long rejected = rejectedWrites.incrementAndGet();
		LOGGER.warn("mapping {} response sink group {} rejected (queue full or draining); the mapping continues (rejected={})",
				correlationId, job.sinkLabel(), rejected);
	}

	@Override
	public void bind(CamelContext context) {
		if (camelContext != context) {
			throw new IllegalArgumentException("response sink resource belongs to another Camel context");
		}
	}

	@Override
	public void close() throws Exception {
		drain(java.time.Duration.ofSeconds(6));
		if (responseExecutor != null) camelContext.getExecutorServiceManager().shutdown(responseExecutor);
		closed = true;
	}

	@Override
	public void drain(java.time.Duration timeout) throws Exception {
		synchronized (lifecycleLock) {
			if (closed) return;
			acceptingWrites = false;
			if (responseExecutor == null) return;
			responseExecutor.shutdown();
		}
		long nanos = timeout.toNanos();
		long deadline = System.nanoTime() + nanos;
		if (!responseExecutor.awaitTermination(Math.max(0, nanos * 5 / 6), TimeUnit.NANOSECONDS)) {
			for (Runnable queued : responseExecutor.shutdownNow()) {
				if (queued instanceof Future<?> future) future.cancel(false);
				// A queued write never reaches its write route: release its admission here.
				pendingWrites.decrementAndGet();
				rejectedWrites.incrementAndGet();
			}
			if (!responseExecutor.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
				throw new IllegalStateException("response writes did not drain before bundle retirement");
			}
		}
	}

	public int pendingWriteCount() { return pendingWrites.get(); }

	@Override public Map<String, Long> metrics() {
		return Map.of("pendingExtraWrites", (long) pendingWrites.get(), "rejectedExtraWrites", rejectedWrites.get(),
				"failedExtraWrites", failedWrites.get(), "extraWriteExecutors", closed || responseExecutor == null ? 0L : 1L);
	}

	public java.util.concurrent.ExecutorService responseExecutor() {
		return responseExecutor;
	}

	public long rejectedWriteCount() {
		return rejectedWrites.get();
	}

	public long failedWriteCount() {
		return failedWrites.get();
	}

	public int workerCount() { return workerCount; }

	public int queueCapacity() { return queueCapacity; }

	private void validateMember(Target binding, Object value) {
		AffordanceValidation validator = memberValidators.get(binding.sinkId);
		if (validator != null) validator.outgoing(value);
	}

	private static final class Group {
		private final String payloadGroup;
		private final boolean jsonPayload;
		private final List<Target> bindings = new ArrayList<>();
		private final Map<String, Object> values = new LinkedHashMap<>();

		private Group(String payloadGroup, boolean jsonPayload) {
			this.payloadGroup = payloadGroup;
			this.jsonPayload = jsonPayload;
		}

		private void add(Target binding, Object value) {
			bindings.add(binding);
			values.put(binding.sinkId, value == null ? JsonPointer.JSON_NULL : value);
		}
	}

	public record DispatchJob(String endpoint, Object body, String sinkLabel, boolean jsonPayload) { }

	/**
	 * A ResponseSink as written: its {@code endpoint}, and where its value goes in the payload ({@code valuePath},
	 * {@code payloadGroup}); {@code jsonPayload} when the payload is sent as JSON text.
	 */
	public record Target(String sinkId, String endpoint, String payloadGroup, String valuePath,
			Map<String, Object> payloadSchema, Map<String, Object> schema, boolean jsonPayload) {
	}
}
