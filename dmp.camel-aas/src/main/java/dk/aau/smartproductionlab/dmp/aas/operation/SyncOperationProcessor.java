/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas.operation;

import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.ResponseProjectionProcessor;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.ResponseSinkFanOutProcessor;
import dk.aau.smartproductionlab.dmp.core.plan.Response;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import java.net.HttpURLConnection;

import dk.aau.smartproductionlab.dmp.core.util.TransformerMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.AffordanceValidation;

/**
 * AAS Operation accept/invoke/reply flow. The source connector owns the AAS
 * request/envelope; a target {@code SynchronousInvocationAdapter} owns the
 * device request/reply wire format and returns named values.
 *
 * <p>
 * Flow per invocation:
 * <ol>
 * <li>run the request-direction Lua blob (REST operation variables → named
 * command payload), keyed by the authored SourceId;</li>
 * <li>select authored SinkId values, assemble one action payload, and invoke the command sink
 * synchronously through the selected connector adapter;</li>
 * <li>validate the correlated reply, project authored ResponseSources, run
 * response Lua once, and return the caller's SinkId while dispatching other
 * per-SinkId values independently.</li>
 * </ol>
 *
 * @author aas-camel-dmp
 */
public class SyncOperationProcessor implements ManagedRouteResource {
	private static final String CALLER_REPLY_RESULT = "dmp.operation.callerReplyResult";
	/** The invocation's id and its start ({@link System#nanoTime()}). */
	static final String INVOCATION_ID = "dmp.operation.invocationId";
	static final String INVOCATION_STARTED = "dmp.operation.invocationStarted";
	/** meta.invocation of the request transformation: the Operation's identity, id, start and caller. */
	private static final String INVOCATION = "dmp.operation.invocation";
	/** The request transformation's input keyed by SourceId. */
	private static final String REQUEST_SOURCES = "dmp.operation.requestSources";
	/** The request transformation's result keyed by SinkId. */
	private static final String REQUEST_RESULT = "dmp.operation.requestResult";
	private static final String COMMAND_REPLY = "dmp.operation.commandReply";
	/** A property write: the value written, which is also the call's reply. */
	private static final String WRITTEN_VALUE = "dmp.operation.writtenValue";

	private static final Logger LOGGER = LoggerFactory.getLogger(SyncOperationProcessor.class);

	private final OperationContext context;
	private final CamelContext camelContext;
	private final AffordanceValidation validation;
	private final OperationReplyValidationProcessor replyValidationProcessor;
	private final java.util.List<Response.ResponseSource> responseSourceBindings;
	private final java.util.List<ResponseSinkFanOutProcessor.Target> responseSinkTargets;
	private final ResponseSinkFanOutProcessor responseFanOut;
	private final java.util.List<SynchronousInvocationAdapter> invocationAdapters;
	private final SynchronousInvocationAdapter.InvocationSchema invocationSchema;
	private volatile Map<String, String> operationIdentity = Map.of();

	public SyncOperationProcessor(CamelContext camelContext, OperationContext context) {
		this(camelContext, context, new AffordanceValidation(null, "operation " + context.action),
				new AffordanceValidation(null, "operation output " + context.action));
	}

	public SyncOperationProcessor(CamelContext camelContext, OperationContext context,
			AffordanceValidation validation) {
		this(camelContext, context, validation,
				new AffordanceValidation(null, "operation output " + context.action));
	}

	public SyncOperationProcessor(CamelContext camelContext, OperationContext context,
			AffordanceValidation validation, AffordanceValidation responseValidation) {
		this(camelContext, context, validation, responseValidation, java.util.List.of(), java.util.List.of());
	}

	public SyncOperationProcessor(CamelContext camelContext, OperationContext context,
			AffordanceValidation validation, AffordanceValidation responseValidation,
			java.util.List<Response.ResponseSource> responseSourceBindings,
			java.util.List<ResponseSinkFanOutProcessor.Target> responseSinkTargets) {
		this(camelContext, context, validation, responseValidation, responseSourceBindings,
				responseSinkTargets, context.action);
	}

	public SyncOperationProcessor(CamelContext camelContext, OperationContext context,
			AffordanceValidation validation, AffordanceValidation responseValidation,
			java.util.List<Response.ResponseSource> responseSourceBindings,
			java.util.List<ResponseSinkFanOutProcessor.Target> responseSinkTargets, String resourceIdentity) {
		this(camelContext, context, validation, responseValidation, responseSourceBindings,
				responseSinkTargets, resourceIdentity, null);
	}

	SyncOperationProcessor(CamelContext camelContext, OperationContext context,
			AffordanceValidation validation, AffordanceValidation responseValidation,
			java.util.List<Response.ResponseSource> responseSourceBindings,
			java.util.List<ResponseSinkFanOutProcessor.Target> responseSinkTargets, String resourceIdentity,
			java.util.List<SynchronousInvocationAdapter> adapters) {
		this.context = context;
		this.camelContext = camelContext;
		this.validation = validation;
		this.replyValidationProcessor = new OperationReplyValidationProcessor(context.action, responseValidation);
		this.responseSourceBindings = java.util.List.copyOf(responseSourceBindings);
		this.responseSinkTargets = java.util.List.copyOf(responseSinkTargets);
		this.invocationSchema = new SynchronousInvocationAdapter.InvocationSchema(context.parameterTypes,
				context.parameterRequired, context.responseTypes, context.responseRequired, context.targetOptions);
		this.invocationAdapters = adapters == null ? java.util.ServiceLoader
				.load(SynchronousInvocationAdapter.class,
						Thread.currentThread().getContextClassLoader())
				.stream().map(java.util.ServiceLoader.Provider::get).toList() : java.util.List.copyOf(adapters);
		this.responseFanOut = new ResponseSinkFanOutProcessor(camelContext,
				resourceIdentity, this.responseSinkTargets);
	}

	@Override
	public void bind(CamelContext owner) {
		if (camelContext != owner) throw new IllegalArgumentException("operation processor belongs to another Camel context");
	}

	@Override
	public java.util.Set<String> endpointUris() {
		// A property write uses the sink endpoint itself, not a connector's invocation endpoints.
		if (writesProperty()) return java.util.Set.of(context.commandSinkUri);
		return invocationAdapters.stream().filter(adapter -> adapter.supports(context.commandSinkUri)).findFirst()
				.map(adapter -> adapter.ownedEndpointUris(context.commandSinkUri, invocationSchema))
				.orElseGet(() -> java.util.Set.of(context.commandSinkUri));
	}

	@Override
	public void close() throws Exception {
		responseFanOut.close();
	}

	@Override
	public void drain(java.time.Duration timeout) throws Exception {
		responseFanOut.drain(timeout);
	}

	/** The Operation's identity (see {@link OperationRoute.Identity#tags()}). */
	void setOperationIdentity(Map<String, String> identity) {
		this.operationIdentity = identity == null ? Map.of() : Map.copyOf(identity);
	}

	public void prepareRequest(Exchange exchange) throws Exception {
		String id = java.util.UUID.randomUUID().toString();
		java.time.Instant started = java.time.Instant.now();
		exchange.setProperty(INVOCATION_ID, id);
		exchange.setProperty(INVOCATION_STARTED, System.nanoTime());
		new OperationRequestProcessor(context.callerInputTemplates, context.callerInputSourcePaths).process(exchange);
		// The request transformation's meta: the invocation, its sources (completed after the fan-in) and the
		// command's sinks.
		Map<String, Object> invocation = new java.util.LinkedHashMap<>(operationIdentity);
		invocation.put("id", id);
		invocation.put("started", started.toString());
		exchange.setProperty(INVOCATION, invocation);
		Map<String, Object> sinks = new java.util.LinkedHashMap<>();
		for (String sinkId : context.commandSinkIds) sinks.put(sinkId, TransformerMeta.sink(context.commandSinkUri));
		exchange.setProperty(TransformerMeta.PROPERTY, TransformerMeta.of(requestSourcesMeta(exchange), sinks, invocation));
	}

	/**
	 * Before the request transformation: keeps its input (the caller's values and fanned-in
	 * AAS values, keyed by SourceId) for the response transformation's {@code requestSources}.
	 */
	public void keepRequestSources(Exchange exchange) {
		@SuppressWarnings("unchecked")
		Map<String, Object> meta = exchange.getProperty(TransformerMeta.PROPERTY, Map.class);
		if (meta != null) exchange.setProperty(TransformerMeta.PROPERTY,
				TransformerMeta.of(requestSourcesMeta(exchange), castMap(meta.get("sinks")), castMap(meta.get("invocation"))));
		if (!context.explicitResponseMapping) return;
		exchange.setProperty(REQUEST_SOURCES, dk.aau.smartproductionlab.dmp.core.util.JsonCodec
				.copyValue(exchange.getMessage().getBody()));
	}

	/** After the request transformation: keeps its result for the command. */
	public void keepRequestResult(Exchange exchange) {
		exchange.setProperty(REQUEST_RESULT, exchange.getMessage().getBody());
	}

	/**
	 * Before the trigger: the action command from the request result, typed by the
	 * declared input schema and validated. The connector's invocation steps send it.
	 */
	public void prepareCommand(Exchange exchange) {
		if (writesProperty()) {
			prepareWrite(exchange);
			return;
		}
		// parse every DECLARED parameter per its AID DataSchema type —
		// the wire representation is data-driven, never name-based
		Map<String, Object> command = OperationPayloads.assembleCommand(exchange.getProperty(REQUEST_RESULT),
				context.commandSinkIds, context.commandSinkValuePaths);
		OperationPayloads.coerceFields(command, context.parameterTypes);
		try {
			validation.outgoing(command);
		} catch (IllegalArgumentException invalid) {
			throw new OperationValidationException("DMP_COMMAND_VALIDATION_FAILED", 500,
					"Mapped command violates the declared input schema",
				java.util.List.of(invalid.getMessage()));
		}
		exchange.getMessage().setBody(command);
	}

	/** An Operation bound to a property affordance writes it (instead of invoking an action). */
	private boolean writesProperty() {
		return context.propertyWrite;
	}

	/**
	 * A property write: the request transformation's value for the property's SinkId,
	 * validated against the property's schema and encoded for its connector (JSON text
	 * for HTTP and MQTT, the value itself otherwise).
	 */
	private void prepareWrite(Exchange exchange) {
		Object value = dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.PayloadAssembler.assemble(
				OperationPayloads.keyedValues(exchange.getProperty(REQUEST_RESULT)),
				context.commandSinkIds, context.commandSinkValuePaths);
		Object written = value == dk.aau.smartproductionlab.dmp.core.util.JsonPointer.JSON_NULL ? null : value;
		try {
			validation.outgoing(written);
		} catch (IllegalArgumentException invalid) {
			throw new OperationValidationException("DMP_COMMAND_VALIDATION_FAILED", 500,
					"Mapped command violates the declared input schema",
				java.util.List.of(invalid.getMessage()));
		}
		exchange.setProperty(WRITTEN_VALUE, written);
		boolean json = context.jsonPayload;
		Object body = written == null ? dk.aau.smartproductionlab.dmp.core.util.JsonPointer.JSON_NULL : written;
		if (json && written instanceof String) body = new dk.aau.smartproductionlab.dmp.core.util.ConnectorJsonPayload(written);
		if (json) exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
		exchange.getMessage().setBody(body);
	}

	/** After the trigger: keeps the connector's reply, validated against the declared output schema. */
	public void acceptReply(Exchange exchange) {
		exchange.setProperty(COMMAND_REPLY, normalizeReply(exchange, exchange.getMessage().getBody()));
	}

	/** The connector's invocation steps for the action endpoint, or a plain InOut send. */
	void configureInvoke(org.apache.camel.model.ProcessorDefinition<?> route) {
		if (writesProperty()) {
			// The connector's producer acknowledges the write (PLC4X, OPC UA, HTTP) or the broker
			// does (MQTT QoS 1/2); any failure is a device or transport error. The reply is the value.
			route.doTry()
					.to(context.commandSinkUri)
					.process(exchange -> exchange.getMessage().setBody(exchange.getProperty(WRITTEN_VALUE)))
				.doCatch(Exception.class)
					.process(SyncOperationProcessor::writeFailed)
				.end();
			return;
		}
		SynchronousInvocationAdapter connector = invocationAdapters.stream()
				.filter(candidate -> candidate.supports(context.commandSinkUri)).findFirst().orElse(null);
		if (connector == null) route.to(org.apache.camel.ExchangePattern.InOut, context.commandSinkUri);
		else connector.configureInvoke(route, context.commandSinkUri, invocationSchema);
	}

	private static void writeFailed(Exchange exchange) {
		Exception caught = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
		for (Throwable cause = caught; cause != null; cause = cause.getCause() == cause ? null : cause.getCause())
			if (cause instanceof dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException typed) throw typed;
		// A device that answered with an error (HTTP non-2xx) is a remote failure; anything else is transport.
		boolean answered = caught != null && "org.apache.camel.http.base.HttpOperationFailedException".equals(caught.getClass().getName());
		throw new dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException(
				answered ? dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException.Category.REMOTE
						: dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException.Category.TRANSPORT,
				"DMP_WRITE_FAILED", "the property write was not acknowledged", Map.of());
	}

	/**
	 * The response transformation's input: the reply (an action's output, or the value a
	 * property write wrote), projected by the ResponseSource bindings. Its second argument,
	 * {@code requestSources}, is what the request transformation received; nothing is read
	 * again. Without a response mapping the call answers with no outputs.
	 */
	public void collectResponseSources(Exchange exchange) {
		if (!context.explicitResponseMapping) {
			exchange.getMessage().setBody(new java.util.LinkedHashMap<String, Object>());
			return;
		}
		exchange.setProperty(dk.aau.smartproductionlab.dmp.core.util.TransformerMeta.REQUEST_SOURCES,
				exchange.getProperty(REQUEST_SOURCES));
		exchange.getMessage().setBody(ResponseProjectionProcessor.project(exchange.getProperty(COMMAND_REPLY),
				responseSourceBindings));
		exchange.setProperty(TransformerMeta.PROPERTY, TransformerMeta.of(sourcesMeta(exchange),
				responseSinksMeta(), finished(exchange, "Succeeded", null)));
	}

	/** Whether a failed call runs the response transformation: it has ResponseSinks that do not answer the caller. */
	boolean answersFailures() {
		return context.responseLuaUri != null && !responseSinkTargets.isEmpty();
	}

	/**
	 * A failed call's response transformation: no ResponseSources, the request's sources, and meta.invocation with
	 * {@code outcome = "Failed"} and the {@code error}. Only ResponseSinks that do not answer the caller are written;
	 * the caller's error reply is built afterwards and is unchanged by anything the transformation does.
	 */
	public void prepareFailureResponse(Exchange exchange) {
		Throwable failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
		if (failure == null) failure = exchange.getException();
		Map<String, Object> error = TransformerMeta.error(failure, "DMP_OPERATION_FAILED");
		Object requestSources = exchange.getProperty(REQUEST_SOURCES);
		exchange.setProperty(TransformerMeta.REQUEST_SOURCES,
				requestSources == null ? new java.util.LinkedHashMap<String, Object>() : requestSources);
		exchange.setProperty(TransformerMeta.PROPERTY, TransformerMeta.of(Map.of(), responseSinksMeta(),
				finished(exchange, "Failed", error)));
		exchange.getMessage().setBody(new java.util.LinkedHashMap<String, Object>());
	}

	/** The failed call's response writes, without warnings for SinkIds it leaves out. */
	public void prepareFailureWrites(Exchange exchange) {
		exchange.getMessage().setBody(responseFanOut.prepareJobs(exchange, false));
	}

	public void failureResponseFailed(Exchange exchange) {
		Throwable failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
		LOGGER.warn("operation {}: the response transformation failed for a failed call; nothing was written ({})",
				context.action, failure == null ? "unknown" : failure.getMessage());
	}

	private Map<String, Object> finished(Exchange exchange, String outcome, Map<String, Object> error) {
		@SuppressWarnings("unchecked")
		Map<String, Object> invocation = new java.util.LinkedHashMap<>(exchange.getProperty(INVOCATION, Map.class) == null
				? operationIdentity : exchange.getProperty(INVOCATION, Map.class));
		Object started = exchange.getProperty(INVOCATION_STARTED);
		if (started instanceof Long nanos) invocation.put("durationMs", (System.nanoTime() - nanos) / 1_000_000L);
		invocation.put("outcome", outcome);
		if (error != null) invocation.put("error", error);
		return invocation;
	}

	/** The request transformation's sources: the Operation's own, the call's trigger, marked as such. */
	private Map<String, Object> requestSourcesMeta(Exchange exchange) {
		java.util.Set<String> operationSources = context.callerInputSourcePaths.keySet();
		Map<String, Object> sources = new java.util.LinkedHashMap<>();
		sourcesMeta(exchange).forEach((id, source) -> sources.put(id, !operationSources.contains(id) ? source
				: TransformerMeta.source(java.time.Instant.parse(String.valueOf(castMap(source).get("time"))), true)));
		return sources;
	}

	/** Each SourceId of the body with the time it was acquired: kept from an earlier step, otherwise now. */
	private static Map<String, Object> sourcesMeta(Exchange exchange) {
		@SuppressWarnings("unchecked")
		Map<String, Object> meta = exchange.getProperty(TransformerMeta.PROPERTY, Map.class);
		Map<String, Object> known = meta == null ? Map.of() : castMap(meta.get("sources"));
		Map<String, Object> sources = new java.util.LinkedHashMap<>();
		if (exchange.getMessage().getBody() instanceof Map<?, ?> values)
			for (Object id : values.keySet()) {
				String key = String.valueOf(id);
				sources.put(key, known.containsKey(key) ? known.get(key) : TransformerMeta.source(java.time.Instant.now()));
			}
		return sources;
	}

	/** The ResponseSinks: the ones the DMP writes with their connector's kind, the caller's as {@code operation}. */
	private Map<String, Object> responseSinksMeta() {
		Map<String, Object> sinks = new java.util.LinkedHashMap<>();
		for (ResponseSinkFanOutProcessor.Target target : responseSinkTargets)
			sinks.put(target.sinkId(), TransformerMeta.sink(target.endpoint()));
		if (context.callerReplySinkId != null) sinks.put(context.callerReplySinkId, Map.of("kind", "operation"));
		context.callerReplyBindings.keySet().forEach(sinkId -> sinks.put(sinkId, Map.of("kind", "operation")));
		return sinks;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> castMap(Object value) {
		return value instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of();
	}

	private Object normalizeReply(Exchange exchange, Object reply) {
		// Connector adapters normalize replies to a named logical object;
		// the generic response path validates/projects/transforms it once.
		Exchange replyExchange = new org.apache.camel.support.DefaultExchange(exchange.getContext());
		replyExchange.getMessage().setBody(reply);
		try {
			replyValidationProcessor.process(replyExchange);
		} catch (RuntimeException failure) {
			throw failure;
		} catch (Exception failure) {
			throw new IllegalStateException(failure);
		}
		return replyExchange.getMessage().getBody();
	}

	public void prepareResponseWrites(Exchange exchange) {
			Object result = exchange.getMessage().getBody();
			new OperationReplyProcessor(context).validate(result);
			exchange.setProperty(CALLER_REPLY_RESULT, result);
			exchange.getMessage().setBody(responseSinkTargets.isEmpty() ? java.util.List.of()
					: responseFanOut.prepareJobs(exchange));
	}

	/** The detached AAS response writes: a bounded Wire Tap per job into {@code writeUri}. */
	void configureResponseDispatch(org.apache.camel.model.ProcessorDefinition<?> route, String writeUri) {
		responseFanOut.configureDispatch(route, writeUri);
	}

	/** The response write route's steps. */
	void configureResponseWrite(org.apache.camel.model.ProcessorDefinition<?> route) {
		responseFanOut.configureWrite(route);
	}

	public void completeReply(Exchange exchange) throws Exception {
			Object result = exchange.getProperty(CALLER_REPLY_RESULT);
			exchange.getMessage().setBody(result);
			new OperationReplyProcessor(context).process(exchange);
	}

	public void handleFailure(Exchange exchange) {
		Throwable failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
		if (failure == null) failure = exchange.getException();
		if (failure == null) failure = new IllegalStateException("operation route failed without a cause");
		fail(exchange, failure);
	}

	private void fail(Exchange exchange, Throwable failure) {
		exchange.setProperty(Exchange.EXCEPTION_CAUGHT, failure);
		new OperationFailureProcessor(context, HttpURLConnection.HTTP_INTERNAL_ERROR).process(exchange);
		exchange.setRouteStop(true);
	}

}
