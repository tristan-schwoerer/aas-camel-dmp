/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.plan.Response;
import dk.aau.smartproductionlab.dmp.core.plan.Response.ResponseSource;
import dk.aau.smartproductionlab.dmp.core.routebuilder.DmpRouteBuilder;
import dk.aau.smartproductionlab.dmp.core.routebuilder.PlanBuild;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.core.util.TransformerMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The response half of an ordinary mapping (SPEC §6): after its sinks, the ResponseSources project the values
 * the sinks were sent, the ResponseTransformation maps them, and the ResponseSinks are written in order before
 * the mapping's next evaluation. A failed evaluation still runs it, without ResponseSources and with
 * {@code meta.invocation.outcome = "Failed"}; whatever the response half does, the evaluation's outcome is
 * unchanged.
 */
public final class ResponseHalf {
	private static final Logger LOGGER = LoggerFactory.getLogger(ResponseHalf.class);
	/** The evaluation's sources, kept for the response transformation's {@code requestSources}. */
	private static final String REQUEST_SOURCES = "dmp.response.requestSources";

	private final String routeId;
	private final String uri;
	private final List<ResponseSource> sourceBindings;
	private final Map<String, Object> sinksMeta = new LinkedHashMap<>();
	private final ResponseSinkFanOutProcessor writer;

	/** The response half of {@code routeId}, a mapping its sources trigger: its routes on {@code routeBuilder}. */
	public ResponseHalf(RouteBuilder routeBuilder, PlanBuild build, Response response, String routeId) {
		this.routeId = routeId;
		this.uri = "direct:" + routeId + "-response";
		this.sourceBindings = response.sources();
		String responseLuaUri = build.uri(response.transformation());
		List<ResponseSinkFanOutProcessor.Target> targets = ResponseSinkTargets.resolve(routeBuilder, build, routeId,
				response.sinks());
		for (ResponseSinkFanOutProcessor.Target target : targets)
			sinksMeta.put(target.sinkId(), TransformerMeta.sink(target.endpoint()));
		this.writer = ResponseSinkFanOutProcessor.ordered(routeBuilder.getContext(), routeId, targets);
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerManagedResource(writer);

		String writeRouteId = routeId + "-response-write";
		register(routeBuilder, writeRouteId);
		writer.configureWrite(routeBuilder.from("direct:" + writeRouteId).routeId(writeRouteId));
		register(routeBuilder, routeId + "-response");
		var flow = routeBuilder.from(uri).routeId(routeId + "-response").doTry();
		flow.to(responseLuaUri).process(this::prepareWrites);
		writer.configureOrderedDispatch(flow, "direct:" + writeRouteId);
		flow.doCatch(Exception.class).process(this::responseFailed).end();
	}

	/** The route running the response half on a copy of the evaluation (a Content Enricher). */
	public String uri() { return uri; }

	/** Before the request transformation: keeps the evaluation's sources (frozen; their values already are). */
	public void keepRequestSources(Exchange exchange) {
		exchange.setProperty(REQUEST_SOURCES, JsonCodec.freeze(exchange.getMessage().getBody()));
	}

	/** After every sink succeeded: the ResponseSources, each projecting the value its Sink was sent. */
	public void succeeded(Exchange exchange) {
		Map<String, Object> written = FanOut.writtenValues(exchange);
		Map<String, Object> sources = new LinkedHashMap<>();
		Map<String, Object> times = new LinkedHashMap<>();
		for (ResponseSource binding : sourceBindings) {
			Object value = written.get(binding.sinkId());
			sources.putAll(ResponseProjectionProcessor.project(value, List.of(binding)));
			times.put(binding.sourceId(), TransformerMeta.source(java.time.Instant.now()));
		}
		prepare(exchange, sources, times, TransformerMeta.finished(invocation(exchange), "Succeeded", null));
	}

	/** After the evaluation failed: no ResponseSources, and the error. */
	public void failed(Exchange exchange) {
		Throwable failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
		prepare(exchange, new LinkedHashMap<>(), Map.of(), TransformerMeta.finished(invocation(exchange), "Failed",
				TransformerMeta.error(failure, "DMP_MAPPING_FAILED")));
	}

	private void prepare(Exchange exchange, Map<String, Object> sources, Map<String, Object> times,
			Map<String, Object> invocation) {
		Object requestSources = exchange.getProperty(REQUEST_SOURCES);
		exchange.setProperty(TransformerMeta.REQUEST_SOURCES,
				requestSources == null ? new LinkedHashMap<String, Object>() : requestSources);
		exchange.setProperty(TransformerMeta.PROPERTY, TransformerMeta.of(times, sinksMeta, invocation));
		exchange.getMessage().setBody(sources);
	}

	private void prepareWrites(Exchange exchange) {
		@SuppressWarnings("unchecked")
		Map<String, Object> meta = exchange.getProperty(TransformerMeta.PROPERTY, Map.class);
		Object invocation = meta == null ? null : meta.get("invocation");
		boolean succeeded = invocation instanceof Map<?, ?> map && "Succeeded".equals(map.get("outcome"));
		// A failed evaluation's response usually writes only its log sinks: omitted SinkIds are expected.
		exchange.getMessage().setBody(writer.prepareJobs(exchange, succeeded));
	}

	private void responseFailed(Exchange exchange) {
		Throwable failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
		LOGGER.warn("mapping {}: the response transformation failed; nothing was written ({})", routeId,
				failure == null ? "unknown" : failure.getMessage());
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> invocation(Exchange exchange) {
		Map<String, Object> meta = exchange.getProperty(TransformerMeta.PROPERTY, Map.class);
		Object invocation = meta == null ? null : meta.get("invocation");
		return invocation instanceof Map<?, ?> map ? (Map<String, Object>) map
				: TransformerMeta.invocation(java.time.Instant.now());
	}

	/** Enricher strategy: the response half leaves the evaluation's exchange as it was. */
	public static Exchange keepOriginal(Exchange original, Exchange response) {
		return original;
	}

	/** After the failed evaluation's response: the evaluation still fails with its own error. */
	public static void rethrow(Exchange exchange) throws Exception {
		Exception failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
		if (failure != null) throw failure;
	}

	private static void register(RouteBuilder routeBuilder, String id) {
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerRouteId(id);
	}
}
