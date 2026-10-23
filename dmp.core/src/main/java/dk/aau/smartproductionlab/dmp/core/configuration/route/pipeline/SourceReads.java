/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataTransformerConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreatorHelper;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.execution.policy.ExecutionPolicyRouteCreator;
import dk.aau.smartproductionlab.dmp.core.routebuilder.DmpRouteBuilder;
import dk.aau.smartproductionlab.dmp.core.util.TransformerMeta;

/**
 * Sources read at evaluation ({@code ReadAtEvaluation}, and an Operation's further sources): one fresh Read per
 * evaluation, decoded like a contribution and never cached. A failed read fails the evaluation with
 * {@code DMP_SOURCE_READ_FAILED}, the read's own failure as its cause and in its message; its category is the
 * connector's where it has one.
 */
public final class SourceReads {
	private SourceReads() { }

	/** A route making one fresh Read of {@code dataSourceId} and decoding it; its body is then the value. */
	public static String route(RouteBuilder routeBuilder, RoutesConfiguration routesConfiguration, String routeId,
			int index, String dataSourceId, FanIn decoder) {
		String readRouteId = (routeId + "-read-" + index).replaceAll("[^A-Za-z0-9_.:-]", "-");
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerRouteId(readRouteId);
		var route = routeBuilder.from("direct:" + readRouteId).routeId(readRouteId).doTry();
		RouteCreatorHelper.configureSyncRead(route, RouteCreatorHelper.getDataSourceEndpoint(routesConfiguration,
				dataSourceId, routeId), ExecutionPolicyRouteCreator.SYNC_POLL_TIMEOUT_MS);
		route.process(exchange -> exchange.getMessage().setBody(decoder.decode(dataSourceId, exchange)))
				.doCatch(Exception.class)
					.process(exchange -> {
						Throwable cause = exchange.getProperty(org.apache.camel.Exchange.EXCEPTION_CAUGHT, Throwable.class);
						ConnectorInvocationException connector = connectorFailure(cause);
						throw new ConnectorInvocationException(connector == null
								? ConnectorInvocationException.Category.TRANSPORT : connector.getCategory(),
								"DMP_SOURCE_READ_FAILED", "a source read at evaluation could not be read"
										+ (cause == null || cause.getMessage() == null ? "" : ": " + cause.getMessage()),
								Map.of("source", FanIn.resolveSourceId(routesConfiguration, dataSourceId)), cause);
					})
				.end();
		return "direct:" + readRouteId;
	}

	private static ConnectorInvocationException connectorFailure(Throwable failure) {
		for (Throwable current = failure; current != null; current = current.getCause())
			if (current instanceof ConnectorInvocationException connector) return connector;
		return null;
	}

	/** Enricher strategy: the read value under {@code sourceId} in the sources map, its time in {@code meta.sources}. */
	public static AggregationStrategy into(String sourceId) {
		return (original, read) -> {
			if (read.getException() != null) {
				original.setException(read.getException());
				return original;
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> sources = (Map<String, Object>) original.getMessage().getBody();
			sources.put(sourceId, read.getMessage().getBody());
			@SuppressWarnings("unchecked")
			Map<String, Object> meta = original.getProperty(DataTransformerConfiguration.META, Map.class);
			if (meta != null) {
				@SuppressWarnings("unchecked")
				Map<String, Object> times = new LinkedHashMap<>((Map<String, Object>) meta.getOrDefault("sources", Map.of()));
				times.put(sourceId, TransformerMeta.source(java.time.Instant.now()));
				@SuppressWarnings("unchecked")
				Map<String, Object> sinks = (Map<String, Object>) meta.get("sinks");
				@SuppressWarnings("unchecked")
				Map<String, Object> invocation = (Map<String, Object>) meta.get("invocation");
				original.setProperty(DataTransformerConfiguration.META, TransformerMeta.of(times, sinks, invocation));
			}
			return original;
		};
	}
}
