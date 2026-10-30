/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.configuration.route.execution.policy.ExecutionPolicyRouteCreator;

/**
 * A mapping its sources trigger (SPEC §6): every evaluation runs the transformation once over the sources and
 * writes its result to the sinks, then the response half, if any.
 *
 * @param transformation the Lua transformation, or {@code null} to pass the sources map on unchanged
 * @param response       the response half, or {@code null}
 */
public record MappingRoute(String routeId, List<Source> sources, Endpoint transformation, List<Sink> sinks,
		Response response) implements RoutePlan {

	public MappingRoute {
		if (routeId == null || routeId.isBlank()) throw new IllegalArgumentException("a mapping route needs a route ID");
		sources = List.copyOf(sources);
		sinks = List.copyOf(sinks);
		if (sources.stream().map(Source::id).distinct().count() != sources.size()
				|| sinks.stream().map(Sink::id).distinct().count() != sinks.size())
			throw new IllegalArgumentException("mapping " + routeId + " has duplicate source or sink IDs");
	}

	@Override
	public List<Endpoint> endpoints() {
		List<Endpoint> endpoints = new ArrayList<>();
		sources.forEach(source -> {
			endpoints.add(source.endpoint());
			if (source.initialRead() != null) endpoints.add(source.initialRead());
		});
		if (transformation != null) endpoints.add(transformation);
		sinks.forEach(sink -> endpoints.add(sink.endpoint()));
		if (response != null) endpoints.addAll(response.endpoints());
		return endpoints;
	}

	@Override
	public void build(RouteBuilder builder) {
		new ExecutionPolicyRouteCreator(Objects.requireNonNull(builder, "builder")).create(this);
	}
}
