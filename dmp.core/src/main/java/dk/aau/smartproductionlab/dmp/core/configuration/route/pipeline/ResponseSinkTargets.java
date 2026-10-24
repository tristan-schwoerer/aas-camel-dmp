/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.routebuilder.DmpRouteBuilder;
import dk.aau.smartproductionlab.dmp.core.routebuilder.PlanBuild;

/**
 * A mapping's ResponseSinks as {@link ResponseSinkFanOutProcessor} targets: each sink with the endpoint it
 * writes. An AAS element is written through an adapter route that wraps the value in the connector's
 * {@code {sinkId: value}} envelope, since an AAS endpoint writes one element per destination.
 */
public final class ResponseSinkTargets {
	private ResponseSinkTargets() { }

	public static List<ResponseSinkFanOutProcessor.Target> resolve(RouteBuilder routeBuilder, PlanBuild build,
			String routeId, List<Sink> sinks) {
		List<ResponseSinkFanOutProcessor.Target> targets = new ArrayList<>();
		int aasRouteIndex = 0;
		for (Sink sink : sinks) {
			String endpoint = build.uri(sink.endpoint());
			if (sink.endpoint().appliesValuePath()) {
				String sinkId = sink.sinkId();
				String adapterRouteId = (routeId + "-aas-response-" + aasRouteIndex++).replaceAll("[^A-Za-z0-9_.-]", "-");
				if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerRouteId(adapterRouteId);
				String adapterUri = "direct:" + adapterRouteId;
				routeBuilder.from(adapterUri)
						.routeId(adapterRouteId)
						.process(exchange -> {
							Map<String, Object> envelope = new LinkedHashMap<>();
							envelope.put(sinkId, exchange.getMessage().getBody());
							exchange.getMessage().setBody(envelope);
						})
						.to(endpoint);
				// The AAS endpoint's own valuePath extracts SinkId from the envelope above.
				targets.add(new ResponseSinkFanOutProcessor.Target(sinkId, adapterUri, null, null, null,
						sink.schema(), false));
			} else {
				targets.add(new ResponseSinkFanOutProcessor.Target(sink.sinkId(), endpoint, sink.payloadGroup(),
						sink.valuePath(), sink.payloadSchema(), sink.schema(), sink.endpoint().jsonWire()));
			}
		}
		return List.copyOf(targets);
	}
}
