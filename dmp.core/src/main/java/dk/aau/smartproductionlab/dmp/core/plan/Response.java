/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.plan;

import java.util.List;
import java.util.Objects;

/**
 * A mapping's response half (SPEC §6, §7): the ResponseSources name what was sent (an ordinary mapping) or the
 * device's reply (an Operation), the ResponseTransformation maps them, and the ResponseSinks receive its result.
 *
 * @param transformation the ResponseTransformation
 * @param sources        the ResponseSources
 * @param sinks          the ResponseSinks the DMP writes; one answering an Operation's caller is not among them
 */
public record Response(Endpoint transformation, List<ResponseSource> sources, List<Sink> sinks) {
	public Response {
		Objects.requireNonNull(transformation, "transformation");
		sources = List.copyOf(sources);
		sinks = List.copyOf(sinks);
	}

	/** The transformation's endpoint and the sinks'. */
	public List<Endpoint> endpoints() {
		List<Endpoint> endpoints = new java.util.ArrayList<>();
		endpoints.add(transformation);
		sinks.forEach(sink -> endpoints.add(sink.endpoint()));
		return endpoints;
	}

	/**
	 * A ResponseSource: the value its Sink was sent, or the reply ({@code sinkId} null), at {@code valuePath}
	 * ({@code null} for the whole value).
	 */
	public record ResponseSource(String sourceId, String sinkId, String valuePath) {
		public ResponseSource {
			Objects.requireNonNull(sourceId, "sourceId");
		}
	}
}
