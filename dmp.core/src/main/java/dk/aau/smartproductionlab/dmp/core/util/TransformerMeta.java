/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.util;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The shape of Lua's global {@code meta} (SPEC §8): {@code sources} and {@code sinks} keyed by SourceId and SinkId,
 * and {@code invocation}, the evaluation or Operation call the transformation belongs to. A source carries the {@code time} its value was
 * acquired, a sink the {@code kind} of connector it writes through (the scheme of its endpoint, e.g. {@code aas},
 * {@code dmp-influxdb2}).
 */
public final class TransformerMeta {
	/** Exchange property: what a transformation knows besides the values, Lua's global {@code meta}. */
	public static final String PROPERTY = "dmp.transformer.meta";
	/**
	 * Exchange property: a SourceId-keyed map a transformation receives beside its sources. Set for a response
	 * transformation: the values its request transformation received.
	 */
	public static final String REQUEST_SOURCES = "dmp.transformer.requestSources";

	private TransformerMeta() { }

	public static Map<String, Object> of(Map<String, Object> sources, Map<String, Object> sinks,
			Map<String, Object> invocation) {
		Map<String, Object> meta = new LinkedHashMap<>();
		meta.put("sources", sources == null ? Map.of() : sources);
		meta.put("sinks", sinks == null ? Map.of() : sinks);
		if (invocation != null) meta.put("invocation", invocation);
		return meta;
	}

	/** A source acquired at {@code time}. */
	public static Map<String, Object> source(Instant time) {
		return Map.of("time", time.toString());
	}

	/** A source acquired at {@code time}; {@code trigger} when its new value caused this evaluation. */
	public static Map<String, Object> source(Instant time, boolean trigger) {
		return trigger ? Map.of("time", time.toString(), "trigger", true) : source(time);
	}

	/** A new invocation: its {@code id} and the time it {@code started}. */
	public static Map<String, Object> invocation(Instant started) {
		Map<String, Object> invocation = new LinkedHashMap<>();
		invocation.put("id", java.util.UUID.randomUUID().toString());
		invocation.put("started", started.toString());
		return invocation;
	}

	/** The invocation once it ended: {@code durationMs} since it started, its {@code outcome} and, if it failed, the {@code error}. */
	public static Map<String, Object> finished(Map<String, Object> invocation, String outcome, Map<String, Object> error) {
		Map<String, Object> finished = new LinkedHashMap<>(invocation);
		Object started = invocation.get("started");
		if (started != null && !finished.containsKey("durationMs"))
			finished.put("durationMs", java.time.Duration.between(Instant.parse(String.valueOf(started)), Instant.now()).toMillis());
		finished.put("outcome", outcome);
		if (error != null) finished.put("error", error);
		return finished;
	}

	/**
	 * {@code meta.invocation.error}: the connector's code and category where it has them (also as the cause of a
	 * failure that reports several sinks), else {@code defaultCode}; and the message.
	 */
	public static Map<String, Object> error(Throwable failure, String defaultCode) {
		Map<String, Object> error = new LinkedHashMap<>();
		dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException cause = null;
		for (Throwable current = failure; current != null && cause == null; current = current.getCause())
			if (current instanceof dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException connector)
				cause = connector;
		if (cause != null) {
			var connector = cause;
			error.put("code", connector.getConnectorCode());
			error.put("category", connector.getCategory().name());
		} else {
			error.put("code", defaultCode);
		}
		error.put("message", failure == null || failure.getMessage() == null ? "" : failure.getMessage());
		return error;
	}

	/** A sink writing through {@code endpointUri}. */
	public static Map<String, Object> sink(String endpointUri) {
		int colon = endpointUri == null ? -1 : endpointUri.indexOf(':');
		return Map.of("kind", colon <= 0 ? "unknown" : endpointUri.substring(0, colon));
	}
}
