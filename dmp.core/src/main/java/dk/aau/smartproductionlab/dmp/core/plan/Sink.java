/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.plan;

import java.util.Map;
import java.util.Objects;

/**
 * One Sink of a mapping (SPEC §6): the value Lua returns under its SinkId, and where it is written.
 *
 * @param id            the sink's identity within its bundle
 * @param sinkId        the key of its value in Lua's result; {@code null} for the whole result
 * @param endpoint      its connector endpoint
 * @param valuePath     a JSON Pointer placing its value in the endpoint's payload, or {@code null} for the whole payload
 * @param schema        its value's JSON schema, or {@code null}
 * @param payloadSchema the schema of the whole payload when sinks share it, or {@code null}
 * @param payloadGroup  the sinks assembling one payload (one AID affordance) share it; {@code null} for none
 */
public record Sink(String id, String sinkId, Endpoint endpoint, String valuePath, Map<String, Object> schema,
		Map<String, Object> payloadSchema, String payloadGroup) {

	public Sink {
		Objects.requireNonNull(id, "id");
		Objects.requireNonNull(endpoint, "endpoint");
	}

	/** A sink without value path, schemas or payload group. */
	public Sink(String id, String sinkId, Endpoint endpoint) {
		this(id, sinkId, endpoint, null, null, null, null);
	}

	public Sink withValuePath(String value) {
		return new Sink(id, sinkId, endpoint, value, schema, payloadSchema, payloadGroup);
	}

	public Sink withSchema(Map<String, Object> value) {
		return new Sink(id, sinkId, endpoint, valuePath, value, payloadSchema, payloadGroup);
	}

	public Sink withPayload(Map<String, Object> schema, String group) {
		return new Sink(id, sinkId, endpoint, valuePath, this.schema, schema, group);
	}

	/** Whether the payload is sent as JSON text: the connector sends JSON and a schema describes JSON. */
	public boolean jsonPayload() {
		return endpoint.jsonWire() && (describesJson(payloadSchema) || describesJson(schema));
	}

	private static boolean describesJson(Map<String, Object> schema) {
		if (schema == null || schema.isEmpty()) return false;
		return schema.containsKey("type") || schema.containsKey("properties") || schema.containsKey("items")
				|| schema.containsKey("const") || schema.containsKey("enum") || schema.containsKey("oneOf")
				|| schema.containsKey("anyOf") || schema.containsKey("allOf");
	}
}
