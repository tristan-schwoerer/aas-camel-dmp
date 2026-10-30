/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.plan;

import java.util.Map;
import java.util.Objects;

/**
 * One Source of a mapping (SPEC §6): where its value comes from and how it enters the sources map.
 *
 * @param id                the source's identity within its bundle, part of its route IDs
 * @param sourceId          the key of its value in Lua's {@code sources}; {@code id} when blank
 * @param endpoint          its connector endpoint
 * @param delivery          whether its endpoint delivers values or is read
 * @param pollingIntervalMs a read source's interval; 0 when it is read only at evaluation
 * @param evaluation        whether its new value evaluates the mapping
 * @param valuePath         a JSON Pointer selecting its value in the endpoint's payload, or {@code null}
 * @param schema            its value's JSON schema (from the AID or the AAS element), or {@code null}
 * @param initialRead       an observed {@code Cached} source's affordance read once, so it has a value before its
 *                          first update; {@code null} when it is not read
 */
public record Source(String id, String sourceId, Endpoint endpoint, Delivery delivery, long pollingIntervalMs,
		Evaluation evaluation, String valuePath, Map<String, Object> schema, Endpoint initialRead) {

	/** Exchange property: false when a connector message carried no value, so the source contributes nothing. */
	public static final String CONTRIBUTION_PROPERTY = "dmp.source.contribution";
	/** Exchange property: set when a connector has decoded the message into its logical Java value. */
	public static final String NORMALIZED_VALUE_PROPERTY = "dmp.source.normalizedValue";
	/**
	 * Header with what the device itself declares for the value, as a JSON-schema map (e.g. an OPC UA variable's
	 * DataType). The source's AID schema must agree with it
	 * ({@link dk.aau.smartproductionlab.dmp.core.util.SchemaCompatibility}).
	 */
	public static final String DEVICE_SCHEMA_HEADER = "DmpDeviceSchema";

	/** How a source's values arrive. */
	public enum Delivery {
		/** The endpoint's consumer delivers them: a subscription, an observation, an event feed. */
		OBSERVED,
		/** The DMP reads the endpoint: at its polling interval, or once per evaluation. */
		READ
	}

	public Source {
		Objects.requireNonNull(id, "id");
		Objects.requireNonNull(endpoint, "endpoint");
		Objects.requireNonNull(delivery, "delivery");
		if (sourceId == null || sourceId.isBlank()) sourceId = id;
		if (evaluation == null) evaluation = Evaluation.ON_UPDATE;
		if (pollingIntervalMs < 0) throw new IllegalArgumentException("source " + id + " has a negative polling interval");
	}

	/** A source without an initial read. */
	public Source(String id, String sourceId, Endpoint endpoint, Delivery delivery, long pollingIntervalMs,
			Evaluation evaluation, String valuePath, Map<String, Object> schema) {
		this(id, sourceId, endpoint, delivery, pollingIntervalMs, evaluation, valuePath, schema, null);
	}

	/** A source with default evaluation, no value path and no schema. */
	public Source(String id, String sourceId, Endpoint endpoint, Delivery delivery, long pollingIntervalMs) {
		this(id, sourceId, endpoint, delivery, pollingIntervalMs, Evaluation.ON_UPDATE, null, null);
	}

	public Source withEvaluation(Evaluation value) {
		return new Source(id, sourceId, endpoint, delivery, pollingIntervalMs, value, valuePath, schema, initialRead);
	}

	public Source withPollingIntervalMs(long value) {
		return new Source(id, sourceId, endpoint, delivery, value, evaluation, valuePath, schema, initialRead);
	}

	public Source withValuePath(String value) {
		return new Source(id, sourceId, endpoint, delivery, pollingIntervalMs, evaluation, value, schema, initialRead);
	}

	public Source withSchema(Map<String, Object> value) {
		return new Source(id, sourceId, endpoint, delivery, pollingIntervalMs, evaluation, valuePath, value, initialRead);
	}

	public Source withInitialRead(Endpoint value) {
		return new Source(id, sourceId, endpoint, delivery, pollingIntervalMs, evaluation, valuePath, schema, value);
	}

	/** Read by the DMP rather than delivered by its endpoint. */
	public boolean read() { return delivery == Delivery.READ; }

	/** This source read once through its {@link #initialRead()} endpoint, decoded as the source itself. */
	public Source asInitialRead() {
		if (initialRead == null) throw new IllegalStateException("source " + id + " has no initial read");
		return new Source(id, sourceId, initialRead, Delivery.READ, 0, evaluation, valuePath, schema, null);
	}
}
