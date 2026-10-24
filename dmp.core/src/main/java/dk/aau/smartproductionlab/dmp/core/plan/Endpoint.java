/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.plan;

import org.apache.camel.Processor;

/**
 * A connector endpoint as the compiler resolved it: an immutable value a plan can be built from any number of
 * times. Each build turns it into a Camel URI once.
 */
@FunctionalInterface
public interface Endpoint {
	/**
	 * The Camel endpoint URI. What must not appear in it (credentials, compiled Lua) is bound in {@code resources}
	 * for the lifetime of the bundle being built, and the URI only names it.
	 */
	String uri(BundleResources resources);

	/**
	 * Steps normalizing what this endpoint's consumer delivers (an OPC UA DataValue, an MQTT payload, an AAS
	 * event) before the source accepts it; {@code null} when the message is the value. The steps set
	 * {@link Source#CONTRIBUTION_PROPERTY} true on a message that carries a value; any other message is dropped.
	 */
	default Processor normalizer(Source source) { return null; }

	/** Whether the endpoint itself places a sink's value at its {@link Sink#valuePath()} (an AAS element). */
	default boolean appliesValuePath() { return false; }

	/** Whether the connector sends JSON text: a value whose schema describes JSON is then encoded as JSON. */
	default boolean jsonWire() { return false; }

	/** An endpoint whose URI binds nothing. */
	static Endpoint of(String uri) {
		return new Fixed(uri);
	}

	record Fixed(String uri) implements Endpoint {
		public Fixed {
			if (uri == null || uri.isBlank()) throw new IllegalArgumentException("endpoint URI is required");
		}

		@Override public String uri(BundleResources resources) { return uri; }
	}
}
