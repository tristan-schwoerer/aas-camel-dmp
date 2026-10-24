/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.observe;

import java.net.URI;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultEndpoint;

/** One observed HTTP resource; consume-only. */
final class DmpHttpObserveEndpoint extends DefaultEndpoint implements org.apache.camel.MultipleConsumersSupport {
	private final DmpHttpObserveComponent.Subprotocol subprotocol;
	private final URI url;
	private final String contentType;
	private final java.util.Map<String, String> headers;
	private java.time.Duration idleTimeout = DmpHttpObserveConsumer.SSE_IDLE_TIMEOUT;
	private java.util.Map<String, String> secretQuery = java.util.Map.of();

	DmpHttpObserveEndpoint(String uri, DmpHttpObserveComponent component, DmpHttpObserveComponent.Subprotocol subprotocol,
			URI url, String contentType, java.util.Map<String, String> headers) {
		super(uri, component);
		this.subprotocol = subprotocol;
		this.url = url;
		this.contentType = contentType;
		this.headers = java.util.Map.copyOf(headers);
	}

	/** Each consumer holds a request or stream of its own, so mappings may observe the same resource. */
	@Override public boolean isMultipleConsumersSupported() { return true; }

	@Override public Producer createProducer() {
		throw new UnsupportedOperationException("dmp-http-observe endpoints only observe");
	}

	@Override public Consumer createConsumer(Processor processor) throws Exception {
		Consumer consumer = new DmpHttpObserveConsumer(this, processor);
		configureConsumer(consumer);
		return consumer;
	}

	DmpHttpObserveComponent.Subprotocol subprotocol() { return subprotocol; }
	URI url() { return url; }
	String contentType() { return contentType; }
	java.util.Map<String, String> headers() { return headers; }
	java.time.Duration idleTimeout() { return idleTimeout; }
	/** The URL a request goes to: {@link #url()} with the secret query parameters (never logged). */
	URI requestUrl() { return dk.aau.smartproductionlab.dmp.http.configuration.HttpClientOptions.withQuery(url, secretQuery); }
	void secretQuery(java.util.Map<String, String> query) { this.secretQuery = java.util.Map.copyOf(query); }
	void idleTimeout(java.time.Duration idleTimeout) { this.idleTimeout = idleTimeout; }
}
