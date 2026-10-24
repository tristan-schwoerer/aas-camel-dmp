/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.configuration;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.camel.CamelContext;
import org.apache.camel.component.http.HttpClientConfigurer;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource;

/**
 * The one set of client options of every HTTP endpoint the DMP opens: polls, writes, observations and actions
 * (SPEC §5.2). Requests are sent once (no automatic retries: a write or an action must not repeat), give up after
 * 30 s unless an action says otherwise, and carry the form's static {@code htv_headers}, an {@code Authorization}
 * header for HTTP Basic, and the secret headers and query parameters of the AID's security scheme (a bearer token,
 * an API key). These values may be secrets, so they stay out of endpoint URIs: while the owning bundle lives, an
 * {@link HttpClientConfigurer} that adds them is bound in the Camel registry under an opaque name, and the URI only
 * names it, as
 * {@link dk.aau.smartproductionlab.dmp.core.security.BoundCredential} does for credentials.
 */
public final class HttpClientOptions implements ManagedRouteResource {
	public static final long TIMEOUT_MILLIS = 30_000;

	private final String name = "dmp-http-" + UUID.randomUUID();
	private final Map<String, String> headers;
	private final Map<String, String> query;
	private CamelContext boundContext;
	private int owners;

	/** Options with the form's static headers and, when {@code username} is given, HTTP Basic credentials. */
	public HttpClientOptions(Map<String, String> headers, String username, String password) {
		this(headers, username, password, Map.of());
	}

	/** As above, with secret query parameters added to every request. */
	public HttpClientOptions(Map<String, String> headers, String username, String password, Map<String, String> query) {
		Map<String, String> all = new LinkedHashMap<>(headers == null ? Map.of() : headers);
		if (username != null) {
			Objects.requireNonNull(password, "password");
			all.put("Authorization", "Basic " + Base64.getEncoder().encodeToString(
					(username + ":" + password).getBytes(StandardCharsets.UTF_8)));
		}
		this.headers = Map.copyOf(all);
		this.query = Map.copyOf(query == null ? Map.of() : query);
	}

	/** The camel-http options: the bound configurer by name and the timeouts; no secret. */
	public String endpointOptions() { return endpointOptions(TIMEOUT_MILLIS); }

	public String endpointOptions(long timeoutMillis) {
		return "httpClientConfigurer=#bean:" + name + "&connectTimeout=" + timeoutMillis + "&responseTimeout=" + timeoutMillis;
	}

	/** The registry reference of the header map itself, for endpoints that take it directly. */
	public String headersReference() { return "#bean:" + name + ".headers"; }

	public Map<String, String> headers() { return headers; }

	/** The registry reference of the secret query parameters, for endpoints that take them directly. */
	public String queryReference() { return "#bean:" + name + ".query"; }

	@Override
	public synchronized void bind(CamelContext context) {
		if (boundContext != null) {
			if (boundContext != context) throw new IllegalStateException("HTTP options are bound in another CamelContext");
			owners++;
			return;
		}
		context.getRegistry().bind(name, HttpClientConfigurer.class, new Configurer(headers, query));
		context.getRegistry().bind(name + ".headers", Map.class, headers);
		context.getRegistry().bind(name + ".query", Map.class, query);
		boundContext = context;
		owners = 1;
	}

	@Override
	public synchronized void close() {
		if (boundContext == null) return;
		if (--owners > 0) return;
		boundContext.getRegistry().unbind(name);
		boundContext.getRegistry().unbind(name + ".headers");
		boundContext.getRegistry().unbind(name + ".query");
		boundContext = null;
	}

	@Override public String toString() {
		return "HttpClientOptions[" + name + ", " + headers.keySet() + ", query " + query.keySet() + ", <values redacted>]";
	}

	/** {@code url} with the secret query parameters appended. */
	public static java.net.URI withQuery(java.net.URI url, Map<String, String> query) {
		if (query.isEmpty()) return url;
		try {
			var builder = new org.apache.hc.core5.net.URIBuilder(url);
			query.forEach(builder::addParameter);
			return builder.build();
		} catch (java.net.URISyntaxException invalid) {
			throw new IllegalArgumentException("the request URI cannot take the security parameters");
		}
	}

	private record Configurer(Map<String, String> headers, Map<String, String> query) implements HttpClientConfigurer {
		@Override
		public void configureHttpClient(HttpClientBuilder builder) {
			builder.disableAutomaticRetries();
			if (!headers.isEmpty()) builder.addRequestInterceptorLast((request, entity, context) -> headers.forEach(request::setHeader));
			if (!query.isEmpty()) builder.addRequestInterceptorLast((request, entity, context) -> {
				try {
					request.setUri(withQuery(request.getUri(), query));
				} catch (java.net.URISyntaxException invalid) {
					throw new org.apache.hc.core5.http.ProtocolException("the request URI cannot take the security parameters");
				}
			});
		}
	}
}
