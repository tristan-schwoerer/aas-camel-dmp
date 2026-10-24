/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.observe;

import java.net.URI;
import java.util.Locale;
import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.support.DefaultComponent;

/**
 * {@code dmp-http-observe:<subprotocol>?url=RAW(…)}: observes an HTTP resource as
 * the WoT HTTP binding's {@code subprotocol} describes it, {@code longpoll} (a GET
 * the server answers on the next change, repeated) or {@code sse} (an
 * {@code text/event-stream} whose events each carry one value). camel-http has no
 * consumer, so the DMP ships this one (SPEC §9).
 */
public final class DmpHttpObserveComponent extends DefaultComponent {
	public static final String SCHEME = "dmp-http-observe";

	public enum Subprotocol { LONGPOLL, SSE }

	/** The endpoint URI; values are passed RAW so Camel leaves them unchanged. */
	public static String uri(String subprotocol, String url, String contentType) {
		StringBuilder uri = new StringBuilder(SCHEME).append(':').append(subprotocol.toLowerCase(Locale.ROOT))
				.append("?url=RAW(").append(url).append(')');
		if (contentType != null) uri.append("&contentType=RAW(").append(contentType).append(')');
		return uri.toString();
	}

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		Subprotocol subprotocol;
		try { subprotocol = Subprotocol.valueOf(remaining.toUpperCase(Locale.ROOT)); }
		catch (IllegalArgumentException unknown) { throw new IllegalArgumentException("dmp-http-observe supports longpoll and sse, not " + remaining); }
		String url = getAndRemoveParameter(parameters, "url", String.class);
		if (url == null || url.isBlank()) throw new IllegalArgumentException("dmp-http-observe endpoint requires a url");
		URI target = URI.create(url);
		if (!"http".equalsIgnoreCase(target.getScheme()) && !"https".equalsIgnoreCase(target.getScheme()))
			throw new IllegalArgumentException("dmp-http-observe requires an http(s) url");
		String contentType = getAndRemoveParameter(parameters, "contentType", String.class, "application/json");
		// The form's static htv_headers and Authorization, bound in the registry and only named in the URI.
		Map<?, ?> headers = getAndRemoveOrResolveReferenceParameter(parameters, "headers", Map.class);
		Map<String, String> fields = new java.util.LinkedHashMap<>();
		if (headers != null) headers.forEach((name, value) -> fields.put(String.valueOf(name), String.valueOf(value)));
		// The security scheme's secret query parameters (an API key), bound the same way; only requests carry them.
		Map<?, ?> query = getAndRemoveOrResolveReferenceParameter(parameters, "query", Map.class);
		Map<String, String> secretQuery = new java.util.LinkedHashMap<>();
		if (query != null) query.forEach((name, value) -> secretQuery.put(String.valueOf(name), String.valueOf(value)));
		DmpHttpObserveEndpoint endpoint = new DmpHttpObserveEndpoint(uri, this, subprotocol, target, contentType, fields);
		endpoint.secretQuery(secretQuery);
		// How long an event stream may stay silent before it is reopened (milliseconds).
		Long idleTimeout = getAndRemoveParameter(parameters, "idleTimeout", Long.class);
		if (idleTimeout != null) endpoint.idleTimeout(java.time.Duration.ofMillis(idleTimeout));
		return endpoint;
	}
}
