/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.configuration;

import java.util.Map;

import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;

/**
 * What every request of an HTTP endpoint carries besides its URL: the form's static {@code htv_headers} and the
 * AID security scheme's headers, HTTP Basic credentials ({@code username} {@code null} for none) and secret query
 * parameters. Its values may be secrets: never printed, and bound as {@link HttpClientOptions} for an endpoint URI
 * to name.
 */
public record HttpRequestOptions(Map<String, String> headers, String username, String password, Map<String, String> query) {
	public HttpRequestOptions {
		headers = headers == null ? Map.of() : Map.copyOf(headers);
		query = query == null ? Map.of() : Map.copyOf(query);
	}

	/** No headers, credentials or query parameters. */
	public static HttpRequestOptions none() {
		return new HttpRequestOptions(Map.of(), null, null, Map.of());
	}

	/** The options bound in {@code resources} for the bundle's lifetime. */
	public HttpClientOptions bind(BundleResources resources) {
		return resources.own(new HttpClientOptions(headers, username, password, query));
	}

	@Override public String toString() {
		return "HttpRequestOptions[" + headers.keySet() + ", query " + query.keySet() + ", <values redacted>]";
	}
}
