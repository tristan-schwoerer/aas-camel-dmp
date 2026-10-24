/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.configuration;

import java.net.URISyntaxException;

import org.apache.hc.core5.net.URIBuilder;
import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;

/**
 * An HTTP resource written, or an HTTP action invoked: a request to {@code scheme://host:port/path?query}.
 *
 * @param method the request's method (the form's htv_methodName), or {@code null} for Camel's default; an action's
 *               method is its invocation's
 * @author rana
 */
public record HttpWrite(String scheme, String host, int port, String path, String query, String method,
		HttpRequestOptions options) implements Endpoint {
	@Override
	public String uri(BundleResources resources) {
		URIBuilder uriBuilder = new URIBuilder();
		uriBuilder.setScheme(scheme);
		uriBuilder.setHost(host);
		uriBuilder.setPort(port);
		uriBuilder.setPath(path);
		// httpMethod is a Camel endpoint option: Camel consumes it and does not send it to the device.
		String endpointOptions = (method == null || method.isBlank() ? "" : "httpMethod=" + method + "&")
				+ options.bind(resources).endpointOptions();
		uriBuilder.setCustomQuery(query == null || query.isBlank() ? endpointOptions : query + "&" + endpointOptions);
		try {
			return uriBuilder.build().toString();
		} catch (URISyntaxException e) {
			throw new IllegalStateException("Exception occurred while creating HttpProducerEndPoint", e);
		}
	}

	@Override public boolean jsonWire() { return true; }
}
