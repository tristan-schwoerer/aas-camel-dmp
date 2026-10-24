/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.configuration;

import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;

/**
 * An HTTP resource read with GET requests: polled, or read once per evaluation.
 *
 * @author n14s - Niklas Mertens
 */
public record HttpPoll(String url, HttpRequestOptions options) implements Endpoint {
	@Override
	public String uri(BundleResources resources) {
		return url + (url.contains("?") ? "&" : "?") + options.bind(resources).endpointOptions();
	}
}
