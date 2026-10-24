/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.configuration;

import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.http.observe.DmpHttpObserveComponent;

/** An HTTP property observed, or an HTTP event subscribed, through a WoT {@code subprotocol} (longpoll, sse). */
public record HttpObservation(String url, String subprotocol, String contentType, HttpRequestOptions options)
		implements Endpoint {
	@Override
	public String uri(BundleResources resources) {
		HttpClientOptions bound = options.bind(resources);
		return DmpHttpObserveComponent.uri(subprotocol, url, contentType) + "&headers=" + bound.headersReference()
				+ "&query=" + bound.queryReference();
	}
}
