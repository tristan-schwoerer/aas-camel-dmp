/*******************************************************************************
 * Copyright (C) 2024 the Eclipse BaSyx Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.core.configuration.route.core;

import dk.aau.smartproductionlab.dmp.core.plan.Source;

/**
 * @author Dmp authors, jungjan
 */
public class RouteCreatorHelper {
	private RouteCreatorHelper() {
	}

	/**
	 * One fresh Read of a sync source: its connector's {@link SynchronousSourceReadAdapter}
	 * when one is registered, otherwise a bounded {@code pollEnrich} of the consumer endpoint.
	 */
	public static void configureSyncRead(org.apache.camel.model.ProcessorDefinition<?> route, String endpointUri,
			long timeoutMs) {
		SynchronousSourceReadAdapter adapter = java.util.ServiceLoader.load(SynchronousSourceReadAdapter.class).stream()
				.map(java.util.ServiceLoader.Provider::get)
				.filter(candidate -> candidate.supports(endpointUri))
				.findFirst().orElse(null);
		if (adapter == null) {
			route.setBody(org.apache.camel.builder.Builder.constant(null)).pollEnrich(endpointUri, timeoutMs);
		} else {
			adapter.configureRead(route, endpointUri);
		}
	}

	/**
	 * Normalizes an observed source's connector messages (e.g. OPC UA DataValues)
	 * when its endpoint has a normalizer; messages that carry no value are filtered out.
	 */
	public static org.apache.camel.model.ProcessorDefinition<?> normalizeSourceEvents(
			org.apache.camel.model.ProcessorDefinition<?> route, Source source) {
		org.apache.camel.Processor normalizer = source.endpoint().normalizer(source);
		if (normalizer == null) return route;
		return route.process(normalizer)
				.filter(exchange -> Boolean.TRUE.equals(exchange.getProperty(Source.CONTRIBUTION_PROPERTY)));
	}

}
