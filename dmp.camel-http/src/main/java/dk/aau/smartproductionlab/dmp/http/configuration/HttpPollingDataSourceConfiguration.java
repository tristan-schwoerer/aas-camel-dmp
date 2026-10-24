/*******************************************************************************
 * Copyright (C) 2022 the Eclipse BaSyx Authors
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
package dk.aau.smartproductionlab.dmp.http.configuration;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;

/**
 * Configuration of an HTTP source polled with GET requests
 * @author n14s - Niklas Mertens
 *
 */
public class HttpPollingDataSourceConfiguration extends DataSourceConfiguration
		implements dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource {
	public HttpPollingDataSourceConfiguration() {}
	private String authUsername;
	private String authPassword;
	/** Retries off, timeouts, the form's static htv_headers and Basic credentials; bound while the bundle lives. */
	private HttpClientOptions options = new HttpClientOptions(null, null, null);
	private java.util.Map<String, String> secretQuery = java.util.Map.of();

	private java.util.Map<String, String> headers;

	/** The secret query parameters of the AID's security scheme (an API key in the query); bound, never in the URI. */
	public void setSecretQuery(java.util.Map<String, String> secretQuery) {
		this.secretQuery = secretQuery == null ? java.util.Map.of() : java.util.Map.copyOf(secretQuery);
		this.options = new HttpClientOptions(headers, authUsername, authPassword, this.secretQuery);
	}

	public void setHeaders(java.util.Map<String, String> headers) {
		this.headers = headers;
		this.options = new HttpClientOptions(headers, authUsername, authPassword, secretQuery);
	}

	@Override
	public void bind(org.apache.camel.CamelContext context) {
		options.bind(context);
	}

	@Override
	public void close() {
		options.close();
	}

	public HttpPollingDataSourceConfiguration(String uniqueId, String serverUrl, int serverPort, String authUsername, String authPassword) {
		super(uniqueId, serverUrl, serverPort);
		this.authUsername = authUsername;
		this.authPassword = authPassword;
		this.options = new HttpClientOptions(headers, authUsername, authPassword, secretQuery);
	}

	@Override
	public String getConnectionURI() {
		String uri = getServerUrl();
		return uri + (uri.contains("?") ? "&" : "?") + options.endpointOptions();
	}
}
