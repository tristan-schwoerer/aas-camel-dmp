/*******************************************************************************
 * Copyright (C) 2023 the Eclipse BaSyx Authors
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

import java.net.URISyntaxException;

import org.apache.hc.core5.net.URIBuilder;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;

/**
 * An implementation of HTTP producer configuration
 * 
 * @author rana
 *
 */
public class HttpDataSinkConfiguration extends DataSinkConfiguration
		implements dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource {
	/** Retries off, timeouts, the form's static htv_headers and Basic credentials; bound while the bundle lives. */
	private HttpClientOptions options = new HttpClientOptions(null, null, null);
	private java.util.Map<String, String> secretQuery = java.util.Map.of();
	private java.util.Map<String, String> headers;
	private String username;
	private String password;

	/** The secret query parameters of the AID's security scheme (an API key in the query); bound, never in the URI. */
	public void setSecretQuery(java.util.Map<String, String> secretQuery) {
		this.secretQuery = secretQuery == null ? java.util.Map.of() : java.util.Map.copyOf(secretQuery);
		this.options = new HttpClientOptions(headers, username, password, this.secretQuery);
	}

	public void setHeaders(java.util.Map<String, String> headers) {
		this.headers = headers;
		this.options = new HttpClientOptions(headers, username, password, secretQuery);
	}

	/** HTTP Basic credentials, sent as a bound {@code Authorization} header. */
	public void setBasicCredentials(String username, String password) {
		this.username = username;
		this.password = password;
		this.options = new HttpClientOptions(headers, username, password, secretQuery);
	}

	@Override
	public void bind(org.apache.camel.CamelContext context) {
		options.bind(context);
	}

	@Override
	public void close() {
		options.close();
	}

	
	private String scheme;
	private String serverUrl;
	private String path;
	private String query;
	private int port;
	/** The HTTP method of the write (the form's htv_methodName); absent, Camel's default applies. */
	private String method;
	
	public HttpDataSinkConfiguration() {}
	
	public HttpDataSinkConfiguration(String uniqueId, String scheme,  String serverUrl, String path, int port) {
		super(uniqueId);
		this.scheme = scheme;
		this.serverUrl = serverUrl;
		this.path = path;
		this.port = port;
	}
	
	public String getScheme() {
		return scheme;
	}

	public void setScheme(String scheme) {
		this.scheme = scheme;
	}

	public String getServerUrl() {
		return serverUrl;
	}

	public void setServerUrl(String hostUrl) {
		this.serverUrl = hostUrl;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public int getPort() {
		return port;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public String getConnectionURI() {
	
		URIBuilder uriBuilder = new URIBuilder();
		uriBuilder.setScheme(getScheme());
		uriBuilder.setHost(getServerUrl());
		uriBuilder.setPort(getPort());
		uriBuilder.setPath(getPath());
		// httpMethod is a Camel endpoint option: Camel consumes it and does not send it to the device.
		String endpointOptions = (method == null || method.isBlank() ? "" : "httpMethod=" + method + "&") + options.endpointOptions();
		uriBuilder.setCustomQuery(query == null || query.isBlank() ? endpointOptions : query + "&" + endpointOptions);
		
		return buildConnectionUri(uriBuilder);
	}

	@Override
	public boolean requiresJsonWireEncoding() {
		return hasJsonSchema(getPayloadSchema()) || hasJsonSchema(getSchema());
	}

	private static boolean hasJsonSchema(Object value) {
		if (!(value instanceof java.util.Map<?, ?> schema) || schema.isEmpty()) return false;
		return schema.containsKey("type") || schema.containsKey("properties") || schema.containsKey("items")
				|| schema.containsKey("const") || schema.containsKey("enum") || schema.containsKey("oneOf")
				|| schema.containsKey("anyOf") || schema.containsKey("allOf");
	}
	
	private String buildConnectionUri(URIBuilder uriBuilder) {
		
		try {
			return uriBuilder.build().toString();
		} catch (URISyntaxException e) {
			throw new IllegalStateException("Exception occurred while creating HttpProducerEndPoint", e);
		}
	}

}
