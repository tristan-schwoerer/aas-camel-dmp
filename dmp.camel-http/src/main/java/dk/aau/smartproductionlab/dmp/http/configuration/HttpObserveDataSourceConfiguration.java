/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.configuration;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.http.observe.DmpHttpObserveComponent;

/** An HTTP property observed, or an HTTP event subscribed, through a WoT {@code subprotocol} (longpoll, sse). */
public class HttpObserveDataSourceConfiguration extends DataSourceConfiguration
		implements dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource {
	/** The form's static htv_headers and Basic credentials; bound in the registry while the bundle lives. */
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

	private String subprotocol;
	private String contentType;
	private String authUsername;
	private String authPassword;

	public HttpObserveDataSourceConfiguration() {}

	public HttpObserveDataSourceConfiguration(String uniqueId, String serverUrl, int serverPort, String subprotocol,
			String contentType, String authUsername, String authPassword) {
		super(uniqueId, serverUrl, serverPort);
		this.subprotocol = subprotocol;
		this.contentType = contentType;
		this.authUsername = authUsername;
		this.authPassword = authPassword;
		this.options = new HttpClientOptions(headers, authUsername, authPassword, secretQuery);
		setDelivery("async");
	}

	public String getSubprotocol() { return subprotocol; }

	@Override
	public String getConnectionURI() {
		return DmpHttpObserveComponent.uri(subprotocol, getServerUrl(), contentType) + "&headers=" + options.headersReference()
				+ "&query=" + options.queryReference();
	}
}
