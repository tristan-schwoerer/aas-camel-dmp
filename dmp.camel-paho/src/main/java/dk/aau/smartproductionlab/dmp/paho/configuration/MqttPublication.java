/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.paho.configuration;

import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.security.BoundCredential;
import dk.aau.smartproductionlab.dmp.core.security.Credentials;

/**
 * An MQTT sink: a {@code dmp-paho-mqtt5:} publish on the context's shared connection to the broker
 * ({@code DmpPahoMqtt5Component}). The client id names the sink's endpoint; the connection has its own.
 *
 * @param credentials the broker credentials, or {@code null}
 * @param qos         the declared {@code mqv_qos}, or {@code null}
 * @param retained    the declared {@code mqv_retain}, or {@code null}
 * @author rana
 */
public record MqttPublication(String host, int port, String topic, String clientId, Credentials credentials,
		Integer qos, Boolean retained) implements Endpoint {
	@Override
	public String uri(BundleResources resources) {
		// brokerUrl already carries the port; do NOT append it again.
		StringBuilder uri = new StringBuilder("dmp-paho-mqtt5:").append(topic)
				.append("?brokerUrl=tcp://").append(host).append(':').append(port)
				.append("&name=").append(clientId);
		if (qos != null) uri.append("&qos=").append(qos.intValue());
		if (retained != null) uri.append("&retained=").append(retained.booleanValue());
		// The URI only names the credentials; the owning bundle binds them in the registry.
		if (credentials != null) {
			BoundCredential credential = credentials.bind(resources);
			uri.append("&userName=").append(credential.userNameReference())
					.append("&password=").append(credential.passwordReference());
		}
		return uri.toString();
	}

	@Override public boolean jsonWire() { return true; }
}
