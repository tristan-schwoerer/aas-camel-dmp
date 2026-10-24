/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.paho.configuration;

import org.apache.camel.Processor;
import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import dk.aau.smartproductionlab.dmp.core.security.BoundCredential;
import dk.aau.smartproductionlab.dmp.core.security.Credentials;
import dk.aau.smartproductionlab.dmp.paho.consumer.MqttSourceEventAdapter;

/**
 * An MQTT source: a {@code dmp-paho-mqtt5:} subscription on the context's shared connection to the broker
 * ({@code DmpPahoMqtt5Component}). The client id names the source's endpoint; the connection has its own.
 *
 * @param credentials the broker credentials, or {@code null}
 * @param qos         the declared subscription quality of service ({@code mqv_qos}), or {@code null}
 * @author haque
 */
public record MqttSubscription(String host, int port, String clientId, String topic, Credentials credentials,
		Integer qos) implements Endpoint {
	@Override
	public String uri(BundleResources resources) {
		StringBuilder uri = new StringBuilder("dmp-paho-mqtt5:").append(topic)
				.append("?brokerUrl=tcp://").append(host).append(':').append(port);
		if (clientId != null && !clientId.isBlank()) uri.append("&name=").append(clientId);
		// The URI only names the credentials; the owning bundle binds them in the registry.
		if (credentials != null) {
			BoundCredential credential = credentials.bind(resources);
			uri.append("&userName=").append(credential.userNameReference())
					.append("&password=").append(credential.passwordReference());
		}
		if (qos != null) uri.append("&qos=").append(qos.intValue());
		return uri.toString();
	}

	/** Payload bytes are decoded only when the message or the source's schema declares JSON. */
	@Override
	public Processor normalizer(Source source) {
		return new MqttSourceEventAdapter().normalizer(source);
	}
}
