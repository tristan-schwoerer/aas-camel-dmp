/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.paho.configuration;

import java.nio.charset.StandardCharsets;

import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.security.BoundCredential;
import dk.aau.smartproductionlab.dmp.core.security.Credentials;

/**
 * An MQTT action: a command published on {@code topic}, as MQTT 5 request/reply on {@code replyTopic}
 * (correlated) or, without one, one way.
 *
 * @param brokerUrl   {@code tcp://host:port}
 * @param credentials the broker credentials, or {@code null}
 */
public record MqttAction(String brokerUrl, String topic, String clientId, int qos, boolean retained,
		String replyTopic, long replyTimeoutSeconds, Credentials credentials) implements Endpoint {
	public MqttAction {
		if (brokerUrl == null || !brokerUrl.startsWith("tcp://") || clientId == null || clientId.isBlank()
				|| topic == null || topic.isBlank() || topic.contains("+") || topic.contains("#")
				|| qos < 0 || qos > 2 || replyTimeoutSeconds < 1) {
			throw new IllegalArgumentException("invalid MQTT 5 operation target settings");
		}
		// No reply topic: an invoke-only action, published one way.
		if (replyTopic != null && (replyTopic.isBlank() || replyTopic.contains("+") || replyTopic.contains("#")))
			throw new IllegalArgumentException("MQTT 5 InOut requires a concrete reply topic");
	}

	@Override
	public String uri(BundleResources resources) {
		StringBuilder uri = new StringBuilder("dmp-paho-mqtt5:").append(encodeTopic(topic)).append("?")
				.append("brokerUrl=").append(encodeValue(brokerUrl))
				.append("&name=").append(encodeValue(clientId))
				.append("&qos=").append(qos)
				.append("&retained=").append(retained)
				.append("&replyTimeout=").append(replyTimeoutSeconds);
		if (replyTopic != null) uri.append("&replyTopic=").append(encodeValue(replyTopic));
		// The URI only names the credentials; the owning bundle binds them in the registry.
		if (credentials != null) {
			BoundCredential credential = credentials.bind(resources);
			uri.append("&userName=").append(credential.userNameReference())
					.append("&password=").append(credential.passwordReference());
		}
		return uri.toString();
	}

	private static String encodeValue(String value) {
		return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}

	private static String encodeTopic(String value) {
		StringBuilder encoded = new StringBuilder();
		for (byte octet : value.getBytes(StandardCharsets.UTF_8)) {
			int ch = octet & 0xff;
			if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || "-._~/".indexOf(ch) >= 0) {
				encoded.append((char) ch);
			} else {
				encoded.append('%').append(Character.forDigit((ch >>> 4) & 0xf, 16)).append(Character.forDigit(ch & 0xf, 16));
			}
		}
		return encoded.toString();
	}
}
