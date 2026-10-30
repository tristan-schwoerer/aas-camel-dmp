/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.aas.event.AasEventComponent;
import dk.aau.smartproductionlab.dmp.aas.event.AasEventHub;
import dk.aau.smartproductionlab.dmp.paho.mqtt5.component.DmpPahoMqtt5Component;
import dk.aau.smartproductionlab.dmp.paho.mqtt5.component.DmpPahoMqtt5Endpoint;

/**
 * Repository events from the BaSyx submodel-repository MQTT feature, each subscription on a dedicated session of
 * the DMP's MQTT component ({@link DmpPahoMqtt5Component#dedicatedEndpoint}).
 *
 * <p><b>Control:</b> two fixed subscriptions, like the Kafka topic: {@code sm-repository/<repo>/submodels/+}
 * (submodels created, updated, deleted) and {@code sm-repository/<repo>/submodels/+/submodelElements/#} (every
 * element event), filtered by relevance in the DMP. Sessions are persistent and QoS 1, so events published while
 * the DMP was disconnected are delivered when it reconnects, as long as the broker keeps the session.
 *
 * <p><b>Values:</b> a separate client with a clean session subscribes {@code sm-repository/<repo>/submodels/#} and
 * feeds event-sourced AAS Property routes through the context's {@link AasEventHub}. A clean session has no
 * backlog, so values published while the DMP was down are never replayed to devices. It runs only while such
 * routes exist.
 *
 * <p>This is only the repository event channel: MQTT sources and sinks from AID forms use their own clients.
 */
public final class MqttControlEventRouteBuilder extends RouteBuilder {
	public static final String SUBMODELS_ROUTE = "dmp-management-mqtt-submodel-events";
	public static final String ELEMENTS_ROUTE = "dmp-management-mqtt-element-events";
	public static final String VALUE_ROUTE = "dmp-management-mqtt-value-events";

	public record Settings(URI brokerUri, String clientId, String username, String password, String repositoryId,
			long sessionExpirySeconds) {
		public Settings {
			if (brokerUri == null || brokerUri.getHost() == null
					|| !Set.of("tcp", "ssl", "ws", "wss").contains(String.valueOf(brokerUri.getScheme()).toLowerCase(java.util.Locale.ROOT)))
				throw new IllegalArgumentException("MQTT control broker must be a tcp, ssl, ws or wss URI");
			// Each subscription has its own client: <clientId>-submodels, -elements and -values.
			if (clientId == null || clientId.isBlank() || clientId.length() > 54 || clientId.chars().anyMatch(Character::isISOControl))
				throw new IllegalArgumentException("MQTT control client ID must be 1..54 characters");
			if (repositoryId == null || repositoryId.isBlank() || repositoryId.contains("/") || repositoryId.contains("#"))
				throw new IllegalArgumentException("MQTT repository ID must be one topic level or +");
			if ((username == null) != (password == null)) throw new IllegalArgumentException("MQTT username and password go together");
			if (sessionExpirySeconds < 0 || sessionExpirySeconds > 4_294_967_295L) throw new IllegalArgumentException("MQTT session expiry is out of range");
		}
		public List<String> topics() {
			String submodels = "sm-repository/" + repositoryId + "/submodels/+";
			return List.of(submodels, submodels + "/submodelElements/#");
		}
		@Override public String toString() {
			return "MqttControlSettings[brokerUri=" + brokerUri + ", clientId=" + clientId + ", credentials="
					+ (username == null ? "none" : "<redacted>") + ", repositoryId=" + repositoryId + "]";
		}
	}

	private final Settings settings;
	private final MappingReconciliationService reconciliation;

	public MqttControlEventRouteBuilder(Settings settings, MappingReconciliationService reconciliation) {
		this.settings = Objects.requireNonNull(settings);
		this.reconciliation = reconciliation;
	}

	@Override public void configure() {
		route(SUBMODELS_ROUTE, settings.topics().get(0), "-submodels", true).process(exchange ->
				reconciliation.acceptEvent(event(exchange)));
		route(ELEMENTS_ROUTE, settings.topics().get(1), "-elements", true).process(exchange ->
				reconciliation.acceptEvent(event(exchange)));
		AasEventHub hub = AasEventComponent.hub(getContext());
		route(VALUE_ROUTE, "sm-repository/" + settings.repositoryId() + "/submodels/#", "-values", false).autoStartup(false)
				.process(exchange -> {
					AasControlEvent event = event(exchange);
					if (hub.listensTo(event.resourceId()) && event.json() != null) hub.publish(event.json(), Map.of());
				});
		new ValueFeedSwitch(getContext(), VALUE_ROUTE, hub, java.time.Duration.ofSeconds(30));
	}

	private static AasControlEvent event(org.apache.camel.Exchange exchange) {
		String topic = exchange.getMessage().getHeader(DmpPahoMqtt5Endpoint.MQTT_TOPIC, String.class);
		return AasControlEvent.parseMqtt(topic == null ? "" : topic, exchange.getMessage().getBody(byte[].class));
	}

	/** A QoS 1 consumer route; {@code persistent} sessions resume, the others start clean without backlog. */
	private org.apache.camel.model.RouteDefinition route(String routeId, String topic, String clientSuffix, boolean persistent) {
		DmpPahoMqtt5Component component = getContext().getComponent(DmpPahoMqtt5Component.SCHEME, DmpPahoMqtt5Component.class);
		// Credentials stay out of the endpoint URI, and so out of logs and diagnostics.
		var endpoint = component.dedicatedEndpoint(routeId, topic, settings.brokerUri().toString(), settings.clientId() + clientSuffix,
				settings.username(), settings.password(), 1, persistent, persistent ? settings.sessionExpirySeconds() : 0);
		return from(endpoint).routeId(routeId);
	}

	/** Both subscriptions are acknowledged by the broker on a live connection. */
	public boolean consumerReady() {
		for (String routeId : List.of(SUBMODELS_ROUTE, ELEMENTS_ROUTE)) {
			var route = getContext().getRoute(routeId);
			if (route == null || !org.apache.camel.support.service.ServiceHelper.isStarted(route.getConsumer())
					|| !(route.getEndpoint() instanceof DmpPahoMqtt5Endpoint endpoint) || !endpoint.isSubscribed()) return false;
		}
		return true;
	}
}
