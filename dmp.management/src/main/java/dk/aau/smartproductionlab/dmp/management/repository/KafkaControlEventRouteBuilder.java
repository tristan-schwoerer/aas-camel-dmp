/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import dk.aau.smartproductionlab.dmp.aas.event.AasEventComponent;
import dk.aau.smartproductionlab.dmp.aas.event.AasEventHub;

/**
 * Repository events over Kafka, read by two independent consumers of the same topics:
 * <ul>
 * <li>the <b>control</b> route reconciles desired state: one durable consumer group, from the last committed
 * offset, one record at a time, committed only after the invalidation entered bounded tracking;</li>
 * <li>the <b>value</b> route feeds event-sourced AAS Property routes through the context's {@link AasEventHub}:
 * a consumer group of its own per process, starting at the latest offset and committing nothing, so values
 * published while the DMP was down are never replayed to devices. It runs only while such routes exist.</li>
 * </ul>
 */
public final class KafkaControlEventRouteBuilder extends RouteBuilder {
	private static final Pattern KAFKA_NAME = Pattern.compile("[A-Za-z0-9._-]{1,249}");
	private static final int MAX_CONTROL_TOPICS = 8;
	private static final Pattern BROKER_LIST = Pattern.compile("(?:[A-Za-z0-9.-]+|\\[[0-9A-Fa-f:]+\\]):[0-9]{1,5}(?:,(?:[A-Za-z0-9.-]+|\\[[0-9A-Fa-f:]+\\]):[0-9]{1,5})*");
	private final Settings settings;
	private final MappingReconciliationService reconciliation;
	private final String clientId = "dmp-control-" + java.util.UUID.randomUUID();

	public record Settings(String brokers, String topic, String groupId) {
		public Settings {
			if (brokers == null || !BROKER_LIST.matcher(brokers).matches()
					|| java.util.Arrays.stream(brokers.split(",")).anyMatch(broker -> {
						int port = Integer.parseInt(broker.substring(broker.lastIndexOf(':') + 1));
						return port < 1 || port > 65535;
					})) throw new IllegalArgumentException("valid Kafka brokers are required");
			if (topic == null) throw new IllegalArgumentException("valid control topic is required");
			String[] configuredTopics = topic.split(",", -1);
			if (configuredTopics.length < 1 || configuredTopics.length > MAX_CONTROL_TOPICS)
				throw new IllegalArgumentException("control topic count must be 1.." + MAX_CONTROL_TOPICS);
			java.util.LinkedHashSet<String> names = new java.util.LinkedHashSet<>();
			for (String configured : configuredTopics) {
				String name = configured.trim();
				if (!KAFKA_NAME.matcher(name).matches() || name.equals(".") || name.equals("..") || !names.add(name))
					throw new IllegalArgumentException("control topics must be valid distinct Kafka topic names");
			}
			topic = String.join(",", names);
			if (groupId == null || !KAFKA_NAME.matcher(groupId).matches()) throw new IllegalArgumentException("valid control group ID is required");
		}
		public List<String> topics() { return List.of(topic.split(",")); }
	}

	public KafkaControlEventRouteBuilder(Settings settings, MappingReconciliationService reconciliation) {
		this.settings = settings;
		this.reconciliation = reconciliation;
	}

	public static final String CONTROL_ROUTE = "dmp-management-control-events";
	public static final String VALUE_ROUTE = "dmp-management-value-events";
	private final String valueGroupId = "-values-" + java.util.UUID.randomUUID();

	public String endpointUri() {
		return "kafka:" + settings.topic() + "?brokers=" + settings.brokers()
				+ "&groupId=" + settings.groupId()
				+ "&clientId=" + clientId
				+ "&consumersCount=1&batching=true&maxPollRecords=500&pollTimeoutMs=100&batchingIntervalMs=100"
				+ "&autoOffsetReset=earliest&autoCommitEnable=false&allowManualCommit=true&breakOnFirstError=true";
	}

	/** The value feed: newest values only, never a backlog, nothing committed. */
	public String valueEndpointUri() {
		return "kafka:" + settings.topic() + "?brokers=" + settings.brokers()
				+ "&groupId=" + settings.groupId() + valueGroupId
				+ "&clientId=" + clientId + "-values"
				+ "&consumersCount=1&autoOffsetReset=latest&autoCommitEnable=false";
	}

	/** Per-instance control identity only; group offsets and data-plane identities remain stable. */
	public String clientId() { return clientId; }

	/** Local task state supplements broker assignment without querying the consumer from another thread. */
	public boolean consumerReady() {
		var route = getContext().getRoute(CONTROL_ROUTE);
		if (route == null || !(route.getConsumer() instanceof org.apache.camel.component.kafka.KafkaConsumer consumer)
				|| !consumer.isStarted() || consumer.isKafkaPaused()) return false;
		var states = consumer.healthStates();
		return !states.isEmpty() && states.stream().allMatch(state -> state.isReady() && !state.isTerminated());
	}

	@Override public void configure() {
		AasEventHub hub = AasEventComponent.hub(getContext());
		from(valueEndpointUri()).routeId(VALUE_ROUTE).autoStartup(false).process(exchange -> {
			AasControlEvent event = AasControlEvent.parse(exchange.getMessage().getBody());
			if (!hub.listensTo(event.resourceId()) || event.json() == null) return;
			Map<String, Object> headers = new java.util.HashMap<>();
			for (String name : new String[] { KafkaConstants.TOPIC, KafkaConstants.PARTITION, KafkaConstants.OFFSET })
				if (exchange.getMessage().getHeader(name) != null) headers.put(name, exchange.getMessage().getHeader(name));
			hub.publish(event.json(), headers);
		});
		new ValueFeedSwitch(getContext(), VALUE_ROUTE, hub, java.time.Duration.ofSeconds(30));
		// Each poll is one batch: its events are accepted in order, then each partition's offset is committed once.
		from(endpointUri()).routeId(CONTROL_ROUTE).process(exchange -> {
			Map<String, KafkaManualCommit> commits = new java.util.LinkedHashMap<>();
			for (Object item : exchange.getMessage().getBody(java.util.List.class)) {
				org.apache.camel.Message record = ((org.apache.camel.Exchange) item).getMessage();
				reconciliation.acceptEvent(AasControlEvent.parse(record.getBody()));
				if (record.getHeader(KafkaConstants.MANUAL_COMMIT) instanceof KafkaManualCommit commit)
					commits.put(record.getHeader(KafkaConstants.TOPIC) + "/" + record.getHeader(KafkaConstants.PARTITION), commit);
			}
			if (commits.isEmpty() && exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT) instanceof KafkaManualCommit last)
				commits.put("batch", last);
			if (commits.isEmpty()) throw new IllegalStateException("Kafka manual commit handle is unavailable");
			commits.values().forEach(KafkaManualCommit::commit);
		});
	}
}
