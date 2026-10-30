/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.TopicPartition;
import dk.aau.smartproductionlab.dmp.management.repository.KafkaControlEventRouteBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.awaitility.Awaitility;


/** Broker-backed check that a control event is committed only after invalidation acceptance. */
@org.junit.jupiter.api.Tag("integration")
class ManagedKafkaControlIntegrationTest {
	private String previousHealthPort;
	private EmbeddedBroker broker;

	@AfterEach void cleanup() throws Exception {
		try { if (broker != null) broker.close(); }
		finally {
				if (previousHealthPort == null) System.clearProperty("dmp.http.port");
				else System.setProperty("dmp.http.port", previousHealthPort);
				ManagedRuntimeStatus.reset();
		}
	}

	@Test
	void kafkaEventIsAcceptedCommittedAndNotReplayedAfterManagedRestart() throws Exception {
		Set<Thread> observersBefore = observerThreads();
		previousHealthPort = System.getProperty("dmp.http.port");
		int healthPort = freePort();
		broker = new EmbeddedBroker(true);
		int kafkaPort = broker.port;
		System.setProperty("dmp.http.port", Integer.toString(healthPort));

		var repository = new EmptyRepository();
		repository.start();
		String aasTopic = "aas-events-integration";
		String submodelTopic = "submodel-events-integration";
		Set<String> topics = Set.of(aasTopic, submodelTopic);
		String group = "dmp-control-integration-group";
		Properties adminProperties = new Properties();
		adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + kafkaPort);
		try (AdminClient admin = AdminClient.create(adminProperties)) {
			admin.createTopics(topics.stream().map(topic -> new NewTopic(topic, 1, (short) 1)).toList()).all().get(10, TimeUnit.SECONDS);
		}
		var settings = new ManagedRuntimeSettings(URI.create("http://127.0.0.1:" + repository.port() + "/repo"), null,
				new KafkaControlEventRouteBuilder.Settings("127.0.0.1:" + kafkaPort,
						aasTopic + "," + submodelTopic, group), Set.of(), 8, 8, 2, 60_000, Duration.ofSeconds(2),
				Duration.ofSeconds(2), 10, 4);
		ManagedDmpRuntime runtime = null;
		try {
			runtime = new ManagedDmpRuntime(settings);
			runtime.start();
			Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> ManagedRuntimeStatus.snapshot().ready());
			try (AdminClient admin = AdminClient.create(adminProperties)) {
				Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> {
					try {
						var members = admin.describeConsumerGroups(Set.of(group)).describedGroups().get(group)
								.get(2, TimeUnit.SECONDS).members();
						return members.size() == 1 && members.iterator().next().assignment().topicPartitions()
								.containsAll(topics.stream().map(topic -> new TopicPartition(topic, 0)).collect(java.util.stream.Collectors.toSet()));
					} catch (Exception notJoinedYet) { return false; }
				});
			}
			try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties(kafkaPort))) {
				producer.send(new ProducerRecord<>(aasTopic, "aas", "{\"type\":\"SM_UPDATED\",\"id\":\"urn:aas-changed\"}"))
						.get(10, TimeUnit.SECONDS);
				producer.send(new ProducerRecord<>(submodelTopic, "submodel", "{\"type\":\"SME_UPDATED\",\"submodelId\":\"urn:submodel-changed\",\"smElementPath\":\"Variables.State\"}"))
						.get(10, TimeUnit.SECONDS);
			}
			var reconciliation = runtime.reconciliation();
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (reconciliation.acceptedInvalidationCount() + reconciliation.ignoredEventCount() < 2 && System.nanoTime() < deadline) Thread.sleep(20);
			assertEquals(1, reconciliation.acceptedInvalidationCount(), "a new submodel may be an AIMC and is checked");
			assertEquals(1, reconciliation.ignoredEventCount(), "an element of a submodel nothing depends on is ignored");
			runtime.close();
			runtime = null;
			Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> observersBefore.containsAll(observerThreads()));
			assertEquals(ManagedRuntimeStatus.State.STOPPED, ManagedRuntimeStatus.snapshot().state());
			assertFalse(ManagedRuntimeStatus.snapshot().ready());

			try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties(kafkaPort, group))) {
				consumer.subscribe(topics);
				var records = consumer.poll(Duration.ofSeconds(3));
				assertTrue(records.isEmpty(), "manual commit makes the acknowledged event durable across restart");
			}
		} finally {
			if (runtime != null) runtime.close();
			repository.stop();
		}
	}

	static Properties producerProperties(int port) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + port);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		return props;
	}

	private static Properties consumerProperties(int port, String group) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + port);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		return props;
	}

	static int freePort() throws java.io.IOException {
		try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
	}

	private static Set<Thread> observerThreads() {
		return Thread.getAllStackTraces().keySet().stream().filter(thread -> thread.isAlive()
				&& (thread.getName().equals("dmp-control-readiness") || thread.getName().contains("dmp-control-readiness-")))
				.collect(java.util.stream.Collectors.toSet());
	}

	@Test void missingTopicThenRebalanceAndResubscriptionPreserveAcceptedOffsets() throws Exception {
		previousHealthPort = System.getProperty("dmp.http.port");
		System.setProperty("dmp.http.port", Integer.toString(freePort()));
		broker = new EmbeddedBroker(false);
		var repository = new EmptyRepository();
		repository.start();
		String topic = "late-control", group = "late-control-group";
		var settings = new ManagedRuntimeSettings(URI.create("http://127.0.0.1:" + repository.port() + "/repo"), null,
				new KafkaControlEventRouteBuilder.Settings(broker.bootstrap(), topic, group), Set.of(), 8, 8, 2, 1000, Duration.ofSeconds(1), Duration.ofSeconds(1), 10, 4);
		try (var runtime = new ManagedDmpRuntime(settings); var admin = broker.admin()) {
			runtime.start();
			assertFalse(ManagedRuntimeStatus.snapshot().ready(), "configured route is not a real topic/assignment");
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> ManagedRuntimeStatus.snapshot().category().equals("control-topic-missing"));
			assertFalse(admin.listTopics().names().get(5, TimeUnit.SECONDS).contains(topic), "broker auto-creation is disabled");
			admin.createTopics(java.util.List.of(new NewTopic(topic, 2, (short) 1))).all().get(10, TimeUnit.SECONDS);
			awaitPartitions(admin, group, 2);
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> ManagedRuntimeStatus.snapshot().ready());
			Properties peerSettings = consumerProperties(broker.port, group);
			peerSettings.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-rebalance-peer");
			try (var peer = new KafkaConsumer<String, String>(peerSettings)) {
				peer.subscribe(Set.of(topic));
				Awaitility.await().atMost(Duration.ofSeconds(35)).until(() -> {
					peer.poll(Duration.ofMillis(200));
					return !peer.assignment().isEmpty() && admin.describeConsumerGroups(Set.of(group)).all()
							.get(3, TimeUnit.SECONDS).get(group).members().size() == 2;
				});
				int runtimePartition = peer.assignment().iterator().next().partition() == 0 ? 1 : 0;
				try (var producer = new KafkaProducer<String, String>(producerProperties(broker.port))) {
					producer.send(new ProducerRecord<>(topic, runtimePartition, "changed", "{\"type\":\"SM_UPDATED\",\"id\":\"urn:changed\"}")).get(10, TimeUnit.SECONDS);
				}
				Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> runtime.reconciliation().acceptedInvalidationCount() == 1);
				Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> committed(admin, group, new TopicPartition(topic, runtimePartition)) == 1);
			}
			awaitPartitions(admin, group, 2);
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> ManagedRuntimeStatus.snapshot().ready());
			try (var producer = new KafkaProducer<String, String>(producerProperties(broker.port))) {
				for (int partition = 0; partition < 2; partition++) producer.send(new ProducerRecord<>(topic, partition, "changed",
						"{\"type\":\"SM_UPDATED\",\"id\":\"urn:after-rebalance\"}")).get(10, TimeUnit.SECONDS);
			}
			Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> runtime.reconciliation().acceptedInvalidationCount() == 3);
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
					.get(3, TimeUnit.SECONDS).values().stream().mapToLong(offset -> offset.offset()).sum() == 3);
		} finally { repository.stop(); }
	}

	@Test void readinessStaysUnreadyUntilEveryConfiguredControlTopicExistsAndIsAssigned() throws Exception {
		previousHealthPort = System.getProperty("dmp.http.port");
		System.setProperty("dmp.http.port", Integer.toString(freePort()));
		broker = new EmbeddedBroker(false);
		var repository = new EmptyRepository();
		repository.start();
		String aasTopic = "aas-events-ready", submodelTopic = "submodel-events-ready", group = "dual-control-ready-group";
		try (var admin = broker.admin()) {
			admin.createTopics(java.util.List.of(new NewTopic(aasTopic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
			var settings = new ManagedRuntimeSettings(URI.create("http://127.0.0.1:" + repository.port() + "/repo"), null,
					new KafkaControlEventRouteBuilder.Settings(broker.bootstrap(),
							aasTopic + "," + submodelTopic, group), Set.of(), 8, 8, 2, 1000, Duration.ofSeconds(1), Duration.ofSeconds(1), 10, 4);
			try (var runtime = new ManagedDmpRuntime(settings)) {
				runtime.start();
				Awaitility.await().atMost(Duration.ofSeconds(10)).until(() ->
						ManagedRuntimeStatus.snapshot().category().equals("control-topic-missing"));
				assertFalse(ManagedRuntimeStatus.snapshot().ready(), "one existing topic cannot qualify a two-topic listener");
				admin.createTopics(java.util.List.of(new NewTopic(submodelTopic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
				Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> ManagedRuntimeStatus.snapshot().ready());
				var assignment = admin.describeConsumerGroups(Set.of(group)).describedGroups().get(group).get(3, TimeUnit.SECONDS)
						.members().iterator().next().assignment().topicPartitions();
				assertEquals(Set.of(new TopicPartition(aasTopic, 0), new TopicPartition(submodelTopic, 0)), assignment);
			}
		} finally { repository.stop(); }
	}

	@Test void staleOtherGroupMemberDoesNotQualifyCurrentClient() throws Exception {
		previousHealthPort = System.getProperty("dmp.http.port");
		broker = new EmbeddedBroker(false);
		String topic = "own-member-control", group = "own-member-control-group";
		try (var admin = broker.admin()) {
			admin.createTopics(java.util.List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
			Properties oldSettings = consumerProperties(broker.port, group);
			oldSettings.put(ConsumerConfig.CLIENT_ID_CONFIG, "old-control-instance");
			try (var oldMember = new KafkaConsumer<String, String>(oldSettings)) {
				oldMember.subscribe(Set.of(topic));
				Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> {
					oldMember.poll(Duration.ofMillis(200)); return !oldMember.assignment().isEmpty();
				});
				var settings = new KafkaControlEventRouteBuilder.Settings(broker.bootstrap(), topic, group);
				try (var observer = new ManagedControlReadiness(settings, "new-control-instance")) {
					observer.poll();
					assertFalse(observer.snapshot().ready(), "another assigned member is not this runtime");
					assertEquals("control-assignment-pending", observer.snapshot().category());
				}
				try (var observer = new ManagedControlReadiness(settings, "old-control-instance")) {
					observer.poll();
					assertTrue(observer.snapshot().ready(), "same real metadata qualifies only its actual client");
				}
				try (var observer = new ManagedControlReadiness(settings, "old-control-instance", () -> false)) {
					observer.poll();
					assertFalse(observer.snapshot().ready(), "a terminated native consumer cannot use stale own broker membership");
					assertEquals("control-consumer-unavailable", observer.snapshot().category());
				}
			}
		}
	}

	static void awaitPartitions(AdminClient admin, String group, int partitions) {
		Awaitility.await().atMost(Duration.ofSeconds(35)).until(() -> {
			try {
				var members = admin.describeConsumerGroups(Set.of(group)).all().get(3, TimeUnit.SECONDS).get(group).members();
				return members.size() == 1 && members.iterator().next().assignment().topicPartitions().size() == partitions;
			} catch (Exception notReady) { return false; }
		});
	}
	static long committed(AdminClient admin, String group, TopicPartition partition) throws Exception {
		var value = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(3, TimeUnit.SECONDS).get(partition);
		return value == null ? -1 : value.offset();
	}

	/** One shared isolated broker harness for in-process and executable acceptance. */
	/** A plaintext single-node KRaft broker on a fixed port. */
	static final class EmbeddedBroker implements AutoCloseable {
		final int port;
		private final KraftTestBroker kafka;
		EmbeddedBroker(boolean autoCreate) throws Exception {
			kafka = new KraftTestBroker(autoCreate);
			port = kafka.port;
		}
		String bootstrap() { return kafka.bootstrap(); }
		AdminClient admin() { return kafka.admin(); }
		void shutdown() { kafka.shutdown(); }
		void restart() { kafka.restart(); }
		@Override public void close() throws Exception { kafka.close(); }
	}

	/** Minimal local AAS repository fixture for managed startup. */
	static final class EmptyRepository {
		private com.sun.net.httpserver.HttpServer server;
		void start() throws Exception {
			server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
			server.createContext("/repo", exchange -> {
				byte[] body = "{\"result\":[],\"paging_metadata\":{}}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
				exchange.getResponseHeaders().set("Content-Type", "application/json");
				exchange.sendResponseHeaders(200, body.length);
				exchange.getResponseBody().write(body);
				exchange.close();
			});
			server.start();
		}
		int port() { return server.getAddress().getPort(); }
		void stop() { if (server != null) server.stop(0); }
	}
}
