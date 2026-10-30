/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ManagedRuntimeSettingsTest {
	@AfterEach void resetStatus() { ManagedRuntimeStatus.reset(); }

	/** The minimal complete environment; every DMP needs a repository and a control stream. */
	private static java.util.Map<String, String> required() {
		return new java.util.HashMap<>(Map.of("DMP_MANAGED_AAS_REPOSITORY_URI", "http://aas.test/repo",
				"DMP_MANAGED_KAFKA_BROKERS", "kafka:9092", "DMP_MANAGED_CONTROL_TOPIC", "control",
				"DMP_MANAGED_CONTROL_GROUP_ID", "control-group"));
	}

	@Test
	void repositoryAndControlSettingsAreAlwaysRequired() {
		assertThrows(IllegalArgumentException.class, () -> ManagedRuntimeSettings.fromEnvironment(Map.of()));
		for (String key : required().keySet()) {
			var env = required();
			env.remove(key);
			assertThrows(IllegalArgumentException.class, () -> ManagedRuntimeSettings.fromEnvironment(env), key);
		}
		assertNotNull(ManagedRuntimeSettings.fromEnvironment(required()));
	}

	@Test
	void managedModeRequiresBoundedRepositoryKafkaAndCapacitySettings() {
		Map<String, String> env = Map.ofEntries(
				Map.entry("DMP_MANAGED_AAS_REPOSITORY_URI", "http://aas.test:8081/repository"),
				Map.entry("DMP_MANAGED_AAS_AUTHORIZATION", "Bearer private-token"),
				Map.entry("DMP_MANAGED_KAFKA_BROKERS", "kafka-a:9092,kafka-b:9092"),
				Map.entry("DMP_MANAGED_CONTROL_TOPIC", "aas-control"),
				Map.entry("DMP_MANAGED_CONTROL_GROUP_ID", "dmp-control"),
				Map.entry("DMP_MANAGED_MAX_MAPPINGS", "256"),
				Map.entry("DMP_MANAGED_MAX_INVALIDATIONS", "512"),
				Map.entry("DMP_MANAGED_MAX_PARALLEL_READS", "3"));
		var settings = ManagedRuntimeSettings.fromEnvironment(env);
		assertEquals(java.util.List.of("aas-control"), settings.kafka().topics());
		assertEquals(256, settings.maxMappings());
		assertEquals(3, settings.maxParallelReads());
		assertEquals("kafka", settings.transport());
		assertEquals(5_000, settings.retryMillis());
		assertFalse(settings.toString().contains("private-token"));
		assertTrue(settings.toString().contains("authorization=<redacted>"));
	}

	@Test void mqttTransportNeedsOnlyItsBrokerAndKeepsCredentialsOutOfDiagnostics() {
		var settings = ManagedRuntimeSettings.fromEnvironment(Map.of(
				"DMP_MANAGED_AAS_REPOSITORY_URI", "http://aas.test/repo",
				"DMP_MANAGED_EVENT_TRANSPORT", "MQTT",
				"DMP_MANAGED_MQTT_BROKER_URI", "tcp://hivemq-broker:1883",
				"DMP_MANAGED_MQTT_USERNAME", "dmp",
				"DMP_MANAGED_MQTT_PASSWORD", "private-mqtt-secret"));
		assertEquals("mqtt", settings.transport());
		assertEquals(null, settings.kafka());
		assertEquals("+", settings.mqtt().repositoryId());
		assertEquals("dmp-control", settings.mqtt().clientId());
		assertFalse(settings.toString().contains("private-mqtt-secret"));
		assertFalse(settings.mqtt().toString().contains("private-mqtt-secret"));
		assertThrows(IllegalArgumentException.class, () -> ManagedRuntimeSettings.fromEnvironment(Map.of(
				"DMP_MANAGED_AAS_REPOSITORY_URI", "http://aas.test/repo", "DMP_MANAGED_EVENT_TRANSPORT", "mqtt")));
		assertThrows(IllegalArgumentException.class, () -> ManagedRuntimeSettings.fromEnvironment(Map.of(
				"DMP_MANAGED_AAS_REPOSITORY_URI", "http://aas.test/repo", "DMP_MANAGED_EVENT_TRANSPORT", "amqp")));
		assertThrows(IllegalArgumentException.class, () -> ManagedRuntimeSettings.fromEnvironment(Map.of(
				"DMP_MANAGED_AAS_REPOSITORY_URI", "http://aas.test/repo", "DMP_MANAGED_EVENT_TRANSPORT", "mqtt",
				"DMP_MANAGED_MQTT_BROKER_URI", "tcp://broker:1883", "DMP_MANAGED_MQTT_USERNAME", "only-user")));
	}

	@Test void pollTransportRescansAtAnIntervalAndHasNoRepositoryEvents() {
		var settings = ManagedRuntimeSettings.fromEnvironment(Map.of(
				"DMP_MANAGED_AAS_REPOSITORY_URI", "http://aas.test/repo", "DMP_MANAGED_EVENT_TRANSPORT", "poll"));
		assertEquals("poll", settings.transport());
		assertFalse(settings.repositoryEvents());
		assertEquals(java.time.Duration.ofSeconds(30), settings.polling().interval(), "PT30S unless configured");
		assertNull(settings.kafka());
		assertNull(settings.mqtt());
		assertEquals(java.time.Duration.ofMinutes(2), ManagedRuntimeSettings.fromEnvironment(Map.of(
				"DMP_MANAGED_AAS_REPOSITORY_URI", "http://aas.test/repo", "DMP_MANAGED_EVENT_TRANSPORT", "poll",
				"DMP_MANAGED_RESCAN_INTERVAL", "PT2M")).polling().interval());
		assertTrue(ManagedRuntimeSettings.fromEnvironment(required()).repositoryEvents());
		for (String invalid : new String[] { "30", "PT0.5S", "P2D" })
			assertThrows(IllegalArgumentException.class, () -> ManagedRuntimeSettings.fromEnvironment(Map.of(
					"DMP_MANAGED_AAS_REPOSITORY_URI", "http://aas.test/repo", "DMP_MANAGED_EVENT_TRANSPORT", "poll",
					"DMP_MANAGED_RESCAN_INTERVAL", invalid)), invalid);
	}

	@Test void controlTopicEnvironmentAcceptsBoundedDistinctKafkaNamesAndRejectsInjection() {
		var env = new java.util.HashMap<>(Map.of("DMP_MANAGED_AAS_REPOSITORY_URI", "http://aas.test/repo", "DMP_MANAGED_KAFKA_BROKERS", "kafka:9092",
				"DMP_MANAGED_CONTROL_GROUP_ID", "control-group"));
		env.put("DMP_MANAGED_CONTROL_TOPIC", "aas-events, submodel-events");
		assertEquals(java.util.List.of("aas-events", "submodel-events"),
				ManagedRuntimeSettings.fromEnvironment(env).kafka().topics());
		for (String invalid : java.util.List.of("", ",submodel-events", "aas-events,", "aas-events,,submodel-events",
				"aas-events,aas-events", "aas-events.*", "aas-events,submodel-events,third,fourth,fifth,sixth,seventh,eighth,ninth")) {
			env.put("DMP_MANAGED_CONTROL_TOPIC", invalid);
			assertThrows(IllegalArgumentException.class, () -> ManagedRuntimeSettings.fromEnvironment(env));
		}
	}

	@Test
	void rejectsCapacityOverflow() {
		var oversized = required();
		oversized.put("DMP_MANAGED_MAX_MAPPINGS", "9000");
		assertThrows(IllegalArgumentException.class, () -> ManagedRuntimeSettings.fromEnvironment(oversized));
	}

	@Test void rejectsCredentialBearingAndNonHttpRepositoryUrisBeforeIo() {
		for (String uri : java.util.List.of("http://user:secret@repo.test", "http://repo.test?token=secret", "ftp://repo.test")) {
			var env = required();
			env.put("DMP_MANAGED_AAS_REPOSITORY_URI", uri);
			assertThrows(IllegalArgumentException.class, () -> ManagedRuntimeSettings.fromEnvironment(env));
		}
		var env = required();
		env.put("DMP_MANAGED_AAS_REPOSITORY_URI", "http://repo.test?token=private secret");
		var malformed = assertThrows(IllegalArgumentException.class, () -> ManagedRuntimeSettings.fromEnvironment(env));
		assertFalse(malformed.toString().contains("private secret"));
		assertNull(malformed.getCause(), "URI parse causes may include credentials and must not reach CLI logs");
	}

	@Test void exactAimcFilterPreservesStringsAndRejectsMalformedOrDuplicateIds() {
		Map<String, String> env = required();
		env.put("DMP_MANAGED_AIMC_IDS", "[\"urn:aimc:a,b\",\" urn:aimc:A \"]");
		var settings = ManagedRuntimeSettings.fromEnvironment(env);
		assertEquals(java.util.Set.of("urn:aimc:a,b", " urn:aimc:A "), settings.aimcIds());
		assertFalse(settings.toString().contains("urn:aimc:a,b"));
		assertThrows(UnsupportedOperationException.class, () -> settings.aimcIds().add("new"));
		for (String invalid : java.util.List.of("{}", "[]", "null", "[1]", "[\"\"]", "[\"id\",\"id\"]", "urn:aimc:a")) {
			env.put("DMP_MANAGED_AIMC_IDS", invalid);
			assertThrows(IllegalArgumentException.class, () -> ManagedRuntimeSettings.fromEnvironment(env));
		}
	}

	@Test void saslSettingsAreControlOnlyValidatedAndRedacted() {
		var env = new java.util.HashMap<>(Map.of("DMP_MANAGED_AAS_REPOSITORY_URI", "http://aas.test/repo", "DMP_MANAGED_KAFKA_BROKERS", "kafka:9092",
				"DMP_MANAGED_CONTROL_TOPIC", "control", "DMP_MANAGED_CONTROL_GROUP_ID", "control-group"));
		env.put("DMP_MANAGED_KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
		env.put("DMP_MANAGED_KAFKA_SASL_MECHANISM", "PLAIN");
		env.put("DMP_MANAGED_KAFKA_SASL_USERNAME", "private-user");
		env.put("DMP_MANAGED_KAFKA_SASL_PASSWORD", "private-\\\"password");
		var settings = ManagedRuntimeSettings.fromEnvironment(env);
		var properties = settings.kafkaSecurity().clientProperties();
		assertEquals("SASL_SSL", properties.get("security.protocol"));
		var entries = org.apache.kafka.common.security.JaasContext.loadClientContext(properties).configurationEntries();
		assertTrue(env.get("DMP_MANAGED_KAFKA_SASL_PASSWORD").equals(entries.get(0).getOptions().get("password")),
				"Kafka's real JAAS parser must preserve quoted/backslash credentials");
		assertFalse(properties.toString().contains("private-"), "even native property-map diagnostics must redact JAAS");
		assertFalse(settings.toString().contains("private-"));
		assertFalse(settings.kafkaSecurity().toString().contains("private-"));
		assertThrows(UnsupportedOperationException.class, () -> properties.put("enable.auto.commit", true));
		for (String protocol : java.util.List.of("PLAINTEXT", "private-invalid")) {
			env.put("DMP_MANAGED_KAFKA_SECURITY_PROTOCOL", protocol);
			var failure = assertThrows(IllegalArgumentException.class, () -> ManagedRuntimeSettings.fromEnvironment(env));
			assertFalse(failure.toString().contains("private-"));
			assertNull(failure.getCause());
		}
		env.put("DMP_MANAGED_KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
		env.remove("DMP_MANAGED_KAFKA_SASL_PASSWORD");
		assertThrows(IllegalArgumentException.class, () -> ManagedRuntimeSettings.fromEnvironment(env));
	}

	@Test void saslRejectsPartialOrUnsafeConfigurationAndSelectsOnlyKnownLoginModules() {
		for (String mechanism : java.util.List.of("SCRAM-SHA-256", "SCRAM-SHA-512")) {
			var security = new ManagedRuntimeSettings.KafkaSecurity("SASL_SSL", mechanism, "user", "password");
			var entry = org.apache.kafka.common.security.JaasContext.loadClientContext(security.clientProperties()).configurationEntries().get(0);
			assertEquals("org.apache.kafka.common.security.scram.ScramLoginModule", entry.getLoginModuleName());
		}
		for (String invalid : java.util.List.of("", "OAUTHBEARER", "GSSAPI", "private-invalid")) {
			assertThrows(IllegalArgumentException.class, () -> new ManagedRuntimeSettings.KafkaSecurity("SASL_SSL", invalid, "user", "password"));
		}
		for (String invalid : java.util.List.of("", " ", "private\npassword", "private\u0000password")) {
			var failure = assertThrows(IllegalArgumentException.class,
					() -> new ManagedRuntimeSettings.KafkaSecurity("SASL_SSL", "PLAIN", "user", invalid));
			assertFalse(failure.toString().contains("private"));
		}
	}
}
