/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.config.types.Password;
import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


/** Loopback SCRAM and TLS interoperability using only temporary test credentials and certificates. */
@org.junit.jupiter.api.Tag("integration")
class ManagedKafkaScramTlsIntegrationTest {
	private static final String TRUST_STORE = "javax.net.ssl.trustStore";
	private static final String TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";
	private static final String TRUST_STORE_TYPE = "javax.net.ssl.trustStoreType";

	@ParameterizedTest @ValueSource(strings = { "SCRAM-SHA-256", "SCRAM-SHA-512" })
	void scramTlsAuthenticatesConsumerAndReadinessAndRejectsBadCredentialsAndTrust(String mechanism) throws Exception {
		var previousTrust = systemProperties();
		var repository = new ManagedKafkaControlIntegrationTest.EmptyRepository();
		try (var broker = new ScramTlsBroker()) {
			repository.start();
			String topic = "scram-tls-control-" + UUID.randomUUID().toString().substring(0, 8);
			String group = "scram-tls-group-" + UUID.randomUUID().toString().substring(0, 8);
			String password = UUID.randomUUID().toString();
			setTrustStore(broker.trustStore, broker.storePassword);
			broker.provision("managed-test-user", password, mechanism);
			var security = new ManagedRuntimeSettings.KafkaSecurity("SASL_SSL", mechanism, "managed-test-user", password);
			try (Admin admin = broker.secureAdmin(security)) {
				admin.createTopics(java.util.List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
			}
			var settings = ManagedRuntimeSettings.fromEnvironment(Map.ofEntries(
					Map.entry("DMP_MANAGED_AAS_REPOSITORY_URI", "http://127.0.0.1:" + repository.port() + "/repo"),
					Map.entry("DMP_MANAGED_KAFKA_BROKERS", broker.secureBootstrap()),
					Map.entry("DMP_MANAGED_CONTROL_TOPIC", topic), Map.entry("DMP_MANAGED_CONTROL_GROUP_ID", group),
					Map.entry("DMP_MANAGED_KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
					Map.entry("DMP_MANAGED_KAFKA_SASL_MECHANISM", mechanism),
					Map.entry("DMP_MANAGED_KAFKA_SASL_USERNAME", "managed-test-user"),
					Map.entry("DMP_MANAGED_KAFKA_SASL_PASSWORD", password),
					Map.entry("DMP_MANAGED_REPAIR_INTERVAL_MS", "60000")));
			try (var runtime = new ManagedDmpRuntime(settings)) {
				runtime.start();
				Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> ManagedRuntimeStatus.snapshot().ready());
				assertTrue(ManagedRuntimeStatus.snapshot().ready(), "runtime readiness requires both the active assignment and authenticated Admin probe");
				assertEquals(ManagedRuntimeStatus.State.READY, ManagedRuntimeStatus.snapshot().state());
				assertFalse(settings.kafkaSecurity().toString().contains(password));
				assertFalse(settings.toString().contains(password));
			}
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> ManagedRuntimeStatus.snapshot().state() == ManagedRuntimeStatus.State.STOPPED);

			var wrongPassword = new ManagedRuntimeSettings.KafkaSecurity("SASL_SSL", mechanism, "managed-test-user", UUID.randomUUID().toString());
			try (var rejected = new ManagedControlReadiness(settings.kafka(), "not-a-member", () -> true, wrongPassword)) {
				Awaitility.await().atMost(Duration.ofSeconds(12)).until(() -> {
					rejected.poll(); return "control-unauthorized".equals(rejected.snapshot().category());
				});
				assertEquals("control-unauthorized", rejected.snapshot().category());
			}

			Path untrustedStore = broker.untrustedTrustStore;
			setTrustStore(untrustedStore, broker.storePassword);
			try (var rejected = new ManagedControlReadiness(settings.kafka(), "not-a-member", () -> true, security)) {
				Awaitility.await().atMost(Duration.ofSeconds(12)).until(() -> {
					rejected.poll(); return "control-unauthorized".equals(rejected.snapshot().category());
				});
				assertFalse(rejected.snapshot().ready(), "untrusted server certificate cannot become readiness");
				assertEquals("control-unauthorized", rejected.snapshot().category(),
						"certificate rejection must not be confused with an empty group or absent member");
			}
		} finally {
			repository.stop();
			restoreSystemProperties(previousTrust);
			ManagedRuntimeStatus.reset();
		}
	}

	private static Map<String, String> systemProperties() {
		var values = new java.util.HashMap<String, String>();
		for (String key : Set.of(TRUST_STORE, TRUST_STORE_PASSWORD, TRUST_STORE_TYPE))
			values.put(key, System.getProperty(key));
		return values;
	}
	private static void setTrustStore(Path store, String password) {
		System.setProperty(TRUST_STORE, store.toString());
		System.setProperty(TRUST_STORE_PASSWORD, password);
		System.setProperty(TRUST_STORE_TYPE, "PKCS12");
	}
	private static void restoreSystemProperties(Map<String, String> previous) {
		previous.forEach((key, value) -> { if (value == null) System.clearProperty(key); else System.setProperty(key, value); });
	}

	private static final class ScramTlsBroker implements AutoCloseable {
		final Path directory;
		final Path keyStore;
		final Path trustStore;
		final String storePassword = UUID.randomUUID().toString();
		KraftTestBroker kafka;

		ScramTlsBroker() throws Exception {
			directory = Files.createTempDirectory("dmp-scram-tls-");
			keyStore = directory.resolve("broker.p12");
			trustStore = directory.resolve("trust.p12");
			untrustedTrustStore = directory.resolve("untrusted-trust.p12");
			try {
				createCertificate(keyStore, trustStore, storePassword);
				createCertificate(directory.resolve("other-broker.p12"), untrustedTrustStore, storePassword);
			} catch (Exception failure) {
				deleteDirectory(directory);
				throw failure;
			}
			try {
				setTrustStore(trustStore, storePassword);
				Map<String, Object> options = new java.util.HashMap<>();
				options.put("sasl.enabled.mechanisms", "SCRAM-SHA-256,SCRAM-SHA-512");
				for (String algorithm : java.util.List.of("scram-sha-256", "scram-sha-512"))
					options.put("listener.name.external." + algorithm + ".sasl.jaas.config", new Password(
							"org.apache.kafka.common.security.scram.ScramLoginModule required;"));
				options.put("listener.name.external.ssl.keystore.location", keyStore.toString());
				options.put("listener.name.external.ssl.keystore.password", new Password(storePassword));
				options.put("listener.name.external.ssl.keystore.type", "PKCS12");
				options.put("listener.name.external.ssl.key.password", new Password(storePassword));
				options.put("listener.name.external.ssl.client.auth", "none");
				kafka = new KraftTestBroker("SASL_SSL", options);
			} catch (Exception failure) {
				close(); throw new IllegalStateException("isolated SCRAM/TLS broker failed to start", failure);
			}
		}
		private final Path untrustedTrustStore;
		String secureBootstrap() { return kafka.bootstrap(); }
		void provision(String user, String password, String mechanism) throws Exception {
			try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.internalBootstrap(),
					"request.timeout.ms", 10000, "default.api.timeout.ms", 10000))) {
				admin.alterUserScramCredentials(java.util.List.of(new UserScramCredentialUpsertion(user,
						new ScramCredentialInfo(ScramMechanism.valueOf(mechanism.replace('-', '_')), 4096), password)))
						.all().get(10, TimeUnit.SECONDS);
			}
		}
		Admin secureAdmin(ManagedRuntimeSettings.KafkaSecurity security) {
			var properties = new java.util.HashMap<String, Object>(security.clientProperties());
			properties.put("bootstrap.servers", secureBootstrap());
			return Admin.create(properties);
		}
		@Override public void close() throws Exception {
			try { if (kafka != null) kafka.close(); }
			finally { deleteDirectory(directory); }
			assertFalse(Files.exists(directory), "ephemeral broker keys, certificates, truststores and logs are removed");
		}
		private static void deleteDirectory(Path directory) throws Exception {
			if (!Files.exists(directory)) return;
			try (var paths = Files.walk(directory)) {
				for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
			}
		}
		private static void createCertificate(Path keyStore, Path trustStore, String password) throws Exception {
			String alias = "loopback";
			runKeytool(password, "-genkeypair", "-noprompt", "-alias", alias, "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
					"-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-storetype", "PKCS12",
					"-keystore", keyStore.toString(), "-storepass:env", "DMP_TEST_KEYSTORE_PASSWORD",
					"-keypass:env", "DMP_TEST_KEYSTORE_PASSWORD");
			Path certificate = keyStore.resolveSibling("broker.cer");
			runKeytool(password, "-exportcert", "-noprompt", "-alias", alias, "-keystore", keyStore.toString(), "-storetype", "PKCS12",
					"-storepass:env", "DMP_TEST_KEYSTORE_PASSWORD", "-file", certificate.toString());
			runKeytool(password, "-importcert", "-noprompt", "-alias", alias, "-file", certificate.toString(), "-storetype", "PKCS12",
					"-keystore", trustStore.toString(), "-storepass:env", "DMP_TEST_KEYSTORE_PASSWORD");
			Files.deleteIfExists(certificate);
		}
		private static void runKeytool(String password, String... arguments) throws Exception {
			var command = new java.util.ArrayList<String>();
			command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
			command.addAll(java.util.List.of(arguments));
			ProcessBuilder builder = new ProcessBuilder(command).redirectOutput(ProcessBuilder.Redirect.DISCARD)
					.redirectError(ProcessBuilder.Redirect.DISCARD);
			builder.environment().put("DMP_TEST_KEYSTORE_PASSWORD", password);
			Process process = builder.start();
			try {
				if (!process.waitFor(20, TimeUnit.SECONDS)) throw new IllegalStateException("temporary certificate generation timed out");
				if (process.exitValue() != 0) throw new IllegalStateException("temporary certificate generation failed");
			} finally {
				if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
			}
		}
	}
}
