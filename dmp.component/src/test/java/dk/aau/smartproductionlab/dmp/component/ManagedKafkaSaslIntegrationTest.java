/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import dk.aau.smartproductionlab.dmp.executable.regression.OperationCalls;
import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.awaitility.Awaitility;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory.RuntimeSettings;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.KafkaControlEventRouteBuilder;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;


/** Real loopback authentication/authorization, not an exception-classification stub. */
@org.junit.jupiter.api.Tag("integration")
public class ManagedKafkaSaslIntegrationTest {
	private final String oldHealthPort = System.getProperty("dmp.http.port");
	private final HttpClient caller = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
	@AfterEach void reset() {
		if (oldHealthPort == null) System.clearProperty("dmp.http.port");
		else System.setProperty("dmp.http.port", oldHealthPort);
		ManagedRuntimeStatus.reset();
	}

	@Test void authenticatedAssignmentAclRecoveryAndRestartNeverReplayActionsOrAcceptedEvents() throws Exception {
		Set<Thread> before = clientThreads();
		int healthPort = ManagedKafkaControlIntegrationTest.freePort();
		System.setProperty("dmp.http.port", Integer.toString(healthPort));
		try (var broker = new AuthenticatedBroker(); var repository = new OperationRepository(); var admin = broker.admin()) {
			String topic = "sasl-control", submodelTopic = "sasl-submodel-control", group = "sasl-control-group";
			var partition = new TopicPartition(topic, 0);
			admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1), new NewTopic(submodelTopic, 1, (short) 1)))
					.all().get(10, TimeUnit.SECONDS);
			broker.allow(admin, ResourceType.TOPIC, topic, AclOperation.READ);
			broker.allow(admin, ResourceType.TOPIC, submodelTopic, AclOperation.READ);
			broker.allow(admin, ResourceType.GROUP, group, AclOperation.READ);
			var settings = broker.settings(repository.uri(), topic + "," + submodelTopic, group);
			var component = new RuntimeComponent();
			try (var runtime = runtime(settings, component)) {
				runtime.start();
				awaitReady();
				assertEquals(1, ManagedRuntimeStatus.snapshot().installedMappings());
				assertEquals(0, repository.sends.get(), "authenticated startup does not invoke ACTION");
				var endpoint = component.control();
				assertFalse(endpoint.getConfiguration().isAutoCommitEnable());
				assertTrue(endpoint.getConfiguration().isAllowManualCommit());
				assertEquals(group, endpoint.getConfiguration().getGroupId());
				assertFalse(endpoint.getEndpointUri().contains("sasl.jaas"));
				assertFalse(endpoint.getEndpointUri().contains(broker.password));
				var other = component.unrelatedKafka();
				assertFalse(other.getConfiguration().createConsumerProperties().containsKey("sasl.jaas.config"),
						"control credentials must not become component/data-plane defaults");
				call(healthPort);
				assertEquals(1, repository.sends.get());

				// Both ACL resources are independently denied; restoring ACLs recovers this same runtime/member.
				String member = member(admin, group);
				for (var resource : List.of(Map.entry(ResourceType.GROUP, group), Map.entry(ResourceType.TOPIC, topic),
						Map.entry(ResourceType.TOPIC, submodelTopic))) {
					ResourceType type = resource.getKey();
					String name = resource.getValue();
					var denied = broker.binding(type, name, AclOperation.DESCRIBE, AclPermissionType.DENY);
					admin.createAcls(List.of(denied)).all().get(10, TimeUnit.SECONDS);
					Awaitility.await().atMost(Duration.ofSeconds(15)).until(() ->
							"control-unauthorized".equals(ManagedRuntimeStatus.snapshot().category()));
					assertSafeHealth(healthPort, 503, broker, group, topic);
					assertTimeoutPreemptively(Duration.ofMillis(500), () -> ManagedRuntimeStatus.asMap());
					admin.deleteAcls(List.of(denied.toFilter())).all().get(10, TimeUnit.SECONDS);
					awaitReady();
					assertSafeHealth(healthPort, 200, broker, group, topic);
					assertEquals(member, member(admin, group), "read-only ACL recovery retains this own consumer assignment");
					assertEquals(1, repository.sends.get(), "readiness recovery never repeats the prior ACTION");
				}

				try (var wrong = new ManagedControlReadiness(settings.kafka(), endpoint.getConfiguration().getClientId(), () -> true,
						new ManagedRuntimeSettings.KafkaSecurity("SASL_PLAINTEXT", "PLAIN", "runtime", UUID.randomUUID().toString()))) {
					Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
						wrong.poll(); return "control-unauthorized".equals(wrong.snapshot().category());
					});
					assertFalse(wrong.snapshot().ready(), "real SASL login failure is not assignment readiness");
				}
				try (var own = new ManagedControlReadiness(settings.kafka(), endpoint.getConfiguration().getClientId(), () -> true, settings.kafkaSecurity());
						var otherInstance = new ManagedControlReadiness(settings.kafka(), "different-instance", () -> true, settings.kafkaSecurity())) {
					own.poll(); otherInstance.poll();
					assertTrue(own.snapshot().ready());
					assertFalse(otherInstance.snapshot().ready(), "an authenticated different instance cannot borrow this member");
				}

				repository.hold.set(true);
				broker.send(topic);
				assertTrue(repository.blocked.await(10, TimeUnit.SECONDS), "repository application is intentionally held");
				Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> runtime.reconciliation().acceptedInvalidationCount() == 1
						&& ManagedKafkaControlIntegrationTest.committed(admin, group, partition) == 1);
				assertEquals(1L, repository.release.getCount(), "manual acceptance/commit precedes blocked model application");
				repository.hold.set(false); repository.release.countDown();
				assertEquals(1, repository.sends.get());
			}
			assertNoMembers(admin, group);
			String previousClient = component.lastClientId;
			try (var restarted = runtime(settings, new RuntimeComponent())) {
				restarted.start(); awaitReady();
				assertNotEquals(previousClient, memberClient(admin, group));
				broker.send(topic);
				Awaitility.await().atMost(Duration.ofSeconds(10)).until(() ->
						ManagedKafkaControlIntegrationTest.committed(admin, group, partition) == 2);
				assertEquals(1, restarted.reconciliation().acceptedInvalidationCount(), "only the new sentinel event is accepted after restart");
				assertEquals(1, repository.sends.get(), "startup and accepted-event repair cannot replay ACTION");
				call(healthPort);
				assertEquals(2, repository.sends.get(), "only an explicit new call sends a second ACTION");
			}
			assertNoMembers(admin, group);
			assertEquals(0, repository.writes());
			assertEquals(ManagedRuntimeStatus.State.STOPPED, ManagedRuntimeStatus.snapshot().state());
			try (var socket = new ServerSocket()) { socket.setReuseAddress(true); socket.bind(new InetSocketAddress("127.0.0.1", healthPort)); }
		}
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> before.containsAll(clientThreads()));
	}

	@Test void deniedControlConsumerRequiresRuntimeRecreationAfterAclGrant() throws Exception {
		Set<Thread> before = clientThreads();
		int healthPort = ManagedKafkaControlIntegrationTest.freePort();
		System.setProperty("dmp.http.port", Integer.toString(healthPort));
		var repository = new ManagedKafkaControlIntegrationTest.EmptyRepository();
		repository.start();
		try (var broker = new AuthenticatedBroker(); var admin = broker.admin()) {
			String topic = "denied-control", group = "denied-control-group";
			var partition = new TopicPartition(topic, 0);
			admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
			broker.send(topic);
			var settings = broker.settings(URI.create("http://127.0.0.1:" + repository.port() + "/repo"), topic, group);
			broker.allow(admin, ResourceType.GROUP, group, AclOperation.READ);
			// First topic access is denied; after granting it, the real consumer still lacks group access.
			for (ResourceType missing : List.of(ResourceType.TOPIC, ResourceType.GROUP)) {
				var component = new RuntimeComponent();
				try (var denied = runtime(settings, component)) {
					denied.start();
					Awaitility.await().atMost(Duration.ofSeconds(15)).until(() ->
							"control-unauthorized".equals(ManagedRuntimeStatus.snapshot().category()));
					Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> RecordingAuthorizer.denied(missing) > 0);
					assertSafeHealth(healthPort, 503, broker, group, topic);
					assertEquals(0, denied.reconciliation().acceptedInvalidationCount());
					assertEquals(-1, ManagedKafkaControlIntegrationTest.committed(admin, group, partition), "denied consumption cannot commit queued hints");
				}
				broker.allow(admin, missing, missing == ResourceType.TOPIC ? topic : group, AclOperation.READ);
				if (missing == ResourceType.TOPIC) admin.deleteAcls(List.of(broker.binding(ResourceType.GROUP, group,
						AclOperation.READ, AclPermissionType.ALLOW).toFilter())).all().get(10, TimeUnit.SECONDS);
			}
			// A native READ/GROUP denial kills this route's consumer; ACL grant alone is not automatic recovery.
			try (var restored = new ManagedDmpRuntime(settings)) {
				restored.start(); awaitReady();
				Awaitility.await().atMost(Duration.ofSeconds(10)).until(() ->
						ManagedKafkaControlIntegrationTest.committed(admin, group, partition) == 1);
				assertEquals(1, restored.reconciliation().acceptedInvalidationCount());
				assertSafeHealth(healthPort, 200, broker, group, topic);
			}
			assertNoMembers(admin, group);
		} finally { repository.stop(); }
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> before.containsAll(clientThreads()));
	}

	private static ManagedDmpRuntime runtime(ManagedRuntimeSettings settings, RuntimeComponent component) {
		var client = new AasRepositoryClient(settings.repositoryUri(), settings.connectTimeout(), settings.requestTimeout(), 50, 4, null);
		return new ManagedDmpRuntime(settings, component, new RepositoryDiscovery(client,
				new RuntimeSettings(settings.repositoryUri())),
				(managed, service) -> new KafkaControlEventRouteBuilder(managed.kafka(), service));
	}
	private static void awaitReady() {
		Awaitility.await().atMost(Duration.ofSeconds(25)).until(() -> ManagedRuntimeStatus.snapshot().ready());
	}
	private static String member(AdminClient admin, String group) throws Exception {
		return admin.describeConsumerGroups(Set.of(group)).all().get(3, TimeUnit.SECONDS).get(group).members().iterator().next().consumerId();
	}
	private static String memberClient(AdminClient admin, String group) throws Exception {
		return admin.describeConsumerGroups(Set.of(group)).all().get(3, TimeUnit.SECONDS).get(group).members().iterator().next().clientId();
	}
	private static void assertNoMembers(AdminClient admin, String group) {
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() ->
				admin.describeConsumerGroups(Set.of(group)).all().get(3, TimeUnit.SECONDS).get(group).members().isEmpty());
	}
	private static Set<Thread> clientThreads() {
		return Thread.getAllStackTraces().keySet().stream().filter(t -> t.isAlive() && (t.getName().contains("dmp-control")
				|| t.getName().startsWith("kafka-admin-client-thread") || t.getName().startsWith("kafka-producer-network-thread")))
				.collect(java.util.stream.Collectors.toSet());
	}
	private HttpResponse<String> get(int port, String path) throws Exception {
		return caller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
				.timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofString());
	}
	private void assertSafeHealth(int port, int code, AuthenticatedBroker broker, String group, String topic) throws Exception {
		var health = get(port, "/health");
		assertEquals(code, health.statusCode());
		String projection = health.body() + get(port, "/management/status").body();
		for (String sensitive : List.of(broker.password, broker.adminPassword, broker.bootstrap(), group, topic, "sasl.jaas", "Exception"))
			assertFalse(projection.contains(sensitive), "health/status must contain only safe fixed categories");
	}
	private void call(int port) throws Exception {
		var response = caller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + OperationCalls.path("a/submodels/ControlComponentInstance", "Restart")))
				.timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
				.POST(HttpRequest.BodyPublishers.ofString(OperationCalls.of("{\"Restart\":{\"x\":7}}"))).build(), HttpResponse.BodyHandlers.ofString());
		assertEquals(200, response.statusCode());
	}
	private static final class RuntimeComponent extends DmpComponent {
		String lastClientId;
		RuntimeComponent() { super(); }
		KafkaEndpoint control() {
			var endpoint = (KafkaEndpoint) camelContext.getRoute("dmp-management-control-events").getEndpoint();
			lastClientId = endpoint.getConfiguration().getClientId();
			return endpoint;
		}
		KafkaEndpoint unrelatedKafka() { return camelContext.getEndpoint("kafka:data-plane?brokers=127.0.0.1:1&groupId=data-plane-unchanged", KafkaEndpoint.class); }
	}

	/** Records the broker's real decision, since Camel does not retain poll failures in TaskHealthState. */
	public static final class RecordingAuthorizer extends org.apache.kafka.metadata.authorizer.StandardAuthorizer {
		private static final Map<ResourceType, AtomicInteger> denied = new java.util.concurrent.ConcurrentHashMap<>();
		static int denied(ResourceType type) { return denied.getOrDefault(type, new AtomicInteger()).get(); }
		@Override public List<org.apache.kafka.server.authorizer.AuthorizationResult> authorize(
				org.apache.kafka.server.authorizer.AuthorizableRequestContext context, List<org.apache.kafka.server.authorizer.Action> actions) {
			var results = super.authorize(context, actions);
			if (context.clientId().startsWith("dmp-control-") && !context.clientId().startsWith("dmp-control-readiness-")) {
				for (int i = 0; i < actions.size(); i++) if (results.get(i) == org.apache.kafka.server.authorizer.AuthorizationResult.DENIED)
					denied.computeIfAbsent(actions.get(i).resourcePattern().resourceType(), ignored -> new AtomicInteger()).incrementAndGet();
			}
			return results;
		}
	}

	/** No global JAAS state, credential files, Docker, deployed endpoints or public listeners. */
	private static final class AuthenticatedBroker implements AutoCloseable {
		final String password = UUID.randomUUID().toString(), adminPassword = UUID.randomUUID().toString();
		final KraftTestBroker kafka;
		final int port;
		AuthenticatedBroker() throws Exception {
			RecordingAuthorizer.denied.clear();
			try {
				kafka = new KraftTestBroker("SASL_PLAINTEXT", Map.of(
						"sasl.enabled.mechanisms", "PLAIN",
						"listener.name.external.plain.sasl.jaas.config", new org.apache.kafka.common.config.types.Password(
								"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"" + adminPassword
								+ "\" user_admin=\"" + adminPassword + "\" user_runtime=\"" + password + "\";"),
						"authorizer.class.name", RecordingAuthorizer.class.getName(),
						// The internal and controller listeners are plaintext: their principal is ANONYMOUS.
						"super.users", "User:admin;User:ANONYMOUS",
						"allow.everyone.if.no.acl.found", "false"));
			} catch (Exception failure) { throw new IllegalStateException("isolated authenticated broker failed to start", failure); }
			port = kafka.port;
		}
		String bootstrap() { return "127.0.0.1:" + port; }
		AdminClient admin() {
			var properties = new java.util.HashMap<>(new ManagedRuntimeSettings.KafkaSecurity("SASL_PLAINTEXT", "PLAIN", "admin", adminPassword).clientProperties());
			properties.put("bootstrap.servers", bootstrap());
			return AdminClient.create(properties);
		}
		ManagedRuntimeSettings settings(URI repository, String topic, String group) {
			return ManagedRuntimeSettings.fromEnvironment(Map.ofEntries(
					Map.entry("DMP_MANAGED_AAS_REPOSITORY_URI", repository.toString()), Map.entry("DMP_MANAGED_KAFKA_BROKERS", bootstrap()),
					Map.entry("DMP_MANAGED_CONTROL_TOPIC", topic), Map.entry("DMP_MANAGED_CONTROL_GROUP_ID", group),
					Map.entry("DMP_MANAGED_KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"), Map.entry("DMP_MANAGED_KAFKA_SASL_MECHANISM", "PLAIN"),
					Map.entry("DMP_MANAGED_KAFKA_SASL_USERNAME", "runtime"), Map.entry("DMP_MANAGED_KAFKA_SASL_PASSWORD", password),
					Map.entry("DMP_MANAGED_RETRY_INTERVAL_MS", "60000")));
		}
		AclBinding binding(ResourceType type, String name, AclOperation operation, AclPermissionType permission) {
			return new AclBinding(new ResourcePattern(type, name, PatternType.LITERAL), new AccessControlEntry("User:runtime", "*", operation, permission));
		}
		void allow(AdminClient admin, ResourceType type, String name, AclOperation operation) throws Exception {
			admin.createAcls(List.of(binding(type, name, operation, AclPermissionType.ALLOW))).all().get(10, TimeUnit.SECONDS);
		}
		void send(String topic) throws Exception {
			var properties = ManagedKafkaControlIntegrationTest.producerProperties(port);
			properties.putAll(new ManagedRuntimeSettings.KafkaSecurity("SASL_PLAINTEXT", "PLAIN", "admin", adminPassword).clientProperties());
			try (var producer = new KafkaProducer<String, String>(properties)) {
				producer.send(new ProducerRecord<>(topic, "changed", "{\"type\":\"SM_UPDATED\",\"id\":\"contractAAS/submodels/AIMC\"}")).get(10, TimeUnit.SECONDS);
			}
		}
		@Override public void close() throws Exception {
			kafka.close();
			assertFalse(Files.exists(kafka.directory));
			try (var socket = new ServerSocket()) { socket.setReuseAddress(true); socket.bind(new InetSocketAddress("127.0.0.1", port)); }
		}
	}

	private static final class OperationRepository implements AutoCloseable {
		final AtomicInteger sends = new AtomicInteger();
		final AtomicBoolean hold = new AtomicBoolean();
		final CountDownLatch blocked = new CountDownLatch(1), release = new CountDownLatch(1);
		final HttpServer device = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		final FakeAasRepository repository = new FakeAasRepository();
		OperationRepository() throws Exception {
			device.createContext("/plc/api/actions/Restart", exchange -> {
				sends.incrementAndGet(); exchange.getRequestBody().readAllBytes(); respond(exchange, 200, "{\"deviceReply\":\"001\"}");
			});
			for (String path : List.of("aimc.json", "aid.json", "dependencies/0.json"))
				repository.put(PublishedModels.publish(resource(path).replace("http://192.168.0.60:8080/plc/api",
						"http://127.0.0.1:" + device.getAddress().getPort() + "/plc/api"), "contractAAS", "contract"));
			repository.override("hold", request -> {
				if (request.startsWith("/repo/submodels/") && hold.get()) {
					blocked.countDown();
					try { release.await(15, TimeUnit.SECONDS); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
				}
				return null;
			});
			device.start();
		}
		URI uri() { return repository.uri(); }
		int writes() { return repository.writes(); }
		@Override public void close() { hold.set(false); release.countDown(); repository.close(); device.stop(0); }
		private static String resource(String path) throws Exception {
			try (var input = ManagedKafkaSaslIntegrationTest.class.getResourceAsStream("/runtime-model-contracts/http-operation/" + path)) {
				assertNotNull(input); return new String(input.readAllBytes(), StandardCharsets.UTF_8);
			}
		}
		private static void respond(com.sun.net.httpserver.HttpExchange exchange, int code, String body) throws java.io.IOException {
			byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/json");
			exchange.sendResponseHeaders(code, bytes.length); exchange.getResponseBody().write(bytes); exchange.close();
		}
	}
}
