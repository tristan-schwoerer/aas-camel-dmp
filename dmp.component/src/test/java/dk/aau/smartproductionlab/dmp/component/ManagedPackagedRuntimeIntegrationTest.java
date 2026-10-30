/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import dk.aau.smartproductionlab.dmp.executable.regression.OperationCalls;
import static org.junit.jupiter.api.Assertions.*;
import static dk.aau.smartproductionlab.dmp.component.ManagedKafkaControlIntegrationTest.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;
import org.junit.jupiter.api.Test;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dk.aau.smartproductionlab.dmp.executable.regression.ResponseMappings;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/** Runs production main, never a test-only runtime engine. Post-package property selects java -jar. */
@org.junit.jupiter.api.Tag("integration")
class ManagedPackagedRuntimeIntegrationTest {
	@Test void executableHostsTwoAimcsOnOnePortAndReconstructsWithoutReplayingActions() throws Exception {
		try (var fixture = new Fixture()) {
			fixture.start(false);
			assertEquals(2, fixture.status().get("installedMappings").getAsInt());
			assertEquals(1, fixture.status().get("blockedMappings").getAsInt(), "invalid sibling does not suppress valid Operation");
			assertEquals(2, fixture.status().getAsJsonObject("resources").get("managedBundles").getAsLong());
			assertEquals(4, fixture.status().getAsJsonObject("resources").get("luaBindings").getAsLong());
			fixture.awaitConsumer();
			fixture.call("contract", "old");
			fixture.call("contractB", "old");
			assertEquals(2, fixture.sends.get());
			fixture.stop(false);
			fixture.assertListenerReleased();
			fixture.awaitNoConsumers();
			fixture.start(false);
			assertEquals(2, fixture.sends.get(), "startup reconstruction never sends an ACTION");
			fixture.awaitConsumer();
			fixture.call("contract", "old");
			fixture.call("contractB", "old");
			assertEquals(4, fixture.sends.get());
			assertEquals(0, fixture.repository.writes());
		}
	}

	@Test void crashAfterCommitBeforeApplyRepairsFromRepositoryWithoutReplay() throws Exception {
		try (var fixture = new Fixture()) {
			fixture.start(true);
			fixture.awaitConsumer();
			assertEquals(1, fixture.status().get("installedMappings").getAsInt(), "exact adoption excludes another owned AIMC");
			assertEquals(0, fixture.status().get("blockedMappings").getAsInt());
			fixture.call("contract", "old");
			String installed = fixture.installedFingerprint();
			fixture.version("new");
			fixture.hold.set(true);
			try (var producer = new KafkaProducer<String, String>(producerProperties(fixture.broker.port))) {
				producer.send(new ProducerRecord<>(fixture.topic, "aimc", "{\"type\":\"SM_UPDATED\",\"id\":\"" + Fixture.AIMC + "\"}"))
						.get(10, TimeUnit.SECONDS);
			}
			assertTrue(fixture.blocked.await(15, TimeUnit.SECONDS), "actual repository fetch is held before compilation/application");
			Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> committed(fixture.admin, fixture.group,
					new TopicPartition(fixture.topic, 0)) == 1);
			assertEquals(installed, fixture.installedFingerprint(), "durable acceptance does not mean deployment succeeded");
			assertTrue(fixture.status().get("repairPending").getAsBoolean());
			assertFalse(fixture.status().get("converged").getAsBoolean(), "last-good readiness cannot hide an accepted unapplied update");
			fixture.call("contract", "old");
			assertEquals(2, fixture.sends.get());
			fixture.stop(true); // SIGKILL, not the graceful shutdown hook exercised by the other test.
			fixture.hold.set(false);
			fixture.release.countDown();
			fixture.start(true);
			fixture.awaitConsumer();
			assertNotEquals(installed, fixture.installedFingerprint());
			assertEquals(0, fixture.status().get("acceptedInvalidations").getAsLong(), "committed event is not redelivered to the cold process");
			assertEquals(2, fixture.sends.get(), "recovery compiles desired state but does not replay either prior command");
			fixture.call("contract", "new");
			String recovered = fixture.installedFingerprint();
			fixture.version("later");
			try (var producer = new KafkaProducer<String, String>(producerProperties(fixture.broker.port))) {
				producer.send(new ProducerRecord<>(fixture.topic, "aimc", "{\"type\":\"SM_UPDATED\",\"id\":\"" + Fixture.AIMC + "\"}"))
						.get(10, TimeUnit.SECONDS);
			}
			Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> !recovered.equals(fixture.installedFingerprint()));
			assertEquals(3, fixture.sends.get(), "a refresh is not a device command");
			fixture.call("contract", "later");
			assertEquals(4, fixture.sends.get());
			assertEquals(0, fixture.repository.writes());
		}
	}

	@Test void brokerOutageDropsCachedHealthAndSameEndpointRecoveryDoesNotReplayActions() throws Exception {
		try (var fixture = new Fixture()) {
			fixture.start(true);
			fixture.awaitConsumer();
			fixture.call("contract", "old");
			assertEquals(1, fixture.sends.get());
			fixture.broker.shutdown();
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> !fixture.status().get("ready").getAsBoolean());
			var down = fixture.status();
			assertFalse(down.get("controlReady").getAsBoolean());
			assertEquals("control-broker-unavailable", down.get("controlCategory").getAsString());
			assertFalse(down.toString().contains(fixture.broker.bootstrap()));
			assertFalse(down.toString().contains(fixture.group));
			assertFalse(down.toString().contains(fixture.joinedMember));
			var health = fixture.caller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + fixture.port + "/health"))
					.timeout(Duration.ofSeconds(1)).GET().build(), HttpResponse.BodyHandlers.ofString());
			assertEquals(1, fixture.sends.get(), "an outage does not invoke ACTIONs");
			fixture.broker.restart();
			Awaitility.await().atMost(Duration.ofSeconds(70)).until(() -> fixture.status().get("ready").getAsBoolean());
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> fixture.caller.send(
					HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + fixture.port + "/health"))
							.timeout(Duration.ofSeconds(1)).GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode() == 200);
			assertEquals(1, fixture.sends.get(), "assignment recovery is not command replay");
			fixture.call("contract", "old");
			assertEquals(2, fixture.sends.get());
			assertEquals(0, fixture.repository.writes());
			assertEquals(503, health.statusCode(), health.body());
			assertFalse(health.body().contains(fixture.broker.bootstrap()));
			assertFalse(health.body().contains(fixture.group));
			assertFalse(health.body().contains(fixture.joinedMember));
		}
	}

	static final class Fixture implements AutoCloseable {
		static final String AIMC = "contractAAS/submodels/AIMC";
		static final String CCI = "a/submodels/ControlComponentInstance";
		final EmbeddedBroker broker = new EmbeddedBroker(false);
		final org.apache.kafka.clients.admin.AdminClient admin = broker.admin();
		final String topic = "executable-control", group = "executable-control-group";
		final AtomicInteger sends = new AtomicInteger();
		final AtomicReference<Throwable> peerFailure = new AtomicReference<>();
		final AtomicBoolean hold = new AtomicBoolean();
		final CountDownLatch blocked = new CountDownLatch(1), release = new CountDownLatch(1);
		final FakeAasRepository repository;
		final Map<String, String> models;
		final HttpServer device;
		final HttpClient caller = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
		final int port = freePort();
		Process process;
		Path log;
		String joinedMember, previousMember;

		Fixture() throws Exception {
			admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
			device = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
			device.createContext("/plc/api/actions/Restart", exchange -> {
				try {
					sends.incrementAndGet();
					var input = JsonParser.parseString(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)).getAsJsonObject();
					assertEquals(7, input.get("x").getAsInt(), "Lua receives decoded caller values, not an invented SourceId envelope");
					assertNotNull(input.get("generation"));
					respond(exchange, 200, "{\"deviceReply\":\"001\"}");
				} catch (Throwable failure) { peerFailure.set(failure); exchange.close(); }
			});
			device.start();
			repository = new FakeAasRepository();
			models = repository.models();
			String aimcPath = "/repo/submodels/" + AasRepositoryClient.base64UrlIdentifier(AIMC);
			repository.override("hold", request -> {
				if (request.equals(aimcPath) && hold.get()) {
					blocked.countDown();
					try { if (!release.await(30, TimeUnit.SECONDS)) throw new AssertionError("repository gate timed out"); }
					catch (InterruptedException failure) { Thread.currentThread().interrupt(); }
				}
				return null;
			});
			put(resource("aid.json").replace("http://192.168.0.60:8080/plc/api", "http://127.0.0.1:" + device.getAddress().getPort() + "/plc/api"));
			put(PublishedModels.publish(cci(), "contractAAS", "contract"));
			version("old");
			JsonObject sibling = JsonParser.parseString(unit("contractBAS", "contractB")).getAsJsonObject();
			JsonArray mappings = sibling.getAsJsonArray("submodelElements").get(0).getAsJsonObject().getAsJsonArray("value");
			JsonObject invalid = mappings.get(0).getAsJsonObject().deepCopy();
			invalid.addProperty("idShort", "invalid-sibling");
			for (var field : invalid.getAsJsonArray("value")) if ("Transformation".equals(field.getAsJsonObject().get("idShort").getAsString()))
				field.getAsJsonObject().addProperty("value", Base64.getEncoder().encodeToString("function !".getBytes(StandardCharsets.UTF_8)));
			mappings.add(invalid); put(sibling.toString());
		}
		/**
		 * Another asset like the first: its own AIMC ({@code <id>/submodels/AIMC}) and CCI, whose Operation BaSyx
		 * delegates to the Operation's own AAS API path. Returns the AIMC, added to the repository.
		 */
		String unit(String id, String idShort) throws Exception {
			String cci = id + "/submodels/ControlComponentInstance";
			put(PublishedModels.publish(cci().replace(CCI, cci), id, idShort));
			String aimc = models.get(AIMC).replace("contractAAS", id).replace(CCI, cci);
			put(aimc);
			return aimc;
		}
		long reads(String id) { return repository.reads(id); }
		/** The fixture CCI whose Restart Operation declares the output deviceReply. */
		String cci() throws Exception {
			JsonObject cci = JsonParser.parseString(resource("dependencies/0.json")).getAsJsonObject();
			ResponseMappings.outputs(ResponseMappings.operation(cci, "Restart"), "deviceReply");
			return cci.toString();
		}
		void version(String value) throws Exception {
			JsonObject model = JsonParser.parseString(resource("aimc.json")).getAsJsonObject();
			JsonObject mapping = model.getAsJsonArray("submodelElements").get(0).getAsJsonObject().getAsJsonArray("value").get(0)
					.getAsJsonObject();
			// The Operation answers deviceReply from the action's output (ResponseSource Restart).
			ResponseMappings.respond(mapping, "Restart", "Restart", "");
			JsonArray fields = mapping.getAsJsonArray("value");
			for (String field : List.of("Transformation", "ResponseTransformation")) {
				JsonObject blob = null;
				for (var raw : fields) if (field.equals(raw.getAsJsonObject().get("idShort").getAsString())) blob = raw.getAsJsonObject();
				if (blob == null) { blob = new JsonObject(); blob.addProperty("modelType", "Blob"); blob.addProperty("idShort", field);
					blob.addProperty("contentType", "text/plain"); fields.add(blob); }
				String script = field.equals("Transformation")
						? "function aimc_main(sources) return { Restart = { x = sources.Restart.x, generation = '" + value + "' } } end"
						: "function aimc_main(sources) return { Restart = { deviceReply = sources.Restart.deviceReply .. '/" + value + "' } } end";
				blob.addProperty("value", Base64.getEncoder().encodeToString(script.getBytes(StandardCharsets.UTF_8)));
			}
			put(model.toString());
		}
		void start(boolean filtered) throws Exception {
			previousMember = joinedMember;
			String jar = System.getProperty("dmp.runtime.jar");
			List<String> arguments = new ArrayList<>(List.of(System.getProperty("java.home") + "/bin/java",
					"-Ddmp.http.port=" + port, "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"));
			if (Boolean.getBoolean("dmp.runtime.traceExceptions")) arguments.add("-Xlog:exceptions=info");
			if (jar == null) arguments.addAll(List.of("-cp", System.getProperty("java.class.path"), DmpExecutable.class.getName()));
			else arguments.addAll(List.of("-jar", jar));
			var command = new ProcessBuilder(arguments);
			command.environment().keySet().removeIf(key -> key.startsWith("DMP_"));
			command.environment().putAll(Map.of("DMP_MANAGED_AAS_REPOSITORY_URI", repository.uri().toString(),
					"DMP_MANAGED_KAFKA_BROKERS", broker.bootstrap(), "DMP_MANAGED_CONTROL_TOPIC", topic,
					"DMP_MANAGED_CONTROL_GROUP_ID", group, "DMP_MANAGED_RETRY_INTERVAL_MS", "1000",
					"DMP_MANAGED_REQUEST_TIMEOUT_MS", "30000", "DMP_AFFORDANCE_VALIDATION", "OFF"));
			if (filtered) command.environment().put("DMP_MANAGED_AIMC_IDS", "[\"" + AIMC + "\"]");
			log = Files.createTempFile("dmp-managed-executable-", ".log");
			process = command.redirectErrorStream(true).redirectOutput(log.toFile()).start();
			Awaitility.await().atMost(Duration.ofSeconds(75)).ignoreExceptionsMatching(failure -> failure instanceof java.io.IOException).untilAsserted(() -> {
				assertTrue(process.isAlive(), Files.readString(log));
				JsonObject current = status();
				assertTrue(current.get("ready").getAsBoolean(), current + "\n" + Files.readString(log));
			});
		}
		JsonObject status() throws Exception {
			var response = caller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/management/status"))
					.timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofString());
			assertEquals(200, response.statusCode(), response.body());
			return JsonParser.parseString(response.body()).getAsJsonObject();
		}
		String installedFingerprint() throws Exception {
			for (var mapping : status().getAsJsonArray("mappings")) {
				var value = mapping.getAsJsonObject();
				if (ManagedRuntimeStatus.token(AIMC).equals(value.get("aimcToken").getAsString()))
					return value.get("installedFingerprint").getAsString();
			}
			throw new AssertionError("installed scope disappeared");
		}
		void call(String owner, String version) throws Exception {
			String cci = "contractB".equals(owner) ? "contractBAS/submodels/ControlComponentInstance" : CCI;
			var response = caller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + OperationCalls.path(cci, "Restart")))
					.timeout(Duration.ofSeconds(8)).header("Content-Type", "application/json")
					.POST(HttpRequest.BodyPublishers.ofString(OperationCalls.of("{\"x\":7}"))).build(), HttpResponse.BodyHandlers.ofString());
			assertEquals(200, response.statusCode(), response.body());
			assertEquals("001/" + version, JsonParser.parseString(OperationCalls.outputs(response.body())).getAsJsonObject().get("deviceReply").getAsString());
			assertNull(peerFailure.get());
		}
		void awaitConsumer() {
			Awaitility.await().atMost(Duration.ofSeconds(70)).until(() -> {
				try { var members = admin.describeConsumerGroups(Set.of(group)).all().get(3, TimeUnit.SECONDS).get(group).members();
					if (members.size() != 1) return false;
					var member = members.iterator().next();
					if (member.consumerId().equals(previousMember) || member.assignment().topicPartitions().size() != 1) return false;
					joinedMember = member.consumerId();
					return true; }
				catch (Exception joining) { return false; }
			});
		}
		void awaitNoConsumers() {
			Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> admin.describeConsumerGroups(Set.of(group)).all()
					.get(3, TimeUnit.SECONDS).get(group).members().isEmpty());
		}
		void stop(boolean abrupt) throws Exception {
			if (process == null) return;
			if (abrupt) process.destroyForcibly(); else process.destroy();
			// Kafka clients may wait out their own close timeout after a broker outage.
			if (!process.waitFor(45, TimeUnit.SECONDS)) {
				String threads = "";
				Path dump = Files.createTempFile("dmp-managed-shutdown-", ".log");
				var diagnostic = new ProcessBuilder(System.getProperty("java.home") + "/bin/jcmd", Long.toString(process.pid()), "Thread.print")
						.redirectErrorStream(true).redirectOutput(dump.toFile()).start();
				if (diagnostic.waitFor(5, TimeUnit.SECONDS)) threads = Files.readString(dump);
				else diagnostic.destroyForcibly().waitFor();
				Files.deleteIfExists(dump);
				process.destroyForcibly().waitFor();
				fail("managed shutdown deadline exceeded: " + Files.readString(log) + "\n" + threads);
			}
			process = null;
			Files.deleteIfExists(log); log = null;
		}
		void assertListenerReleased() throws Exception {
			try (var socket = new ServerSocket()) { socket.setReuseAddress(true); socket.bind(new InetSocketAddress("127.0.0.1", port)); }
		}
		void put(String body) { models.put(JsonParser.parseString(body).getAsJsonObject().get("id").getAsString(), body); }
		@Override public void close() throws Exception {
			hold.set(false); release.countDown();
			try { stop(false); }
			finally {
				repository.close(); device.stop(0);
				try { admin.close(Duration.ofSeconds(3)); } finally { broker.close(); }
			}
			assertNull(peerFailure.get());
		}
	}
	private static String resource(String path) throws Exception {
		try (var input = ManagedPackagedRuntimeIntegrationTest.class.getResourceAsStream("/runtime-model-contracts/http-operation/" + path)) {
			assertNotNull(input, path); return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
	private static void respond(HttpExchange exchange, int status, String body) throws java.io.IOException {
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().set("Content-Type", "application/json");
		exchange.sendResponseHeaders(status, bytes.length); exchange.getResponseBody().write(bytes); exchange.close();
	}
}
