/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;
import dk.aau.smartproductionlab.dmp.management.repository.KafkaControlEventRouteBuilder;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/** End-to-end managed startup with two repository AIMCs and one isolated bad mapping. */
class ManagedRuntimeIntegrationTest {
	private String oldHealthPort;
	@AfterEach void restoreHealthPort() {
		if (oldHealthPort == null) System.clearProperty("dmp.http.port");
		else System.setProperty("dmp.http.port", oldHealthPort);
		ManagedRuntimeStatus.reset();
	}

	@Test
	void discoversTwoOperationsOnOnePortAndKeepsAValidSiblingBesideABadMapping() throws Exception {
		oldHealthPort = System.getProperty("dmp.http.port");
		int port;
		try (ServerSocket socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
		System.setProperty("dmp.http.port", Integer.toString(port));
		AtomicInteger deviceSends = new AtomicInteger();
		AtomicReference<Throwable> deviceFailure = new AtomicReference<>();
		HttpServer device = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		device.createContext("/plc/api/actions/Restart", exchange -> {
			try {
				deviceSends.incrementAndGet();
				exchange.getRequestBody().readAllBytes();
				respond(exchange, 200, "{\"deviceReply\":\"001\"}");
			} catch (Throwable failure) { deviceFailure.set(failure); exchange.close(); }
		});
		device.start();
		// Two assets, each with its own AIMC and CCI; BaSyx delegates each Operation to its own DMP path.
		var repository = new FakeAasRepository();
		String cci = "a/submodels/ControlComponentInstance";
		String baseAimc = resource("http-operation/aimc.json");
		JsonObject aimcBObject = JsonParser.parseString(baseAimc.replace("contractAAS", "contractBAS")
				.replace(cci, "b/submodels/ControlComponentInstance")).getAsJsonObject();
		addMalformedSibling(aimcBObject);
		repository.put(baseAimc).put(aimcBObject.toString());
		repository.put(resource("http-operation/aid.json").replace("http://192.168.0.60:8080/plc/api",
				"http://127.0.0.1:" + device.getAddress().getPort() + "/plc/api"));
		repository.put(PublishedModels.publish(resource("http-operation/dependencies/0.json"), "contractAAS", "contract"));
		repository.put(PublishedModels.publish(resource("http-operation/dependencies/0.json")
				.replace(cci, "b/submodels/ControlComponentInstance"), "contractBAS", "contractB"));
		var settings = new ManagedRuntimeSettings(repository.uri(),
				null, new KafkaControlEventRouteBuilder.Settings("127.0.0.1:9092", "control", "dmp-control"), Set.of(), 16, 16, 4, 30_000, Duration.ofSeconds(2), Duration.ofSeconds(2), 50, 4);
		var client = new AasRepositoryClient(settings.repositoryUri(), settings.connectTimeout(), settings.requestTimeout(),
				settings.pageSize(), settings.maxPages(), null);
		Runtime component = new Runtime();
		var runtime = new ManagedDmpRuntime(settings, component,
				new RepositoryDiscovery(client, new ConnectorConfigurationFactory.RuntimeSettings(settings.repositoryUri())),
				(ignored, service) -> new RouteBuilder() {
					@Override public void configure() { from("direct:managed-control").routeId("managed-control").process(exchange -> { }); }
				});
		try {
			runtime.start();
			assertEquals(ManagedRuntimeStatus.State.READY, ManagedRuntimeStatus.snapshot().state(), ManagedRuntimeStatus.asMap().toString());
			assertEquals(2, ManagedRuntimeStatus.snapshot().installedMappings());
			assertEquals(1, ManagedRuntimeStatus.snapshot().blockedMappings(), "bad mapping is isolated from its valid sibling");
			HttpClient caller = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
			for (String idShort : List.of("contract", "contractB")) {
				var response = caller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
						+ "/operations/" + idShort + "/Restart")).timeout(Duration.ofSeconds(5))
						.header("Content-Type", "application/json")
						.POST(HttpRequest.BodyPublishers.ofString("{\"Restart\":{\"x\":1}}")).build(),
						HttpResponse.BodyHandlers.ofString());
				assertEquals(200, response.statusCode(), response.body());
			}
			assertNull(deviceFailure.get());
			assertEquals(2, deviceSends.get(), "both AIMCs share one port and each invokes the device once");
			runtime.close();
			assertEquals(0, component.routeCount(), "shutdown retires every managed and control route");

			Runtime restartedComponent = new Runtime();
			var restarted = new ManagedDmpRuntime(settings, restartedComponent,
					new RepositoryDiscovery(client, new ConnectorConfigurationFactory.RuntimeSettings(settings.repositoryUri())),
					(ignored, service) -> new RouteBuilder() {
						@Override public void configure() { from("direct:managed-control-restart").routeId("managed-control-restart").process(exchange -> { }); }
					});
			try {
				restarted.start();
				assertEquals(2, ManagedRuntimeStatus.snapshot().installedMappings(), "restart reconstructs routes from repository state");
				HttpClient restartedCaller = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
				for (String idShort : List.of("contract", "contractB")) {
					var response = restartedCaller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
							+ "/operations/" + idShort + "/Restart")).timeout(Duration.ofSeconds(5))
							.header("Content-Type", "application/json")
							.POST(HttpRequest.BodyPublishers.ofString("{\"Restart\":{\"x\":1}}")).build(),
							HttpResponse.BodyHandlers.ofString());
					assertEquals(200, response.statusCode(), response.body());
				}
			} finally { restarted.close(); }
			assertEquals(4, deviceSends.get(), "restarted routes each invoke exactly once");
		} finally {
			try { runtime.close(); } finally { repository.close(); device.stop(0); }
		}
	}

	private static void addMalformedSibling(JsonObject aimc) {
		JsonArray mappings = aimc.getAsJsonArray("submodelElements").get(0).getAsJsonObject().getAsJsonArray("value");
		JsonObject invalid = mappings.get(0).getAsJsonObject().deepCopy();
		invalid.addProperty("idShort", "bad-mapping");
		JsonArray groups = invalid.getAsJsonArray("value");
		for (JsonElement item : groups) {
			JsonObject group = item.getAsJsonObject();
			if ("Sources".equals(group.get("idShort").getAsString())) {
				JsonArray endpoints = group.getAsJsonArray("value");
				JsonArray fields = endpoints.get(0).getAsJsonObject().getAsJsonArray("value");
				for (JsonElement rawField : fields) {
					JsonObject field = rawField.getAsJsonObject();
					if ("SourceId".equals(field.get("idShort").getAsString())) field.addProperty("idShort", "unexpected");
				}
			}
		}
		mappings.add(invalid);
	}

	private static String resource(String path) throws Exception {
		try (var input = ManagedRuntimeIntegrationTest.class.getResourceAsStream("/runtime-model-contracts/" + path)) {
			assertNotNull(input, "fixture " + path);
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static void respond(HttpExchange exchange, int status, String body) throws java.io.IOException {
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().set("Content-Type", "application/json");
		exchange.sendResponseHeaders(status, bytes.length);
		exchange.getResponseBody().write(bytes);
		exchange.close();
	}

	private static final class Runtime extends DmpComponent {
		Runtime() { super(); }
		int routeCount() { return camelContext.getRoutesSize(); }
	}
}
