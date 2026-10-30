/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler;
import dk.aau.smartproductionlab.dmp.management.model.WireProjection;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;

/** Executes authored wire fixtures through real repository, builders, Lua, REST and HTTP I/O. */
class ManagedModelBehaviorTest {
	private static final class Runtime extends DmpComponent {
		Runtime() { super(); }
		int endpoints() { return camelContext.getEndpoints().size(); }
		void listener(int port) throws Exception {
			camelContext.addRoutes(new RouteBuilder() {
				@Override public void configure() {
					restConfiguration().component("jetty").host("127.0.0.1").port(port).inlineRoutes(false);
				}
			});
		}
	}

	@Test
	void generatedHttpPollingPreservesDecodedTextThroughLuaAndTheAasWriteBoundary() throws Exception {
		var reads = new AtomicInteger();
		var writes = new AtomicInteger();
		var written = new AtomicReference<String>();
		var writeMethod = new AtomicReference<String>();
		var received = new java.util.concurrent.CountDownLatch(1);
		var repository = new FakeAasRepository();
		HttpServer peer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		peer.start();
		peer.createContext("/device", exchange -> {
			reads.incrementAndGet();
			respond(exchange, 200, "\"001\"");
		});
		peer.createContext("/submodels", exchange -> {
			writeMethod.set(exchange.getRequestMethod());
			written.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
			writes.incrementAndGet();
			respond(exchange, 200, "{}");
			received.countDown();
		});
		// The AAS write sink addresses the peer; the repository only serves models.
		URI base = URI.create("http://127.0.0.1:" + peer.getAddress().getPort());
		String aimc = PublishedModels.publish(resource("http-poll/aimc.json"), "contractAAS", "contract");
		repository.put(aimc);
		repository.put(PublishedModels.publish(resource("http-poll/aid.json").replace("http://192.168.0.30:8080", base + "/device"), "contractAAS", "contract"));
		repository.put(PublishedModels.publish(resource("http-poll/dependencies/0.json"), "contractAAS", "contract"));
		Runtime runtime = new Runtime();
		try {
			var client = repository.client();
			var plan = new MappingPlanCompiler(new AasReferenceResolver(client)).compile(WireProjection.aimc(dk.aau.smartproductionlab.dmp.aas.model.Aas.read(aimc, org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
			assertEquals(AasRepositoryClient.Status.FOUND, plan.status(), String.valueOf(plan));
			var compiled = new ConnectorConfigurationFactory().createBundles(plan.value(),
					new ConnectorConfigurationFactory.RuntimeSettings(base));
			assertTrue(compiled.issues().isEmpty(), String.valueOf(compiled.issues()));
			assertEquals(1, compiled.bundles().size());
			runtime.startComponent();
			var bundle = compiled.bundles().get(0);
			var installed = runtime.installBundle(bundle.bundleId(), bundle.route());
			assertTrue(received.await(6, java.util.concurrent.TimeUnit.SECONDS), "generated polling must execute a real AAS write");
			runtime.removeBundle(installed);
			assertEquals("PATCH", writeMethod.get());
			assertEquals(JsonParser.parseString("\"001\""), JsonParser.parseString(written.get()), "numeric-looking text is not parsed into a number");
			assertEquals(reads.get(), writes.get(), "one affordance write per successful source read");
		} finally {
			try { runtime.stopComponent(); } finally { repository.close(); peer.stop(0); }
		}
	}

	@Test
	void generatedHttpOperationPreservesRequestAndRunsResponseLuaOnlyAfterOneDeviceCall() throws Exception {
		AtomicInteger sends = new AtomicInteger();
		AtomicReference<String> command = new AtomicReference<>();
		AtomicReference<String> method = new AtomicReference<>();
		AtomicReference<String> query = new AtomicReference<>();
		AtomicReference<Throwable> handlerFailure = new AtomicReference<>();
		AtomicReference<String> deviceReply = new AtomicReference<>("{\"deviceReply\":\"001\"}");
		HttpServer device = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		device.createContext("/plc/api/actions/Restart", exchange -> {
			try {
				sends.incrementAndGet();
				command.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
				method.set(exchange.getRequestMethod());
				query.set(exchange.getRequestURI().getQuery());
				respond(exchange, 200, deviceReply.get());
			} catch (Throwable failure) {
				handlerFailure.set(failure);
				exchange.close();
			}
		});
		device.start();
		var repository = new FakeAasRepository();
		repository.put(resource("http-operation/aid.json").replace("http://192.168.0.60:8080/plc/api",
				"http://127.0.0.1:" + device.getAddress().getPort() + "/plc/api"));
		Runtime runtime = new Runtime();
		try {
			var repositoryUri = repository.uri();
			var client = new AasRepositoryClient(repositoryUri, Duration.ofSeconds(2), Duration.ofSeconds(2), 100, 4, null);
			runtime.startComponent();
			int port;
			try (ServerSocket socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
			runtime.listener(port);
			int baselineEndpoints = runtime.endpoints();
			HttpClient caller = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
			String[] responseScripts = { null,
					"function aimc_main(sources) assert(sources.Restart.deviceReply == '001'); "
							+ "return {Restart = {seen = sources.Restart.deviceReply}} end",
					"function aimc_main(sources) error('response-only failure') end", null };
			for (int scenario = 0; scenario < responseScripts.length; scenario++) {
				deviceReply.set(scenario == 3 ? "invalid mqtt://user:secret-password@host reply" : "{\"deviceReply\":\"001\"}");
				JsonObject aimc = JsonParser.parseString(resource("http-operation/aimc.json")).getAsJsonObject();
				// With a response script the Operation declares the output seen; without one it has
				// no outputs, and the device reply is not passed through.
				JsonObject cci = JsonParser.parseString(resource("http-operation/dependencies/0.json")).getAsJsonObject();
				if (responseScripts[scenario] != null) {
					ResponseMappings.outputs(ResponseMappings.operation(cci, "Restart"), "seen");
					ResponseMappings.respond(child(aimc, "MappingConfigurations").getAsJsonArray("value").get(0)
							.getAsJsonObject(), "Restart", "Restart", responseScripts[scenario]);
				}
				repository.put(PublishedModels.publish(cci.toString(), "contractAAS", "contract"));
				repository.put(aimc.toString());
				var plan = new MappingPlanCompiler(new AasReferenceResolver(client)).compile(WireProjection.aimc(dk.aau.smartproductionlab.dmp.aas.model.Aas.read(aimc.toString(), org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
				assertEquals(AasRepositoryClient.Status.FOUND, plan.status(), String.valueOf(plan));
				var compiled = new ConnectorConfigurationFactory().createBundles(plan.value(),
						new ConnectorConfigurationFactory.RuntimeSettings(repositoryUri));
				assertTrue(compiled.issues().isEmpty(), String.valueOf(compiled.issues()));
				assertEquals(1, compiled.bundles().size());
				var bundle = compiled.bundles().get(0);
				var installed = runtime.installBundle(bundle.bundleId(), bundle.route());
				try {
					for (String payload : List.of("{\"Restart\":{\"x\":1}}",
							"{\"Restart\":{\"x\":1},\"other\":\"001\"}")) {
						int before = sends.get();
						var response = caller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
								+ OperationCalls.path("a/submodels/ControlComponentInstance", "Restart"))).timeout(Duration.ofSeconds(5))
								.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(OperationCalls.of(payload))).build(),
								HttpResponse.BodyHandlers.ofString());
						assertEquals(before + 1, sends.get(), "one device invocation, including response failure");
						assertNull(handlerFailure.get(), "device simulator must not fail on its handler thread");
						assertEquals("POST", method.get());
						assertEquals("mode=quick", query.get());
						assertEquals(JsonParser.parseString(payload), JsonParser.parseString(command.get()), "caller field names cannot imply internal framing");
						assertEquals(scenario == 2 ? 500 : scenario == 3 ? 502 : 200, response.statusCode(), response.body());
						assertFalse(response.body().contains("secret-password"), "caller diagnostics must not expose connector causes");
						if (scenario < 2) assertEquals(JsonParser.parseString(scenario == 0 ? "{}"
								: "{\"seen\":\"001\"}"), JsonParser.parseString(OperationCalls.outputs(response.body())));
					}
				} finally { runtime.removeBundle(installed); }
				assertEquals(baselineEndpoints, runtime.endpoints(), "real HTTP invocation endpoints must not outlive their bundle");
			}
		} finally {
			try { runtime.stopComponent(); } finally { repository.close(); device.stop(0); }
		}
	}

	private static JsonObject child(JsonObject parent, String id) {
		JsonArray elements = parent.getAsJsonArray(parent.has("submodelElements") ? "submodelElements" : "value");
		for (var item : elements) {
			JsonObject element = item.getAsJsonObject();
			if (element.has("idShort") && id.equals(element.get("idShort").getAsString())) return element;
		}
		throw new IllegalStateException("missing fixture child " + id);
	}

	private static String resource(String path) throws Exception {
		try (var stream = ManagedModelBehaviorTest.class.getClassLoader().getResourceAsStream("runtime-model-contracts/" + path)) {
			assertNotNull(stream, "mandatory fixture " + path);
			return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static void respond(HttpExchange exchange, int status, String body) throws java.io.IOException {
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().set("Content-Type", "application/json");
		exchange.sendResponseHeaders(status, bytes.length);
		exchange.getResponseBody().write(bytes);
		exchange.close();
	}
}
