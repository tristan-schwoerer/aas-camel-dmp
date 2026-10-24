/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.core.plan.RoutePlan;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler;
import dk.aau.smartproductionlab.dmp.management.model.WireProjection;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;

/**
 * HTTP observation through the WoT HTTP binding's {@code subprotocol} (the http-poll fixture,
 * Pressure → KPI/Out): an observable property served as server-sent events and an event
 * affordance served by long polling. Each value runs the mapping and writes the AAS.
 */
class HttpObserveSourcesTest {
	private HttpServer device;
	private HttpServer aas;
	private DmpComponent runtime;
	private FakeAasRepository repository;
	private final List<String> writes = new CopyOnWriteArrayList<>();
	private final AtomicInteger alarms = new AtomicInteger();

	@BeforeEach
	void start() throws Exception {
		device = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		device.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
		// Two pressure changes per stream; the stream ends and is reopened (retry 100 ms).
		device.createContext("/pressure", exchange -> {
			exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
			exchange.sendResponseHeaders(200, 0);
			try (OutputStream body = exchange.getResponseBody()) {
				body.write("retry: 100\ndata: 41.5\n\ndata: 42.5\n\n".getBytes(StandardCharsets.UTF_8));
			}
		});
		// Each long poll is answered with the next alarm code.
		device.createContext("/alarms", exchange -> {
			byte[] body = Integer.toString(100 + alarms.incrementAndGet()).getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().add("Content-Type", "application/json");
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream out = exchange.getResponseBody()) { out.write(body); }
		});
		device.start();
		aas = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		aas.createContext("/", exchange -> {
			if (!"GET".equals(exchange.getRequestMethod()))
				writes.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
			exchange.sendResponseHeaders(204, -1);
			exchange.close();
		});
		aas.start();
		runtime = new DmpComponent(Duration.ofSeconds(4)) { };
		runtime.startComponent();
	}

	@AfterEach
	void stop() throws Exception {
		try { runtime.stopComponent(); } finally {
			device.stop(0);
			aas.stop(0);
			if (repository != null) repository.close();
		}
	}

	@Test
	void serverSentEventsOfAnObservablePropertyWriteEachValue() throws Exception {
		runtime.installBundle("sse", compile(false, "function aimc_main(sources) return { Out = sources.Pressure * 2 } end"));
		await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("83")));
		await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("85")));
	}

	@Test
	void aLongPolledEventWritesEachNotification() throws Exception {
		runtime.installBundle("longpoll", compile(true, "function aimc_main(sources) return { Out = sources.Pressure } end"));
		await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("101")));
		await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("102")));
	}

	// ---- models: the http-poll fixture with Pressure observed by sse, or replaced by a long-polled event ----

	private RoutePlan compile(boolean event, String lua) throws Exception {
		JsonObject aid = json(resource("http-poll/aid.json")).getAsJsonObject();
		JsonObject http = child(aid, "interface_http");
		child(child(http, "EndpointMetadata"), "base").addProperty("value", "http://127.0.0.1:" + device.getAddress().getPort() + "/");
		JsonObject interaction = child(http, "InteractionMetadata");
		JsonObject pressure = child(child(interaction, "properties"), "Pressure");
		JsonObject forms = child(pressure, "forms");
		String sourcePath = "properties";
		String affordance = "Pressure";
		if (event) {
			interaction.getAsJsonArray("value").add(alarmEvents(pressure));
			sourcePath = "events";
			affordance = "Alarm";
		} else {
			child(forms, "href").addProperty("value", "pressure");
			forms.getAsJsonArray("value").add(property("subprotocol", "sse"));
			pressure.getAsJsonArray("value").add(json("{\"modelType\":\"Property\",\"idShort\":\"observable\","
					+ "\"valueType\":\"xs:boolean\",\"value\":\"true\"}"));
			pressure.getAsJsonArray("value").add(property("type", "number"));
		}
		JsonObject aimc = json(resource("http-poll/aimc.json")).getAsJsonObject();
		JsonObject mapping = child(aimc, "MappingConfigurations").getAsJsonArray("value").get(0).getAsJsonObject();
		mapping.getAsJsonArray("value").asList().removeIf(element ->
				"DefaultPollingInterval".equals(element.getAsJsonObject().get("idShort").getAsString()));
		child(mapping, "Transformation").addProperty("value",
				Base64.getEncoder().encodeToString(lua.getBytes(StandardCharsets.UTF_8)));
		JsonObject source = child(mapping, "Sources").getAsJsonArray("value").get(0).getAsJsonObject();
		var keys = child(source, "Source").getAsJsonObject("value").getAsJsonArray("keys");
		keys.get(3).getAsJsonObject().addProperty("value", sourcePath);
		keys.get(4).getAsJsonObject().addProperty("value", affordance);

		repository = new FakeAasRepository();
		String publishedAimc = PublishedModels.publish(aimc.toString(), "contractAAS", "contract");
		repository.put(publishedAimc);
		for (String model : List.of(aid.toString(), resource("http-poll/dependencies/0.json")))
			repository.put(PublishedModels.publish(model, "contractAAS", "contract"));
		var plan = new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compile(WireProjection.aimc(
				dk.aau.smartproductionlab.dmp.aas.model.Aas.read(publishedAimc,
						org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
		assertEquals(AasRepositoryClient.Status.FOUND, plan.status(), String.valueOf(plan));
		var compiled = new ConnectorConfigurationFactory().createBundles(plan.value(),
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://127.0.0.1:" + aas.getAddress().getPort())));
		assertTrue(compiled.issues().isEmpty(), String.valueOf(compiled.issues()));
		return compiled.bundles().get(0).route();
	}

	/** An event affordance whose notifications are numbers, subscribed by long polling {@code alarms}. */
	private static JsonObject alarmEvents(JsonObject template) {
		JsonObject event = template.deepCopy();
		event.addProperty("idShort", "Alarm");
		event.add("semanticId", semantic("https://www.w3.org/2019/wot/td#EventAffordance"));
		event.getAsJsonArray("value").asList().removeIf(element ->
				!"forms".equals(element.getAsJsonObject().get("idShort").getAsString()));
		JsonObject forms = child(event, "forms");
		child(forms, "href").addProperty("value", "alarms");
		forms.getAsJsonArray("value").add(property("subprotocol", "longpoll"));
		JsonObject data = json("{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"data\",\"value\":[]}").getAsJsonObject();
		data.add("semanticId", semantic("https://www.w3.org/2019/wot/td#hasNotificationSchema"));
		data.getAsJsonArray("value").add(property("type", "number"));
		event.getAsJsonArray("value").add(data);
		JsonObject events = json("{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"events\",\"value\":[]}").getAsJsonObject();
		events.add("semanticId", semantic("https://www.w3.org/2019/wot/td#EventAffordance"));
		events.getAsJsonArray("value").add(event);
		return events;
	}

	private static JsonObject property(String idShort, String value) {
		return json("{\"modelType\":\"Property\",\"idShort\":\"" + idShort + "\",\"valueType\":\"xs:string\",\"value\":\""
				+ value + "\"}").getAsJsonObject();
	}

	private static JsonObject semantic(String id) {
		return json("{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"" + id + "\"}]}")
				.getAsJsonObject();
	}

	private static JsonObject child(JsonObject parent, String name) {
		for (var entry : parent.getAsJsonArray(parent.has("submodelElements") ? "submodelElements" : "value")) {
			JsonObject child = entry.getAsJsonObject();
			if (child.has("idShort") && name.equals(child.get("idShort").getAsString())) return child;
		}
		throw new IllegalStateException("missing fixture element " + name);
	}

	private static String resource(String path) throws Exception {
		try (var input = HttpObserveSourcesTest.class.getClassLoader().getResourceAsStream("runtime-model-contracts/" + path)) {
			assertNotNull(input, path);
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static JsonElement json(String value) { return JsonParser.parseString(value); }
}
