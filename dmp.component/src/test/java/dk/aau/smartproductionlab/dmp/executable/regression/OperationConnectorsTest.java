package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.core.plan.RoutePlan;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler;
import dk.aau.smartproductionlab.dmp.management.model.WireProjection;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;
import dk.aau.smartproductionlab.dmp.paho.testing.Mqtt5TestBroker;
import dk.aau.smartproductionlab.dmp.plc4x.testing.ModbusTestServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;

/**
 * Operations bound to each connector (the opcua-method Operation, its Sink moved to
 * another fixture's AID): a property write over Modbus (PLC4X), HTTP and MQTT, each
 * answering once the connector acknowledged the write, and concurrent calls of one
 * MQTT 5 request/reply action whose replies arrive out of order.
 */
class OperationConnectorsTest {
	private static final String WRITE_LUA = "function aimc_main(sources) return { Setpoint = sources.Calibrate.Setpoint } end";

	private Runtime runtime;
	private FakeAasRepository repository;
	private String operationUri;

	private static final class Runtime extends DmpComponent {
		Runtime() { super(Duration.ofSeconds(4)); }
		void listener(int port) throws Exception {
			camelContext.addRoutes(new RouteBuilder() {
				@Override public void configure() {
					restConfiguration().component("jetty").host("127.0.0.1").port(port).inlineRoutes(false);
				}
			});
		}
	}

	@BeforeEach
	void start() throws Exception {
		runtime = new Runtime();
		runtime.startComponent();
		int port = freePort();
		runtime.listener(port);
		operationUri = "http://127.0.0.1:" + port + OperationCalls.path("a/submodels/ControlComponentInstance", "Calibrate");
	}

	@AfterEach
	void stop() throws Exception {
		try { runtime.stopComponent(); } finally { if (repository != null) repository.close(); }
	}

	@Test
	void aModbusPropertyWriteIsAcknowledgedByTheDevice() throws Exception {
		try (ModbusTestServer device = new ModbusTestServer()) {
			runtime.installBundle("modbus", compile("plc4x", "interface_modbus", "Coil1",
					"modbus+tcp://127.0.0.1:" + device.port(), form -> {
						// Unit 1, protocol address 2 of the holding registers; the property is writable by default.
						child(form, "href").addProperty("value", "/1/2");
						form.getAsJsonArray("value").add(json("{\"modelType\":\"Property\",\"idShort\":\"modv_entity\","
								+ "\"valueType\":\"xs:string\",\"value\":\"HoldingRegister\"}"));
					}, setpointInput(), WRITE_LUA));
			var reply = call("{\"Setpoint\":1234}");
			assertEquals(200, reply.statusCode(), reply.body());
			assertEquals(1234, device.register(2), String.valueOf(device.writes()));
		}
	}

	@Test
	void anHttpPropertyWriteAnswersWithTheDevicesResponse() throws Exception {
		List<String> received = new CopyOnWriteArrayList<>();
		AtomicInteger status = new AtomicInteger(204);
		HttpServer device = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		device.createContext("/setpoint", exchange -> {
			received.add(exchange.getRequestMethod() + " " + new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
			exchange.sendResponseHeaders(status.get(), -1);
			exchange.close();
		});
		device.start();
		try {
			runtime.installBundle("http", compile("http-poll", "interface_http", "Pressure",
					"http://127.0.0.1:" + device.getAddress().getPort(), form -> {
						// Without htv_methodName a write is a PUT (WoT HTTP binding default).
						child(form, "href").addProperty("value", "/setpoint");
					}, setpointInput(), WRITE_LUA));
			var reply = call("{\"Setpoint\":7}");
			assertEquals(200, reply.statusCode(), reply.body());
			assertEquals(1, received.size(), String.valueOf(received));
			assertTrue(received.get(0).endsWith(" 7"), received.get(0));

			status.set(500);
			reply = call("{\"Setpoint\":8}");
			assertEquals(502, reply.statusCode(), reply.body());
			assertEquals("DMP_DEVICE_ERROR", OperationCalls.error(reply.body()).get("code"));
		} finally {
			device.stop(0);
		}
	}

	@Test
	void anMqttPropertyWriteIsAcceptedByTheBroker() throws Exception {
		try (Mqtt5TestBroker broker = new Mqtt5TestBroker()) {
			List<Mqtt5TestBroker.Publication> published = new CopyOnWriteArrayList<>();
			broker.onPublish(published::add);
			runtime.installBundle("mqtt", compile("fan-in-out", "interface_mqtt", "State",
					"mqtt://127.0.0.1:" + broker.port() + "/base/", form -> {
						child(form, "href").addProperty("value", "setpoint");
						child(form, "mqv_retain").addProperty("value", "false");
						child(form, "mqv_qos").addProperty("value", "1");
					}, setpointInput(), WRITE_LUA));
			var reply = call("{\"Setpoint\":3}");
			assertEquals(200, reply.statusCode(), reply.body());
			await().atMost(Duration.ofSeconds(5)).until(() -> published.stream()
					.anyMatch(publication -> "base/setpoint".equals(publication.topic()) && "3".equals(publication.text())));
		}
	}

	@Test
	void concurrentCallsOfAnMqttActionEachGetTheirOwnReply() throws Exception {
		int calls = 8;
		try (Mqtt5TestBroker broker = new Mqtt5TestBroker()) {
			List<Mqtt5TestBroker.Publication> commands = new CopyOnWriteArrayList<>();
			var replier = Executors.newSingleThreadExecutor();
			broker.onPublish(publication -> {
				if (!"base/CMD/Calibrate".equals(publication.topic())) return;
				commands.add(publication);
				if (commands.size() < calls) return;
				// The device answers only when all requests are in, last request first.
				replier.submit(() -> {
					List<Mqtt5TestBroker.Publication> reversed = new ArrayList<>(commands);
					java.util.Collections.reverse(reversed);
					for (var command : reversed) {
						String uuid = json(command.text()).getAsJsonObject().get("Uuid").getAsString();
						broker.reply("base/DATA/Calibrate", command.correlationData(),
								"{\"Uuid\":\"" + uuid + "\",\"State\":\"SUCCESS\",\"Outcome\":0,\"TimeStamp\":\"now\"}");
					}
					return null;
				});
			});
			try {
				runtime.installBundle("action", compileMqttAction(broker, true));
				List<CompletableFuture<HttpResponse<String>>> replies = new ArrayList<>();
				HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
				for (int i = 0; i < calls; i++) replies.add(client.sendAsync(request("{\"Uuid\":\"job-" + i + "\"}"),
						HttpResponse.BodyHandlers.ofString()));
				for (int i = 0; i < calls; i++) {
					var reply = replies.get(i).get(20, java.util.concurrent.TimeUnit.SECONDS);
					assertEquals(200, reply.statusCode(), reply.body());
					assertTrue(reply.body().contains("job-" + i), "call " + i + " got another call's reply: " + reply.body());
				}
				assertEquals(calls, commands.size(), "one command per call");
			} finally {
				replier.shutdownNow();
			}
		}
	}

	@Test
	void anOperationWithoutOutputsAnswersOnlyAfterTheActionsTerminalReply() throws Exception {
		long delayMillis = 600;
		try (Mqtt5TestBroker broker = new Mqtt5TestBroker()) {
			var replier = Executors.newSingleThreadScheduledExecutor();
			java.util.concurrent.atomic.AtomicLong repliedAt = new java.util.concurrent.atomic.AtomicLong();
			broker.onPublish(publication -> {
				if (!"base/CMD/Calibrate".equals(publication.topic())) return;
				replier.schedule(() -> {
					repliedAt.set(System.nanoTime());
					broker.reply("base/DATA/Calibrate", publication.correlationData(),
							"{\"Uuid\":\"ack-1\",\"State\":\"SUCCESS\",\"TimeStamp\":\"now\"}");
					return null;
				}, delayMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
			});
			try {
				// The Occupy action answers on its response topic; the Operation declares no outputs.
				runtime.installBundle("terminal-ack", compileMqttAction(broker, false));
				var reply = call("{\"Uuid\":\"ack-1\"}");
				long answeredAt = System.nanoTime();
				assertEquals(200, reply.statusCode(), reply.body());
				assertEquals(json("{}"), json(OperationCalls.outputs(reply.body())), "no outputs, and the reply is not passed through");
				assertTrue(repliedAt.get() != 0 && answeredAt >= repliedAt.get(),
						"the caller is answered only once the device's terminal reply arrived");
			} finally {
				replier.shutdownNow();
			}
		}
	}

	@Test
	void anInvokeOnlyMqttActionIsPublishedAndAnswersWithNoOutputs() throws Exception {
		try (Mqtt5TestBroker broker = new Mqtt5TestBroker()) {
			List<Mqtt5TestBroker.Publication> commands = new CopyOnWriteArrayList<>();
			broker.onPublish(publication -> {
				if ("base/CMD/Calibrate".equals(publication.topic())) commands.add(publication);
			});
			runtime.installBundle("invoke-only", compileInvokeOnlyMqttAction(broker));
			var reply = call("{\"Uuid\":\"halt-1\"}");
			assertEquals(200, reply.statusCode(), reply.body());
			assertEquals(json("{}"), json(OperationCalls.outputs(reply.body())), "an action without an output answers nothing");
			assertEquals(1, commands.size(), "the broker accepted the command before the call returned");
			assertEquals(json("{\"Uuid\":\"halt-1\"}"), json(commands.get(0).text()));
			assertEquals(null, commands.get(0).correlationData(), "a one-way publish carries no correlation data");
		}
	}

	private HttpResponse<String> call(String body) throws Exception {
		return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build().send(request(body),
				HttpResponse.BodyHandlers.ofString());
	}

	private HttpRequest request(String body) {
		return HttpRequest.newBuilder(URI.create(operationUri)).timeout(Duration.ofSeconds(15))
				.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(OperationCalls.of(body))).build();
	}

	private static Consumer<JsonObject> setpointInput() {
		return operation -> operation.add("inputVariables",
				json("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Setpoint\",\"valueType\":\"xs:int\"}}]"));
	}

	// ---- models: the opcua-method Operation bound to an affordance of another fixture's AID ----

	/** The Operation bound to {@code property} of {@code fixture}'s {@code interfaceId}, whose form {@code form} edits. */
	private RoutePlan compile(String fixture, String interfaceId, String property, String base,
			Consumer<JsonObject> form, Consumer<JsonObject> operation, String lua) throws Exception {
		JsonObject aid = json(resource(fixture + "/aid.json")).getAsJsonObject();
		JsonObject iface = child(aid, interfaceId);
		child(child(iface, "EndpointMetadata"), "base").addProperty("value", base);
		form.accept(child(child(child(child(iface, "InteractionMetadata"), "properties"), property), "forms"));
		return compile(aid, interfaceId, "properties", property, "Setpoint", operation, lua, null);
	}

	/** The Operation bound to the stoppering Occupy action (MQTT 5 request/reply), renamed Calibrate. */
	private RoutePlan compileMqttAction(Mqtt5TestBroker broker, boolean outputs) throws Exception {
		JsonObject occupy = child(child(child(child(json(resource("stoppering/aid.json")).getAsJsonObject(), "interface_mqtt"),
				"InteractionMetadata"), "actions"), "Occupy").deepCopy();
		occupy.addProperty("idShort", "Calibrate");
		child(occupy, "title").addProperty("value", "Calibrate");
		JsonObject forms = child(occupy, "forms");
		child(forms, "href").addProperty("value", "CMD/Calibrate");
		child(forms, "mqv_responseTopic").addProperty("value", "DATA/Calibrate");
		JsonObject aid = json(resource("fan-in-out/aid.json")).getAsJsonObject();
		JsonObject mqtt = child(aid, "interface_mqtt");
		child(child(mqtt, "EndpointMetadata"), "base").addProperty("value", "mqtt://127.0.0.1:" + broker.port() + "/base/");
		JsonObject actions = child(child(mqtt, "InteractionMetadata"), "actions");
		if (!actions.has("value")) actions.add("value", new JsonArray());
		actions.getAsJsonArray("value").add(occupy);
		String request = "function aimc_main(sources) return { Calibrate = { Uuid = sources.Calibrate.Uuid } } end";
		if (!outputs) return compile(aid, "interface_mqtt", "actions", "Calibrate", "Calibrate", operation -> operation.add(
				"inputVariables", json("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Uuid\",\"valueType\":\"xs:string\"}}]")),
				request, null);
		return compile(aid, "interface_mqtt", "actions", "Calibrate", "Calibrate", operation -> {
			operation.add("inoutputVariables", json("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Uuid\",\"valueType\":\"xs:string\"}}]"));
			operation.add("outputVariables", json("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:string\"}}]"));
		}, request, "function aimc_main(sources) local reply = sources.Reply "
						+ "return { Calibrate = { Uuid = reply.Uuid, State = reply.State } } end");
	}

	/** {@code responseLua}, when given, answers the Operation (SinkId {@code sinkId}) from the action's output Reply. */
	/** The stoppering Halt action (no output, no response topic) as Calibrate, bound to an Operation with only an input. */
	private RoutePlan compileInvokeOnlyMqttAction(Mqtt5TestBroker broker) throws Exception {
		JsonObject halt = child(child(child(child(json(resource("stoppering/aid.json")).getAsJsonObject(), "interface_mqtt"),
				"InteractionMetadata"), "actions"), "Halt").deepCopy();
		halt.addProperty("idShort", "Calibrate");
		child(halt, "title").addProperty("value", "Calibrate");
		child(child(halt, "forms"), "href").addProperty("value", "CMD/Calibrate");
		JsonObject aid = json(resource("fan-in-out/aid.json")).getAsJsonObject();
		JsonObject mqtt = child(aid, "interface_mqtt");
		child(child(mqtt, "EndpointMetadata"), "base").addProperty("value", "mqtt://127.0.0.1:" + broker.port() + "/base/");
		JsonObject actions = child(child(mqtt, "InteractionMetadata"), "actions");
		if (!actions.has("value")) actions.add("value", new JsonArray());
		actions.getAsJsonArray("value").add(halt);
		return compile(aid, "interface_mqtt", "actions", "Calibrate", "Calibrate", operation -> operation.add("inputVariables",
				json("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Uuid\",\"valueType\":\"xs:string\"}}]")),
				"function aimc_main(sources) return { Calibrate = { Uuid = sources.Calibrate.Uuid } } end", null);
	}

	private RoutePlan compile(JsonObject aid, String interfaceId, String container, String affordance, String sinkId,
			Consumer<JsonObject> operation, String lua, String responseLua) throws Exception {
		JsonObject cci = json(resource("opcua-method/dependencies/0.json")).getAsJsonObject();
		operation.accept(child(child(child(cci, "Skills"), "Calibrate"), "operation"));
		JsonObject aimc = json(resource("opcua-method/aimc.json")).getAsJsonObject();
		JsonObject mapping = child(aimc, "MappingConfigurations").getAsJsonArray("value").get(0).getAsJsonObject();
		child(mapping, "Transformation").addProperty("value", Base64.getEncoder().encodeToString(lua.getBytes(StandardCharsets.UTF_8)));
		JsonObject sink = child(mapping, "Sinks").getAsJsonArray("value").get(0).getAsJsonObject();
		JsonArray keys = child(sink, "Sink").getAsJsonObject("value").getAsJsonArray("keys");
		keys.get(1).getAsJsonObject().addProperty("value", interfaceId);
		keys.get(3).getAsJsonObject().addProperty("value", container);
		keys.get(4).getAsJsonObject().addProperty("value", affordance);
		child(sink, "SinkId").addProperty("value", sinkId);
		if (responseLua != null) ResponseMappings.respond(mapping, "Reply", sinkId, responseLua);

		if (repository != null) repository.close();
		repository = new FakeAasRepository();
		String publishedAimc = PublishedModels.publish(aimc.toString(), "contractAAS", "contract");
		repository.put(publishedAimc);
		for (JsonObject model : List.of(aid, cci)) repository.put(PublishedModels.publish(model.toString(), "contractAAS", "contract"));
		var plan = new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compile(WireProjection.aimc(
				dk.aau.smartproductionlab.dmp.aas.model.Aas.read(publishedAimc, org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
		assertEquals(AasRepositoryClient.Status.FOUND, plan.status(), String.valueOf(plan));
		var compiled = new ConnectorConfigurationFactory().createBundles(plan.value(),
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://127.0.0.1:1")));
		assertTrue(compiled.issues().isEmpty(), String.valueOf(compiled.issues()));
		return compiled.bundles().get(0).route();
	}

	private static JsonObject child(JsonObject parent, String name) {
		for (var entry : parent.getAsJsonArray(parent.has("submodelElements") ? "submodelElements" : "value")) {
			JsonObject child = entry.getAsJsonObject();
			if (child.has("idShort") && name.equals(child.get("idShort").getAsString())) return child;
		}
		throw new IllegalStateException("missing fixture element " + name);
	}

	private static String resource(String path) throws Exception {
		try (var input = OperationConnectorsTest.class.getClassLoader().getResourceAsStream("runtime-model-contracts/" + path)) {
			assertNotNull(input, path);
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static JsonElement json(String value) { return JsonParser.parseString(value); }

	private static int freePort() throws Exception {
		for (int attempt = 0; attempt < 200; attempt++) {
			int candidate = 20000 + java.util.concurrent.ThreadLocalRandom.current().nextInt(10000);
			try (ServerSocket socket = new ServerSocket(candidate, 1, java.net.InetAddress.getLoopbackAddress())) {
				return candidate;
			} catch (java.io.IOException taken) {
				// try another
			}
		}
		throw new IllegalStateException("no free port below the ephemeral range");
	}
}
