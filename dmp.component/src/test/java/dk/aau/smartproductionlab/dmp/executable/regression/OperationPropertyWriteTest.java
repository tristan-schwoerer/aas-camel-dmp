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
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.eclipse.milo.opcua.sdk.core.AccessLevel;
import org.eclipse.milo.opcua.sdk.server.AddressSpaceFilter;
import org.eclipse.milo.opcua.sdk.server.EndpointConfig;
import org.eclipse.milo.opcua.sdk.server.ManagedAddressSpaceFragmentWithLifecycle;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.OpcUaServerConfig;
import org.eclipse.milo.opcua.sdk.server.SimpleAddressSpaceFilter;
import org.eclipse.milo.opcua.sdk.server.items.DataItem;
import org.eclipse.milo.opcua.sdk.server.items.MonitoredItem;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.sdk.server.util.SubscriptionModel;
import org.eclipse.milo.opcua.stack.core.NodeIds;
import org.eclipse.milo.opcua.stack.core.security.DefaultCertificateManager;
import org.eclipse.milo.opcua.stack.core.security.MemoryCertificateQuarantine;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.transport.server.tcp.OpcTcpServerTransport;
import org.eclipse.milo.opcua.stack.transport.server.tcp.OpcTcpServerTransportConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;

/**
 * An AAS Operation bound to a property affordance (the opcua-method fixture with its
 * Sink moved from the action to a writable OPC UA variable): the call writes the
 * variable and returns once the server accepted the Write. On the AAS side the mapping
 * fans in a further AAS property (read at invocation) and fans the written value out
 * to an AAS property (ResponseSinks).
 */
class OperationPropertyWriteTest {
	private static final String REQUEST_LUA =
			// A fanned-in AAS Property arrives as its value, typed by its valueType (SPEC §5.3).
			"function aimc_main(sources) return { Setpoint = sources.Calibrate.Setpoint + sources.Offset } end";
	private static final String RESPONSE_LUA =
			"function aimc_main(sources) return { Calibrate = {}, LastSetpoint = sources.Written } end";

	private OpcUaServer server;
	private Device device;
	private Runtime runtime;
	private FakeAasRepository repository;
	private HttpServer aas;
	private final List<String> writes = new CopyOnWriteArrayList<>();
	private final AtomicInteger offsetReads = new AtomicInteger();
	private int devicePort;
	private String operationUri;

	@BeforeEach
	void start() throws Exception {
		devicePort = freePort();
		var endpoint = EndpointConfig.newBuilder().setBindAddress("127.0.0.1").setBindPort(devicePort)
				.setHostname("127.0.0.1").setPath("").setSecurityPolicy(SecurityPolicy.None)
				.setSecurityMode(MessageSecurityMode.None).addTokenPolicy(OpcUaServerConfig.USER_TOKEN_POLICY_ANONYMOUS).build();
		server = new OpcUaServer(OpcUaServerConfig.builder().setEndpoints(Set.of(endpoint))
				.setApplicationName(LocalizedText.english("setpoint device"))
				.setApplicationUri("urn:dmp:setpoint:server").setProductUri("urn:dmp:setpoint")
				.setCertificateManager(new DefaultCertificateManager(new MemoryCertificateQuarantine())).build(),
				profile -> new OpcTcpServerTransport(OpcTcpServerTransportConfig.newBuilder().build()));
		device = new Device(server);
		device.startup();
		server.startup().get(10, TimeUnit.SECONDS);
		// The AAS repository at run time: serves the fan-in property and records the response writes.
		aas = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		aas.createContext("/", exchange -> {
			if ("GET".equals(exchange.getRequestMethod())) {
				offsetReads.incrementAndGet();
				byte[] body = (exchange.getRequestURI().getPath().endsWith("$value") ? "5"
						: "{\"modelType\":\"Property\",\"idShort\":\"Offset\",\"valueType\":\"xs:int\",\"value\":\"5\"}")
						.getBytes(StandardCharsets.UTF_8);
				exchange.getResponseHeaders().set("Content-Type", "application/json");
				exchange.sendResponseHeaders(200, body.length);
				exchange.getResponseBody().write(body);
			} else {
				writes.add(exchange.getRequestURI().getPath() + " " + new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
				exchange.sendResponseHeaders(204, -1);
			}
			exchange.close();
		});
		aas.start();
		runtime = new Runtime();
		runtime.startComponent();
		int port = freePort();
		runtime.listener(port);
		operationUri = "http://127.0.0.1:" + port + OperationCalls.path("a/submodels/ControlComponentInstance", "Calibrate");
	}

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

	@AfterEach
	void stop() throws Exception {
		try { runtime.stopComponent(); } finally {
			try { device.shutdown(); } finally {
				server.shutdown().get(10, TimeUnit.SECONDS);
				aas.stop(0);
				if (repository != null) repository.close();
			}
		}
	}

	@Test
	void theCallWritesTheVariableWithFannedInValuesAndFansTheWrittenValueOut() throws Exception {
		runtime.installBundle("setpoint", compile());
		var reply = call("{\"Setpoint\":42}");
		assertEquals(200, reply.statusCode(), reply.body());
		assertEquals(47, device.setpoint(), "Setpoint + the fanned-in Offset, written before the reply");
		assertTrue(offsetReads.get() >= 1, "the fan-in property is read at invocation");
		await().atMost(Duration.ofSeconds(5)).until(() -> writes.stream()
				.anyMatch(write -> write.contains("LastSetpoint") && write.contains("47")));
	}

	@Test
	void aVariableNamedByItsAidBrowsePathIsWritten() throws Exception {
		// AID 1.2: uav_browsePath is an absolute path from the Root node; the href names no node.
		runtime.installBundle("setpoint", compile("/Root/Objects/1:Setpoint"));
		var reply = call("{\"Setpoint\":42}");
		assertEquals(200, reply.statusCode(), reply.body());
		assertEquals(47, device.setpoint());
	}

	@Test
	void aRejectedWriteFailsTheCall() throws Exception {
		runtime.installBundle("setpoint", compile());
		device.readOnly();
		var reply = call("{\"Setpoint\":42}");
		assertEquals(502, reply.statusCode(), reply.body());
		assertEquals("DMP_DEVICE_ERROR", OperationCalls.error(reply.body()).get("code"));
		assertTrue(writes.isEmpty(), "nothing is fanned out after a failed write: " + writes);
	}

	private HttpResponse<String> call(String body) throws Exception {
		return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build().send(
				HttpRequest.newBuilder(URI.create(operationUri)).timeout(Duration.ofSeconds(10))
						.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(OperationCalls.of(body))).build(),
				HttpResponse.BodyHandlers.ofString());
	}

	/** One writable Int32 variable, Setpoint. */
	private static final class Device extends ManagedAddressSpaceFragmentWithLifecycle {
		private final UaVariableNode setpoint;
		private final SubscriptionModel subscriptions;

		Device(OpcUaServer server) {
			super(server);
			subscriptions = new SubscriptionModel(server, this);
			getLifecycleManager().addLifecycle(subscriptions);
			server.getNamespaceTable().add("urn:dmp:setpoint:device");
			setpoint = UaVariableNode.builder(getNodeContext()).setNodeId(new NodeId(1, "Setpoint"))
					.setBrowseName(new QualifiedName(1, "Setpoint")).setDisplayName(LocalizedText.english("Setpoint"))
					.setDataType(NodeIds.Int32).setTypeDefinition(NodeIds.BaseDataVariableType)
					.setAccessLevel(AccessLevel.READ_WRITE).setUserAccessLevel(AccessLevel.READ_WRITE).buildAndAdd();
			setpoint.setValue(new DataValue(new Variant(0), StatusCode.GOOD, DateTime.now()));
			// Browsable from the Root node: /Root/Objects/1:Setpoint.
			setpoint.addReference(new org.eclipse.milo.opcua.sdk.core.Reference(setpoint.getNodeId(), NodeIds.Organizes,
					NodeIds.ObjectsFolder.expanded(), false));
		}

		int setpoint() { return ((Number) setpoint.getValue().getValue().getValue()).intValue(); }

		void readOnly() {
			setpoint.setAccessLevel(AccessLevel.toValue(AccessLevel.READ_ONLY));
			setpoint.setUserAccessLevel(AccessLevel.toValue(AccessLevel.READ_ONLY));
		}

		@Override public AddressSpaceFilter getFilter() {
			return SimpleAddressSpaceFilter.create(id -> id.getNamespaceIndex().intValue() == 1);
		}
		@Override public void onDataItemsCreated(List<DataItem> items) { subscriptions.onDataItemsCreated(items); }
		@Override public void onDataItemsModified(List<DataItem> items) { subscriptions.onDataItemsModified(items); }
		@Override public void onDataItemsDeleted(List<DataItem> items) { subscriptions.onDataItemsDeleted(items); }
		@Override public void onMonitoringModeChanged(List<MonitoredItem> items) { subscriptions.onMonitoringModeChanged(items); }
	}

	// ---- models: the opcua-method fixture, its Operation bound to a Setpoint variable ----

	private RoutePlan compile() throws Exception {
		return compile(null);
	}

	private RoutePlan compile(String browsePath) throws Exception {
		JsonObject aid = json(resource("opcua-method/aid.json")).getAsJsonObject();
		JsonObject opcua = child(aid, "interface_opcua");
		child(child(opcua, "EndpointMetadata"), "base").addProperty("value", "opc.tcp://127.0.0.1:" + devicePort);
		JsonObject readProperties = child(child(child(json(resource("opcua-read/aid.json")).getAsJsonObject(), "interface_opcua"),
				"InteractionMetadata"), "properties");
		JsonObject pressure = child(readProperties, "Pressure");
		// The Method interface has no properties yet: give it the container, empty.
		JsonObject interaction = child(opcua, "InteractionMetadata");
		if (interaction.getAsJsonArray("value").asList().stream().noneMatch(e -> "properties".equals(e.getAsJsonObject().get("idShort").getAsString()))) {
			JsonObject container = readProperties.deepCopy();
			container.add("value", new JsonArray());
			interaction.getAsJsonArray("value").add(container);
		}
		JsonObject properties = child(interaction, "properties");
		JsonObject variable = pressure.deepCopy();
		variable.addProperty("idShort", "Setpoint");
		child(variable, "title").addProperty("value", "Setpoint");
		child(child(variable, "forms"), "href").addProperty("value", browsePath == null ? "/?id=ns=1;s=Setpoint" : "/");
		if (browsePath != null) child(variable, "forms").getAsJsonArray("value").add(json(
				"{\"modelType\":\"Property\",\"idShort\":\"uav_browsePath\",\"valueType\":\"xs:string\",\"value\":\"" + browsePath + "\"}"));
		properties.getAsJsonArray("value").add(variable);

		JsonObject cci = json(resource("opcua-method/dependencies/0.json")).getAsJsonObject();
		child(child(child(cci, "Skills"), "Calibrate"), "operation").add("inputVariables",
				json("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Setpoint\",\"valueType\":\"xs:int\"}}]"));
		cci.getAsJsonArray("submodelElements").add(json(
				"{\"modelType\":\"Property\",\"idShort\":\"Offset\",\"valueType\":\"xs:int\",\"value\":\"5\"}"));
		cci.getAsJsonArray("submodelElements").add(json(
				"{\"modelType\":\"Property\",\"idShort\":\"LastSetpoint\",\"valueType\":\"xs:int\"}"));

		JsonObject aimc = json(resource("opcua-method/aimc.json")).getAsJsonObject();
		JsonObject mapping = child(aimc, "MappingConfigurations").getAsJsonArray("value").get(0).getAsJsonObject();
		child(mapping, "Transformation").addProperty("value", base64(REQUEST_LUA));
		JsonObject sink = child(mapping, "Sinks").getAsJsonArray("value").get(0).getAsJsonObject();
		JsonArray sinkKeys = child(sink, "Sink").getAsJsonObject("value").getAsJsonArray("keys");
		sinkKeys.get(3).getAsJsonObject().addProperty("value", "properties");
		sinkKeys.get(4).getAsJsonObject().addProperty("value", "Setpoint");
		child(sink, "SinkId").addProperty("value", "Setpoint");
		child(mapping, "Sources").getAsJsonArray("value").add(endpoint("Source", "Offset",
				key("Submodel", "a/submodels/ControlComponentInstance"), key("Property", "Offset")));
		mapping.getAsJsonArray("value").add(list("ResponseSources", endpoint("Source", "Written",
				key("Submodel", "a/submodels/AID"), key("SubmodelElementCollection", "interface_opcua"),
				key("SubmodelElementCollection", "InteractionMetadata"), key("SubmodelElementCollection", "properties"),
				key("SubmodelElementCollection", "Setpoint"))));
		mapping.getAsJsonArray("value").add(json("{\"modelType\":\"Blob\",\"idShort\":\"ResponseTransformation\","
				+ "\"contentType\":\"text/plain\",\"value\":\"" + base64(RESPONSE_LUA) + "\"}"));
		mapping.getAsJsonArray("value").add(list("ResponseSinks", endpoint("Sink", "LastSetpoint",
				key("Submodel", "a/submodels/ControlComponentInstance"), key("Property", "LastSetpoint"))));

		if (repository != null) repository.close();
		repository = new FakeAasRepository();
		String publishedAimc = PublishedModels.publish(aimc.toString(), "contractAAS", "contract");
		repository.put(publishedAimc);
		for (JsonObject model : List.of(aid, cci)) repository.put(PublishedModels.publish(model.toString(), "contractAAS", "contract"));
		var plan = new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compile(WireProjection.aimc(
				dk.aau.smartproductionlab.dmp.aas.model.Aas.read(publishedAimc, org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
		assertEquals(AasRepositoryClient.Status.FOUND, plan.status(), String.valueOf(plan));
		var compiled = new ConnectorConfigurationFactory().createBundles(plan.value(),
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://127.0.0.1:" + aas.getAddress().getPort())));
		assertTrue(compiled.issues().isEmpty(), String.valueOf(compiled.issues()));
		return compiled.bundles().get(0).route();
	}

	private static JsonObject key(String type, String value) {
		JsonObject key = new JsonObject();
		key.addProperty("type", type);
		key.addProperty("value", value);
		return key;
	}

	private static JsonObject endpoint(String role, String id, JsonObject... keys) {
		JsonArray typed = new JsonArray();
		for (JsonObject key : keys) typed.add(key);
		JsonObject reference = new JsonObject();
		reference.addProperty("type", "ModelReference");
		reference.add("keys", typed);
		JsonObject element = json("{\"modelType\":\"ReferenceElement\",\"idShort\":\"" + role + "\"}").getAsJsonObject();
		element.add("value", reference);
		JsonObject endpoint = json("{\"modelType\":\"SubmodelElementCollection\",\"value\":[]}").getAsJsonObject();
		endpoint.getAsJsonArray("value").add(element);
		endpoint.getAsJsonArray("value").add(json("{\"modelType\":\"Property\",\"idShort\":\"" + role
				+ "Id\",\"valueType\":\"xs:string\",\"value\":\"" + id + "\"}"));
		return endpoint;
	}

	private static JsonObject list(String idShort, JsonObject member) {
		JsonObject list = json("{\"modelType\":\"SubmodelElementList\",\"idShort\":\"" + idShort
				+ "\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":[]}").getAsJsonObject();
		list.getAsJsonArray("value").add(member);
		return list;
	}

	private static JsonObject child(JsonObject parent, String name) {
		for (var entry : parent.getAsJsonArray(parent.has("submodelElements") ? "submodelElements" : "value")) {
			JsonObject child = entry.getAsJsonObject();
			if (child.has("idShort") && name.equals(child.get("idShort").getAsString())) return child;
		}
		throw new IllegalStateException("missing fixture element " + name);
	}

	private static String base64(String text) {
		return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
	}

	private static String resource(String path) throws Exception {
		try (var input = OperationPropertyWriteTest.class.getClassLoader().getResourceAsStream("runtime-model-contracts/" + path)) {
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
