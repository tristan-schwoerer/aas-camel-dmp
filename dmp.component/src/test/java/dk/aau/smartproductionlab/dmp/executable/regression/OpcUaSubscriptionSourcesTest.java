package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
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
import org.eclipse.milo.opcua.sdk.server.model.objects.TransitionEventTypeNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.sdk.server.util.SubscriptionModel;
import org.eclipse.milo.opcua.stack.core.NodeIds;
import org.eclipse.milo.opcua.stack.core.security.DefaultCertificateManager;
import org.eclipse.milo.opcua.stack.core.security.MemoryCertificateQuarantine;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
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
 * OPC UA subscriptions as ordinary mapping Sources (the opcua-read fixture,
 * Pressure → KPI/Out): an observed variable (OPC 10101 observeproperty, a
 * monitored item) and the events of a notifier (a DMP extension: an event
 * affordance filtered to ProgramTransitionEventType). Each change or event
 * runs the mapping and writes the AAS property.
 */
class OpcUaSubscriptionSourcesTest {
	private OpcUaServer server;
	private Device device;
	private DmpComponent runtime;
	private FakeAasRepository repository;
	private HttpServer aas;
	private final List<String> writes = new CopyOnWriteArrayList<>();
	private int devicePort;
	private static final String USER = "dmp";
	/** URI delimiters that broke the user info form (SPEC §11). */
	private static final String PASSWORD = "p@ss&w#rd:1";
	private final List<String> authenticated = new CopyOnWriteArrayList<>();

	@BeforeEach
	void start() throws Exception {
		devicePort = freePort();
		var endpoint = EndpointConfig.newBuilder().setBindAddress("127.0.0.1").setBindPort(devicePort)
				.setHostname("127.0.0.1").setPath("").setSecurityPolicy(SecurityPolicy.None)
				.setSecurityMode(MessageSecurityMode.None).addTokenPolicy(OpcUaServerConfig.USER_TOKEN_POLICY_ANONYMOUS)
				// A plain-text password policy: the test server has no certificate to encrypt one with.
				.addTokenPolicy(new org.eclipse.milo.opcua.stack.core.types.structured.UserTokenPolicy("username",
						org.eclipse.milo.opcua.stack.core.types.enumerated.UserTokenType.UserName, null, null,
						SecurityPolicy.None.getUri())).build();
		var users = new org.eclipse.milo.opcua.sdk.server.identity.UsernameIdentityValidator(challenge -> {
			boolean valid = USER.equals(challenge.getUsername()) && PASSWORD.equals(challenge.getPassword());
			if (valid) authenticated.add(challenge.getUsername());
			return valid;
		});
		server = new OpcUaServer(OpcUaServerConfig.builder().setEndpoints(Set.of(endpoint))
				.setIdentityValidator(new org.eclipse.milo.opcua.sdk.server.identity.CompositeValidator(
						org.eclipse.milo.opcua.sdk.server.identity.AnonymousIdentityValidator.INSTANCE, users))
				.setApplicationName(LocalizedText.english("subscription device"))
				.setApplicationUri("urn:dmp:subscription:server").setProductUri("urn:dmp:subscription")
				.setCertificateManager(new DefaultCertificateManager(new MemoryCertificateQuarantine())).build(),
				profile -> new OpcTcpServerTransport(OpcTcpServerTransportConfig.newBuilder().build()));
		device = new Device(server);
		device.startup();
		server.startup().get(10, TimeUnit.SECONDS);
		// The AAS repository the mapping writes to: records each write's body.
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
			try { device.shutdown(); } finally {
				server.shutdown().get(10, TimeUnit.SECONDS);
				aas.stop(0);
				if (repository != null) repository.close();
			}
		}
	}

	@Test
	void anObservedVariableWritesEachChange() throws Exception {
		runtime.installBundle("observe", compile(false, "function aimc_main(sources) return { Out = sources.Pressure } end"));
		// A monitored item reports the current value first, then each change.
		await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("41")));
		device.level(42);
		await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("42")));
	}

	@Test
	void mappingsObservingOneVariableEachGetEveryChange() throws Exception {
		List<RoutesConfiguration> bundles = compile(false, "function aimc_main(sources) return { Out = sources.Pressure + 1000 } end",
				"function aimc_main(sources) return { Out = sources.Pressure + 2000 } end");
		assertEquals(2, bundles.size());
		var first = runtime.installBundle("first", bundles.get(0));
		runtime.installBundle("second", bundles.get(1));
		await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("1041"))
				&& writes.stream().anyMatch(body -> body.contains("2041")));
		device.level(42);
		await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("1042"))
				&& writes.stream().anyMatch(body -> body.contains("2042")));
		// Removing one mapping leaves the other's subscription in place.
		runtime.removeBundle(first);
		device.level(43);
		await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("2043")));
		Thread.sleep(500);
		assertTrue(writes.stream().noneMatch(body -> body.contains("1043")), String.valueOf(writes));
	}

	@Test
	void filteredEventsWriteTheirSelectedFields() throws Exception {
		runtime.installBundle("events", compile(true,
				"function aimc_main(sources) return { Out = sources.Pressure.ToState .. \":\" .. sources.Pressure.Message } end"));
		// A subscription only sees events fired after it exists: fire until the first one arrives.
		await().atMost(Duration.ofSeconds(8)).pollInterval(Duration.ofMillis(200)).until(() -> {
			device.transition(NodeIds.ProgramTransitionEventType, "Halted", "calibration done");
			return writes.stream().anyMatch(body -> body.contains("Halted:calibration done"));
		});
		// A TransitionEventType event is not a ProgramTransitionEventType: the where clause drops it.
		device.transition(NodeIds.TransitionEventType, "Faulted", "not a program transition");
		device.transition(NodeIds.ProgramTransitionEventType, "Faulted", "calibration faulted");
		await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("Faulted:calibration faulted")));
		assertTrue(writes.stream().noneMatch(body -> body.contains("not a program transition")), String.valueOf(writes));
	}

	@Test
	void aPolledVariableWrittenToAnotherVariableIsWrittenNotRead() throws Exception {
		// The poll's Read leaves its node list on the source's message; the evaluation must not hand it to the
		// sink, whose producer would then Read the polled node instead of writing (and report success).
		runtime.installBundle("copy", compileWrite("function aimc_main(sources) return { Out = sources.Pressure + 1 } end"));
		await().atMost(Duration.ofSeconds(8)).until(() -> Integer.valueOf(42).equals(device.setpoint()));
		device.level(50);
		await().atMost(Duration.ofSeconds(8)).until(() -> Integer.valueOf(51).equals(device.setpoint()));
	}

	@Test
	void aSubscriptionThatCannotBeSetUpIsDeletedNotLeaked() throws Exception {
		// The path names no node: each attempt creates a subscription, then fails resolving the node.
		RoutesConfiguration bundle = compile(false, "function aimc_main(sources) return { Out = sources.Pressure } end");
		for (var source : bundle.getDatasources().values())
			if (source instanceof dk.aau.smartproductionlab.dmp.opcua.configuration.OpcuaDataSourceConfiguration opcua) {
				opcua.setNodeInformation("/Root/Objects/1:Missing(1)");
			}
		runtime.installBundle("missing", bundle);
		Thread.sleep(3_000);
		assertTrue(server.getSubscriptions().size() <= 1, "subscriptions left on the server: " + server.getSubscriptions().size());
	}

	/**
	 * The connector's user path, which opcua_authentication_sc (UserName) will feed from the DMP's credentials
	 * (PLAN H4): the user is bound, not written into the URI, and reaches the server with its password intact.
	 */
	@Test
	void aSessionAuthenticatesAsTheBoundUserWithItsPasswordIntact() throws Exception {
		RoutesConfiguration bundle = compileWrite("function aimc_main(sources) return { Out = sources.Pressure + 1 } end");
		for (var source : bundle.getDatasources().values())
			if (source instanceof dk.aau.smartproductionlab.dmp.opcua.configuration.OpcuaDataSourceConfiguration opcua) {
				opcua.setUsername(USER);
				opcua.setPassword(PASSWORD);
				assertTrue(!opcua.getConnectionURI().contains("p@ss"), opcua.getConnectionURI());
			}
		for (var sink : bundle.getDatasinks().values())
			if (sink instanceof dk.aau.smartproductionlab.dmp.opcua.configuration.OpcuaDataSinkConfiguration opcua) {
				opcua.setUsername(USER);
				opcua.setPassword(PASSWORD);
			}
		runtime.installBundle("user", bundle);
		await().atMost(Duration.ofSeconds(8)).until(() -> Integer.valueOf(42).equals(device.setpoint()));
		assertTrue(authenticated.contains(USER), "the session is the user's, not anonymous: " + authenticated);
	}

	/** One variable (Level), a writable one (Setpoint) and the Server object's transition events. */
	private static final class Device extends ManagedAddressSpaceFragmentWithLifecycle {
		private final OpcUaServer server;
		private final UaVariableNode level;
		private final UaVariableNode setpoint;
		private final SubscriptionModel subscriptions;

		Device(OpcUaServer server) {
			super(server);
			this.server = server;
			subscriptions = new SubscriptionModel(server, this);
			getLifecycleManager().addLifecycle(subscriptions);
			server.getNamespaceTable().add("urn:dmp:subscription:device");
			level = UaVariableNode.builder(getNodeContext()).setNodeId(new NodeId(1, "Level"))
					.setBrowseName(new QualifiedName(1, "Level")).setDisplayName(LocalizedText.english("Level"))
					.setDataType(NodeIds.Int32).setTypeDefinition(NodeIds.BaseDataVariableType)
					.setAccessLevel(AccessLevel.CurrentRead).setUserAccessLevel(AccessLevel.CurrentRead).buildAndAdd();
			level(41);
			setpoint = UaVariableNode.builder(getNodeContext()).setNodeId(new NodeId(1, "Setpoint"))
					.setBrowseName(new QualifiedName(1, "Setpoint")).setDisplayName(LocalizedText.english("Setpoint"))
					.setDataType(NodeIds.Int32).setTypeDefinition(NodeIds.BaseDataVariableType)
					.setAccessLevel(AccessLevel.READ_WRITE).setUserAccessLevel(AccessLevel.READ_WRITE).buildAndAdd();
			setpoint.setValue(new DataValue(new Variant(0), StatusCode.GOOD, DateTime.now()));
		}

		Object setpoint() {
			return setpoint.getValue().getValue().getValue();
		}

		void level(int value) {
			level.setValue(new DataValue(new Variant(value), StatusCode.GOOD, DateTime.now()));
		}

		/** Fires a transition event of {@code type} on the Server object. */
		void transition(NodeId type, String toState, String message) {
			try {
				var event = (TransitionEventTypeNode) server.getEventFactory().createEvent(
						new NodeId(1, java.util.UUID.randomUUID()), type);
				event.setBrowseName(new QualifiedName(1, "Transition"));
				event.setDisplayName(LocalizedText.english("Transition"));
				event.setEventId(ByteString.of(java.util.UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
				event.setEventType(type);
				event.setSourceNode(NodeIds.Server);
				event.setSourceName("Server");
				event.setTime(DateTime.now());
				event.setReceiveTime(DateTime.NULL_VALUE);
				event.setMessage(LocalizedText.english(message));
				event.setSeverity(Unsigned.ushort(500));
				event.setToState(LocalizedText.english(toState));
				server.getEventNotifier().fire(event);
				event.delete();
			} catch (org.eclipse.milo.opcua.stack.core.UaException failure) {
				throw new IllegalStateException(failure);
			}
		}

		@Override public AddressSpaceFilter getFilter() {
			return SimpleAddressSpaceFilter.create(id -> id.getNamespaceIndex().intValue() == 1);
		}
		@Override public void onDataItemsCreated(List<DataItem> items) { subscriptions.onDataItemsCreated(items); }
		@Override public void onDataItemsModified(List<DataItem> items) { subscriptions.onDataItemsModified(items); }
		@Override public void onDataItemsDeleted(List<DataItem> items) { subscriptions.onDataItemsDeleted(items); }
		@Override public void onMonitoringModeChanged(List<MonitoredItem> items) { subscriptions.onMonitoringModeChanged(items); }
	}

	// ---- models: the opcua-read fixture with Pressure observed, or replaced by an event affordance ----

	/** The opcua-read fixture with Pressure read every 50 ms (not observed) and written to the Setpoint variable. */
	private RoutesConfiguration compileWrite(String lua) throws Exception {
		JsonObject aid = json(resource("opcua-read/aid.json")).getAsJsonObject();
		JsonObject opcua = child(aid, "interface_opcua");
		child(child(opcua, "EndpointMetadata"), "base").addProperty("value", "opc.tcp://127.0.0.1:" + devicePort);
		JsonObject properties = child(child(opcua, "InteractionMetadata"), "properties");
		JsonObject pressure = child(properties, "Pressure");
		child(child(pressure, "forms"), "href").addProperty("value", "/?id=ns=1;s=Level");
		JsonObject setpoint = pressure.deepCopy();
		setpoint.addProperty("idShort", "Setpoint");
		child(setpoint, "title").addProperty("value", "Setpoint");
		child(child(setpoint, "forms"), "href").addProperty("value", "/?id=ns=1;s=Setpoint");
		properties.getAsJsonArray("value").add(setpoint);
		JsonObject aimc = json(resource("opcua-read/aimc.json")).getAsJsonObject();
		JsonObject mapping = child(aimc, "MappingConfigurations").getAsJsonArray("value").get(0).getAsJsonObject();
		JsonObject source = child(mapping, "Sources").getAsJsonArray("value").get(0).getAsJsonObject();
		source.getAsJsonArray("value").add(json("{\"modelType\":\"Property\",\"idShort\":\"PollingInterval\","
				+ "\"valueType\":\"xs:double\",\"value\":\"0.05\"}"));
		JsonArray sinkKeys = child(source, "Source").getAsJsonObject("value").getAsJsonArray("keys").deepCopy();
		sinkKeys.get(4).getAsJsonObject().addProperty("value", "Setpoint");
		JsonObject sink = child(mapping, "Sinks").getAsJsonArray("value").get(0).getAsJsonObject();
		child(sink, "Sink").getAsJsonObject("value").add("keys", sinkKeys);
		child(mapping, "Transformation").addProperty("value",
				Base64.getEncoder().encodeToString(lua.getBytes(StandardCharsets.UTF_8)));
		return bundles(aid, aimc).get(0);
	}

	private RoutesConfiguration compile(boolean events, String lua) throws Exception {
		return compile(events, new String[] { lua }).get(0);
	}

	/** One AIMC with a mapping per Lua script, all on the same source; one bundle each. */
	private List<RoutesConfiguration> compile(boolean events, String... luas) throws Exception {
		JsonObject aid = json(resource("opcua-read/aid.json")).getAsJsonObject();
		JsonObject opcua = child(aid, "interface_opcua");
		child(child(opcua, "EndpointMetadata"), "base").addProperty("value", "opc.tcp://127.0.0.1:" + devicePort);
		JsonObject interaction = child(opcua, "InteractionMetadata");
		JsonObject pressure = child(child(interaction, "properties"), "Pressure");
		JsonObject forms = child(pressure, "forms");
		String sourcePath;
		if (events) {
			interaction.getAsJsonArray("value").add(transitionEvents(pressure));
			sourcePath = "events";
		} else {
			// An observable variable is a monitored item (OPC 10101 observeproperty).
			child(forms, "href").addProperty("value", "/?id=ns=1;s=Level");
			pressure.getAsJsonArray("value").add(json("{\"modelType\":\"Property\",\"idShort\":\"observable\","
					+ "\"valueType\":\"xs:boolean\",\"value\":\"true\"}"));
			sourcePath = "properties";
		}
		JsonObject aimc = json(resource("opcua-read/aimc.json")).getAsJsonObject();
		JsonArray mappings = child(aimc, "MappingConfigurations").getAsJsonArray("value");
		JsonObject mapping = mappings.get(0).getAsJsonObject();
		JsonObject source = child(mapping, "Sources").getAsJsonArray("value").get(0).getAsJsonObject();
		JsonArray keys = child(source, "Source").getAsJsonObject("value").getAsJsonArray("keys");
		keys.get(3).getAsJsonObject().addProperty("value", sourcePath);
		keys.get(4).getAsJsonObject().addProperty("value", events ? "Transition" : "Pressure");
		if (!events) source.getAsJsonArray("value").add(json("{\"modelType\":\"Property\",\"idShort\":\"PollingInterval\","
				+ "\"valueType\":\"xs:double\",\"value\":\"0.05\"}"));
		while (mappings.size() > 0) mappings.remove(0);
		for (String lua : luas) {
			JsonObject copy = mapping.deepCopy();
			child(copy, "Transformation").addProperty("value",
					Base64.getEncoder().encodeToString(lua.getBytes(StandardCharsets.UTF_8)));
			mappings.add(copy);
		}
		return bundles(aid, aimc);
	}

	/** Publishes the models to a fake repository and compiles the AIMC's bundles. */
	private List<RoutesConfiguration> bundles(JsonObject aid, JsonObject aimc) throws Exception {
		if (repository != null) repository.close();
		repository = new FakeAasRepository();
		String publishedAimc = PublishedModels.publish(aimc.toString(), "contractAAS", "contract");
		repository.put(publishedAimc);
		for (String model : List.of(aid.toString(), resource("opcua-read/dependencies/0.json")))
			repository.put(PublishedModels.publish(model, "contractAAS", "contract"));
		var plan = new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compile(WireProjection.aimc(
				dk.aau.smartproductionlab.dmp.aas.model.Aas.read(publishedAimc,
						org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
		assertEquals(AasRepositoryClient.Status.FOUND, plan.status(), String.valueOf(plan));
		var compiled = new ConnectorConfigurationFactory().createBundles(plan.value(),
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://127.0.0.1:" + aas.getAddress().getPort())));
		assertTrue(compiled.issues().isEmpty(), String.valueOf(compiled.issues()));
		return compiled.bundles().stream().map(bundle -> bundle.configuration()).toList();
	}

	/**
	 * The Server object's transitions as an event affordance (aas-model OpcuaEvent): href = the
	 * notifier, uav_eventType = the where clause, data properties = the select clauses (ToState by
	 * browse path, Message by its namespace-0 name).
	 */
	private static JsonObject transitionEvents(JsonObject template) {
		JsonObject event = template.deepCopy();
		event.addProperty("idShort", "Transition");
		event.add("semanticId", semantic("https://www.w3.org/2019/wot/td#EventAffordance"));
		event.getAsJsonArray("value").asList().removeIf(element ->
				!List.of("title", "forms").contains(element.getAsJsonObject().get("idShort").getAsString()));
		child(event, "title").addProperty("value", "Transition");
		JsonObject forms = child(event, "forms");
		child(forms, "href").addProperty("value", "/?id=i=2253");
		JsonObject eventType = json("{\"modelType\":\"Property\",\"idShort\":\"uav_eventType\",\"valueType\":\"xs:string\","
				+ "\"value\":\"i=2378\"}").getAsJsonObject();
		eventType.add("semanticId", semantic("https://smartproductionlab.aau.dk/aid/OpcuaEventType/1/0"));
		event.getAsJsonArray("value").add(eventType);
		JsonObject toState = field("ToState");
		JsonObject path = json("{\"modelType\":\"Property\",\"idShort\":\"uav_eventField\",\"valueType\":\"xs:string\","
				+ "\"value\":\"0:ToState\"}").getAsJsonObject();
		path.add("semanticId", semantic("https://smartproductionlab.aau.dk/aid/OpcuaEventField/1/0"));
		toState.getAsJsonArray("value").add(path);
		JsonObject fields = json("{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"properties\",\"value\":[]}").getAsJsonObject();
		fields.add("semanticId", semantic("https://www.w3.org/2019/wot/json-schema#properties"));
		fields.getAsJsonArray("value").add(toState);
		fields.getAsJsonArray("value").add(field("Message"));
		JsonObject data = json("{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"data\",\"value\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"type\",\"valueType\":\"xs:string\",\"value\":\"object\"}]}").getAsJsonObject();
		data.add("semanticId", semantic("https://www.w3.org/2019/wot/td#hasNotificationSchema"));
		data.getAsJsonArray("value").add(fields);
		event.getAsJsonArray("value").add(data);
		JsonObject events = json("{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"events\",\"value\":[]}").getAsJsonObject();
		events.add("semanticId", semantic("https://www.w3.org/2019/wot/td#EventAffordance"));
		events.getAsJsonArray("value").add(event);
		return events;
	}

	/** A string-typed property of a data schema. */
	private static JsonObject field(String name) {
		JsonObject field = json("{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"" + name + "\",\"value\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"type\",\"valueType\":\"xs:string\",\"value\":\"string\"}]}").getAsJsonObject();
		field.add("semanticId", semantic("https://www.w3.org/2019/wot/json-schema#propertyName"));
		return field;
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
		try (var input = OpcUaSubscriptionSourcesTest.class.getClassLoader().getResourceAsStream("runtime-model-contracts/" + path)) {
			assertNotNull(input, path);
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static JsonElement json(String value) { return JsonParser.parseString(value); }

	/** A free port below the ephemeral range (client sockets take ephemeral ports). */
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
