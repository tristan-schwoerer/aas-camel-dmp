/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

import static org.awaitility.Awaitility.await;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.core.plan.RoutePlan;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler;
import dk.aau.smartproductionlab.dmp.management.model.WireProjection;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;
import java.net.ServerSocket;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.opcua.canonical.OpcUaCanonical;
import dk.aau.smartproductionlab.dmp.opcua.metadata.CamelOpcUaMethodMetadataProvider;
import dk.aau.smartproductionlab.dmp.opcua.metadata.OpcUaMethodMetadataProvider;
import dk.aau.smartproductionlab.dmp.opcua.metadata.OpcUaSynchronousSourceReadAdapter;
import org.eclipse.milo.opcua.sdk.core.AccessLevel;
import org.eclipse.milo.opcua.sdk.core.Reference;
import org.eclipse.milo.opcua.sdk.core.types.DynamicStructType;
import org.eclipse.milo.opcua.sdk.core.types.codec.DynamicCodecFactory;
import org.eclipse.milo.opcua.sdk.core.typetree.DataType;
import org.eclipse.milo.opcua.sdk.server.AddressSpaceFilter;
import org.eclipse.milo.opcua.sdk.server.EndpointConfig;
import org.eclipse.milo.opcua.sdk.server.ManagedAddressSpaceFragmentWithLifecycle;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.OpcUaServerConfig;
import org.eclipse.milo.opcua.sdk.server.SimpleAddressSpaceFilter;
import org.eclipse.milo.opcua.sdk.server.items.DataItem;
import org.eclipse.milo.opcua.sdk.server.items.MonitoredItem;
import org.eclipse.milo.opcua.sdk.server.methods.AbstractMethodInvocationHandler;
import org.eclipse.milo.opcua.sdk.server.nodes.UaDataTypeNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaMethodNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaObjectNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.sdk.server.util.SubscriptionModel;
import org.eclipse.milo.opcua.stack.core.NodeIds;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.security.DefaultCertificateManager;
import org.eclipse.milo.opcua.stack.core.security.MemoryCertificateQuarantine;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExtensionObject;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.StructureType;
import org.eclipse.milo.opcua.stack.core.types.structured.Argument;
import org.eclipse.milo.opcua.stack.core.types.structured.CallMethodResult;
import org.eclipse.milo.opcua.stack.core.types.structured.EnumDefinition;
import org.eclipse.milo.opcua.stack.core.types.structured.EnumField;
import org.eclipse.milo.opcua.sdk.core.types.DynamicEnumType;
import org.eclipse.milo.opcua.stack.core.types.structured.Range;
import org.eclipse.milo.opcua.stack.core.types.structured.StructureDefinition;
import org.eclipse.milo.opcua.stack.core.types.structured.StructureField;
import org.eclipse.milo.opcua.stack.transport.server.tcp.OpcTcpServerTransport;
import org.eclipse.milo.opcua.stack.transport.server.tcp.OpcTcpServerTransportConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Structures the server describes (a DataType with a DataTypeDefinition, OPC UA 1.04) reach
 * Lua as objects and are written from objects (SPEC §5.2): a {@code Recipe} structure with a
 * String, a Double, a server-defined enumeration, a namespace-0 structure (Range) and an
 * Int32 array, read, observed, written and passed through a Method.
 */
class OpcUaStructuresTest {
	private static final NodeId MODE = new NodeId(1, "Mode");
	private static final NodeId RECIPE = new NodeId(1, "Recipe");
	private static final NodeId RECIPE_BINARY = new NodeId(1, "Recipe.BinaryEncoding");
	private static final NodeId RECIPE_VARIABLE = new NodeId(1, "CurrentRecipe");

	private OpcUaServer server;
	private Device device;
	private CamelContext camel;
	private String url;

	@BeforeEach
	void start() throws Exception {
		int port = freePort();
		var endpoint = EndpointConfig.newBuilder().setBindAddress("127.0.0.1").setBindPort(port)
				.setHostname("127.0.0.1").setPath("").setSecurityPolicy(SecurityPolicy.None)
				.setSecurityMode(MessageSecurityMode.None).addTokenPolicy(OpcUaServerConfig.USER_TOKEN_POLICY_ANONYMOUS).build();
		server = new OpcUaServer(OpcUaServerConfig.builder().setEndpoints(Set.of(endpoint))
				.setApplicationName(LocalizedText.english("structure device"))
				.setApplicationUri("urn:dmp:structures:server").setProductUri("urn:dmp:structures")
				.setCertificateManager(new DefaultCertificateManager(new MemoryCertificateQuarantine())).build(),
				profile -> new OpcTcpServerTransport(OpcTcpServerTransportConfig.newBuilder().build()));
		device = new Device(server);
		device.startup();
		server.startup().get(10, TimeUnit.SECONDS);
		device.registerTypes();
		url = "dmp-milo-client:opc.tcp://127.0.0.1:" + port + "/";
		camel = new DefaultCamelContext();
	}

	@AfterEach
	void stop() throws Exception {
		try { if (camel != null) camel.stop(); } finally {
			try { device.shutdown(); } finally { server.shutdown().get(10, TimeUnit.SECONDS); }
		}
	}

	@Test
	void aStructureIsReadAsAnObjectKeyedByFieldName() throws Exception {
		camel.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				new OpcUaSynchronousSourceReadAdapter().configureRead(from("direct:read"), url + "?node=RAW(ns=1;s=CurrentRecipe)");
			}
		});
		camel.start();
		assertEquals(recipe("Cake", 1.5, 1, 0.0, 10.0, List.of(1, 2, 3)),
				camel.createProducerTemplate().requestBody("direct:read", null, Map.class));
	}

	@Test
	void anObservedStructureDeliversEachChangeAsAnObject() throws Exception {
		BlockingQueue<Object> samples = new LinkedBlockingQueue<>();
		camel.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				from(url + "?node=RAW(ns=1;s=CurrentRecipe)").process(exchange -> samples.add(OpcUaCanonical.jsonValue(
						exchange.getMessage().getBody(DataValue.class).getValue())));
			}
		});
		camel.start();
		assertEquals(recipe("Cake", 1.5, 1, 0.0, 10.0, List.of(1, 2, 3)), samples.poll(10, TimeUnit.SECONDS));
		device.set(device.recipe("Bread", 2.5, 0, 1.0, 5.0, new Integer[] { 4 }));
		assertEquals(recipe("Bread", 2.5, 0, 1.0, 5.0, List.of(4)), samples.poll(10, TimeUnit.SECONDS));
	}

	@Test
	void anObjectIsWrittenAsTheVariablesStructure() throws Exception {
		camel.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				from("direct:write").to(url + "?node=RAW(ns=1;s=CurrentRecipe)");
			}
		});
		camel.start();
		ProducerTemplate template = camel.createProducerTemplate();
		Map<String, Object> written = new LinkedHashMap<>(recipe("Bread", 2.5, 0, 1.0, 5.0, List.of(4, 5)));
		written.put("Mode", "Running"); // an enumeration value by its name
		template.requestBody("direct:write", written);
		assertEquals(recipe("Bread", 2.5, 1, 1.0, 5.0, List.of(4, 5)), device.current());

		Map<String, Object> unknownField = new LinkedHashMap<>(written);
		unknownField.put("Colour", "red");
		var failure = assertThrows(CamelExecutionException.class, () -> template.requestBody("direct:write", unknownField));
		assertEquals("OPCUA_VALUE_MISMATCH", assertInstanceOf(ConnectorInvocationException.class, failure.getCause()).getConnectorCode());
	}

	@Test
	void aMethodTakesAndReturnsStructures() throws Exception {
		camel.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				from("direct:call").to(url + "?node=RAW(ns=1;s=Machine)&method=RAW(ns=1;s=Scale)");
			}
		});
		camel.start();
		// The metadata provider leaves a structured argument to the producer, which holds the client.
		Object arguments = new CamelOpcUaMethodMetadataProvider().toVariantArray(
				List.of(recipe("Cake", 1.5, 1, 0.0, 10.0, List.of(1)), 2.0),
				List.of(new OpcUaMethodMetadataProvider.Argument("Recipe", RECIPE.toParseableString(), -1, List.of()),
						new OpcUaMethodMetadataProvider.Argument("Factor", "i=11", -1, List.of())));
		CallMethodResult result = camel.createProducerTemplate().requestBody("direct:call", arguments, CallMethodResult.class);
		assertTrue(result.getStatusCode().isGood(), result.getStatusCode() + " " + java.util.Arrays.toString(result.getInputArgumentResults()));
		assertEquals(recipe("Cake", 3.0, 1, 0.0, 10.0, List.of(1)), result.getOutputArguments()[0].getValue());
	}

	@Test
	void aMethodCallWaitsItsOwnTimeoutNotTheClientsRequestTimeout() throws Exception {
		camel.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				// The session's requestTimeout (1 s) is shorter than the Method (2.5 s) and the call's timeout (5 s).
				from("direct:call").to(url + "?node=RAW(ns=1;s=Machine)&method=RAW(ns=1;s=Slow)&requestTimeout=1000");
			}
		});
		camel.start();
		CallMethodResult result = camel.createProducerTemplate().requestBodyAndHeader("direct:call", new Variant[0],
				dk.aau.smartproductionlab.dmp.opcua.component.DmpMiloMethodProducer.CALL_TIMEOUT_HEADER, 5_000L,
				CallMethodResult.class);
		assertTrue(result.getStatusCode().isGood(), result.getStatusCode().toString());
		assertEquals("done", result.getOutputArguments()[0].getValue());
	}

	@Test
	void luaReadsTheFieldsOfAnObservedStructure() throws Exception {
		List<String> writes = new java.util.concurrent.CopyOnWriteArrayList<>();
		HttpServer aas = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		aas.createContext("/", exchange -> {
			if (!"GET".equals(exchange.getRequestMethod()))
				writes.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
			exchange.sendResponseHeaders(204, -1);
			exchange.close();
		});
		aas.start();
		var runtime = new DmpComponent(Duration.ofSeconds(4)) { };
		try (var repository = new FakeAasRepository()) {
			runtime.startComponent();
			runtime.installBundle("structure", compile(repository, aas.getAddress().getPort(),
					"function aimc_main(sources) local r = sources.Pressure "
					+ "return { Out = r.Name .. \":\" .. r.Mode .. \":\" .. r.Steps[#r.Steps] } end"));
			await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("Cake:1:3")));
			device.set(device.recipe("Bread", 2.5, 0, 1.0, 5.0, new Integer[] { 4, 7 }));
			await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("Bread:0:7")));
		} finally {
			try { runtime.stopComponent(); } finally { aas.stop(0); }
		}
	}

	@Test
	void anAidReferenceIntoTheStructureSelectsOneField() throws Exception {
		List<String> writes = new java.util.concurrent.CopyOnWriteArrayList<>();
		HttpServer aas = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		aas.createContext("/", exchange -> {
			if (!"GET".equals(exchange.getRequestMethod()))
				writes.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
			exchange.sendResponseHeaders(204, -1);
			exchange.close();
		});
		aas.start();
		var runtime = new DmpComponent(Duration.ofSeconds(4)) { };
		try (var repository = new FakeAasRepository()) {
			runtime.startComponent();
			runtime.installBundle("field", compile(repository, aas.getAddress().getPort(),
					"function aimc_main(sources) return { Out = sources.Pressure * 2 } end", "Speed"));
			await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("3")));
		} finally {
			try { runtime.stopComponent(); } finally { aas.stop(0); }
		}
	}

	private RoutePlan compile(FakeAasRepository repository, int aasPort, String lua) throws Exception {
		return compile(repository, aasPort, lua, null);
	}

	private RoutePlan compile(FakeAasRepository repository, int aasPort, String lua, String field) throws Exception {
		return compile(repository, aasPort, lua, field, "ns=1;s=CurrentRecipe", true);
	}

	@Test
	void anAidThatMisdescribesTheDevicesStructureFailsTheSource() throws Exception {
		for (boolean observed : List.of(true, false)) {
			List<String> writes = new java.util.concurrent.CopyOnWriteArrayList<>();
			HttpServer aas = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
			aas.createContext("/", exchange -> {
				if (!"GET".equals(exchange.getRequestMethod()))
					writes.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
				exchange.sendResponseHeaders(204, -1);
				exchange.close();
			});
			aas.start();
			var runtime = new DmpComponent(Duration.ofSeconds(4)) { };
			try (var repository = new FakeAasRepository()) {
				runtime.startComponent();
				// The AID says Speed is a string; the server's Recipe has a Double.
				runtime.installBundle("wrong-type", compile(repository, aas.getAddress().getPort(),
						"function aimc_main(sources) return { Out = sources.Pressure } end", "Speed:string",
						"ns=1;s=CurrentRecipe", observed));
				Thread.sleep(3000);
				assertEquals(List.of(), writes, (observed ? "observed" : "polled") + ": a misdescribed source contributes nothing");
			} finally {
				try { runtime.stopComponent(); } finally { aas.stop(0); }
			}
		}
	}

	@Test
	void aPolledStringVariableIsItsValue() throws Exception {
		List<String> writes = new java.util.concurrent.CopyOnWriteArrayList<>();
		HttpServer aas = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		aas.createContext("/", exchange -> {
			if (!"GET".equals(exchange.getRequestMethod()))
				writes.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
			exchange.sendResponseHeaders(204, -1);
			exchange.close();
		});
		aas.start();
		var runtime = new DmpComponent(Duration.ofSeconds(4)) { };
		try (var repository = new FakeAasRepository()) {
			runtime.startComponent();
			// Read (not observed) at the fixture's DefaultPollingInterval; a string is not parsed as JSON text.
			runtime.installBundle("label", compile(repository, aas.getAddress().getPort(),
					"function aimc_main(sources) return { Out = sources.Pressure .. \"!\" } end", null, "ns=1;s=Label", false));
			await().atMost(Duration.ofSeconds(8)).until(() -> writes.stream().anyMatch(body -> body.contains("batch-7!")));
		} finally {
			try { runtime.stopComponent(); } finally { aas.stop(0); }
		}
	}

	/** The opcua-read fixture (Pressure → KPI/Out) with Pressure observing the Recipe variable. */
	private RoutePlan compile(FakeAasRepository repository, int aasPort, String lua, String field,
			String node, boolean observed) throws Exception {
		JsonObject aid = JsonParser.parseString(resource("opcua-read/aid.json")).getAsJsonObject();
		JsonObject opcua = child(aid, "interface_opcua");
		child(child(opcua, "EndpointMetadata"), "base").addProperty("value", url.substring("dmp-milo-client:".length()));
		JsonObject pressure = child(child(child(opcua, "InteractionMetadata"), "properties"), "Pressure");
		child(child(pressure, "forms"), "href").addProperty("value", "/?id=" + node);
		if (observed) pressure.getAsJsonArray("value").add(JsonParser.parseString("{\"modelType\":\"Property\",\"idShort\":\"observable\","
				+ "\"valueType\":\"xs:boolean\",\"value\":\"true\"}"));
		JsonObject aimc = JsonParser.parseString(resource("opcua-read/aimc.json")).getAsJsonObject();
		JsonObject mapping = child(aimc, "MappingConfigurations").getAsJsonArray("value").get(0).getAsJsonObject();
		if (field != null) {
			// The AID describes the structure (an object with a number member, as in fine-protocols)
			// and the Source references that member.
			JsonObject level = child(child(child(child(JsonParser.parseString(resource("fine-protocols/aid.json")).getAsJsonObject(),
					"interface_opcua"), "InteractionMetadata"), "properties"), "Level");
			JsonObject properties = child(level, "properties").deepCopy();
			// "Speed" or "Speed:string" (a type the AID declares for the member).
			String[] spec = field.split(":");
			field = spec[0];
			JsonObject member = child(properties, "Reading");
			member.addProperty("idShort", field);
			child(member, "key").addProperty("value", field);
			if (spec.length > 1) child(member, "type").addProperty("value", spec[1]);
			pressure.getAsJsonArray("value").add(child(level, "type").deepCopy());
			pressure.getAsJsonArray("value").add(properties);
			JsonObject source = child(mapping, "Sources").getAsJsonArray("value").get(0).getAsJsonObject();
			var keys = child(source, "Source").getAsJsonObject("value").getAsJsonArray("keys");
			keys.add(keys.get(keys.size() - 1).deepCopy());
			keys.get(keys.size() - 1).getAsJsonObject().addProperty("value", field);
		}
		child(mapping, "Transformation").addProperty("value",
				Base64.getEncoder().encodeToString(lua.getBytes(StandardCharsets.UTF_8)));
		String publishedAimc = PublishedModels.publish(aimc.toString(), "contractAAS", "contract");
		repository.put(publishedAimc);
		for (String model : List.of(aid.toString(), resource("opcua-read/dependencies/0.json")))
			repository.put(PublishedModels.publish(model, "contractAAS", "contract"));
		var plan = new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compile(WireProjection.aimc(
				dk.aau.smartproductionlab.dmp.aas.model.Aas.read(publishedAimc, org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
		assertEquals(AasRepositoryClient.Status.FOUND, plan.status(), String.valueOf(plan));
		var compiled = new ConnectorConfigurationFactory().createBundles(plan.value(),
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://127.0.0.1:" + aasPort)));
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
		try (var input = OpcUaStructuresTest.class.getClassLoader().getResourceAsStream("runtime-model-contracts/" + path)) {
			assertNotNull(input, path);
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static Map<String, Object> recipe(String name, double speed, int mode, double low, double high, List<Integer> steps) {
		Map<String, Object> recipe = new LinkedHashMap<>();
		recipe.put("Name", name);
		recipe.put("Speed", speed);
		recipe.put("Mode", mode);
		recipe.put("Limits", Map.of("Low", low, "High", high));
		recipe.put("Steps", steps);
		return recipe;
	}

	/** The Recipe and Mode DataTypes, a Recipe variable and the Machine's Scale Method. */
	private static final class Device extends ManagedAddressSpaceFragmentWithLifecycle {
		private final OpcUaServer server;
		private final SubscriptionModel subscriptions;
		private UaVariableNode variable;

		Device(OpcUaServer server) {
			super(server);
			this.server = server;
			subscriptions = new SubscriptionModel(server, this);
			getLifecycleManager().addLifecycle(subscriptions);
			server.getNamespaceTable().add("urn:dmp:structures:device");
			UaDataTypeNode mode = new UaDataTypeNode(getNodeContext(), MODE, new QualifiedName(1, "Mode"),
					LocalizedText.english("Mode"), LocalizedText.NULL_VALUE, uint(0), uint(0), null, null, null, false,
					new EnumDefinition(new EnumField[] {
							new EnumField(0L, LocalizedText.english("Idle"), LocalizedText.NULL_VALUE, "Idle"),
							new EnumField(1L, LocalizedText.english("Running"), LocalizedText.NULL_VALUE, "Running") }));
			mode.addReference(new Reference(MODE, NodeIds.HasSubtype, NodeIds.Enumeration.expanded(), false));
			getNodeManager().addNode(mode);
			UaDataTypeNode recipe = new UaDataTypeNode(getNodeContext(), RECIPE, new QualifiedName(1, "Recipe"),
					LocalizedText.english("Recipe"), LocalizedText.NULL_VALUE, uint(0), uint(0), null, null, null, false,
					new StructureDefinition(RECIPE_BINARY, NodeIds.Structure, StructureType.Structure, new StructureField[] {
							field("Name", NodeIds.String, -1), field("Speed", NodeIds.Double, -1), field("Mode", MODE, -1),
							field("Limits", NodeIds.Range, -1), field("Steps", NodeIds.Int32, 1) }));
			recipe.addReference(new Reference(RECIPE, NodeIds.HasSubtype, NodeIds.Structure.expanded(), false));
			recipe.addReference(new Reference(RECIPE, NodeIds.HasEncoding, RECIPE_BINARY.expanded(), true));
			getNodeManager().addNode(recipe);
			UaObjectNode.build(getNodeContext(), builder -> builder.setNodeId(RECIPE_BINARY)
					.setBrowseName(new QualifiedName(0, "Default Binary")).setDisplayName(LocalizedText.english("Default Binary"))
					.setTypeDefinition(NodeIds.DataTypeEncodingType).buildAndAdd());
			variable = UaVariableNode.builder(getNodeContext()).setNodeId(RECIPE_VARIABLE)
					.setBrowseName(new QualifiedName(1, "CurrentRecipe")).setDisplayName(LocalizedText.english("CurrentRecipe"))
					.setDataType(RECIPE).setTypeDefinition(NodeIds.BaseDataVariableType)
					.setAccessLevel(AccessLevel.toValue(AccessLevel.READ_WRITE))
					.setUserAccessLevel(AccessLevel.toValue(AccessLevel.READ_WRITE)).buildAndAdd();

			UaVariableNode.builder(getNodeContext()).setNodeId(new NodeId(1, "Label"))
					.setBrowseName(new QualifiedName(1, "Label")).setDisplayName(LocalizedText.english("Label"))
					.setDataType(NodeIds.String).setTypeDefinition(NodeIds.BaseDataVariableType)
					.setAccessLevel(AccessLevel.toValue(AccessLevel.READ_ONLY))
					.setUserAccessLevel(AccessLevel.toValue(AccessLevel.READ_ONLY)).buildAndAdd()
					.setValue(new DataValue(new Variant("batch-7")));

			UaObjectNode machine = UaObjectNode.build(getNodeContext(), builder -> builder.setNodeId(new NodeId(1, "Machine"))
					.setBrowseName(new QualifiedName(1, "Machine")).setDisplayName(LocalizedText.english("Machine"))
					.setTypeDefinition(NodeIds.BaseObjectType).buildAndAdd());
			UaMethodNode scale = UaMethodNode.builder(getNodeContext()).setNodeId(new NodeId(1, "Scale"))
					.setBrowseName(new QualifiedName(1, "Scale")).setDisplayName(LocalizedText.english("Scale"))
					.setExecutable(true).setUserExecutable(true).buildAndAdd();
			Argument[] inputs = { new Argument("Recipe", RECIPE, -1, null, LocalizedText.english("recipe")),
					new Argument("Factor", NodeIds.Double, -1, null, LocalizedText.english("factor")) };
			Argument[] outputs = { new Argument("Recipe", RECIPE, -1, null, LocalizedText.english("scaled recipe")) };
			scale.setInputArguments(inputs);
			scale.setOutputArguments(outputs);
			scale.setInvocationHandler(new AbstractMethodInvocationHandler(scale) {
				@Override public Argument[] getInputArguments() { return inputs; }
				@Override public Argument[] getOutputArguments() { return outputs; }
				@Override protected Variant[] invoke(InvocationContext context, Variant[] input) throws UaException {
					Object argument = input[0].getValue(); // Milo has decoded it already
					var given = (DynamicStructType) (argument instanceof ExtensionObject xo
							? xo.decode(server.getDynamicEncodingContext()) : argument);
					LinkedHashMap<String, Object> members = new LinkedHashMap<>(given.getMembers());
					members.put("Speed", (Double) members.get("Speed") * (Double) input[1].getValue());
					return new Variant[] { new Variant(ExtensionObject.encode(server.getDynamicEncodingContext(),
							new DynamicStructType(given.getDataType(), members))) };
				}
			});
			machine.addComponent(scale);

			UaMethodNode slow = UaMethodNode.builder(getNodeContext()).setNodeId(new NodeId(1, "Slow"))
					.setBrowseName(new QualifiedName(1, "Slow")).setDisplayName(LocalizedText.english("Slow"))
					.setExecutable(true).setUserExecutable(true).buildAndAdd();
			Argument[] none = {};
			Argument[] done = { new Argument("Done", NodeIds.String, -1, null, LocalizedText.english("done")) };
			slow.setOutputArguments(done);
			slow.setInvocationHandler(new AbstractMethodInvocationHandler(slow) {
				@Override public Argument[] getInputArguments() { return none; }
				@Override public Argument[] getOutputArguments() { return done; }
				@Override protected Variant[] invoke(InvocationContext context, Variant[] input) throws UaException {
					try { Thread.sleep(2_500); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
					return new Variant[] { new Variant("done") };
				}
			});
			machine.addComponent(slow);
		}

		/** After startup: the server learns its own types, so it can encode and decode Recipes. */
		void registerTypes() {
			var tree = server.updateDataTypeTree();
			DataType recipe = tree.getDataType(RECIPE);
			assertNotNull(recipe, "the Recipe DataType is in the server's type tree");
			var codec = DynamicCodecFactory.create(recipe, tree);
			server.getDynamicDataTypeManager().registerType(RECIPE, codec, RECIPE_BINARY, null, null);
			// Milo's Method handler decodes structured inputs with the static context.
			server.getStaticDataTypeManager().registerType(RECIPE, codec, RECIPE_BINARY, null, null);
			set(recipe("Cake", 1.5, 1, 0.0, 10.0, new Integer[] { 1, 2, 3 }));
		}

		DynamicStructType recipe(String name, double speed, int mode, double low, double high, Integer[] steps) {
			var tree = server.getDataTypeTree();
			LinkedHashMap<String, Object> members = new LinkedHashMap<>();
			members.put("Name", name);
			members.put("Speed", speed);
			members.put("Mode", new DynamicEnumType(tree.getDataType(MODE), mode));
			members.put("Limits", new Range(low, high));
			members.put("Steps", steps);
			return new DynamicStructType(tree.getDataType(RECIPE), members);
		}

		void set(DynamicStructType recipe) {
			variable.setValue(new DataValue(new Variant(ExtensionObject.encode(server.getDynamicEncodingContext(), recipe))));
		}

		/** The variable's current value, decoded by the server, as an object. */
		Object current() {
			Object value = variable.getValue().getValue().getValue();
			return OpcUaCanonical.jsonValue(((ExtensionObject) value).decode(server.getDynamicEncodingContext()));
		}

		private static StructureField field(String name, NodeId type, int rank) {
			return new StructureField(name, LocalizedText.NULL_VALUE, type, rank, null, uint(0), false);
		}

		@Override public AddressSpaceFilter getFilter() {
			return SimpleAddressSpaceFilter.create(id -> id.getNamespaceIndex().intValue() == 1);
		}
		@Override public void onDataItemsCreated(List<DataItem> items) { subscriptions.onDataItemsCreated(items); }
		@Override public void onDataItemsModified(List<DataItem> items) { subscriptions.onDataItemsModified(items); }
		@Override public void onDataItemsDeleted(List<DataItem> items) { subscriptions.onDataItemsDeleted(items); }
		@Override public void onMonitoringModeChanged(List<MonitoredItem> items) { subscriptions.onMonitoringModeChanged(items); }
	}

	private static int freePort() throws Exception {
		try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
	}
}
