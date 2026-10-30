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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
import org.eclipse.milo.opcua.sdk.server.AddressSpaceFilter;
import org.eclipse.milo.opcua.sdk.server.EndpointConfig;
import org.eclipse.milo.opcua.sdk.server.ManagedAddressSpaceFragmentWithLifecycle;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.OpcUaServerConfig;
import org.eclipse.milo.opcua.sdk.server.SimpleAddressSpaceFilter;
import org.eclipse.milo.opcua.sdk.server.items.DataItem;
import org.eclipse.milo.opcua.sdk.server.items.MonitoredItem;
import org.eclipse.milo.opcua.sdk.server.methods.AbstractMethodInvocationHandler;
import org.eclipse.milo.opcua.sdk.server.nodes.UaMethodNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaObjectNode;
import org.eclipse.milo.opcua.stack.core.NodeIds;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.security.DefaultCertificateManager;
import org.eclipse.milo.opcua.stack.core.security.MemoryCertificateQuarantine;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.structured.Argument;
import org.eclipse.milo.opcua.stack.transport.server.tcp.OpcTcpServerTransport;
import org.eclipse.milo.opcua.stack.transport.server.tcp.OpcTcpServerTransportConfig;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dk.aau.smartproductionlab.dmp.paho.testing.Mqtt5TestBroker;


/** Static Python-oracle descriptors and Java plans, one exclusive production owner at a time. */
class ManagedProtocolOutcomeParityTest {
    private static final class Runtime extends DmpComponent {
        Runtime() { super(Duration.ofSeconds(4)); }
        int routes() { return camelContext.getRoutesSize(); }
        int endpoints() { return camelContext.getEndpoints().size(); }
        void listener(int port) throws Exception {
            camelContext.addRoutes(new RouteBuilder() {
                @Override public void configure() {
                    restConfiguration().component("jetty").host("127.0.0.1").port(port).inlineRoutes(false);
                }
            });
        }
    }

    private record Request(String method, String path, String query, JsonElement body) { }
    private record Reply(int status, JsonElement body) { }

    @Test
    void opcUaTypedOutputsReachDirectAndBaSyxCallersWithoutUnwrappingOrReplay() throws Exception {
        Map<String, String> models = typedOutputModels();
        int devicePort = freePort();
        var endpoint = EndpointConfig.newBuilder().setBindAddress("127.0.0.1").setBindPort(devicePort)
                .setHostname("127.0.0.1").setPath("").setSecurityPolicy(SecurityPolicy.None)
                .setSecurityMode(MessageSecurityMode.None).addTokenPolicy(OpcUaServerConfig.USER_TOKEN_POLICY_ANONYMOUS).build();
        var server = new OpcUaServer(OpcUaServerConfig.builder().setEndpoints(Set.of(endpoint))
                .setApplicationName(LocalizedText.english("typed output simulator"))
                .setApplicationUri("urn:dmp:typed-output:server").setProductUri("urn:dmp:typed-output")
                .setCertificateManager(new DefaultCertificateManager(new MemoryCertificateQuarantine())).build(),
                profile -> new OpcTcpServerTransport(OpcTcpServerTransportConfig.newBuilder().build()));
        var device = new MethodDevice(server);
        Runtime runtime = new Runtime();
        try (Repository repository = new Repository("opcua-method", "opc.tcp://127.0.0.1:" + devicePort,
                models)) {
            device.startup();
            server.startup().get(10, TimeUnit.SECONDS);
            // The declared outputs come from the whole Method result (the ResponseSource Reply).
            repository.respond("Reply", "Calibrate", "function aimc_main(sources) local reply = sources.Reply "
                    + "return { Calibrate = { Calibrate = reply.Calibrate, State = reply.State, Label = reply.Label, "
                    + "Count = reply.Count } } end");
            RoutePlan plan = repository.compile();
            JsonObject fullCci = json(repository.models.get("a/submodels/ControlComponentInstance")).getAsJsonObject();
            child(child(child(fullCci, "Skills"), "Calibrate"), "operation").remove("outputVariables");
            repository.models.put("a/submodels/ControlComponentInstance", fullCci.toString());
            repository.withoutResponse();
            RoutePlan nativePlan = repository.compile();
            runtime.startComponent();
            int port = freePort();
            runtime.listener(port);
            int baselineRoutes = runtime.routes();
            int baselineEndpoints = runtime.endpoints();
            HttpClient caller = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
            var nativeOwner = runtime.installBundle("exclusive-opcua-native-output", nativePlan);
            try {
                var nativeReply = caller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                        + OperationCalls.path("a/submodels/ControlComponentInstance", "Calibrate"))).timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString("[]"))
                        .build(), HttpResponse.BodyHandlers.ofString());
                assertEquals(200, nativeReply.statusCode(), nativeReply.body());
                assertEquals(json("{}"), json(OperationCalls.outputs(nativeReply.body())),
                        "an Operation without outputs answers nothing; the Method's outputs are not passed through");
                assertEquals(json("{}"), device.calls.poll(2, TimeUnit.SECONDS));
                assertEquals(1, device.sends.get());
            } finally { runtime.removeBundle(nativeOwner); }
            assertFalse(nativeOwner.active());
            assertEquals(baselineRoutes, runtime.routes());
            assertEquals(baselineEndpoints, runtime.endpoints());
            var owner = runtime.installBundle("exclusive-opcua-typed-output", plan);
            try {
                assertEquals(1, device.sends.get(), "compile/install must not call or replay the Method");
                var direct = caller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                        + OperationCalls.path("a/submodels/ControlComponentInstance", "Calibrate"))).timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("[]")).build(), HttpResponse.BodyHandlers.ofString());
                assertEquals(200, direct.statusCode(), direct.body());
                assertEquals(2, device.sends.get(), "one real OPC UA Call for direct REST invocation");
                assertEquals(json("{}"), device.calls.poll(2, TimeUnit.SECONDS));
                JsonElement directReply = json(OperationCalls.outputs(direct.body()));
                assertEquals(json("{\"Calibrate\":true,\"State\":\"001\",\"Label\":\"007\",\"Count\":7}"), directReply);
                assertEquals(List.of("Calibrate", "State", "Label", "Count"),
                        directReply.getAsJsonObject().keySet().stream().toList(), "caller declaration order, not UA argument order");

                var basyx = caller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                        + OperationCalls.path("a/submodels/ControlComponentInstance", "Calibrate"))).timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("[]"))
                        .build(), HttpResponse.BodyHandlers.ofString());
                assertEquals(200, basyx.statusCode(), basyx.body());
                assertEquals(3, device.sends.get(), "one real OPC UA Call for BaSyx invocation");
                assertEquals(json("{}"), device.calls.poll(2, TimeUnit.SECONDS));
                assertEquals(json("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Calibrate\",\"valueType\":\"xs:boolean\",\"value\":\"true\"}},"
                        + "{\"value\":{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:string\",\"value\":\"001\"}},"
                        + "{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Label\",\"valueType\":\"xs:string\",\"value\":\"007\"}},"
                        + "{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Count\",\"valueType\":\"xs:integer\",\"value\":\"7\"}}]"),
                        json(basyx.body()));

                device.failNext.set("status");
                var badStatus = caller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                        + OperationCalls.path("a/submodels/ControlComponentInstance", "Calibrate"))).timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("[]")).build(), HttpResponse.BodyHandlers.ofString());
                assertEquals(502, badStatus.statusCode(), badStatus.body());
                assertEquals(4, device.sends.get(), "bad native status is not replayed");
                assertNotNull(device.calls.poll(2, TimeUnit.SECONDS));

                device.failNext.set("type");
                var incompatible = caller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                        + OperationCalls.path("a/submodels/ControlComponentInstance", "Calibrate"))).timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("[]")).build(), HttpResponse.BodyHandlers.ofString());
                assertEquals(502, incompatible.statusCode(), incompatible.body());
                assertEquals(5, device.sends.get(), "incompatible native output is not retried");
                assertNotNull(device.calls.poll(2, TimeUnit.SECONDS));
                assertNull(device.calls.poll(150, TimeUnit.MILLISECONDS), "no queued native call replay");
            } finally {
                runtime.removeBundle(owner);
            }
            assertFalse(owner.active());
            assertNull(runtime.managedBundle("exclusive-opcua-typed-output"));
            assertEquals(baselineRoutes, runtime.routes());
            assertEquals(baselineEndpoints, runtime.endpoints(), "owned Milo client endpoint retires on removal");
            assertEquals(0, repository.nonGetRequests(), "model compilation remains read-only");
        } finally {
            try { runtime.stopComponent(); } finally {
                try { device.shutdown(); } finally { server.shutdown().get(10, TimeUnit.SECONDS); }
            }
        }
    }

    @Test
    void compiledUuidInoutputSurvivesAuthoredRequestAndResponseLuaWithOneNativeCall() throws Exception {
        BlockingQueue<Request> commands = new LinkedBlockingQueue<>();
        AtomicInteger sends = new AtomicInteger();
        HttpServer device = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        device.createContext("/plc/api/actions/Restart", exchange -> {
            Request command = request(exchange);
            commands.add(command);
            sends.incrementAndGet();
            String uuid = command.body().getAsJsonObject().get("Uuid").getAsString();
            respond(exchange, 200, "{\"Uuid\":\"" + uuid
                    + "\",\"State\":\"READY\",\"Count\":7}");
        });
        device.start();
        Runtime runtime = new Runtime();
        String uuid = "00000000-0000-0000-0000-00000000000a";
        try {
            JsonObject cci = json(resource("http-operation/dependencies/0.json")).getAsJsonObject();
            String cciId = cci.get("id").getAsString();
            JsonObject operation = child(child(child(cci, "Skills"), "Restart"), "operation");
            operation.add("inoutputVariables", json("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Uuid\",\"valueType\":\"xs:string\"}}]"));
            operation.add("outputVariables", json("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:string\"}},"
                    + "{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Count\",\"valueType\":\"xs:integer\"}}]"));
            try (Repository repository = new Repository("http-operation", "http://127.0.0.1:"
                    + device.getAddress().getPort() + "/plc/api", Map.of(cciId, cci.toString()))) {
                repository.respond("Reply", "Restart", "function aimc_main(sources) local reply = sources.Reply "
                        + "return { Restart = { Uuid = reply.Uuid, State = reply.State, Count = reply.Count } } end");
                RoutePlan plan = repository.compile();
                runtime.startComponent();
                int port = freePort();
                runtime.listener(port);
                int baselineRoutes = runtime.routes();
                int baselineEndpoints = runtime.endpoints();
                var owner = runtime.installBundle("uuid-inoutput-authored-lua", plan);
                try {
                    assertEquals(0, sends.get(), "compile and activation cannot invoke the native action");
                    String requestBody = "[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Uuid\","
                            + "\"valueType\":\"xs:string\",\"value\":\"" + uuid + "\"}}]";
                    var response = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build().send(
                            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                                    + OperationCalls.path("a/submodels/ControlComponentInstance", "Restart"))).timeout(Duration.ofSeconds(8))
                                    .header("Content-Type", "application/json")
                                    .POST(HttpRequest.BodyPublishers.ofString(requestBody)).build(),
                            HttpResponse.BodyHandlers.ofString());
                    assertEquals(200, response.statusCode(), response.body());
                    assertEquals(1, sends.get(), "one invocation produces exactly one native command");
                    assertEquals(new Request("POST", "/plc/api/actions/Restart", "mode=quick",
                            json("{\"Uuid\":\"" + uuid + "\"}")), commands.poll(2, TimeUnit.SECONDS),
                            "request Lua carries the caller's exact Uuid into the native request");
                    assertEquals(json("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Uuid\","
                            + "\"valueType\":\"xs:string\",\"value\":\"" + uuid + "\"}},{"
                            + "\"value\":{\"modelType\":\"Property\",\"idShort\":\"State\","
                            + "\"valueType\":\"xs:string\",\"value\":\"READY\"}},{"
                            + "\"value\":{\"modelType\":\"Property\",\"idShort\":\"Count\","
                            + "\"valueType\":\"xs:integer\",\"value\":\"7\"}}]"), json(response.body()),
                            "authored response Lua returns the inoutput identity followed by typed outputs");
                    assertNull(commands.poll(150, TimeUnit.MILLISECONDS), "no native request replay");
                } finally {
                    runtime.removeBundle(owner);
                }
                assertFalse(owner.active());
                assertNull(runtime.managedBundle("uuid-inoutput-authored-lua"));
                assertEquals(baselineRoutes, runtime.routes());
                assertEquals(baselineEndpoints, runtime.endpoints());
                assertEquals(0, repository.nonGetRequests());
            }
        } finally {
            try { runtime.stopComponent(); } finally { device.stop(0); }
        }
    }

    private static final class MethodDevice extends ManagedAddressSpaceFragmentWithLifecycle {
        final BlockingQueue<JsonElement> calls = new LinkedBlockingQueue<>();
        final AtomicInteger sends = new AtomicInteger();
        /** The next Call fails: "status" with a bad UA status, "type" with an output of the wrong type. */
        final AtomicReference<String> failNext = new AtomicReference<>();
        MethodDevice(OpcUaServer server) {
            super(server);
            server.getNamespaceTable().add("urn:dmp:parity:object");
            server.getNamespaceTable().add("urn:dmp:parity:method");
            UaObjectNode object = UaObjectNode.build(getNodeContext(), builder -> builder
                    .setNodeId(new NodeId(1, "CalibrationObject"))
                    .setBrowseName(new QualifiedName(1, "CalibrationObject"))
                    .setDisplayName(LocalizedText.english("CalibrationObject"))
                    .setTypeDefinition(NodeIds.BaseObjectType).buildAndAdd());
            UaMethodNode method = UaMethodNode.builder(getNodeContext()).setNodeId(new NodeId(2, "RunCalibration"))
                    .setBrowseName(new QualifiedName(2, "RunCalibration")).setDisplayName(LocalizedText.english("RunCalibration"))
                    .setExecutable(true).setUserExecutable(true).buildAndAdd();
            Argument[] inputs = new Argument[0];
            Argument[] outputs = {
                    new Argument("State", NodeIds.String, -1, null, LocalizedText.english("state")),
                    new Argument("Samples", NodeIds.Int32, 1, null, LocalizedText.english("samples")),
                    new Argument("Label", NodeIds.String, -1, null, LocalizedText.english("label")),
                    new Argument("Nullable", NodeIds.BaseDataType, -1, null, LocalizedText.english("nullable")),
                    new Argument("Calibrate", NodeIds.Boolean, -1, null, LocalizedText.english("action-named output")),
                    new Argument("Count", NodeIds.Int32, -1, null, LocalizedText.english("count"))
            };
            method.setInputArguments(inputs);
            method.setOutputArguments(outputs);
            method.setInvocationHandler(new AbstractMethodInvocationHandler(method) {
                @Override public Argument[] getInputArguments() { return inputs; }
                @Override public Argument[] getOutputArguments() { return outputs; }
                @Override protected Variant[] invoke(InvocationContext context, Variant[] input) throws UaException {
                    assertEquals(0, input.length);
                    calls.add(new JsonObject());
                    sends.incrementAndGet();
                    String failure = failNext.getAndSet(null);
                    if ("status".equals(failure)) throw new UaException(StatusCodes.Bad_InternalError);
                    if ("type".equals(failure)) return new Variant[] { new Variant("001"),
                            new Variant(new int[] { 3, 5 }), new Variant("007"), new Variant(null),
                            new Variant(true), new Variant("not-an-integer") };
                    return new Variant[] { new Variant("001"), new Variant(new int[] { 2, 4 }),
                            new Variant("007"), new Variant(null), new Variant(true), new Variant(7) };
                }
            });
            object.addComponent(method);
        }
        @Override public AddressSpaceFilter getFilter() {
            return SimpleAddressSpaceFilter.create(id -> id.getNamespaceIndex().intValue() == 1 || id.getNamespaceIndex().intValue() == 2);
        }
        // The fixture only serves Method metadata and Calls, never subscriptions.
        @Override public void onDataItemsCreated(List<DataItem> items) { }
        @Override public void onDataItemsModified(List<DataItem> items) { }
        @Override public void onDataItemsDeleted(List<DataItem> items) { }
        @Override public void onMonitoringModeChanged(List<MonitoredItem> items) { }
    }

    @Test
    void httpOperationValuesTypesOrderAndFailureSendCountsMatchWithSequentialOwners() throws Exception {
        BlockingQueue<Request> commands = new LinkedBlockingQueue<>();
        AtomicInteger sends = new AtomicInteger();
        AtomicInteger deviceStatus = new AtomicInteger(200);
        AtomicReference<String> deviceReply = new AtomicReference<>();
        HttpServer device = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        device.createContext("/plc/api/actions/Restart", exchange -> {
            commands.add(request(exchange));
            sends.incrementAndGet();
            respond(exchange, deviceStatus.get(), deviceReply.get());
        });
        device.start();
        Runtime runtime = new Runtime();
        try (Repository repository = new Repository("http-operation", "http://127.0.0.1:"
                + device.getAddress().getPort() + "/plc/api")) {
            RoutePlan javaPlan = repository.compile();
            runtime.startComponent();
            int port = freePort();
            runtime.listener(port);
            int baselineRoutes = runtime.routes();
            int baselineEndpoints = runtime.endpoints();
            HttpClient caller = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
            // Each the variables BaSyx delegates; the request Lua passes them to the device unchanged.
            List<String> payloads = List.of("{\"Restart\":{\"x\":1},\"text\":\"001\",\"flag\":true,\"nil\":null}",
                    "{\"Restart\":[2,3],\"text\":\"002\"}", "{}", "{\"last\":4}");
            for (RoutePlan configuration : List.of(javaPlan)) {
                int before = sends.get();
                var owner = runtime.installBundle("exclusive-http-parity", configuration);
                List<Request> observedCommands = new ArrayList<>();
                List<Reply> observedReplies = new ArrayList<>();
                try {
                    assertEquals(before, sends.get(), "install must not invoke an ACTION");
                    for (int scenario = 0; scenario < payloads.size(); scenario++) {
                        String payload = payloads.get(scenario);
                        deviceStatus.set(scenario == 2 ? 503 : 200);
                        deviceReply.set(scenario == 3 ? "not-json" : payload);
                        var reply = caller.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                                + OperationCalls.path("a/submodels/ControlComponentInstance", "Restart"))).timeout(Duration.ofSeconds(6))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(OperationCalls.of(payload))).build(), HttpResponse.BodyHandlers.ofString());
                        assertEquals(before + scenario + 1, sends.get(), "exactly one send even on remote/response failure");
                        Request command = commands.poll(2, TimeUnit.SECONDS);
                        assertNotNull(command, "real device command");
                        assertEquals(new Request("POST", "/plc/api/actions/Restart", "mode=quick", json(payload)), command);
                        assertEquals(scenario < 2 ? 200 : 502, reply.statusCode(), reply.body());
                        JsonElement body = json(OperationCalls.outputs(reply.body()));
                        assertNotNull(body, "a caller reply, including JSON null, must remain parseable");
                        if (scenario < 2) assertEquals(json("{}"), body,
                                "without a response mapping the Operation answers with no outputs");
                        observedCommands.add(command);
                        observedReplies.add(new Reply(reply.statusCode(), body));
                    }
                } finally { runtime.removeBundle(owner); }
                assertFalse(owner.active());
                assertNull(runtime.managedBundle("exclusive-http-parity"));
                assertEquals(baselineRoutes, runtime.routes(), "old routes retire before installing the next owner");
                assertEquals(baselineEndpoints, runtime.endpoints());
                assertNull(commands.poll(150, TimeUnit.MILLISECONDS), "no command replay after removal");
            }
            assertEquals(4, sends.get());
            assertEquals(0, repository.nonGetRequests(), "compiler is read-only");
        } finally { try { runtime.stopComponent(); } finally { device.stop(0); } }
    }

    @Test
    void mqttLatestValueDeliveryAndOrderedAasWritesMatchWithoutOverlappingSubscribers() throws Exception {
        Mqtt5TestBroker broker = new Mqtt5TestBroker();
        int brokerPort = broker.port();
        Runtime runtime = new Runtime();
        try (Repository repository = new Repository("fan-in-out", "mqtt://127.0.0.1:" + brokerPort + "/base")) {
            BlockingQueue<Request> writes = new LinkedBlockingQueue<>();
            repository.server.createContext("/submodels", exchange -> {
                writes.add(request(exchange));
                respond(exchange, 200, "{}");
            });
            RoutePlan javaPlan = repository.compile();
            runtime.startComponent();
            int baselineRoutes = runtime.routes();
            int baselineEndpoints = runtime.endpoints();
            String path = "/submodels/" + Base64.getUrlEncoder().withoutPadding().encodeToString("contractAAS/submodels/KPI"
                    .getBytes(StandardCharsets.UTF_8)) + "/submodel-elements/";
            for (RoutePlan configuration : List.of(javaPlan)) {
                var owner = runtime.installBundle("exclusive-mqtt-parity", configuration);
                List<Request> observedWrites = new ArrayList<>();
                try {
                    assertNull(writes.poll(150, TimeUnit.MILLISECONDS), "no unsolicited writes on activation");
                    broker.publish("base/state", "\"001\"");
                    assertNull(writes.poll(250, TimeUnit.MILLISECONDS), "ALL-source gate must hold before Mode arrives, including after owner change");
                    broker.publish("base/mode", "true");
                    collectWrites(writes, observedWrites, path);
                    broker.publish("base/state", "[false,2,\"002\"]");
                    collectWrites(writes, observedWrites, path);
                    broker.publish("base/mode", "{\"0\":\"003\",\"nested\":[true,4]}");
                    collectWrites(writes, observedWrites, path);
                    assertNull(writes.poll(200, TimeUnit.MILLISECONDS), "two writes per accepted update after ALL-source gate, no duplicates");
                } finally { runtime.removeBundle(owner); }
                assertFalse(owner.active());
                assertNull(runtime.managedBundle("exclusive-mqtt-parity"));
                assertEquals(baselineRoutes, runtime.routes(), "old subscribers retire before new owner activation");
                assertEquals(baselineEndpoints, runtime.endpoints(), "old MQTT/AAS endpoints retire before new owner activation");
                broker.publish("base/state", "\"retired-owner\"");
                assertNull(writes.poll(250, TimeUnit.MILLISECONDS), "retired MQTT subscriber cannot write");
                assertEquals(6, observedWrites.size(), "two ordered writes per accepted update");
            }
            assertEquals(0, repository.nonGetRequests());
        } finally {
            try { runtime.stopComponent(); } finally { broker.close(); }
        }
    }

    private static void collectWrites(BlockingQueue<Request> writes, List<Request> observed, String path) throws Exception {
        for (var sink : List.of(path + "Out/$value", path + "Mirror/$value")) {
            Request actual = writes.poll(5, TimeUnit.SECONDS);
            assertNotNull(actual, "real AAS write must arrive");
            assertEquals(new Request("PATCH", sink, null, actual.body()), actual, "descriptor sink order and target");
            assertNotNull(actual.body(), "the actual old-runtime sink body must remain valid typed JSON");
            observed.add(actual);
        }
    }


    private static ClassLoader loader() { return ManagedProtocolOutcomeParityTest.class.getClassLoader(); }
    private static String resource(String path) throws Exception {
        try (var input = loader().getResourceAsStream("runtime-model-contracts/" + path)) {
            assertNotNull(input, path);
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
    private static JsonElement json(String value) { return JsonParser.parseString(value); }

    private static Map<String, String> typedOutputModels() throws Exception {
        JsonObject aid = JsonParser.parseString(resource("opcua-method/aid.json")).getAsJsonObject();
        JsonObject action = child(child(child(child(aid, "interface_opcua"), "InteractionMetadata"), "actions"), "Calibrate");
        JsonObject output = schemaElement("output", "object");
        output.add("semanticId", json("{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\","
                + "\"value\":\"https://www.w3.org/2019/wot/td#hasOutputSchema\"}]}"));
        JsonObject properties = schemaElement("properties", null);
        for (String[] field : List.of(new String[] { "State", "string" }, new String[] { "Label", "string" },
                new String[] { "Nullable", "null" }, new String[] { "Calibrate", "boolean" }, new String[] { "Count", "integer" }))
            properties.getAsJsonArray("value").add(schemaElement(field[0], field[1]));
        JsonObject samples = schemaElement("Samples", "array");
        samples.getAsJsonArray("value").add(schemaElement("items", "integer"));
        properties.getAsJsonArray("value").add(samples);
        output.getAsJsonArray("value").add(properties);
        action.getAsJsonArray("value").add(output);
        JsonObject cci = JsonParser.parseString(resource("opcua-method/dependencies/0.json")).getAsJsonObject();
        JsonObject operation = child(child(child(cci, "Skills"), "Calibrate"), "operation");
        operation.add("outputVariables", json("[{\"idShort\":\"Calibrate\",\"value\":{\"modelType\":\"Property\",\"idShort\":\"Calibrate\",\"valueType\":\"xs:boolean\"}},"
                + "{\"idShort\":\"State\",\"value\":{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:string\"}},"
                + "{\"idShort\":\"Label\",\"value\":{\"modelType\":\"Property\",\"idShort\":\"Label\",\"valueType\":\"xs:string\"}},"
                + "{\"idShort\":\"Count\",\"value\":{\"modelType\":\"Property\",\"idShort\":\"Count\",\"valueType\":\"xs:integer\"}}]"));
        return Map.of("a/submodels/AID", aid.toString(), "a/submodels/ControlComponentInstance", cci.toString());
    }
    private static JsonObject child(JsonObject parent, String name) {
        for (var entry : parent.getAsJsonArray(parent.has("submodelElements") ? "submodelElements" : "value")) {
            JsonObject child = entry.getAsJsonObject();
            if (child.has("idShort") && name.equals(child.get("idShort").getAsString())) return child;
        }
        throw new IllegalStateException("missing fixture element " + name);
    }
    private static JsonObject schemaElement(String name, String type) {
        JsonObject result = new JsonObject();
        result.addProperty("modelType", "SubmodelElementCollection");
        result.addProperty("idShort", name);
        JsonArray value = new JsonArray();
        if (type != null) value.add(json("{\"modelType\":\"Property\",\"idShort\":\"type\",\"valueType\":\"xs:string\",\"value\":\"" + type + "\"}"));
        result.add("value", value);
        return result;
    }
    private static int freePort() throws Exception { try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); } }
    private static Request request(HttpExchange exchange) throws java.io.IOException {
        return new Request(exchange.getRequestMethod(), exchange.getRequestURI().getPath(), exchange.getRequestURI().getQuery(),
                json(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
    }
    private static void respond(HttpExchange exchange, int status, String body) throws java.io.IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static final class Repository implements AutoCloseable {
        /** Device-side peer: AAS write sinks address it; models are served by {@link #repository}. */
        final HttpServer server;
        final URI base;
        final FakeAasRepository repository = new FakeAasRepository();
        final Map<String, String> models = repository.models();
        Repository(String fixture, String deviceUri) throws Exception {
            this(fixture, deviceUri, Map.of());
        }
        Repository(String fixture, String deviceUri, Map<String, String> modelOverrides) throws Exception {
            // What aas-model publishes: resolved templates and the Operation's delegation URL.
            var context = json(resource(fixture + "/expected.json")).getAsJsonObject().getAsJsonObject("compilerContext");
            String aasId = context.get("aas_id").getAsString(), aasIdShort = context.get("aas_id_short").getAsString();
            for (String file : List.of("aimc.json", "aid.json", "dependencies/0.json")) {
                String model = resource(fixture + "/" + file).replace("http://192.168.0.60:8080/plc/api", deviceUri)
                        .replace("mqtt://192.168.0.104:1883/base", deviceUri).replace("opc.tcp://192.168.0.90:4840", deviceUri);
                repository.put(PublishedModels.publish(model, aasId, aasIdShort));
            }
            modelOverrides.forEach((id, model) -> models.put(id, PublishedModels.publish(
                    model.replace("opc.tcp://192.168.0.90:4840", deviceUri), aasId, aasIdShort)));
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            base = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            server.start();
        }
        int nonGetRequests() { return repository.writes(); }
        /** Authors the caller response on the fixture's operation mapping. */
        void respond(String sourceId, String callerSinkId, String lua) {
            editMapping(mapping -> ResponseMappings.respond(mapping, sourceId, callerSinkId, lua));
        }
        void withoutResponse() { editMapping(ResponseMappings::none); }
        private void editMapping(java.util.function.Consumer<JsonObject> edit) {
            String id = models.entrySet().stream().filter(entry -> entry.getValue()
                    .contains("AssetInterfacesMappingConfiguration/2/0/Submodel")).findFirst().orElseThrow().getKey();
            JsonObject aimc = json(models.get(id)).getAsJsonObject();
            edit.accept(child(aimc, "MappingConfigurations").getAsJsonArray("value").get(0).getAsJsonObject());
            models.put(id, aimc.toString());
        }
        RoutePlan compile() throws Exception {
            var client = repository.client();
            String aimc = models.values().stream().filter(model -> json(model).getAsJsonObject().toString()
                    .contains("AssetInterfacesMappingConfiguration/2/0/Submodel")).findFirst().orElseThrow();
            var plan = new MappingPlanCompiler(new AasReferenceResolver(client)).compile(WireProjection.aimc(dk.aau.smartproductionlab.dmp.aas.model.Aas.read(aimc, org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
            assertEquals(AasRepositoryClient.Status.FOUND, plan.status(), String.valueOf(plan));
            var compiled = new ConnectorConfigurationFactory().createBundles(plan.value(),
                    new ConnectorConfigurationFactory.RuntimeSettings(base));
            assertTrue(compiled.issues().isEmpty(), String.valueOf(compiled.issues()));
            assertEquals(1, compiled.bundles().size());
            return compiled.bundles().get(0).route();
        }
        @Override public void close() { server.stop(0); repository.close(); }
    }
}
