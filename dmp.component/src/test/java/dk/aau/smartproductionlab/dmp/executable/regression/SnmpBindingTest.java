/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.core.plan.RoutePlan;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler;
import dk.aau.smartproductionlab.dmp.management.model.WireProjection;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;
import dk.aau.smartproductionlab.dmp.snmp.testing.SnmpTestAgent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.TimeTicks;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;

/**
 * The AID 1.2 SNMP binding end to end (the generated {@code snmp} case): sysUpTime is read with
 * GET at the polling interval and written to the AAS; sysContact is written with SET as the
 * agent's OCTET STRING.
 */
class SnmpBindingTest {
	private SnmpTestAgent agent;
	private HttpServer aas;
	private final List<String> writes = new CopyOnWriteArrayList<>();
	private DmpComponent runtime;

	@BeforeEach
	void start() throws Exception {
		agent = new SnmpTestAgent()
				.put("1.3.6.1.2.1.1.3.0", new TimeTicks(4242), false)
				.put("1.3.6.1.2.1.1.4.0", new OctetString("nobody"), true);
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
			aas.stop(0);
			agent.close();
		}
	}

	@Test
	void anObjectIsReadWithGetAndAnotherWrittenWithSet() throws Exception {
		runtime.installBundle("snmp", compile());
		await().atMost(Duration.ofSeconds(10)).until(() -> writes.stream().anyMatch(body -> body.contains("4242")));
		await().atMost(Duration.ofSeconds(10)).until(() -> new OctetString("ops@example.com").equals(agent.get("1.3.6.1.2.1.1.4.0")));
	}

	private RoutePlan compile() throws Exception {
		JsonObject aid = JsonParser.parseString(resource("snmp/aid.json")).getAsJsonObject();
		child(child(child(aid, "interface_snmp"), "EndpointMetadata"), "base").addProperty("value", "snmp://127.0.0.1:" + agent.port() + "/");
		try (var repository = new FakeAasRepository()) {
			String aimc = PublishedModels.publish(resource("snmp/aimc.json"), "contractAAS", "contract");
			repository.put(aimc).put(PublishedModels.publish(aid.toString(), "contractAAS", "contract"))
					.put(PublishedModels.publish(resource("snmp/dependencies/0.json"), "contractAAS", "contract"));
			var plan = new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compile(WireProjection.aimc(
					dk.aau.smartproductionlab.dmp.aas.model.Aas.read(aimc, org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
			assertEquals(AasRepositoryClient.Status.FOUND, plan.status(), String.valueOf(plan));
			var compiled = new ConnectorConfigurationFactory().createBundles(plan.value(),
					new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://127.0.0.1:" + aas.getAddress().getPort())));
			assertTrue(compiled.issues().isEmpty(), String.valueOf(compiled.issues()));
			return compiled.bundles().get(0).route();
		}
	}

	private static JsonObject child(JsonObject parent, String name) {
		for (var entry : parent.getAsJsonArray(parent.has("submodelElements") ? "submodelElements" : "value")) {
			JsonObject child = entry.getAsJsonObject();
			if (child.has("idShort") && name.equals(child.get("idShort").getAsString())) return child;
		}
		throw new IllegalStateException("missing fixture element " + name);
	}

	private static String resource(String path) throws Exception {
		try (var input = SnmpBindingTest.class.getClassLoader().getResourceAsStream("runtime-model-contracts/" + path)) {
			assertNotNull(input, path);
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
