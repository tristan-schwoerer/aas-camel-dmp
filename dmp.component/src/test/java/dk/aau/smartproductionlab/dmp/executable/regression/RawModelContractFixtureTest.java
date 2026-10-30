/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler;
import dk.aau.smartproductionlab.dmp.management.model.WireModel;
import dk.aau.smartproductionlab.dmp.management.model.WireProjection;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * The models aas-model authors, as the DMP reads and compiles them. The fixtures are generated
 * by {@code fixtures/contracts.py} with aas-model: synthetic cases, one protocol or mapping
 * form each, and every resource of aas-model's catalog as registration builds it. A changed
 * outcome after regenerating is a change in aas-model or in the DMP, to be reviewed.
 */
public class RawModelContractFixtureTest {
	private static final String AIMC_SEMANTIC = "https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel";
	private static final String AID_SEMANTIC = "https://admin-shell.io/idta/AssetInterfacesDescription/1/2/Submodel";

	@Test
	public void theFixturesAreExactlyWhatAasModelGenerated() throws Exception {
		JsonObject manifest = json("manifest.json");
		assertEquals(2, manifest.get("formatVersion").getAsInt());
		assertEquals("aas-camel-dmp/fixtures/contracts.py", manifest.get("generator").getAsString());
		for (var entry : manifest.getAsJsonObject("resources").entrySet()) {
			byte[] bytes = resource(entry.getKey());
			JsonObject integrity = entry.getValue().getAsJsonObject();
			assertEquals(integrity.get("byteLength").getAsInt(), bytes.length, entry.getKey());
			assertEquals(integrity.get("sha256").getAsString(),
					HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)),
					entry.getKey() + " was edited by hand; regenerate it with fixtures/contracts.py");
		}
		for (JsonObject fixture : cases()) {
			for (String role : List.of("aimc", "aid", "expected"))
				assertTrue(manifest.getAsJsonObject("resources").has(fixture.get(role).getAsString()), fixture.get(role).getAsString());
			for (JsonElement dependency : fixture.getAsJsonArray("dependencies"))
				assertTrue(manifest.getAsJsonObject("resources").has(fixture.get("name").getAsString() + "/"
						+ dependency.getAsJsonObject().get("path").getAsString()));
		}
	}

	@Test
	public void everyCaseProjectsAsAid12AndAimc20() throws Exception {
		for (JsonObject fixture : cases()) {
			String name = fixture.get("name").getAsString();
			WireModel.Aimc aimc = WireProjection.aimc(submodel(fixture.get("aimc").getAsString()));
			WireModel.Aid aid = WireProjection.aid(submodel(fixture.get("aid").getAsString()));
			assertEquals(AIMC_SEMANTIC, aimc.submodel().getSemanticId().getKeys().get(0).getValue(), name);
			assertEquals(AID_SEMANTIC, aid.submodel().getSemanticId().getKeys().get(0).getValue(), name);
			JsonArray authored = json(fixture.get("expected").getAsString()).getAsJsonArray("authoredMappings");
			assertEquals(authored.size(), aimc.mappings().size(), name);
			for (int i = 0; i < authored.size(); i++) {
				JsonObject mapping = authored.get(i).getAsJsonObject();
				assertEquals(strings(mapping.getAsJsonArray("sourceIds")),
						aimc.mappings().get(i).sources().stream().map(WireModel.Endpoint::id).toList(), name);
				assertEquals(strings(mapping.getAsJsonArray("sinkIds")),
						aimc.mappings().get(i).sinks().stream().map(WireModel.Endpoint::id).toList(), name);
			}
			for (JsonElement dependency : fixture.getAsJsonArray("dependencies")) {
				String path = name + "/" + dependency.getAsJsonObject().get("path").getAsString();
				assertEquals(dependency.getAsJsonObject().get("id").getAsString(), submodel(path).getId(), path);
			}
		}
	}

	@Test
	public void everyCaseResolvesAndCompilesAsTheManifestExpects() throws Exception {
		List<String> unexpected = new ArrayList<>();
		try (var repository = new FakeAasRepository("")) {
			var client = new AasRepositoryClient(repository.uri(), Duration.ofSeconds(1), Duration.ofSeconds(1), 10, 2, null);
			for (JsonObject fixture : cases()) {
				String name = fixture.get("name").getAsString();
				var plan = plan(fixture, repository, client);
				var compiled = plan.status() == AasRepositoryClient.Status.FOUND ? compile(fixture, plan.value(), true) : null;
				boolean compiles = compiled != null && compiled.issues().isEmpty()
						&& compiled.bundles().size() == plan.value().mappings().size();
				if (compiles != fixture.get("compiles").getAsBoolean())
					unexpected.add(name + (compiled == null ? ": " + plan.status() + " " + plan.detail() : ": " + compiled.issues()));
			}
		}
		assertTrue(unexpected.isEmpty(), "outcomes differ from the manifest:\n" + String.join("\n", unexpected));
	}

	/** Without InfluxDB a station's series sinks do not compile; each issue names the endpoint it is on. */
	@Test
	public void issuesNameTheirEndpointIncludingResponseSinks() throws Exception {
		try (var repository = new FakeAasRepository("")) {
			var client = new AasRepositoryClient(repository.uri(), Duration.ofSeconds(1), Duration.ofSeconds(1), 10, 2, null);
			JsonObject fixture = cases().stream().filter(c -> c.get("name").getAsString().equals("station-imaDispensing"))
					.findFirst().orElseThrow();
			var plan = plan(fixture, repository, client);
			assertEquals(AasRepositoryClient.Status.FOUND, plan.status());
			var paths = compile(fixture, plan.value(), false).issues().stream().map(ConnectorConfigurationFactory.Issue::path).toList();
			assertFalse(paths.isEmpty());
			assertTrue(paths.stream().allMatch(path -> path.matches("MappingConfigurations/value/\\d+/(Sinks|ResponseSinks)/value/\\d{1,2}")),
					String.valueOf(paths));
			assertTrue(paths.stream().anyMatch(path -> path.contains("/ResponseSinks/")), String.valueOf(paths));
		}
	}

	private AasRepositoryClient.Result<MappingPlanCompiler.Plan> plan(JsonObject fixture, FakeAasRepository repository, AasRepositoryClient client) throws Exception {
		String name = fixture.get("name").getAsString();
		JsonObject context = context(fixture);
		String aasId = context.get("aas_id").getAsString(), aasIdShort = context.get("aas_id_short").getAsString();
		repository.models().clear();
		String aimcJson = PublishedModels.publish(text(fixture.get("aimc").getAsString()), aasId, aasIdShort);
		repository.put(aimcJson).put(PublishedModels.publish(text(fixture.get("aid").getAsString()), aasId, aasIdShort));
		for (JsonElement dependency : fixture.getAsJsonArray("dependencies"))
			repository.put(PublishedModels.publish(text(name + "/" + dependency.getAsJsonObject().get("path").getAsString()),
					aasId, aasIdShort));
		return new MappingPlanCompiler(new AasReferenceResolver(client)).compile(WireProjection.aimc(Aas.read(aimcJson, Submodel.class)));
	}

	/** With {@code influx}, the workspace's InfluxDB that TimeSeries segments name, as the generator recorded it. */
	private ConnectorConfigurationFactory.BundleResult compile(JsonObject fixture, MappingPlanCompiler.Plan plan,
			boolean influx) throws Exception {
		JsonObject context = context(fixture);
		var settings = influx && context.has("influx_url") ? new dk.aau.smartproductionlab.dmp.influxdb2.InfluxSettings(
				URI.create(context.get("influx_url").getAsString()), context.get("influx_org").getAsString(), "fixture-token") : null;
		return new ConnectorConfigurationFactory().createBundles(plan,
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create(context.get("aas_base_url").getAsString()), settings));
	}

	private JsonObject context(JsonObject fixture) throws Exception {
		return json(fixture.get("expected").getAsString()).getAsJsonObject("compilerContext");
	}

	private List<JsonObject> cases() throws Exception {
		List<JsonObject> cases = new ArrayList<>();
		for (JsonElement fixture : json("manifest.json").getAsJsonArray("cases")) cases.add(fixture.getAsJsonObject());
		assertFalse(cases.isEmpty());
		return cases;
	}

	private byte[] resource(String path) throws Exception {
		try (var stream = getClass().getClassLoader().getResourceAsStream("runtime-model-contracts/" + path)) {
			assertNotNull(stream, "fixture missing: " + path);
			return stream.readAllBytes();
		}
	}

	private String text(String path) throws Exception {
		return new String(resource(path), StandardCharsets.UTF_8);
	}

	private JsonObject json(String path) throws Exception {
		return JsonParser.parseString(text(path)).getAsJsonObject();
	}

	private Submodel submodel(String path) throws Exception {
		return Aas.read(text(path), Submodel.class);
	}

	private static List<String> strings(JsonArray values) {
		List<String> result = new ArrayList<>();
		for (JsonElement value : values) result.add(value.getAsString());
		return result;
	}
}
