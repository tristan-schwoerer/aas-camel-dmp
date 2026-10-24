package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import dk.aau.smartproductionlab.dmp.aas.operation.OperationRoute;
import dk.aau.smartproductionlab.dmp.core.plan.MappingRoute;
import dk.aau.smartproductionlab.dmp.core.plan.RoutePlan;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.AffordanceValidation;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler;
import dk.aau.smartproductionlab.dmp.management.model.WireProjection;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * The TD DataSchema terms aas-model adds to AID (required at every depth, oneOf,
 * exclusive bounds, multipleOf, pattern, content terms, items holding objects) reach the
 * schemas the DMP validates commands and replies with; additionalResponses are accepted.
 * Both fixtures are aas-model's builder output.
 */
class AidDataSchemaTermsTest {

	@Test
	void requiredListsOfActionInputAndOutputAreEnforced() throws Exception {
		// Before, required was an AID template qualifier the compiler skipped: nothing was required.
		OperationRoute dispensing = bridge(compile("station-imaDispensing"), "Dispensing");
		assertEquals(List.of("Uuid", "Volume"), dispensing.command().parameterRequired());
		assertTrue(dispensing.command().responseRequired().containsAll(List.of("Uuid", "State")),
				String.valueOf(dispensing.command().responseRequired()));
	}

	@Test
	void everyDataSchemaTermReachesTheCompiledValidation() throws Exception {
		List<RoutePlan> routes = compile("td-data-schemas");
		OperationRoute inspect = bridge(routes, "Inspect");
		AffordanceValidation command = validation(commandSchema(routes), "command");
		AffordanceValidation reply = validation(inspect.command().responseSchema(), "reply");

		String uuid = "\"00000000-0000-0000-0000-000000000001\"";
		assertTrue(command.incoming(json("{\"Uuid\":" + uuid + ",\"Speed\":1.5,\"Note\":null,"
				+ "\"Points\":[{\"X\":1,\"Label\":\"a\"}]}")));
		assertTrue(command.incoming(json("{\"Uuid\":" + uuid + ",\"Speed\":0.5,\"Note\":\"short\"}")));
		for (String invalid : List.of(
				"{\"Speed\":1.5}",                                   // required Uuid
				"{\"Uuid\":\"not-a-uuid!\",\"Speed\":1.5}",          // pattern
				"{\"Uuid\":" + uuid + ",\"Speed\":0}",              // exclusiveMinimum
				"{\"Uuid\":" + uuid + ",\"Speed\":1.2}",            // multipleOf
				"{\"Uuid\":" + uuid + ",\"Speed\":1,\"Note\":\"far too long\"}", // oneOf (maxLength branch)
				"{\"Uuid\":" + uuid + ",\"Speed\":1,\"Note\":7}",                // oneOf (no branch)
				"{\"Uuid\":" + uuid + ",\"Speed\":1,\"Points\":[{\"Label\":\"a\"}]}")) // required inside items
			assertFalse(command.incoming(json(invalid)), invalid);

		assertTrue(reply.incoming(json("{\"State\":\"SUCCESS\",\"Image\":\"aGk=\"}")));
		assertFalse(reply.incoming(json("{\"Image\":\"aGk=\"}")), "required State");
		assertFalse(reply.incoming(json("{\"State\":\"DONE\"}")), "enum");
	}

	private static Object json(String text) {
		return JsonCodec.parseJson(text);
	}

	private static AffordanceValidation validation(Object schema, String label) {
		assertNotNull(schema, label + " schema");
		return new AffordanceValidation(schema, label, AffordanceValidation.Mode.ON);
	}

	private static Object commandSchema(List<RoutePlan> routes) {
		return routes.stream().flatMap(route -> sinks(route).stream()).map(Sink::schema)
				.filter(schema -> schema != null && schema.containsKey("properties")).findFirst().orElse(null);
	}

	/** Every sink a route writes: the command's or the mapping's, and the response half's. */
	private static List<Sink> sinks(RoutePlan route) {
		List<Sink> sinks = new ArrayList<>();
		if (route instanceof OperationRoute operation) {
			sinks.add(operation.command().sink());
			if (operation.response() != null) sinks.addAll(operation.response().sinks());
		} else if (route instanceof MappingRoute mapping) {
			sinks.addAll(mapping.sinks());
			if (mapping.response() != null) sinks.addAll(mapping.response().sinks());
		}
		return sinks;
	}

	private static OperationRoute bridge(List<RoutePlan> routes, String action) {
		for (RoutePlan route : routes)
			if (route instanceof OperationRoute bridge && action.equals(bridge.identity().action())) return bridge;
		throw new AssertionError("no bridge route for " + action);
	}

	/** The fixture published as aas-model's registration publishes it, compiled; the routes of all bundles. */
	private static List<RoutePlan> compile(String fixture) throws Exception {
		JsonObject manifest = JsonParser.parseString(resource("manifest.json")).getAsJsonObject();
		JsonObject entry = null;
		for (var item : manifest.getAsJsonArray("cases"))
			if (fixture.equals(item.getAsJsonObject().get("name").getAsString())) entry = item.getAsJsonObject();
		assertNotNull(entry, fixture);
		JsonObject context = JsonParser.parseString(resource(fixture + "/expected.json")).getAsJsonObject()
				.getAsJsonObject("compilerContext");
		String aasId = context.get("aas_id").getAsString(), aasIdShort = context.get("aas_id_short").getAsString();
		List<String> files = new ArrayList<>(List.of("aimc.json", "aid.json"));
		for (var dependency : entry.getAsJsonArray("dependencies"))
			files.add(dependency.getAsJsonObject().get("path").getAsString());
		try (FakeAasRepository repository = new FakeAasRepository()) {
			String aimc = PublishedModels.publish(resource(fixture + "/aimc.json"), aasId, aasIdShort);
			for (String file : files)
				repository.put(file.equals("aimc.json") ? aimc : PublishedModels.publish(resource(fixture + "/" + file), aasId, aasIdShort));
			var plan = new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compile(WireProjection.aimc(
					dk.aau.smartproductionlab.dmp.aas.model.Aas.read(aimc, org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
			assertEquals(AasRepositoryClient.Status.FOUND, plan.status(), String.valueOf(plan));
			// The station's history and skill logs name the workspace's InfluxDB.
			var influx = context.has("influx_url") ? new dk.aau.smartproductionlab.dmp.influxdb2.InfluxSettings(
					URI.create(context.get("influx_url").getAsString()), context.get("influx_org").getAsString(), "fixture-token") : null;
			var compiled = new ConnectorConfigurationFactory().createBundles(plan.value(),
					new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://127.0.0.1:1"), influx));
			assertTrue(compiled.issues().isEmpty(), String.valueOf(compiled.issues()));
			return compiled.bundles().stream().map(ConnectorConfigurationFactory.CompiledBundle::route).toList();
		}
	}

	private static String resource(String path) throws Exception {
		try (var input = AidDataSchemaTermsTest.class.getClassLoader().getResourceAsStream("runtime-model-contracts/" + path)) {
			assertNotNull(input, path);
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
