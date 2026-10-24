/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import dk.aau.smartproductionlab.dmp.influxdb2.InfluxSettings;
import dk.aau.smartproductionlab.dmp.influxdb2.RecordColumn;
import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.MappingRoute;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.influxdb2.configuration.InfluxDb2Endpoint;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory.DesiredBundle;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler;
import org.junit.jupiter.api.Test;

/**
 * A Sink on a TimeSeries LinkedSegment, compiled through discovery from the Variables and Parameters submodels
 * aas-model generates ({@code timeseries/*.json}): one series {@code State.Station} with fields PackMLState, Count,
 * Position.X and a reference to {@code Parameters/Location} (x, y), owned by {@code imaDispensing}.
 */
class TimeSeriesDiscoveryTest {
	private static final String VARIABLES = "https://smartproductionlab.aau.dk/aas/imaDispensing/submodels/Variables";
	private static final String PARAMETERS = "https://smartproductionlab.aau.dk/aas/imaDispensing/submodels/Parameters";
	private static final String SKILLS = "urn:fixture/submodels/Skills";
	private static final String AIMC_ID = "urn:fixture/submodels/AIMC";
	private static final String DATA_ID = "urn:fixture/submodels/Data";
	private static final String SEGMENT = "State.Station.Segments.Live";
	private static final InfluxSettings INFLUX = new InfluxSettings(URI.create("http://influxdb:8086"), "ap2030", "token");
	/** An InfluxDB endpoint binds nothing; a URI is resolved without a bundle. */
	private static final BundleResources RESOURCES = new BundleResources() {
		@Override public <T extends ManagedRouteResource> T own(T resource) { return resource; }
	};

	@Test
	void aSegmentSinkWritesTheSeriesRecordToItsMeasurement() throws Exception {
		try (var repository = fixture(variables())) {
			DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
			assertTrue(bundle.valid(), bundle.issues().toString());
			Sink sink = influxSink(bundle);
			var target = (InfluxDb2Endpoint) sink.endpoint();
			assertEquals("Live", sink.sinkId());
			assertEquals("station_state", target.measurement());
			assertEquals("ap2030", target.bucket());
			assertEquals(Map.of("asset", "imaDispensing"), target.tags());
			assertEquals(List.of(new RecordColumn("PackMLState", "xs:string"), new RecordColumn("Count", "xs:long"),
					new RecordColumn("Position.X", "xs:double"), new RecordColumn("Location.x", "xs:double"),
					new RecordColumn("Location.y", "xs:double")), target.columns(), "a reference counts as if declared inline");
			String uri = target.uri(RESOURCES);
			assertTrue(uri.startsWith("dmp-influxdb2:station_state?"), uri);
			assertFalse(uri.contains("token"));
			assertTrue(bundle.desired().dependencies().contains(new MappingPlanCompiler.Dependency("series", VARIABLES, "State.Station")),
					bundle.desired().dependencies().toString());
			assertTrue(bundle.desired().dependencies().contains(new MappingPlanCompiler.Dependency("submodel", PARAMETERS, "Location")),
					"the referenced element is part of the row shape: " + bundle.desired().dependencies());
		}
	}

	@Test
	void aChangeBesideTheSegmentIsRelevantButAnUnrelatedGroupIsNot() throws Exception {
		try (var repository = fixture(variables())) {
			var discovery = discovery(repository, INFLUX);
			discovery.scan();
			assertTrue(relevant(discovery, "State.Station.Metadata.Record.Count"), "the Record is the row shape");
			assertTrue(relevant(discovery, "State.Station.Segments.Live.InfluxDb2Target.Measurement"));
			assertFalse(relevant(discovery, "Measurements.Weight"));
			assertTrue(discovery.relevant(AasControlEvent.parse("{\"type\":\"SME_CREATED\",\"id\":\"" + PARAMETERS
					+ "\",\"smElementPath\":\"Location.z\"}")), "a new member of a referenced element changes the row");
		}
	}

	@Test
	void withoutAConfiguredInfluxDbTheMappingIsBlocked() throws Exception {
		try (var repository = fixture(variables())) {
			DesiredBundle bundle = only(discovery(repository, null).scan());
			assertFalse(bundle.valid());
			assertTrue(issues(bundle).contains(InfluxSettings.URL), issues(bundle));
		}
	}

	@Test
	void aSegmentOnAnotherInfluxDbOrOrgIsBlocked() throws Exception {
		try (var repository = fixture(variables())) {
			var elsewhere = new InfluxSettings(URI.create("http://influxdb:8086"), "other-org", "token");
			DesiredBundle bundle = only(discovery(repository, elsewhere).scan());
			assertFalse(bundle.valid());
			assertTrue(issues(bundle).contains("does not name the configured InfluxDB"), issues(bundle));
		}
	}

	@Test
	void anOperationAndAListAreResolvedThroughAReference() throws Exception {
		String withSkill = reference(variables(), "Occupy", SKILLS, "SubmodelElementCollection:Occupy", "Operation:operation");
		try (var repository = fixture(withSkill).put(skills())) {
			DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
			assertTrue(bundle.valid(), issues(bundle));
			var sink = (InfluxDb2Endpoint) influxSink(bundle).endpoint();
			assertEquals(List.of(new RecordColumn("Occupy.Uuid", "xs:string"), new RecordColumn("Occupy.Target.x", "xs:double"),
					new RecordColumn("Occupy.Waypoints[0]", "xs:int"), new RecordColumn("Occupy.Waypoints[1]", "xs:int"),
					new RecordColumn("Occupy.State", "xs:string")),
					sink.columns().stream().filter(column -> column.path().startsWith("Occupy.")).toList(),
					"input, InOut and output variables in order, collections by idShort, lists by index");
		}
	}

	@Test
	void rangesFilesAndTextsAreFields() throws Exception {
		String extended = entries(variables(),
				"{\"modelType\":\"Range\",\"idShort\":\"Tolerance\",\"valueType\":\"xs:double\"}",
				"{\"modelType\":\"File\",\"idShort\":\"Report\",\"contentType\":\"application/pdf\"}",
				"{\"modelType\":\"MultiLanguageProperty\",\"idShort\":\"Batch\",\"value\":["
						+ "{\"language\":\"en\",\"text\":\"Batch name\"},{\"language\":\"de\",\"text\":\"Chargenname\"}]}",
				"{\"modelType\":\"Property\",\"idShort\":\"Season\",\"valueType\":\"xs:gYear\"}");
		try (var repository = fixture(extended)) {
			DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
			assertTrue(bundle.valid(), issues(bundle));
			var sink = (InfluxDb2Endpoint) influxSink(bundle).endpoint();
			assertEquals(List.of(new RecordColumn("Tolerance.min", "xs:double"), new RecordColumn("Tolerance.max", "xs:double"),
					new RecordColumn("Report", "xs:string"), new RecordColumn("Batch.en", "xs:string"),
					new RecordColumn("Batch.de", "xs:string"), new RecordColumn("Season", "xs:gYear")),
					sink.columns().subList(5, sink.columns().size()));
		}
	}

	@Test
	void aTextWithoutLanguagesOrABlobBlocksTheMapping() throws Exception {
		for (String[] entry : new String[][] {
				{ "{\"modelType\":\"MultiLanguageProperty\",\"idShort\":\"Batch\"}", "declares no language" },
				{ "{\"modelType\":\"Blob\",\"idShort\":\"Image\",\"contentType\":\"image/png\"}", "has no InfluxDB form" } }) {
			try (var repository = fixture(entries(variables(), entry[0]))) {
				DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
				assertFalse(bundle.valid());
				assertTrue(issues(bundle).contains(entry[1]), issues(bundle));
			}
		}
	}

	@Test
	void aReferenceToAMissingElementBlocksTheMapping() throws Exception {
		String broken = reference(variables(), "Ghost", PARAMETERS, "SubmodelElementCollection:Nowhere");
		try (var repository = fixture(broken)) {
			DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
			assertFalse(bundle.valid());
			assertTrue(issues(bundle).contains("Record reference Ghost"), issues(bundle));
		}
	}

	@Test
	void aSegmentSourceReadsTheLatestRowAtItsPollingInterval() throws Exception {
		try (var repository = new FakeAasRepository().put(variables()).put(resource("parameters")).put(data())
				.put(aimc(segmentRef(), "2", refProperty(DATA_ID, "Value")))) {
			DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
			assertTrue(bundle.valid(), issues(bundle));
			Source source = influxSource(bundle);
			assertEquals("State", source.sourceId());
			assertEquals(2000, source.pollingIntervalMs());
			assertTrue(source.read());
			assertEquals("station_state", ((InfluxDb2Endpoint) source.endpoint()).measurement());
			assertEquals(Map.of("asset", "imaDispensing"), ((InfluxDb2Endpoint) source.endpoint()).tags());
		}
	}

	@Test
	void aSegmentSourceWithoutAPollingIntervalIsBlocked() throws Exception {
		try (var repository = new FakeAasRepository().put(variables()).put(resource("parameters")).put(data())
				.put(aimc(segmentRef(), null, refProperty(DATA_ID, "Value")))) {
			DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
			assertFalse(bundle.valid());
			assertTrue(issues(bundle).contains("needs a PollingInterval"), issues(bundle));
		}
	}

	@Test
	void aCompletedSegmentCannotBeWritten() throws Exception {
		try (var repository = fixture(segment(variables(), "Completed", "2026-10-20T06:00:00Z", "2026-10-20T14:00:00Z"))) {
			DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
			assertFalse(bundle.valid());
			assertTrue(issues(bundle).contains("only an open segment can be written"), issues(bundle));
		}
	}

	@Test
	void aCompletedSegmentIsReadWithinItsPeriod() throws Exception {
		try (var repository = new FakeAasRepository().put(resource("parameters")).put(segment(variables(), "Completed", "2026-10-20T06:00:00Z",
				"2026-10-20T16:00:00+02:00")).put(data()).put(aimc(segmentRef(), "2", refProperty(DATA_ID, "Value")))) {
			DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
			assertTrue(bundle.valid(), issues(bundle));
			var source = (InfluxDb2Endpoint) influxSource(bundle).endpoint();
			assertEquals(java.time.Instant.parse("2026-10-20T06:00:00Z"), source.start());
			assertEquals(java.time.Instant.parse("2026-10-20T14:00:00Z"), source.end());
		}
	}

	@Test
	void anOpenSegmentWithAStartTimeRejectsEarlierRowsAtRuntime() throws Exception {
		try (var repository = fixture(segment(variables(), "InProgress", "2026-10-20T06:00:00Z", null))) {
			DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
			assertTrue(bundle.valid(), issues(bundle));
			var sink = (InfluxDb2Endpoint) influxSink(bundle).endpoint();
			assertEquals(java.time.Instant.parse("2026-10-20T06:00:00Z"), sink.start());
			assertTrue(sink.uri(RESOURCES).contains("start=RAW(2026-10-20T06:00:00Z)"), sink.uri(RESOURCES));
		}
	}

	@Test
	void contradictoryOrMalformedSegmentMetadataBlocksTheMapping() throws Exception {
		for (String[] metadata : new String[][] {
				{ "Paused", null, null, "must be InProgress or Completed" },
				{ "InProgress", null, "2026-10-20T14:00:00Z", "an InProgress segment has no EndTime" },
				{ "Completed", "2026-10-20T14:00:00Z", "2026-10-20T06:00:00Z", "EndTime must lie after its StartTime" },
				{ "InProgress", "2026-10-20 06:00", null, "not an xs:dateTime with an offset" },
				{ "InProgress", "2026-10-20T06:00:00", null, "not an xs:dateTime with an offset" } }) {
			try (var repository = fixture(segment(variables(), metadata[0], metadata[1], metadata[2]))) {
				DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
				assertFalse(bundle.valid(), String.join(",", java.util.Arrays.asList(metadata)));
				assertTrue(issues(bundle).contains(metadata[3]), issues(bundle));
			}
		}
	}

	/** The fixture with its Live segment's State, StartTime and EndTime set ({@code null}: not authored). */
	private static String segment(String variables, String state, String start, String end) {
		var model = com.google.gson.JsonParser.parseString(variables).getAsJsonObject();
		var live = child(child(child(child(model.getAsJsonArray("submodelElements"), "State").getAsJsonArray("value"),
				"Station").getAsJsonArray("value"), "Segments").getAsJsonArray("value"), "Live").getAsJsonArray("value");
		for (var element : live) {
			var object = element.getAsJsonObject();
			if ("State".equals(object.get("idShort").getAsString())) object.addProperty("value", state);
		}
		if (start != null) live.add(segmentTime("StartTime", start));
		if (end != null) live.add(segmentTime("EndTime", end));
		return model.toString();
	}

	private static com.google.gson.JsonObject child(com.google.gson.JsonArray children, String idShort) {
		for (var element : children)
			if (idShort.equals(element.getAsJsonObject().get("idShort").getAsString())) return element.getAsJsonObject();
		throw new IllegalStateException("fixture has no " + idShort);
	}

	private static com.google.gson.JsonObject segmentTime(String idShort, String value) {
		return com.google.gson.JsonParser.parseString("{\"modelType\":\"Property\",\"idShort\":\"" + idShort
				+ "\",\"valueType\":\"xs:string\",\"value\":\"" + value + "\",\"semanticId\":{\"type\":\"ExternalReference\","
				+ "\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/TimeSeries/Segment/" + idShort
				+ "/1/1\"}]}}").getAsJsonObject();
	}

	private static FakeAasRepository fixture(String variables) throws IOException {
		return new FakeAasRepository().put(variables).put(resource("parameters")).put(data())
				.put(aimc(refProperty(DATA_ID, "Value"), segmentRef()));
	}

	/** The fixture with one more Record entry: a reference named {@code idShort} to {@code submodel}'s path. */
	private static String reference(String variables, String idShort, String submodel, String... typedPath) {
		var model = com.google.gson.JsonParser.parseString(variables).getAsJsonObject();
		var record = child(child(child(child(model.getAsJsonArray("submodelElements"), "State").getAsJsonArray("value"),
				"Station").getAsJsonArray("value"), "Metadata").getAsJsonArray("value"), "Record").getAsJsonArray("value");
		StringBuilder keys = new StringBuilder(key("Submodel", submodel));
		for (String typed : typedPath) keys.append(',').append(key(typed.split(":")[0], typed.split(":")[1]));
		record.add(com.google.gson.JsonParser.parseString("{\"modelType\":\"ReferenceElement\",\"idShort\":\"" + idShort
				+ "\",\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":"
				+ "\"https://smartproductionlab.aau.dk/timeseries/RecordReference/1/0\"}]},"
				+ "\"value\":{\"type\":\"ModelReference\",\"keys\":[" + keys + "]}}"));
		return model.toString();
	}

	/** The fixture with further Record entries, given as AAS JSON. */
	private static String entries(String variables, String... elements) {
		var model = com.google.gson.JsonParser.parseString(variables).getAsJsonObject();
		var record = child(child(child(child(model.getAsJsonArray("submodelElements"), "State").getAsJsonArray("value"),
				"Station").getAsJsonArray("value"), "Metadata").getAsJsonArray("value"), "Record").getAsJsonArray("value");
		for (String element : elements) record.add(com.google.gson.JsonParser.parseString(element));
		return model.toString();
	}

	/** A skill whose Operation has an input, an InOut collection with a list, and an output. */
	private static String skills() {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + SKILLS + "\",\"idShort\":\"Skills\",\"submodelElements\":["
				+ "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Occupy\",\"value\":["
				+ "{\"modelType\":\"Operation\",\"idShort\":\"operation\","
				+ "\"inputVariables\":[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Uuid\",\"valueType\":\"xs:string\"}}],"
				+ "\"inoutputVariables\":[{\"value\":{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Target\",\"value\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"x\",\"valueType\":\"xs:double\"}]}},"
				+ "{\"value\":{\"modelType\":\"SubmodelElementList\",\"idShort\":\"Waypoints\",\"typeValueListElement\":\"Property\","
				+ "\"valueTypeListElement\":\"xs:int\",\"value\":[{\"modelType\":\"Property\",\"valueType\":\"xs:int\"},"
				+ "{\"modelType\":\"Property\",\"valueType\":\"xs:int\"}]}}],"
				+ "\"outputVariables\":[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:string\"}}]}"
				+ "]}]}";
	}

	private static RepositoryDiscovery discovery(FakeAasRepository repository, InfluxSettings influx) {
		return new RepositoryDiscovery(repository.client(), new ConnectorConfigurationFactory.RuntimeSettings(repository.uri(), influx));
	}

	/** The bundle's sink on an InfluxDB segment. */
	private static Sink influxSink(DesiredBundle bundle) {
		return assertInstanceOf(MappingRoute.class, bundle.bundle().route()).sinks().stream()
				.filter(sink -> sink.endpoint() instanceof InfluxDb2Endpoint).findFirst().orElseThrow();
	}

	/** The bundle's source on an InfluxDB segment. */
	private static Source influxSource(DesiredBundle bundle) {
		return assertInstanceOf(MappingRoute.class, bundle.bundle().route()).sources().stream()
				.filter(source -> source.endpoint() instanceof InfluxDb2Endpoint).findFirst().orElseThrow();
	}

	private static DesiredBundle only(RepositoryDiscovery.Scan scan) {
		assertEquals(1, scan.mappings().size(), scan.toString());
		return scan.mappings().values().iterator().next().desired();
	}

	private static String issues(DesiredBundle bundle) {
		return bundle.issues().toString() + bundle.desired().failures();
	}

	private static boolean relevant(RepositoryDiscovery discovery, String path) {
		String shape = "{\"modelType\":\"Property\",\"idShort\":\"x\",\"valueType\":\"xs:string\",\"value\":\"\"}";
		return discovery.relevant(AasControlEvent.parse("{\"type\":\"SME_UPDATED\",\"id\":\"" + VARIABLES
				+ "\",\"smElementPath\":\"" + path + "\",\"submodelElement\":" + shape + "}"));
	}

	private static String variables() throws IOException {
		return resource("variables");
	}

	private static String resource(String name) throws IOException {
		try (InputStream in = TimeSeriesDiscoveryTest.class.getResourceAsStream("/timeseries/" + name + ".json")) {
			return new String(in.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static String segmentRef() {
		return "{\"type\":\"ModelReference\",\"keys\":[" + key("Submodel", VARIABLES) + ","
				+ String.join(",", java.util.Arrays.stream(SEGMENT.split("\\.")).map(id -> key("SubmodelElementCollection", id)).toList())
				+ "]}";
	}

	private static String refProperty(String submodel, String property) {
		return "{\"type\":\"ModelReference\",\"keys\":[" + key("Submodel", submodel) + "," + key("Property", property) + "]}";
	}

	private static String key(String type, String value) {
		return "{\"type\":\"" + type + "\",\"value\":\"" + value + "\"}";
	}

	private static String data() {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + DATA_ID + "\",\"idShort\":\"Data\",\"submodelElements\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"Value\",\"valueType\":\"xs:string\",\"value\":\"IDLE\"}]}";
	}

	private static String aimc(String source, String sink) {
		return aimc(source, null, sink);
	}

	private static String aimc(String source, String pollingInterval, String sink) {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + AIMC_ID + "\",\"idShort\":\"AIMC\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel\"}]},"
				+ "\"submodelElements\":[{\"modelType\":\"SubmodelElementList\",\"idShort\":\"MappingConfigurations\",\"typeValueListElement\":\"SubmodelElementCollection\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/1/0/MappingConfigurations\"}]},"
				+ "\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"state\",\"value\":["
				+ endpoint("Sources", "Source", "SourceId", "State", source, pollingInterval) + ","
				+ endpoint("Sinks", "Sink", "SinkId", "Live", sink, null)
				+ "]}]}]}";
	}

	private static String endpoint(String listName, String referenceName, String idName, String id, String reference,
			String pollingInterval) {
		String polling = pollingInterval == null ? ""
				: ",{\"modelType\":\"Property\",\"idShort\":\"PollingInterval\",\"valueType\":\"xs:string\",\"value\":\"" + pollingInterval + "\"}";
		return "{\"modelType\":\"SubmodelElementList\",\"idShort\":\"" + listName
				+ "\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"value\":["
				+ "{\"modelType\":\"ReferenceElement\",\"idShort\":\"" + referenceName + "\",\"value\":" + reference + "},"
				+ "{\"modelType\":\"Property\",\"idShort\":\"" + idName + "\",\"valueType\":\"xs:string\",\"value\":\"" + id + "\"}"
				+ polling + "]}]}";
	}
}
