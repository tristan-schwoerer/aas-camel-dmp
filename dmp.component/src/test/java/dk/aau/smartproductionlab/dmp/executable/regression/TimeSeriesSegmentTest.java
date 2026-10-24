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
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.core.plan.RoutePlan;
import dk.aau.smartproductionlab.dmp.influxdb2.InfluxSettings;
import dk.aau.smartproductionlab.dmp.influxdb2.component.DmpInfluxDb2Component;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler;
import dk.aau.smartproductionlab.dmp.management.model.WireProjection;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

/**
 * TimeSeries LinkedSegments end to end. As a Sink: a polled AAS Property feeds Lua, whose row for the segment's
 * SinkId (dot notation, a nested group, Time left to the DMP) becomes one acknowledged InfluxDB point. As a
 * Source: the segment's latest row reaches Lua as a table keyed by Record idShort.
 */
class TimeSeriesSegmentTest {
	private static final String VARIABLES = "https://smartproductionlab.aau.dk/aas/imaDispensing/submodels/Variables";
	private static final String DATA_ID = "urn:fixture/submodels/Data";
	private static final String LUA = "function aimc_main(sources)\n"
			+ "  local row = { PackMLState = sources.State, Count = 2, Position = {} }\n"
			+ "  row.Position.X = 1.5\n"
			+ "  return { Live = row }\n"
			+ "end\n";
	private static final String ECHO = "function aimc_main(sources)\n"
			+ "  local seen = sources.State\n"
			+ "  return { Live = { PackMLState = seen.PackMLState, Count = seen.Count + 1, Position = { X = seen.Position.X * 2 } } }\n"
			+ "end\n";
	/** Reads Lua's global meta: the sink's connector kind and the source value's acquisition time. */
	private static final String META = "function aimc_main(sources)\n"
			+ "  return { Live = { PackMLState = meta.sinks.Live.kind, Time = meta.sources.State.time } }\n"
			+ "end\n";
	private static final String LATEST = table("string", "PackMLState", "EXECUTE") + table("long", "Count", "7")
			+ table("double", "Position.X", "1.5");

	private final List<String> lines = new CopyOnWriteArrayList<>();
	private HttpServer influx;
	private FakeAasRepository repository;
	private DmpComponent runtime;

	@BeforeEach
	void start() throws Exception {
		influx = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		influx.createContext("/api/v2/write", exchange -> {
			lines.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
			exchange.sendResponseHeaders(204, -1);
			exchange.close();
		});
		influx.createContext("/api/v2/query", exchange -> {
			exchange.getRequestBody().readAllBytes();
			byte[] csv = LATEST.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/csv; charset=utf-8");
			exchange.sendResponseHeaders(200, csv.length);
			exchange.getResponseBody().write(csv);
			exchange.close();
		});
		influx.start();
		repository = new FakeAasRepository().put(variables()).put(resource("timeseries/parameters.json")).put(data());
		runtime = new DmpComponent(Duration.ofSeconds(4)) { };
		runtime.startComponent();
		runtime.runtimeContext().getComponent(DmpInfluxDb2Component.SCHEME, DmpInfluxDb2Component.class).configure(settings());
	}

	@AfterEach
	void stop() throws Exception {
		try { runtime.stopComponent(); } finally {
			repository.close();
			influx.stop(0);
		}
	}

	@Test
	void theTransformationsRowIsWrittenAsOnePoint() throws Exception {
		runtime.installBundle("series", compile(aimc(LUA, value())));
		await().atMost(Duration.ofSeconds(10)).until(() -> !lines.isEmpty());
		String line = lines.get(0).strip();
		assertTrue(line.startsWith("station_state,asset=imaDispensing "), line);
		assertTrue(line.contains("PackMLState=\"IDLE\"") && line.contains("Count=2i") && line.contains("Position.X=1.5"), line);
	}

	@Test
	void theLatestRowReachesTheTransformationAsATable() throws Exception {
		runtime.installBundle("echo", compile(aimc(ECHO, segment())));
		await().atMost(Duration.ofSeconds(10)).until(() -> !lines.isEmpty());
		String line = lines.get(0).strip();
		assertTrue(line.contains("PackMLState=\"EXECUTE\"") && line.contains("Count=8i") && line.contains("Position.X=3"), line);
	}

	@Test
	void theTransformationSeesItsSourcesAndSinksInMeta() throws Exception {
		long before = java.time.Instant.now().minusSeconds(5).getEpochSecond();
		runtime.installBundle("meta", compile(aimc(META, value())));
		await().atMost(Duration.ofSeconds(10)).until(() -> !lines.isEmpty());
		String line = lines.get(0).strip();
		assertTrue(line.contains("PackMLState=\"dmp-influxdb2\""), line);
		long seconds = Long.parseLong(line.substring(line.lastIndexOf(' ') + 1)) / 1_000_000_000L;
		assertTrue(seconds >= before && seconds <= java.time.Instant.now().getEpochSecond(),
				"the point's time is the source's acquisition time: " + line);
	}

	private static String table(String type, String field, String value) {
		return "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339," + type + ",string,string,string\n"
				+ "#group,false,false,true,true,false,false,true,true,true\n"
				+ "#default,_result,,,,,,,,\n"
				+ ",result,table,_start,_stop,_time,_value,_field,_measurement,asset\n"
				+ ",,0,2026-09-21T00:00:00Z,2026-10-21T13:00:00Z,2026-10-21T12:00:00Z," + value + "," + field + ",station_state,imaDispensing\n\n";
	}

	private InfluxSettings settings() {
		return new InfluxSettings(URI.create("http://127.0.0.1:" + influx.getAddress().getPort()), "ap2030", "token");
	}

	private RoutePlan compile(String aimc) throws Exception {
		repository.put(aimc);
		var plan = new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compile(WireProjection.aimc(
				dk.aau.smartproductionlab.dmp.aas.model.Aas.read(aimc, org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
		assertEquals(AasRepositoryClient.Status.FOUND, plan.status(), String.valueOf(plan));
		// The fixture's segment names the workspace's InfluxDB; the test's InfluxDB stands in for it.
		var named = new InfluxSettings(URI.create("http://influxdb:8086"), "ap2030", "token");
		var compiled = new ConnectorConfigurationFactory().createBundles(plan.value(),
				new ConnectorConfigurationFactory.RuntimeSettings(repository.uri(), named));
		assertTrue(compiled.issues().isEmpty(), String.valueOf(compiled.issues()));
		return compiled.bundles().get(0).route();
	}

	private static String variables() throws Exception {
		return resource("timeseries/variables.json");
	}

	private static String resource(String path) throws Exception {
		try (var input = TimeSeriesSegmentTest.class.getClassLoader().getResourceAsStream(path)) {
			assertNotNull(input, path);
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static String data() {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + DATA_ID + "\",\"idShort\":\"Data\",\"submodelElements\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"Value\",\"valueType\":\"xs:string\",\"value\":\"IDLE\"}]}";
	}

	private static String segment() {
		return "{\"type\":\"ModelReference\",\"keys\":[" + key("Submodel", VARIABLES)
				+ "," + key("SubmodelElementCollection", "State") + "," + key("SubmodelElementCollection", "Station")
				+ "," + key("SubmodelElementCollection", "Segments") + "," + key("SubmodelElementCollection", "Live") + "]}";
	}

	private static String value() {
		return "{\"type\":\"ModelReference\",\"keys\":[" + key("Submodel", DATA_ID) + "," + key("Property", "Value") + "]}";
	}

	/** A mapping from {@code source} (polled every second) through {@code lua} to the segment. */
	private static String aimc(String lua, String source) {
		String segment = segment();
		return "{\"modelType\":\"Submodel\",\"id\":\"urn:fixture/submodels/AIMC\",\"idShort\":\"AIMC\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel\"}]},"
				+ "\"submodelElements\":[{\"modelType\":\"SubmodelElementList\",\"idShort\":\"MappingConfigurations\",\"typeValueListElement\":\"SubmodelElementCollection\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/1/0/MappingConfigurations\"}]},"
				+ "\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"state\",\"value\":["
				+ "{\"modelType\":\"Blob\",\"idShort\":\"Transformation\",\"contentType\":\"text/plain\",\"value\":\""
				+ Base64.getEncoder().encodeToString(lua.getBytes(StandardCharsets.UTF_8)) + "\"},"
				+ "{\"modelType\":\"SubmodelElementList\",\"idShort\":\"Sources\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"value\":["
				+ "{\"modelType\":\"ReferenceElement\",\"idShort\":\"Source\",\"value\":" + source + "},"
				+ "{\"modelType\":\"Property\",\"idShort\":\"SourceId\",\"valueType\":\"xs:string\",\"value\":\"State\"},"
				+ "{\"modelType\":\"Property\",\"idShort\":\"PollingInterval\",\"valueType\":\"xs:string\",\"value\":\"1\"}]}]},"
				+ "{\"modelType\":\"SubmodelElementList\",\"idShort\":\"Sinks\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"value\":["
				+ "{\"modelType\":\"ReferenceElement\",\"idShort\":\"Sink\",\"value\":" + segment + "},"
				+ "{\"modelType\":\"Property\",\"idShort\":\"SinkId\",\"valueType\":\"xs:string\",\"value\":\"Live\"}]}]}"
				+ "]}]}]}";
	}

	private static String key(String type, String value) {
		return "{\"type\":\"" + type + "\",\"value\":\"" + value + "\"}";
	}
}
