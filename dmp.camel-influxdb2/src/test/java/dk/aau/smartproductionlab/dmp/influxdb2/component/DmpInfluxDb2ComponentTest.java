/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.influxdb2.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.influxdb2.InfluxSettings;
import dk.aau.smartproductionlab.dmp.influxdb2.RecordColumn;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

class DmpInfluxDb2ComponentTest {
	private record Write(String query, String authorization, String body) { }

	private static final List<RecordColumn> COLUMNS = List.of(
			new RecordColumn("PackMLState", "xs:string"),
			new RecordColumn("Count", "xs:long"),
			new RecordColumn("Position.X", "xs:double"),
			new RecordColumn("Ready", "xs:boolean"),
			new RecordColumn("Waypoints[0].x", "xs:double"),
			new RecordColumn("Waypoints[1].x", "xs:double"));
	private static final String URI_ = DmpInfluxDb2Component.uri("station_state", "ap2030", "Time",
			Map.of("asset", "imaDispensing"), COLUMNS, null, null);
	/** The same segment, written asynchronously. */
	private static final String ASYNC = DmpInfluxDb2Component.uri("station_state", "ap2030", "Time",
			Map.of("asset", "imaDispensing"), COLUMNS, null, null, true);
	/** A completed segment: 2026-10-20, 06:00 to 14:00 UTC. */
	private static final String BATCH = DmpInfluxDb2Component.uri("station_state", "ap2030", "Time",
			Map.of("asset", "imaDispensing"), COLUMNS, Instant.parse("2026-10-20T06:00:00Z"), Instant.parse("2026-10-20T14:00:00Z"));

	private final List<Write> writes = new CopyOnWriteArrayList<>();
	private final List<String> queries = new CopyOnWriteArrayList<>();
	private volatile int status = 204;
	/** The annotated CSV InfluxDB answers a query with: one table per field, as {@code last()} returns. */
	private volatile String answer = "";
	private HttpServer influx;
	private DefaultCamelContext context;
	private ProducerTemplate template;

	@BeforeEach
	void start() throws Exception {
		influx = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		influx.createContext("/api/v2/write", exchange -> {
			writes.add(new Write(exchange.getRequestURI().getRawQuery(), exchange.getRequestHeaders().getFirst("Authorization"),
					new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
			byte[] answer = status == 204 ? new byte[0]
					: "{\"code\":\"invalid\",\"message\":\"field type conflict\"}".getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/json");
			exchange.sendResponseHeaders(status, status == 204 ? -1 : answer.length);
			if (answer.length > 0) exchange.getResponseBody().write(answer);
			exchange.close();
		});
		influx.createContext("/api/v2/query", exchange -> {
			String request = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
			queries.add(com.google.gson.JsonParser.parseString(request).getAsJsonObject().get("query").getAsString());
			byte[] csv = answer.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/csv; charset=utf-8");
			exchange.sendResponseHeaders(200, csv.length == 0 ? -1 : csv.length);
			if (csv.length > 0) exchange.getResponseBody().write(csv);
			exchange.close();
		});
		influx.start();
		context = new DefaultCamelContext();
		context.start();
		context.getComponent(DmpInfluxDb2Component.SCHEME, DmpInfluxDb2Component.class).configure(new InfluxSettings(
				URI.create("http://127.0.0.1:" + influx.getAddress().getPort()), "ap2030", "secret-token"));
		template = context.createProducerTemplate();
	}

	@AfterEach
	void stop() throws IOException {
		context.stop();
		influx.stop(0);
	}

	@Test
	void aRowIsOneAcknowledgedPointWithTypedFields() {
		template.requestBody(URI_, Map.of("PackMLState", "EXECUTE", "Count", 3L,
				"Position", Map.of("X", 1.5), "Ready", true, "Time", "2026-10-21T12:00:00Z"));

		assertEquals(1, writes.size());
		Write write = writes.get(0);
		assertTrue(write.query().contains("org=ap2030") && write.query().contains("bucket=ap2030")
				&& write.query().contains("precision=ns"), write.query());
		assertEquals("Token secret-token", write.authorization());
		String line = write.body().strip();
		assertTrue(line.startsWith("station_state,asset=imaDispensing "), line);
		assertTrue(line.contains("PackMLState=\"EXECUTE\"") && line.contains("Count=3i")
				&& line.contains("Position.X=1.5") && line.contains("Ready=true"), line);
		assertTrue(line.endsWith(" " + Instant.parse("2026-10-21T12:00:00Z").getEpochSecond() + "000000000"), line);
	}

	@Test
	void listMembersAreFieldsByIndex() {
		template.requestBody(URI_, Map.of("Waypoints", List.of(Map.of("x", 1.5), Map.of("x", 2.5))));
		String line = writes.get(0).body();
		assertTrue(line.contains("Waypoints[0].x=1.5") && line.contains("Waypoints[1].x=2.5"), line);
	}

	@Test
	void rangesAndTextsAreWrittenFromTheirLuaShape() {
		String uri = DmpInfluxDb2Component.uri("dosing", "ap2030", "Time", Map.of(), List.of(
				new RecordColumn("Tolerance.min", "xs:double"), new RecordColumn("Tolerance.max", "xs:double"),
				new RecordColumn("Batch.en", "xs:string"), new RecordColumn("Report", "xs:string")), null, null);
		template.requestBody(uri, Map.of("Tolerance", Map.of("min", 0.5, "max", 1.5), "Batch", Map.of("en", "B-17"),
				"Report", "/files/b17.pdf"));
		String line = writes.get(0).body();
		assertTrue(line.contains("Tolerance.min=0.5") && line.contains("Tolerance.max=1.5") && line.contains("Batch.en=\"B-17\"")
				&& line.contains("Report=\"/files/b17.pdf\""), line);
	}

	@Test
	void aFieldTheRowOmitsIsNotWritten() {
		template.requestBody(URI_, Map.of("PackMLState", "IDLE"));
		String line = writes.get(0).body();
		assertTrue(line.contains("PackMLState=\"IDLE\""), line);
		assertFalse(line.contains("Count="), line);
	}

	@Test
	void anUnknownFieldOrAWrongTypeRejectsTheRowBeforeAnyWrite() {
		assertEquals("INFLUX_ROW_INVALID", failure(Map.of("Unknown", "x")).getConnectorCode());
		assertEquals("INFLUX_ROW_INVALID", failure(Map.of("Count", "three")).getConnectorCode());
		assertEquals("INFLUX_ROW_INVALID", failure(Map.of("Waypoints", List.of(Map.of("x", 1.0), Map.of("x", 2.0),
				Map.of("x", 3.0)))).getConnectorCode(), "a list member the Record does not declare");
		assertEquals("INFLUX_ROW_INVALID", failure(Map.of("Count", 1.5)).getConnectorCode());
		assertEquals("INFLUX_ROW_INVALID", failure(Map.of("PackMLState", "IDLE", "Time", "yesterday")).getConnectorCode());
		assertEquals("INFLUX_ROW_INVALID", failure(Map.of()).getConnectorCode());
		assertEquals("INFLUX_ROW_INVALID", failure("EXECUTE").getConnectorCode());
		assertTrue(writes.isEmpty());
	}

	@Test
	void aRejectedPointFailsTheExchange() {
		status = 422;
		ConnectorInvocationException failure = failure(Map.of("PackMLState", "IDLE"));
		assertEquals("INFLUX_WRITE_REJECTED", failure.getConnectorCode());
		assertEquals(ConnectorInvocationException.Category.REMOTE, failure.getCategory());
	}

	@Test
	void anUnreachableInfluxDbFailsTheExchange() throws IOException {
		influx.stop(0);
		ConnectorInvocationException failure = failure(Map.of("PackMLState", "IDLE"));
		assertEquals("INFLUX_WRITE_FAILED", failure.getConnectorCode());
		assertEquals(ConnectorInvocationException.Category.TRANSPORT, failure.getCategory());
	}

	/** Batches leave after {@code flush}; long enough that rows sent together share one. */
	private DmpInfluxDb2Component batching(Duration flush) {
		DmpInfluxDb2Component component = context.getComponent(DmpInfluxDb2Component.SCHEME, DmpInfluxDb2Component.class);
		component.configure(new InfluxSettings(URI.create("http://127.0.0.1:" + influx.getAddress().getPort()), "ap2030",
				"secret-token", InfluxSettings.DEFAULT_LOOKBACK, flush, 100));
		return component;
	}

	private static void await(java.util.function.BooleanSupplier condition, String what) throws InterruptedException {
		for (int i = 0; i < 100 && !condition.getAsBoolean(); i++) Thread.sleep(50);
		assertTrue(condition.getAsBoolean(), what);
	}

	@Test
	void anAsynchronousSegmentBatchesItsRowsWithoutWaitingForInfluxDb() throws InterruptedException {
		DmpInfluxDb2Component component = batching(Duration.ofMillis(500));
		for (String state : List.of("IDLE", "EXECUTE", "COMPLETE")) template.requestBody(ASYNC, Map.of("PackMLState", state));
		assertTrue(writes.isEmpty(), "the exchanges did not wait for InfluxDB");

		await(() -> writes.size() == 1, "the rows left as one batch");
		List<String> lines = writes.get(0).body().strip().lines().toList();
		assertEquals(3, lines.size(), writes.get(0).body());
		assertTrue(lines.get(2).contains("PackMLState=\"COMPLETE\""), lines.get(2));
		await(() -> Long.valueOf(3).equals(component.asynchronousWrites().get("writtenPoints")), "InfluxDB's answer is counted");
		assertEquals(3L, component.asynchronousWrites().get("submittedPoints"));
	}

	@Test
	void aBatchInfluxDbRejectsIsCountedAndLoggedWithoutFailingTheExchange() throws InterruptedException {
		DmpInfluxDb2Component component = batching(Duration.ofMillis(50));
		status = 422;
		template.requestBody(ASYNC, Map.of("PackMLState", "IDLE"));

		await(() -> Long.valueOf(1).equals(component.asynchronousWrites().get("failedBatches")), "the rejected batch is counted");
		assertEquals(0L, component.asynchronousWrites().get("writtenPoints"));
		assertTrue(((String) component.asynchronousWrites().get("lastError")).contains("field type conflict"),
				String.valueOf(component.asynchronousWrites().get("lastError")));
	}

	@Test
	void anAsynchronousSegmentStillRejectsARowThatDoesNotFitItsRecord() {
		batching(Duration.ofMillis(50));
		CamelExecutionException failure = assertThrows(CamelExecutionException.class,
				() -> template.requestBody(ASYNC, Map.of("Count", "three")));
		assertEquals("INFLUX_ROW_INVALID", assertInstanceOf(ConnectorInvocationException.class, failure.getCause()).getConnectorCode());
	}

	@Test
	void stoppingSendsTheBufferedRows() throws Exception {
		batching(Duration.ofMinutes(1));
		template.requestBody(ASYNC, Map.of("PackMLState", "IDLE"));
		assertTrue(writes.isEmpty());
		context.stop();
		assertEquals(1, writes.size(), "the buffer is flushed when the component stops");
	}

	@Test
	void theTokenIsNotPartOfTheEndpointUri() {
		assertFalse(URI_.contains("secret-token"));
		assertFalse(context.getEndpoint(URI_).getEndpointUri().contains("secret-token"));
	}

	@Test
	void aReadIsTheLatestRowWithTheNewestTime() {
		answer = table("string", "PackMLState", "EXECUTE", "2026-10-21T12:00:00Z")
				+ table("long", "Count", "7", "2026-10-21T12:00:05Z")
				+ table("double", "Waypoints[1].x", "2.5", "2026-10-21T11:00:00Z")
				+ table("double", "Position.X", "1.5", "2026-10-21T11:00:00Z");
		Object row = context.createConsumerTemplate().receiveBody(URI_, 1000);
		assertEquals(Map.of("PackMLState", "EXECUTE", "Count", 7L, "Position", Map.of("X", 1.5),
				"Waypoints", java.util.Arrays.asList(null, Map.of("x", 2.5)), "Time", "2026-10-21T12:00:05Z"), row);
		String query = queries.get(0);
		assertTrue(query.contains("range(start: -2592000s)"), query);
		assertTrue(query.contains("r._measurement == \"station_state\""), query);
		assertTrue(query.contains("r[\"asset\"] == \"imaDispensing\""), query);
		assertTrue(query.contains("r._field == \"Count\" or"), query);
		assertTrue(query.contains("last()"), query);
	}

	@Test
	void aSegmentsPeriodBoundsTheReadInsteadOfTheLookback() {
		answer = table("string", "PackMLState", "IDLE", "2026-10-20T13:59:00Z");
		assertEquals("IDLE", ((Map<?, ?>) context.createConsumerTemplate().receiveBody(BATCH, 1000)).get("PackMLState"));
		String query = queries.get(0);
		assertTrue(query.contains("range(start: 2026-10-20T06:00:00Z, stop: 2026-10-20T14:00:00.000000001Z)"), query);
	}

	@Test
	void aRowOutsideTheSegmentsPeriodIsRejected() {
		CamelExecutionException thrown = assertThrows(CamelExecutionException.class, () -> template.requestBody(BATCH,
				Map.of("PackMLState", "IDLE", "Time", "2026-10-20T05:00:00Z")));
		assertEquals("INFLUX_ROW_INVALID", assertInstanceOf(ConnectorInvocationException.class, thrown.getCause()).getConnectorCode());
		assertTrue(writes.isEmpty());
	}

	@Test
	void aSeriesWithoutAPointYetHasNoRow() {
		answer = "";
		assertEquals(null, context.createConsumerTemplate().receiveBody(URI_, 1000));
	}

	@Test
	void aStoredValueOfAnotherTypeFailsTheRead() {
		answer = table("string", "Count", "seven", "2026-10-21T12:00:00Z");
		var thrown = assertThrows(RuntimeException.class, () -> context.createConsumerTemplate().receiveBody(URI_, 1000));
		Throwable cause = thrown;
		while (cause != null && !(cause instanceof ConnectorInvocationException)) cause = cause.getCause();
		assertEquals("INFLUX_VALUE_MISMATCH", assertInstanceOf(ConnectorInvocationException.class, cause).getConnectorCode());
	}

	@Test
	void fluxStringsAreQuotedWithoutInterpolation() {
		assertEquals("\"a\\\"b\\\\c\\${d}\"", dk.aau.smartproductionlab.dmp.influxdb2.InfluxValues.flux("a\"b\\c${d}"));
	}

	private static String table(String type, String field, String value, String time) {
		return "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339," + type + ",string,string,string\n"
				+ "#group,false,false,true,true,false,false,true,true,true\n"
				+ "#default,_result,,,,,,,,\n"
				+ ",result,table,_start,_stop,_time,_value,_field,_measurement,asset\n"
				+ ",,0,2026-09-21T00:00:00Z,2026-10-21T13:00:00Z," + time + "," + value + "," + field + ",station_state,imaDispensing\n\n";
	}

	private ConnectorInvocationException failure(Object row) {
		CamelExecutionException thrown = assertThrows(CamelExecutionException.class, () -> template.requestBody(URI_, row));
		return assertInstanceOf(ConnectorInvocationException.class, thrown.getCause());
	}
}
