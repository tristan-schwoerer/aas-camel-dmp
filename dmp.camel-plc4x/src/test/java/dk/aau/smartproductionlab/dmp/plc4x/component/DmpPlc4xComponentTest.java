/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.plc4x.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.ExchangePattern;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.plc4x.configuration.Plc4xEndpoint;
import dk.aau.smartproductionlab.dmp.plc4x.configuration.Tag;
import dk.aau.smartproductionlab.dmp.plc4x.testing.ModbusTestServer;
import org.junit.jupiter.api.Test;

/** {@code dmp-plc4x:} writes and reads against a real Modbus TCP socket. */
class DmpPlc4xComponentTest {
	private static String uri(int port, String tag, String address) {
		return new Plc4xEndpoint("127.0.0.1", port, "modbus-tcp", "", List.of(),
				List.of(new Tag(tag, address))).uri(null);
	}

	@Test
	void writesTheBodyToTheEndpointsOneTagAndAnswersARequestWithAnEmptyReply() throws Exception {
		try (ModbusTestServer device = new ModbusTestServer(); DefaultCamelContext camel = new DefaultCamelContext()) {
			camel.start();
			var template = camel.createProducerTemplate();
			template.sendBody(uri(device.port(), "Start", "holding-register:5"), 42);
			Object reply = template.sendBody(uri(device.port(), "Start", "holding-register:5"), ExchangePattern.InOut,
					Map.of("Start", 43));
			assertEquals(List.of("register:4=42", "register:4=43"), device.writes(), "a value, or a value per tag");
			assertEquals(Map.of(), reply);
		}
	}

	@Test
	void theTagsOfOneDeviceShareOneConnection() throws Exception {
		try (ModbusTestServer device = new ModbusTestServer(); DefaultCamelContext camel = new DefaultCamelContext()) {
			String read = uri(device.port(), "Done", "holding-register:2");
			camel.addRoutes(new RouteBuilder() {
				@Override public void configure() {
					from("direct:read").pollEnrich(read, 3000);
				}
			});
			camel.start();
			device.register(1, 7);
			var template = camel.createProducerTemplate();
			// as routes do: the endpoints exist before they are used concurrently
			String start = uri(device.port(), "Start", "holding-register:5"), stop = uri(device.port(), "Stop", "coil:3");
			camel.getEndpoint(start);
			camel.getEndpoint(stop);
			var writers = java.util.concurrent.Executors.newFixedThreadPool(4);
			try {
				var calls = new java.util.ArrayList<java.util.concurrent.Future<?>>();
				for (int i = 0; i < 20; i++) {
					int value = i;
					calls.add(writers.submit(() -> template.sendBody(start, value)));
					calls.add(writers.submit(() -> template.sendBody(stop, value % 2 == 0)));
					calls.add(writers.submit(() -> template.requestBody("direct:read", (Object) null)));
				}
				for (var call : calls) call.get(10, java.util.concurrent.TimeUnit.SECONDS);
			} finally { writers.shutdownNow(); }
			assertEquals(40, device.writes().size());
			assertEquals(1, device.acceptedConnections(), "three endpoints of one device, one connection");
			var component = camel.getComponent("dmp-plc4x", DmpPlc4xComponent.class);
			assertEquals(1, component.openConnections());
			camel.stop();
			assertEquals(0, component.openConnections());
			org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(5), () -> {
				while (device.openConnections() > 0) Thread.sleep(20);
			}, "the last endpoint closes the device's connection");
		}
	}

	@Test
	void aWriteToAnUnreachableDeviceFails() throws Exception {
		int port;
		try (ModbusTestServer device = new ModbusTestServer()) { port = device.port(); }
		try (DefaultCamelContext camel = new DefaultCamelContext()) {
			camel.start();
			var failure = assertThrows(CamelExecutionException.class,
					() -> camel.createProducerTemplate().sendBody(uri(port, "Start", "coil:1"), true));
			var cause = assertInstanceOf(ConnectorInvocationException.class, failure.getCause());
			assertEquals(ConnectorInvocationException.Category.TRANSPORT, cause.getCategory());
			assertEquals("PLC4X_WRITE_FAILED", cause.getConnectorCode());
			assertTrue(String.valueOf(cause.getDetails().get("cause")).contains("Connection refused"),
					"the failure names its cause: " + cause.getDetails());
		}
	}

	@Test
	void pollEnrichReadsTheTagValueAndARejectedReadFails() throws Exception {
		try (ModbusTestServer device = new ModbusTestServer(); DefaultCamelContext camel = new DefaultCamelContext()) {
			String endpoint = uri(device.port(), "Done", "holding-register:2");
			camel.addRoutes(new RouteBuilder() {
				@Override public void configure() {
					from("direct:read").pollEnrich(endpoint, 3000);
				}
			});
			camel.start();
			device.register(1, 7);
			var template = camel.createProducerTemplate();
			assertEquals(7, ((Number) template.requestBody("direct:read", (Object) null)).intValue());
			device.failReads(true);
			var failure = assertThrows(CamelExecutionException.class, () -> template.requestBody("direct:read", (Object) null));
			// pollEnrich reports "Error during poll" with the connector's failure as its cause.
			assertEquals(ConnectorInvocationException.Category.REMOTE,
					assertInstanceOf(ConnectorInvocationException.class, failure.getCause().getCause()).getCategory());
		}
	}

	@Test
	void driverOptionsBecomePartOfTheConnectionString() throws Exception {
		try (DefaultCamelContext camel = new DefaultCamelContext()) {
			camel.start();
			var endpoint = camel.getEndpoint("dmp-plc4x:modbus-tcp://127.0.0.1:502?tag.Done=holding-register:2"
					+ "&unit-identifier=2&autoReconnect=true", DmpPlc4xEndpoint.class);
			assertEquals("modbus-tcp://127.0.0.1:502?unit-identifier=2", endpoint.getUri());
			assertEquals(Map.of("Done", "holding-register:2"), endpoint.getTags());
			assertEquals(true, endpoint.isAutoReconnect());
		}
	}
}
