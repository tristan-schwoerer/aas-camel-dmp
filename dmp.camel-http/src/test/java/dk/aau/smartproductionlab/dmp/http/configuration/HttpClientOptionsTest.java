/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;

/** A form's static htv_headers reach the device on writes, polls and observations, and never the URI. */
class HttpClientOptionsTest {
	private static final Map<String, String> HEADERS = Map.of("X-Api-Key", "s3cret", "Authorization", "Bearer token-1");
	private HttpServer device;
	private CamelContext camel;
	private final BlockingQueue<Headers> received = new LinkedBlockingQueue<>();
	private final BlockingQueue<String> queries = new LinkedBlockingQueue<>();

	@BeforeEach void start() throws Exception {
		device = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		device.createContext("/", exchange -> {
			received.add(exchange.getRequestHeaders());
			queries.add(String.valueOf(exchange.getRequestURI().getRawQuery()));
			exchange.getRequestBody().readAllBytes();
			byte[] body = "{\"v\":1}".getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().add("Content-Type", "application/json");
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream out = exchange.getResponseBody()) { out.write(body); }
		});
		device.start();
		camel = new DefaultCamelContext();
		camel.start();
	}

	@AfterEach void stop() throws Exception {
		camel.stop();
		device.stop(0);
	}

	private int port() { return device.getAddress().getPort(); }

	private void assertSent() throws InterruptedException {
		Headers headers = received.poll(5, TimeUnit.SECONDS);
		assertEquals("s3cret", headers.getFirst("X-api-key"));
		assertEquals("Bearer token-1", headers.getFirst("Authorization"));
	}

	@Test void aWriteSendsTheHeaders() throws Exception {
		HttpDataSinkConfiguration sink = new HttpDataSinkConfiguration("sink", "http", "127.0.0.1", "/value", port());
		sink.setMethod("PUT");
		sink.setHeaders(HEADERS);
		sink.bind(camel);
		assertFalse(sink.getConnectionURI().contains("s3cret"), sink.getConnectionURI());
		camel.createProducerTemplate().sendBody(sink.getConnectionURI(), "{\"v\":2}");
		assertSent();
		sink.close();
		assertNull(camel.getRegistry().lookupByName(sink.getConnectionURI().replaceAll(".*#bean:", "")));
	}

	@Test void aPollSendsTheHeaders() throws Exception {
		HttpPollingDataSourceConfiguration source = new HttpPollingDataSourceConfiguration("source",
				"http://127.0.0.1:" + port() + "/value", port(), null, null);
		source.setHeaders(HEADERS);
		source.bind(camel);
		assertFalse(source.getConnectionURI().contains("token-1"), source.getConnectionURI());
		camel.createConsumerTemplate().receiveBody(source.getConnectionURI(), 5000);
		assertSent();
		source.close();
	}

	/** A password with URI delimiters reaches the device intact and never appears in an endpoint URI. */
	@Test void basicCredentialsAreAHeaderNotAUri() throws Exception {
		String password = "p@ss&w#rd";
		String expected = "Basic " + java.util.Base64.getEncoder().encodeToString(("dmp:" + password).getBytes(StandardCharsets.UTF_8));
		HttpPollingDataSourceConfiguration source = new HttpPollingDataSourceConfiguration("source",
				"http://127.0.0.1:" + port() + "/value", port(), "dmp", password);
		source.bind(camel);
		HttpDataSinkConfiguration sink = new HttpDataSinkConfiguration("sink", "http", "127.0.0.1", "/value", port());
		sink.setMethod("PUT");
		sink.setBasicCredentials("dmp", password);
		sink.setHeaders(Map.of("X-Api-Key", "s3cret"));
		sink.bind(camel);
		for (String uri : new String[] { source.getConnectionURI(), sink.getConnectionURI() }) {
			assertFalse(uri.contains("p@ss") || uri.contains("dmp:") || uri.contains("authUsername"), uri);
			assertTrue(uri.contains("connectTimeout=30000&responseTimeout=30000"), uri);
		}
		camel.createConsumerTemplate().receiveBody(source.getConnectionURI(), 5000);
		assertEquals(expected, received.poll(5, TimeUnit.SECONDS).getFirst("Authorization"));
		camel.createProducerTemplate().sendBody(sink.getConnectionURI(), "{\"v\":2}");
		Headers written = received.poll(5, TimeUnit.SECONDS);
		assertEquals(expected, written.getFirst("Authorization"));
		assertEquals("s3cret", written.getFirst("X-api-key"));
		source.close();
		sink.close();
	}

	/** An API key in the query and a bearer token reach the device; neither is in an endpoint URI. */
	@Test void securitySchemeSecretsReachTheDeviceAndStayOutOfTheUri() throws Exception {
		HttpPollingDataSourceConfiguration poll = new HttpPollingDataSourceConfiguration("source",
				"http://127.0.0.1:" + port() + "/value?unit=C", port(), null, null);
		poll.setSecretQuery(Map.of("key", "k3y&x"));
		poll.setHeaders(Map.of("Authorization", "Bearer t0ken"));
		poll.bind(camel);
		assertFalse(poll.getConnectionURI().contains("k3y") || poll.getConnectionURI().contains("t0ken"), poll.getConnectionURI());
		camel.createConsumerTemplate().receiveBody(poll.getConnectionURI(), 5000);
		assertEquals("Bearer t0ken", received.poll(5, TimeUnit.SECONDS).getFirst("Authorization"));
		assertEquals("unit=C&key=k3y%26x", queries.poll(5, TimeUnit.SECONDS));
		poll.close();

		HttpObserveDataSourceConfiguration observed = new HttpObserveDataSourceConfiguration("observe",
				"http://127.0.0.1:" + port() + "/value", port(), "longpoll", "application/json", null, null);
		observed.setSecretQuery(Map.of("key", "k3y"));
		observed.bind(camel);
		assertFalse(observed.getConnectionURI().contains("k3y"), observed.getConnectionURI());
		camel.addRoutes(new org.apache.camel.builder.RouteBuilder() {
			@Override public void configure() { from(observed.getConnectionURI()).to("mock:observed"); }
		});
		received.poll(5, TimeUnit.SECONDS);
		assertEquals("key=k3y", queries.poll(5, TimeUnit.SECONDS));
		camel.getRouteController().stopAllRoutes();
		observed.close();
	}

	@Test void anObservationSendsTheHeaders() throws Exception {
		HttpObserveDataSourceConfiguration source = new HttpObserveDataSourceConfiguration("observe",
				"http://127.0.0.1:" + port() + "/value", port(), "longpoll", "application/json", null, null);
		source.setHeaders(HEADERS);
		source.bind(camel);
		assertFalse(source.getConnectionURI().contains("s3cret"), source.getConnectionURI());
		camel.addRoutes(new org.apache.camel.builder.RouteBuilder() {
			@Override public void configure() { from(source.getConnectionURI()).to("mock:sink"); }
		});
		assertSent();
		camel.getRouteController().stopAllRoutes();
		source.close();
	}
}
