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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;
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

	/** What a bundle binds in the context while it lives; {@link #retire} releases it. */
	private final class Bundle implements BundleResources {
		private final List<ManagedRouteResource> owned = new ArrayList<>();
		@Override public <T extends ManagedRouteResource> T own(T resource) {
			try { resource.bind(camel); } catch (Exception e) { throw new IllegalStateException(e); }
			owned.add(resource);
			return resource;
		}
		void retire() throws Exception { for (var resource : owned) resource.close(); }
	}

	private static HttpRequestOptions headers(Map<String, String> headers) {
		return new HttpRequestOptions(headers, null, null, Map.of());
	}

	private void assertSent() throws InterruptedException {
		Headers headers = received.poll(5, TimeUnit.SECONDS);
		assertEquals("s3cret", headers.getFirst("X-api-key"));
		assertEquals("Bearer token-1", headers.getFirst("Authorization"));
	}

	@Test void aWriteSendsTheHeaders() throws Exception {
		var bundle = new Bundle();
		String sink = new HttpWrite("http", "127.0.0.1", port(), "/value", null, "PUT", headers(HEADERS)).uri(bundle);
		assertFalse(sink.contains("s3cret"), sink);
		camel.createProducerTemplate().sendBody(sink, "{\"v\":2}");
		assertSent();
		bundle.retire();
		assertNull(camel.getRegistry().lookupByName(sink.replaceAll(".*#bean:", "").replaceAll("&.*", "")));
	}

	@Test void aPollSendsTheHeaders() throws Exception {
		var bundle = new Bundle();
		String source = new HttpPoll("http://127.0.0.1:" + port() + "/value", headers(HEADERS)).uri(bundle);
		assertFalse(source.contains("token-1"), source);
		camel.createConsumerTemplate().receiveBody(source, 5000);
		assertSent();
		bundle.retire();
	}

	/** A password with URI delimiters reaches the device intact and never appears in an endpoint URI. */
	@Test void basicCredentialsAreAHeaderNotAUri() throws Exception {
		String password = "p@ss&w#rd";
		String expected = "Basic " + java.util.Base64.getEncoder().encodeToString(("dmp:" + password).getBytes(StandardCharsets.UTF_8));
		var bundle = new Bundle();
		var basic = new HttpRequestOptions(Map.of(), "dmp", password, Map.of());
		String source = new HttpPoll("http://127.0.0.1:" + port() + "/value", basic).uri(bundle);
		var withHeader = new HttpRequestOptions(Map.of("X-Api-Key", "s3cret"), "dmp", password, Map.of());
		assertFalse(withHeader.toString().contains("p@ss") || withHeader.toString().contains("s3cret"), withHeader.toString());
		String sink = new HttpWrite("http", "127.0.0.1", port(), "/value", null, "PUT", withHeader).uri(bundle);
		for (String uri : new String[] { source, sink }) {
			assertFalse(uri.contains("p@ss") || uri.contains("dmp:") || uri.contains("authUsername"), uri);
			assertTrue(uri.contains("connectTimeout=30000&responseTimeout=30000"), uri);
		}
		camel.createConsumerTemplate().receiveBody(source, 5000);
		assertEquals(expected, received.poll(5, TimeUnit.SECONDS).getFirst("Authorization"));
		camel.createProducerTemplate().sendBody(sink, "{\"v\":2}");
		Headers written = received.poll(5, TimeUnit.SECONDS);
		assertEquals(expected, written.getFirst("Authorization"));
		assertEquals("s3cret", written.getFirst("X-api-key"));
		bundle.retire();
	}

	/** An API key in the query and a bearer token reach the device; neither is in an endpoint URI. */
	@Test void securitySchemeSecretsReachTheDeviceAndStayOutOfTheUri() throws Exception {
		var polls = new Bundle();
		String poll = new HttpPoll("http://127.0.0.1:" + port() + "/value?unit=C",
				new HttpRequestOptions(Map.of("Authorization", "Bearer t0ken"), null, null, Map.of("key", "k3y&x"))).uri(polls);
		assertFalse(poll.contains("k3y") || poll.contains("t0ken"), poll);
		camel.createConsumerTemplate().receiveBody(poll, 5000);
		assertEquals("Bearer t0ken", received.poll(5, TimeUnit.SECONDS).getFirst("Authorization"));
		assertEquals("unit=C&key=k3y%26x", queries.poll(5, TimeUnit.SECONDS));
		polls.retire();

		var observations = new Bundle();
		String observed = new HttpObservation("http://127.0.0.1:" + port() + "/value", "longpoll", "application/json",
				new HttpRequestOptions(Map.of(), null, null, Map.of("key", "k3y"))).uri(observations);
		assertFalse(observed.contains("k3y"), observed);
		camel.addRoutes(new org.apache.camel.builder.RouteBuilder() {
			@Override public void configure() { from(observed).to("mock:observed"); }
		});
		received.poll(5, TimeUnit.SECONDS);
		assertEquals("key=k3y", queries.poll(5, TimeUnit.SECONDS));
		camel.getRouteController().stopAllRoutes();
		observations.retire();
	}

	@Test void anObservationSendsTheHeaders() throws Exception {
		var bundle = new Bundle();
		String source = new HttpObservation("http://127.0.0.1:" + port() + "/value", "longpoll", "application/json",
				headers(HEADERS)).uri(bundle);
		assertFalse(source.contains("s3cret"), source);
		camel.addRoutes(new org.apache.camel.builder.RouteBuilder() {
			@Override public void configure() { from(source).to("mock:sink"); }
		});
		assertSent();
		camel.getRouteController().stopAllRoutes();
		bundle.retire();
	}
}
