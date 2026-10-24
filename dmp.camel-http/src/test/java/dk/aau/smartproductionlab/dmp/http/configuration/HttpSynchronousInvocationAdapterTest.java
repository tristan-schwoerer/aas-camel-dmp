package dk.aau.smartproductionlab.dmp.http.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;
import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;
import org.junit.jupiter.api.Test;

public class HttpSynchronousInvocationAdapterTest {
	@Test
	public void usesAIDFormMethodAndReturnsNamedJsonReply() throws Exception {
		AtomicReference<String> method = new AtomicReference<>();
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/invoke", request -> {
			method.set(request.getRequestMethod());
			byte[] body = "{\"result\":\"accepted\"}".getBytes(StandardCharsets.UTF_8);
			request.getResponseHeaders().add("Content-Type", "application/json");
			request.sendResponseHeaders(200, body.length);
			request.getResponseBody().write(body);
			request.close();
		});
		server.start();
		var camel = new DefaultCamelContext();
		camel.start();
		try {
			var adapter = new HttpSynchronousInvocationAdapter();
			String uri = "http://127.0.0.1:" + server.getAddress().getPort() + "/invoke";
			int baseline = camel.getEndpoints().size();
			var inventory = adapter.ownedEndpointUris(uri, schema("PUT"));
			assertEquals(1, inventory.size());
			assertEquals(baseline, camel.getEndpoints().size(), "preflight inventory must not allocate endpoints");
			Map<String, Object> result = invoke(adapter, camel,
					uri,
					Map.of("input", "value"), schema("PUT"));
			org.junit.jupiter.api.Assertions.assertNotNull(camel.hasEndpoint(inventory.iterator().next()), "inventory must identify the actual lazy invocation endpoint");
			assertEquals("PUT", method.get());
			assertEquals("accepted", result.get("result"));
		} finally {
			camel.stop();
			server.stop(0);
		}
	}

	@Test
	public void mapsDeviceHttpErrorsToAConnectorFailureWithoutAssigningAasStatus() throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/invoke", request -> {
			byte[] body = "{\"code\":\"BUSY\",\"message\":\"try later\"}"
					.getBytes(StandardCharsets.UTF_8);
			request.sendResponseHeaders(409, body.length);
			request.getResponseBody().write(body);
			request.close();
		});
		server.start();
		var camel = new DefaultCamelContext();
		camel.start();
		try {
			var adapter = new HttpSynchronousInvocationAdapter();
			ConnectorInvocationException failure;
			try {
				invoke(adapter, camel,
						"http://127.0.0.1:" + server.getAddress().getPort() + "/invoke",
						Map.of("input", "value"), schema("POST"));
				fail("HTTP error should be propagated as a connector outcome");
				return;
			} catch (ConnectorInvocationException expected) {
				failure = expected;
			}
			assertEquals(ConnectorInvocationException.Category.REMOTE, failure.getCategory());
			assertEquals("HTTP_409", failure.getConnectorCode());
		} finally {
			camel.stop();
			server.stop(0);
		}
	}

	@Test
	public void retryableResponsesCauseExactlyOneWireCommand() throws Exception {
		AtomicInteger commands = new AtomicInteger();
		AtomicInteger status = new AtomicInteger();
		AtomicReference<String> query = new AtomicReference<>();
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/invoke", request -> {
			commands.incrementAndGet();
			query.set(request.getRequestURI().getRawQuery());
			request.getRequestBody().readAllBytes();
			request.getResponseHeaders().add("Retry-After", "1");
			request.sendResponseHeaders(status.get(), -1);
			request.close();
		});
		server.start();
		var camel = new DefaultCamelContext();
		camel.start();
		try {
			var adapter = new HttpSynchronousInvocationAdapter();
			String uri = "http://127.0.0.1:" + server.getAddress().getPort() + "/invoke?mode=quick";
			for (int retryableStatus : new int[] {429, 503}) {
				for (String method : new String[] {"POST", "PUT"}) {
					status.set(retryableStatus);
					commands.set(0);
					ConnectorInvocationException failure = assertThrows(ConnectorInvocationException.class,
							() -> invoke(adapter, camel, uri, Map.of("input", "value"), schema(method)));
					assertEquals(ConnectorInvocationException.Category.REMOTE, failure.getCategory());
					assertEquals("HTTP_" + retryableStatus, failure.getConnectorCode());
					assertEquals(1, commands.get(), method + " must not retry HTTP " + retryableStatus);
					assertEquals("mode=quick", query.get());
				}
			}
		} finally {
			camel.stop();
			server.stop(0);
		}
	}

	private static SynchronousInvocationAdapter.InvocationSchema schema(String method) {
		return new SynchronousInvocationAdapter.InvocationSchema(Map.of(), java.util.Set.of(),
				Map.of(), java.util.Set.of(), Map.of("httpMethod", method));
	}

	/** Runs the adapter's invocation steps once, as the bridge's command route does. */
	static Map<String, Object> invoke(SynchronousInvocationAdapter adapter, org.apache.camel.CamelContext camel,
			String uri, Map<String, Object> request, SynchronousInvocationAdapter.InvocationSchema schema) throws Exception {
		String route = "direct:invoke-" + java.util.UUID.randomUUID();
		camel.addRoutes(new org.apache.camel.builder.RouteBuilder() {
			@Override public void configure() { adapter.configureInvoke(from(route), uri, schema); }
		});
		if (!camel.isStarted()) camel.start();
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> reply = camel.createProducerTemplate().requestBody(route, request, Map.class);
			return reply;
		} catch (org.apache.camel.CamelExecutionException failure) {
			if (failure.getCause() instanceof RuntimeException cause) throw cause;
			throw failure;
		}
	}

	/** Resources only named, never bound: the URI is all the test reads. */
	private static final BundleResources BOUND = new BundleResources() {
		@Override public <T extends ManagedRouteResource> T own(T resource) { return resource; }
	};

	@Test
	void anActionsTimeoutReplacesTheFormsDefaultOnce() {
		String action = new HttpWrite("http", "device", 80, "/move", null, null, HttpRequestOptions.none()).uri(BOUND);
		String uri = HttpSynchronousInvocationAdapter.invocationUri(action, 5000);
		assertEquals(1, uri.split("responseTimeout=", -1).length - 1, uri);
		assertTrue(uri.contains("connectTimeout=5000&responseTimeout=5000"), uri);
		assertTrue(uri.contains("httpClientConfigurer="), uri);
		assertTrue(HttpSynchronousInvocationAdapter.invocationUri("http://device/move", 5000).contains("NoRetryHttpClientConfigurer"));
	}

	/** A device that accepts the connection and never answers: the write is sent once and gives up at its timeout. */
	@Test
	void aWriteToAHungDeviceIsSentOnceAndGivesUp() throws Exception {
		var requests = new java.util.concurrent.atomic.AtomicInteger();
		var hung = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
		hung.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
		var release = new java.util.concurrent.CountDownLatch(1);
		hung.createContext("/value", exchange -> {
			requests.incrementAndGet();
			try { release.await(10, java.util.concurrent.TimeUnit.SECONDS); } catch (InterruptedException stopped) { Thread.currentThread().interrupt(); }
			exchange.close();
		});
		hung.start();
		try (var camel = new org.apache.camel.impl.DefaultCamelContext()) {
			camel.start();
			String sink = new HttpWrite("http", "127.0.0.1", hung.getAddress().getPort(), "/value", null, "PUT",
					HttpRequestOptions.none()).uri(new BundleResources() {
						@Override public <T extends ManagedRouteResource> T own(T resource) {
							try { resource.bind(camel); } catch (Exception e) { throw new IllegalStateException(e); }
							return resource;
						}
					});
			String uri = HttpSynchronousInvocationAdapter.invocationUri(sink, 500);
			long started = System.nanoTime();
			assertThrows(Exception.class, () -> camel.createProducerTemplate().sendBody(uri, "{\"v\":1}"));
			assertTrue(System.nanoTime() - started < 5_000_000_000L, "the write ends at its response timeout");
			assertEquals(1, requests.get(), "no automatic retry of a write");
		} finally {
			release.countDown();
			hung.stop(0);
		}
	}
}
