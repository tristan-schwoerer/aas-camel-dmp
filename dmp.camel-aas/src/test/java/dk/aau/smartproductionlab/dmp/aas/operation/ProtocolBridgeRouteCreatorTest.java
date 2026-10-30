/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.Response;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.util.TransformerMeta;
import org.junit.jupiter.api.Test;

/**
 * ProtocolBridge flow (ADR-020): REST caller → request transform → native
 * command sink → response transform. The undeclared MQTT path must not
 * silently fall back to payload-Uuid correlation.
 */
class ProtocolBridgeRouteCreatorTest extends CamelTestSupport {

	private static final String HTTP_ROUTE_ID = "packingCellAAS_Restart_interaction";
	private static final String MEMBER_ROUTE_ID = "packingCellAAS_Restart_member_interaction";
	private static final String REAL_LUA_ROUTE_ID = "packingCellAAS_Restart_real_lua_interaction";
	private static final ConcurrentMap<String, String> LAST_COMMAND = new ConcurrentHashMap<>();
	private static final ConcurrentMap<String, String> LAST_REQUEST_CHAIN_INPUT = new ConcurrentHashMap<>();
	private static final AtomicInteger COMMAND_INVOCATIONS = new AtomicInteger();
	private static final AtomicInteger RESPONSE_TRANSFORMS = new AtomicInteger();
	private static volatile CountDownLatch AUDIT_ENTERED = new CountDownLatch(0);
	private static volatile CountDownLatch AUDIT_RELEASE = new CountDownLatch(0);
	private static volatile CountDownLatch AUDIT_COMPLETED = new CountDownLatch(0);
	private static final AtomicInteger AUDIT_WRITES = new AtomicInteger();
	private static final AtomicReference<Object> AAS_RESPONSE_BODY = new AtomicReference<>();
	private static volatile CountDownLatch AAS_RESPONSE_WRITTEN = new CountDownLatch(0);
	/** What the log bridges' response transformation saw (meta, requestSources) and what their Log sink got. */
	private static final java.util.concurrent.BlockingQueue<Map<String, Object>> LOG_META = new java.util.concurrent.LinkedBlockingQueue<>();
	private static final java.util.concurrent.BlockingQueue<Object> LOG_REQUEST_SOURCES = new java.util.concurrent.LinkedBlockingQueue<>();
	private static final java.util.concurrent.BlockingQueue<Object> LOG_ROWS = new java.util.concurrent.LinkedBlockingQueue<>();
	/** The consolidated REST surface port (bound once per class). */
	private static int restPort;

	/** The Operation's reply, as the response mapping of the Restart action answers it. */
	static final Response.ResponseSource RESTART_REPLY = new Response.ResponseSource("Restart", null, "/Restart");

	/**
	 * Generic bridge route: connector target semantics are in connector options/adapters. The authored response
	 * mapping: the reply's Restart member is the ResponseSource, the Operation (SinkId Restart) declares the output
	 * "state".
	 */
	private static OperationRoute syncBridge(String routeId, String restPath, String requestLuaUri, String commandId,
			String commandUri) {
		return new OperationRoute(routeId, new OperationRoute.Identity(restPath, "Restart", null, null),
				Endpoint.of(requestLuaUri), List.of(), command(commandId, commandUri, new OperationRoute.Command.Member("Restart", null)),
				caller(Map.of("Restart", "$"), "Restart", List.of("state")),
				new Response(Endpoint.of("direct:luaResp"), List.of(RESTART_REPLY), List.of()));
	}

	/** An action invoked through {@code uri}, its command assembled from {@code members}. */
	static OperationRoute.Command command(String id, String uri, OperationRoute.Command.Member... members) {
		return new OperationRoute.Command(new Sink(id, null, Endpoint.of(uri)), List.of(members), false, Map.of(), List.of(),
				Map.of(), List.of(), null, Map.of());
	}

	/** The caller's side: the Operation's Sources, the ResponseSink answering it, its outputs in order. */
	static OperationRoute.Caller caller(Map<String, String> inputSourcePaths, String replySinkId, List<String> outputOrder) {
		return new OperationRoute.Caller(inputSourcePaths, Map.of(), Map.of(), Map.of(), outputOrder, Map.of(), replySinkId);
	}

	/** {@code bridge} with another response half. */
	static OperationRoute withResponse(OperationRoute bridge, Response response) {
		return new OperationRoute(bridge.routeId(), bridge.identity(), bridge.requestTransformation(), bridge.fanIn(),
				bridge.command(), bridge.caller(), response);
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				// bind the consolidated REST surface to a free ephemeral port
				try (java.net.ServerSocket probe = new java.net.ServerSocket(0)) {
					restPort = probe.getLocalPort();
				}
				restConfiguration().component("jetty").host("localhost").port(restPort);

				syncBridge(HTTP_ROUTE_ID, "/operations/packingCellAAS/Restart", "direct:luaReqRestart",
						"httpCmd", "direct:httpSink").build(this);
				// Two request transformations in a row: the second sees what the first made.
				from("direct:luaReqMemberChain").to("direct:luaReqMember").to("direct:luaReqMemberSecond");
				OperationRoute member = syncBridge(MEMBER_ROUTE_ID, "/operations/packingCellAAS/Restart/member",
						"direct:luaReqMemberChain", "infraCommand", "direct:infraCommand");
				new OperationRoute(member.routeId(), member.identity(), member.requestTransformation(), member.fanIn(),
						command("infraCommand", "direct:infraCommand",
								new OperationRoute.Command.Member("AuthoredCommand", "$.value")),
						member.caller(), member.response()).build(this);
				syncBridge("packingCellAAS_Restart_failure_interaction",
						"/operations/packingCellAAS/Restart/failure", "direct:luaReqFail", "httpCmd", "direct:httpSink")
						.build(this);
				withResponse(syncBridge("packingCellAAS_Restart_reply_failure_interaction",
						"/operations/packingCellAAS/Restart/reply-failure", "direct:luaReqRestart", "httpCmd", "direct:httpSink"),
						new Response(Endpoint.of("direct:luaRespFail"), List.of(RESTART_REPLY), List.of())).build(this);
				Endpoint audit = Endpoint.of("direct:auditSink");
				withResponse(syncBridge(REAL_LUA_ROUTE_ID, "/operations/packingCellAAS/Restart/real-lua",
						"direct:luaReqRestart", "httpCmd", "direct:httpSink"),
						new Response(Endpoint.of(realLuaResponseUri()), List.of(RESTART_REPLY), IntStream.range(0, 131)
								.mapToObj(i -> new Sink("auditWrite", "Audit" + i, audit, null, null, null, "audit-" + i)).toList()))
						.build(this);
				// An AAS element written as a ResponseSink: its endpoint places the SinkId's value itself.
				Endpoint aas = new Endpoint() {
					@Override public String uri(BundleResources resources) { return "direct:aasTarget"; }
					@Override public boolean appliesValuePath() { return true; }
				};
				withResponse(syncBridge("packingCellAAS_Restart_aas_response_interaction",
						"/operations/packingCellAAS/Restart/aas-response", "direct:luaReqRestart", "httpCmd", "direct:httpSink"),
						new Response(Endpoint.of("direct:luaAasResponse"), List.of(RESTART_REPLY),
								List.of(new Sink("aasWrite", "Audit", aas, null, null, null, "device")))).build(this);

				for (String[] bridge : new String[][] { { "log", "httpCmd", "direct:httpSink" },
						{ "log-failure", "failCmd", "direct:failCmd" } }) {
					withResponse(syncBridge("packingCellAAS_Restart_" + bridge[0].replace('-', '_') + "_interaction",
							"/operations/packingCellAAS/Restart/" + bridge[0], "direct:luaReqRestart", bridge[1], bridge[2]),
							new Response(Endpoint.of("direct:luaLog"), List.of(RESTART_REPLY),
									List.of(new Sink("logWrite", "Log", Endpoint.of("direct:logTarget"), null, null, null, "log"))))
							.build(this);
				}

				// test doubles for the Lua blob query endpoints
				from("direct:luaReq").process(e -> e.getMessage()
						.setBody(Map.of("Halt", e.getMessage().getBody())));
				from("direct:luaReqRestart").process(e -> {
					Map<?, ?> input = e.getMessage().getBody(Map.class);
					e.getMessage().setBody(Map.of("Restart", input.get("Restart")));
				});
				from("direct:luaReqMember").setBody().constant(Map.of(
						"AuthoredCommand", Map.of("value", "original")));
				from("direct:luaReqFail").process(exchange -> {
					throw new IllegalStateException("request transformer failed");
				});
				from("direct:luaRespFail").process(exchange -> {
					throw new IllegalStateException("response transformer failed");
				});
				from("direct:luaAasResponse").setBody().constant(Map.of(
						"Restart", Map.of("state", "restarting"), "Audit", "best-effort"));
				from("direct:luaReqMemberSecond").process(exchange ->
					LAST_REQUEST_CHAIN_INPUT.put(MEMBER_ROUTE_ID,
							dk.aau.smartproductionlab.dmp.core.util.JsonCodec.toJson(
									exchange.getMessage().getBody())));
				from("direct:luaResp").process(e -> {
					RESPONSE_TRANSFORMS.incrementAndGet();
				});


				// generic synchronous endpoint used as the Invoke adapter seam
				from("direct:httpSink").process(e -> {
					COMMAND_INVOCATIONS.incrementAndGet();
					LAST_COMMAND.put(HTTP_ROUTE_ID, String.valueOf(e.getMessage().getBody()));
					e.getMessage().setBody(Map.of("Restart", Map.of("state", "restarting")));
				});
				from("direct:infraCommand").process(e -> {
					LAST_COMMAND.put(MEMBER_ROUTE_ID, String.valueOf(e.getMessage().getBody()));
					e.getMessage().setBody(Map.of("Restart", Map.of("state", "restarting")));
				});
				from("direct:auditSink").process(exchange -> {
					AUDIT_WRITES.incrementAndGet();
					AUDIT_ENTERED.countDown();
					if (!AUDIT_RELEASE.await(5, TimeUnit.SECONDS)) {
						throw new IllegalStateException("test did not release extra response sink");
					}
					AUDIT_COMPLETED.countDown();
				});
				from("direct:luaLog").process(exchange -> {
					@SuppressWarnings("unchecked")
					Map<String, Object> meta = exchange.getProperty(TransformerMeta.PROPERTY, Map.class);
					LOG_META.add(meta);
					LOG_REQUEST_SOURCES.add(exchange.getProperty(TransformerMeta.REQUEST_SOURCES));
					Object outcome = ((Map<?, ?>) meta.get("invocation")).get("outcome");
					exchange.getMessage().setBody("Succeeded".equals(outcome)
							? Map.of("Restart", Map.of("state", "restarting"), "Log", Map.of("outcome", outcome))
							: Map.of("Log", Map.of("outcome", outcome)));
				});
				from("direct:logTarget").process(exchange -> LOG_ROWS.add(exchange.getMessage().getBody()));
				from("direct:failCmd").process(exchange -> {
					throw new dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException(
							dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException.Category.REMOTE,
							"DEVICE_REJECTED", "the device rejected the command", Map.of());
				});
				from("direct:aasTarget").process(exchange -> {
					AAS_RESPONSE_BODY.set(exchange.getMessage().getBody());
					AAS_RESPONSE_WRITTEN.countDown();
				});
			}
		};
	}

	/**
	 * Drives the bridge through its REST surface (the rest-DSL merge folds the
	 * direct assembly route into the jetty consumer, so the operation is
	 * invoked like BaSyx would): {@code body}'s variables are delegated as an
	 * OperationVariable array, and a successful reply's variables are read back
	 * as one JSON object.
	 */
	private static int post(String path, String body, StringBuilder out) throws Exception {
		java.net.HttpURLConnection connection = (java.net.HttpURLConnection)
				java.net.URI.create("http://localhost:" + restPort + path).toURL().openConnection();
		connection.setRequestMethod("POST");
		connection.setRequestProperty("Content-Type", "application/json");
		connection.setDoOutput(true);
		connection.setConnectTimeout(2000);
		try (var os = connection.getOutputStream()) {
			os.write(Delegation.of(body).getBytes(java.nio.charset.StandardCharsets.UTF_8));
		}
		int status = connection.getResponseCode();
		try (var is = status >= 400 ? connection.getErrorStream() : connection.getInputStream()) {
			String reply = is == null ? "" : new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
			out.append(status >= 400 ? reply : Delegation.outputs(reply));
		}
		return status;
	}

	/** Compiles the response Lua fixture in memory and binds it like the runtime does. */
	private String realLuaResponseUri() {
		try (var input = ProtocolBridgeRouteCreatorTest.class.getResourceAsStream("/response-object.lua")) {
			String name = "response-object-" + java.util.UUID.randomUUID();
			context.getRegistry().bind(name, dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource
					.fromBytes(input.readAllBytes(), "response-object.lua", 1 << 20));
			return "lua:managed:" + name;
		} catch (Exception e) {
			throw new IllegalStateException("could not compile response Lua fixture", e);
		}
	}

	@Test
	void operationBridgeAnswersInlineThroughAConnectorAdapter() throws Exception {
		int transformsBefore = RESPONSE_TRANSFORMS.get();
		StringBuilder body = new StringBuilder();
		int status = post("/operations/packingCellAAS/Restart",
				"{\"mode\":\"warm\"}", body);

		assertEquals(200, status);
		String command = LAST_COMMAND.get(HTTP_ROUTE_ID);
		assertTrue(command.contains("warm"), "the request transform shaped the command: " + command);
		assertEquals("{\"state\":\"restarting\"}", body.toString(),
				"the inline HTTP reply is unwrapped for the caller");
		assertEquals(transformsBefore + 1, RESPONSE_TRANSFORMS.get());
	}

	@Test
	void mapsInfrastructureSinkIdentityToTheAuthoredSinkId() throws Exception {
		StringBuilder body = new StringBuilder();
		int status = post("/operations/packingCellAAS/Restart/member", "{}", body);
		assertEquals(200, status);
		assertEquals("{\"AuthoredCommand\":{\"value\":\"original\"}}",
				LAST_REQUEST_CHAIN_INPUT.get(MEMBER_ROUTE_ID));
		assertTrue(LAST_COMMAND.get(MEMBER_ROUTE_ID).contains("original"),
				"the authored SinkId's value is the command although infra and authored IDs differ: "
						+ LAST_COMMAND.get(MEMBER_ROUTE_ID));
	}

	@Test
	void requestTransformerFailureIsHandledWithoutCommandInvocation() throws Exception {
		int before = COMMAND_INVOCATIONS.get();
		StringBuilder body = new StringBuilder();
		int status = post("/operations/packingCellAAS/Restart/failure", "{}", body);
		assertEquals(500, status);
		assertEquals(java.util.Map.of("code", "DMP_OPERATION_FAILED", "error", "Operation failed"),
				Delegation.error(body.toString()));
		assertEquals(before, COMMAND_INVOCATIONS.get());
	}

	@Test
	void responseTransformerFailureDoesNotRetryDeviceInvocation() throws Exception {
		int before = COMMAND_INVOCATIONS.get();
		StringBuilder body = new StringBuilder();
		int status = post("/operations/packingCellAAS/Restart/reply-failure", "{}", body);
		assertEquals(500, status);
		assertEquals(java.util.Map.of("code", "DMP_OPERATION_FAILED", "error", "Operation failed"),
				Delegation.error(body.toString()));
		assertEquals(before + 1, COMMAND_INVOCATIONS.get());
	}

	@Test
	void aSuccessfulCallsResponseTransformationSeesItsInvocation() throws Exception {
		LOG_META.clear(); LOG_ROWS.clear();
		assertEquals(200, post("/operations/packingCellAAS/Restart/log", "{}", new StringBuilder()));
		Map<String, Object> meta = LOG_META.poll(5, TimeUnit.SECONDS);
		Map<?, ?> invocation = (Map<?, ?>) meta.get("invocation");
		assertEquals("Succeeded", invocation.get("outcome"));
		assertEquals("Restart", invocation.get("skill"));
		assertTrue(invocation.get("id") instanceof String id && !id.isBlank(), String.valueOf(invocation));
		assertTrue(invocation.get("durationMs") instanceof Long, String.valueOf(invocation));
		assertEquals(Map.of("kind", "direct"), ((Map<?, ?>) meta.get("sinks")).get("Log"));
		assertEquals(Map.of("kind", "operation"), ((Map<?, ?>) meta.get("sinks")).get("Restart"));
		assertTrue(((Map<?, ?>) ((Map<?, ?>) meta.get("sources")).get("Restart")).get("time") instanceof String);
		assertEquals(Map.of("outcome", "Succeeded"), LOG_ROWS.poll(5, TimeUnit.SECONDS));
	}

	@Test
	void aFailedCallStillWritesItsLogSinksAndKeepsTheCallersError() throws Exception {
		LOG_META.clear(); LOG_REQUEST_SOURCES.clear(); LOG_ROWS.clear();
		StringBuilder body = new StringBuilder();
		int status = post("/operations/packingCellAAS/Restart/log-failure", "{}", body);
		assertEquals(502, status, "a device's rejection answers the caller as before: " + body);
		var reply = (Map<?, ?>) dk.aau.smartproductionlab.dmp.core.util.JsonCodec.parseJson(body.toString());
		assertFalse(String.valueOf(reply).contains("outcome"), "the caller's error is not the log row: " + reply);
		Map<String, Object> meta = LOG_META.poll(5, TimeUnit.SECONDS);
		Map<?, ?> invocation = (Map<?, ?>) meta.get("invocation");
		assertEquals("Failed", invocation.get("outcome"));
		assertEquals(Map.of("code", "DEVICE_REJECTED", "category", "REMOTE", "message", "the device rejected the command"),
				invocation.get("error"));
		assertEquals(Map.of(), meta.get("sources"), "a failed call has no ResponseSources");
		assertTrue(LOG_REQUEST_SOURCES.poll(5, TimeUnit.SECONDS) instanceof Map, "the request's sources reach the failure response");
		assertEquals(Map.of("outcome", "Failed"), LOG_ROWS.poll(5, TimeUnit.SECONDS));
	}

	@Test
	void realLuaReceivesAndReturnsAJsonObjectForProjectedMapReplies() throws Exception {
		AUDIT_ENTERED = new CountDownLatch(1);
		AUDIT_RELEASE = new CountDownLatch(1);
		AUDIT_COMPLETED = new CountDownLatch(129);
		AUDIT_WRITES.set(0);
		StringBuilder body = new StringBuilder();
		long started = System.nanoTime();
		int status = post("/operations/packingCellAAS/Restart/real-lua", "{\"mode\":\"warm\"}", body);
		long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
		try {
			assertEquals(200, status);
			assertEquals("{\"state\":\"restarting\"}", body.toString());
			assertTrue(elapsedMillis < 2_000, "slow extra response sink delayed the caller");
			assertTrue(AUDIT_ENTERED.await(2, TimeUnit.SECONDS));
		} finally {
			AUDIT_RELEASE.countDown();
		}
		assertTrue(AUDIT_COMPLETED.await(3, TimeUnit.SECONDS));
		assertEquals(129, AUDIT_WRITES.get(), "the writer and its queue of 128 take 129 writes; the other two are rejected");
	}

	@Test
	void aasConnectorRouteAddsTheSinkIdEnvelopeAfterCoreDispatch() throws Exception {
		AAS_RESPONSE_BODY.set(null);
		AAS_RESPONSE_WRITTEN = new CountDownLatch(1);
		StringBuilder body = new StringBuilder();
		int status = post("/operations/packingCellAAS/Restart/aas-response", "{}", body);

		assertEquals(200, status);
		assertTrue(AAS_RESPONSE_WRITTEN.await(2, TimeUnit.SECONDS));
		assertEquals(Map.of("Audit", "best-effort"), AAS_RESPONSE_BODY.get());
	}
}
