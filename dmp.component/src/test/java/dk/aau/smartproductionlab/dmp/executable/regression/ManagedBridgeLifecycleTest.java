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
package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import dk.aau.smartproductionlab.dmp.aas.operation.OperationRoute;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.Response;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.routebuilder.DmpRouteBuilder;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler;
import dk.aau.smartproductionlab.dmp.management.model.WireProjection;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonParser;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;

class ManagedBridgeLifecycleTest {
	private static final class Runtime extends DmpComponent {
		Runtime() { super(Duration.ofSeconds(4)); }
		org.apache.camel.CamelContext context() { return camelContext; }
		void listener(int port) throws Exception {
			camelContext.addRoutes(new org.apache.camel.builder.RouteBuilder() {
				@Override public void configure() {
					restConfiguration().component("jetty").host("127.0.0.1").port(port).inlineRoutes(false);
				}
			});
		}
	}
	private static OperationRoute operationWithReply(String id, String path, String replyEndpoint) {
		return operation(id, path, replyEndpoint);
	}

	@Test
	void inFlightActionAndDetachedWriteDrainAgainstOldTargetsWhileSiblingContinuesWithoutReplay() throws Exception {
		Runtime runtime = new Runtime();
		var commandEntered = new java.util.concurrent.CountDownLatch(1);
		var commandRelease = new java.util.concurrent.CountDownLatch(1);
		var writeEntered = new java.util.concurrent.CountDownLatch(1);
		var writeRelease = new java.util.concurrent.CountDownLatch(1);
		var commands = new java.util.concurrent.atomic.AtomicInteger();
		var oldWrites = new java.util.concurrent.atomic.AtomicInteger();
		var newWrites = new java.util.concurrent.atomic.AtomicInteger();
		var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
		try {
			runtime.startComponent();
			int port;
			try (var socket = new java.net.ServerSocket(0)) { port = socket.getLocalPort(); }
			runtime.listener(port);
			var caller = java.net.http.HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
			runtime.context().addRoutes(new org.apache.camel.builder.RouteBuilder() {
				@Override public void configure() {
					from("direct:managed-request-transform").process(exchange ->
							exchange.getMessage().setBody(Map.of("command", exchange.getMessage().getBody(Map.class).get("Action"))));
					from("direct:managed-response-transform").process(exchange -> { });
					from("direct:managed-command-sink").process(exchange -> {
						commands.incrementAndGet();
						if (Boolean.TRUE.equals(exchange.getMessage().getBody(Map.class).get("slow"))) {
							commandEntered.countDown();
							if (!commandRelease.await(5, java.util.concurrent.TimeUnit.SECONDS)) throw new IllegalStateException("test command deadline");
						}
						exchange.getMessage().setBody(Map.of("reply", Map.of("result", "accepted")));
					});
					from("direct:old-reply").process(exchange -> {
						writeEntered.countDown();
						if (!writeRelease.await(5, java.util.concurrent.TimeUnit.SECONDS)) throw new IllegalStateException("test write deadline");
						oldWrites.incrementAndGet();
					});
					from("direct:new-reply").process(exchange -> newWrites.incrementAndGet());
					from("direct:sibling-reply").process(exchange -> { });
				}
			});
			var old = runtime.installBundle(runtime.stageBundle("A", runtime.nextBundleGeneration("A"),
					operationWithReply("operation-A", "/draining/A", "direct:old-reply")));
			var sibling = runtime.installBundle(runtime.stageBundle("B", runtime.nextBundleGeneration("B"),
					operationWithReply("operation-B", "/draining/B", "direct:sibling-reply")));
			var siblingRoute = runtime.context().getRoute("operation-B");
			{
				var request = caller.sendAsync(post(port, "/draining/A", "{\"slow\":true}"),
						java.net.http.HttpResponse.BodyHandlers.ofString());
				assertTrue(commandEntered.await(2, java.util.concurrent.TimeUnit.SECONDS));
				var candidate = runtime.stageBundle("A", runtime.nextBundleGeneration("A"),
						operationWithReply("operation-A", "/draining/A", "direct:new-reply"));
				var replace = pool.submit(() -> runtime.replaceBundle(old, candidate));
				long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
				while (old.active()) {
					if (System.nanoTime() > deadline) throw new AssertionError("operation did not quiesce");
					Thread.sleep(10);
				}
				assertFalse(replace.isDone());
				var unavailable = caller.send(post(port, "/draining/A", "{\"slow\":false}"), java.net.http.HttpResponse.BodyHandlers.ofString());
				assertEquals(503, unavailable.statusCode());
				assertEquals("DMP mapping temporarily unavailable", unavailable.body());
				assertEquals(1, commands.get(), "quiesced caller must never invoke a device");
				var siblingReply = caller.send(post(port, "/draining/B", "{\"slow\":false}"), java.net.http.HttpResponse.BodyHandlers.ofString());
				assertEquals(200, siblingReply.statusCode());
				// The untyped reply output reaches BaSyx as one xs:string variable.
				assertTrue(siblingReply.body().contains("{result=accepted}"), siblingReply.body());
				assertSame(siblingRoute, runtime.context().getRoute("operation-B"));
				commandRelease.countDown();
				var accepted = request.get(3, java.util.concurrent.TimeUnit.SECONDS);
				assertEquals(200, accepted.statusCode());
				assertTrue(accepted.body().contains("{result=accepted}"), accepted.body());
				assertTrue(writeEntered.await(2, java.util.concurrent.TimeUnit.SECONDS));
				assertFalse(replace.isDone(), "old detached write must drain before old helpers/targets retire");
				assertTrue(runtime.context().getRouteController().getRouteStatus("operation-A-response-write").isStarted());
				writeRelease.countDown();
				var installed = replace.get(3, java.util.concurrent.TimeUnit.SECONDS);
				assertSame(sibling, runtime.managedBundle("B"));
				assertTrue(installed.active());
				assertEquals(2, commands.get(), "replacement must not replay the old action");
				assertEquals(1, oldWrites.get());
				assertEquals(0, newWrites.get(), "accepted old work must never migrate to the new target");
				assertEquals(200, caller.send(post(port, "/draining/A", "{\"slow\":false}"), java.net.http.HttpResponse.BodyHandlers.ofString()).statusCode());
				runtime.removeBundle(installed);
				assertEquals(3, commands.get());
				assertEquals(1, newWrites.get());
				assertEquals(404, caller.send(post(port, "/draining/A", "{}"), java.net.http.HttpResponse.BodyHandlers.ofString()).statusCode());
			}
		} finally {
			commandRelease.countDown(); writeRelease.countDown(); pool.shutdownNow(); runtime.stopComponent();
		}
	}

	private static java.net.http.HttpRequest post(int port, String path, String body) {
		return java.net.http.HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
				.timeout(Duration.ofSeconds(2)).header("Content-Type", "application/json")
				.POST(java.net.http.HttpRequest.BodyPublishers.ofString(OperationCalls.of(body))).build();
	}

	@Test
	void operationChurnAndConflictingRestPreflightPreserveExactResourceCounts() throws Exception {
		Runtime runtime = new Runtime();
		org.apache.camel.ProducerTemplate template = null;
		var writes = new java.util.concurrent.atomic.AtomicInteger();
		try {
			runtime.startComponent();
			runtime.context().addRoutes(new org.apache.camel.builder.RouteBuilder() {
				@Override public void configure() {
					from("direct:managed-request-transform").process(exchange ->
							exchange.getMessage().setBody(Map.of("command", exchange.getMessage().getBody(Map.class).get("Action"))));
					from("direct:managed-response-transform").process(exchange -> { });
					from("direct:managed-command-sink").setBody(constant(Map.of("reply", "accepted")));
					from("direct:churn-reply").process(exchange -> writes.incrementAndGet());
				}
			});
			template = runtime.context().createProducerTemplate();
			// Warm Camel's persistent shutdown executor before comparing steady-state inventories.
			var warm = runtime.installBundle("warm", operationWithReply("warm", "/churn/warm", "direct:churn-reply"));
			template.requestBody("direct:warm", OperationCalls.of("{\"value\":1}"));
			runtime.removeBundle(warm);
			int routes = runtime.context().getRoutesSize();
			int endpoints = runtime.context().getEndpoints().size();
			int restModels = runtime.context().getCamelContextExtension().getContextPlugin(org.apache.camel.model.Model.class).getRestDefinitions().size();
			int registrations = org.apache.camel.support.PluginHelper.getRestRegistry(runtime.context()).listAllRestServices().size();
			int templates = runtime.context().hasServices(org.apache.camel.ProducerTemplate.class).size();
			int executors = executorCount(runtime);
			for (int i = 0; i < 6; i++) {
				var first = runtime.installBundle(runtime.stageBundle("A", runtime.nextBundleGeneration("A"), operationWithReply("operation-A", "/churn/A", "direct:churn-reply")));
				var second = runtime.installBundle(runtime.stageBundle("B", runtime.nextBundleGeneration("B"), operationWithReply("operation-B", "/churn/B", "direct:churn-reply")));
				template.requestBody("direct:operation-A", OperationCalls.of("{\"value\":1}"));
				template.requestBody("direct:operation-B", OperationCalls.of("{\"value\":1}"));
				try (var conflict = runtime.stageBundle("A", runtime.nextBundleGeneration("A"), operation("operation-A", "/churn/B"))) {
					assertThrows(IllegalStateException.class, () -> runtime.replaceBundle(first, conflict));
					assertSame(first, runtime.managedBundle("A"));
					assertTrue(first.active());
					assertSame(second, runtime.managedBundle("B"));
				}
				var replaced = runtime.replaceBundle(first, runtime.stageBundle("A", runtime.nextBundleGeneration("A"), operationWithReply("operation-A", "/churn/A", "direct:churn-reply")));
				template.requestBody("direct:operation-A", OperationCalls.of("{\"value\":1}"));
				runtime.removeBundle(replaced);
				assertTrue(second.active());
				runtime.removeBundle(second);
				assertEquals(routes, runtime.context().getRoutesSize());
				assertEquals(endpoints, runtime.context().getEndpoints().size());
				assertEquals(restModels, runtime.context().getCamelContextExtension().getContextPlugin(org.apache.camel.model.Model.class).getRestDefinitions().size());
				assertEquals(registrations, org.apache.camel.support.PluginHelper.getRestRegistry(runtime.context()).listAllRestServices().size());
				// Operation mappings own no ProducerTemplate: per-sink chains and response writes are route steps.
				assertEquals(templates, runtime.context().hasServices(org.apache.camel.ProducerTemplate.class).size());
				assertEquals(executors, executorCount(runtime));
				// Response-sink writes run asynchronously on the bounded Wire Tap executor.
				int expectedWrites = 1 + (i + 1) * 3;
				org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).until(() -> writes.get() >= expectedWrites);
				assertEquals(expectedWrites, writes.get());
			}
		} finally { if (template != null) template.stop(); runtime.stopComponent(); }
	}

	private static int executorCount(Runtime runtime) throws Exception {
		// Camel 4.22 has no public executor inventory; verify retention, not merely live threads.
		var field = org.apache.camel.impl.engine.BaseExecutorServiceManager.class.getDeclaredField("executorServices");
		field.setAccessible(true);
		return ((List<?>) field.get(runtime.context().getExecutorServiceManager())).size();
	}

	@Test
	void operationRestReservationsAndProcessorResourcesRetireAndReaddIndependently() {
		DmpComponent dmp = new DmpComponent();
		try {
			dmp.startComponent();
			var first = dmp.installBundle("operation-A", operation("operation-A", "/managed/A"));
			var second = dmp.installBundle("operation-B", operation("operation-B", "/managed/B"));
		// An explicit response mapping has a ResponseTransformation, which a failed call also runs for the ResponseSinks.
		assertEquals(Set.of("operation-A", "operation-A-rest-consumer", "operation-A-response-write", "operation-A-command",
				"operation-A-failure-response"), first.routeIds());
		assertEquals(Set.of("operation-B", "operation-B-rest-consumer", "operation-B-response-write", "operation-B-command",
				"operation-B-failure-response"), second.routeIds());
			assertEquals(Set.of(new DmpRouteBuilder.RestReservation("POST", "/managed/A")), first.restReservations());
			assertSame(second, dmp.managedBundle("operation-B"));

			dmp.removeBundle(first);
			assertNull(dmp.managedBundle("operation-A"));
			assertSame(second, dmp.managedBundle("operation-B"));
			var readded = dmp.installBundle("operation-A", operation("operation-A", "/managed/A"));
			assertEquals(Set.of(new DmpRouteBuilder.RestReservation("POST", "/managed/A")), readded.restReservations());

			dmp.removeBundle(readded);
			dmp.removeBundle(second);
			assertNull(dmp.managedBundle("operation-B"));
		} finally {
			dmp.stopComponent();
		}
	}

	@Test
	void authoredOperationFixturesInstallIndependentlyWithoutPythonOrGeneratedDescriptors() throws Exception {
		String[] cases={"opcua-method","http-operation","grouped-action"};
		var repository=new FakeAasRepository();
		DmpComponent dmp=new DmpComponent();
		try {
			var client=repository.client();
			dmp.startComponent();
			for(String name:cases) {
				String prefix="runtime-model-contracts/"+name+"/";
				// What aas-model publishes: resolved templates and the Operation's delegation URL.
				var context=JsonParser.parseString(resource(prefix+"expected.json")).getAsJsonObject().getAsJsonObject("compilerContext");
				String aasId=context.get("aas_id").getAsString(),aasIdShort=context.get("aas_id_short").getAsString();
				String aimcJson=PublishedModels.publish(resource(prefix+"aimc.json"),aasId,aasIdShort);
				repository.models().clear();
				repository.put(aimcJson).put(PublishedModels.publish(resource(prefix+"aid.json"),aasId,aasIdShort));
				for(var entry:JsonParser.parseString(resource("runtime-model-contracts/manifest.json"))
						.getAsJsonObject().getAsJsonArray("cases")) {
					var fixture=entry.getAsJsonObject();
					if(!name.equals(fixture.get("name").getAsString()))continue;
					for(var dependency:fixture.getAsJsonArray("dependencies")) {
						var descriptor=dependency.getAsJsonObject();
						repository.put(PublishedModels.publish(resource(prefix+descriptor.get("path").getAsString()),aasId,aasIdShort));
					}
				}
				var plan=new MappingPlanCompiler(new AasReferenceResolver(client))
						.compile(WireProjection.aimc(dk.aau.smartproductionlab.dmp.aas.model.Aas.read(aimcJson, org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
				assertEquals(AasRepositoryClient.Status.FOUND,plan.status(),name);
				var result=new ConnectorConfigurationFactory().createBundles(plan.value(),
						new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas-server:8081")));
				assertTrue(result.issues().isEmpty(),name+" "+result.issues());
				assertEquals(1,result.bundles().size(),name);
				var bundle=result.bundles().get(0);
				String action=switch(name) {
					case "opcua-method" -> "Calibrate";
					case "http-operation" -> "Restart";
					default -> "Move";
				};
				assertInstanceOf(OperationRoute.class,bundle.route(),name);
				var installedBundle=dmp.installBundle(bundle.bundleId(),bundle.route());
				assertTrue(installedBundle.restReservations().stream().anyMatch(reservation ->
						reservation.path().startsWith("/submodels/")
								&& reservation.path().endsWith("/submodel-elements/Skills."+action+".operation/invoke")),name);
				Set<String> installedRouteIds=installedBundle.routeIds();
				dmp.removeBundle(installedBundle);

				var replacement=new ConnectorConfigurationFactory().createBundles(plan.value(),
						new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas-server:8081")));
				var readded=dmp.installBundle(replacement.bundles().get(0).bundleId(),replacement.bundles().get(0).route());
				assertEquals(installedRouteIds,readded.routeIds(),name);
				dmp.removeBundle(readded);
			}
		} finally {
			dmp.stopComponent();
			repository.close();
		}
	}

	private static String resource(String path) throws Exception {
		try(var stream=ManagedBridgeLifecycleTest.class.getClassLoader().getResourceAsStream(path)) {
			if(stream==null)throw new IllegalStateException("missing fixture resource: "+path);
			return new String(stream.readAllBytes(),StandardCharsets.UTF_8);
		}
	}

	private static OperationRoute operation(String routeId, String path) {
		return operation(routeId, path, "direct:managed-command-sink");
	}

	private static OperationRoute operation(String routeId, String path, String replyEndpoint) {
		var command = new OperationRoute.Command(new Sink("command", "command", Endpoint.of("direct:managed-command-sink")),
				List.of(new OperationRoute.Command.Member("command", null)), false, Map.of(), List.of(), Map.of(),
				List.of(), null, Map.of());
		// The whole device reply is the ResponseSource reply; it is both the Operation's output
		// reply (caller SinkId reply) and the value written to the reply sink. The response
		// transformation passes it on unchanged.
		var caller = new OperationRoute.Caller(Map.of("Action", "$"), Map.of(), Map.of(), Map.of(), List.of("reply"),
				Map.of(), "reply");
		var response = new Response(Endpoint.of("direct:managed-response-transform"),
				List.of(new Response.ResponseSource("reply", null, null)),
				List.of(new Sink("reply", "reply", Endpoint.of(replyEndpoint))));
		return new OperationRoute(routeId, new OperationRoute.Identity(path, "Action", null, null),
				Endpoint.of("direct:managed-request-transform"), List.of(), command, caller, response);
	}
}
