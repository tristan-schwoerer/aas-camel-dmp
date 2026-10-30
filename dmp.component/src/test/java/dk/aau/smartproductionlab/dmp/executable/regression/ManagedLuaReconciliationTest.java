/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.lua.component.LuaEndpoint;
import dk.aau.smartproductionlab.dmp.lua.configuration.LuaScript;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;

/** Actual managed Lua, repository compilation and Operation transport during reconciliation. */
class ManagedLuaReconciliationTest {

	@Test
	void unchangedReconcilePreservesRouteBindingSourceAndCompiledPrototype() throws Exception {
		try (Fixture fixture = new Fixture()) {
			fixture.version("old");
			assertEquals(1, fixture.service.reconcileNow().installedOrUpdated());
			var installed = fixture.bundle();
			var routes = installed.routeIds().stream().map(fixture.runtime.context()::getRoute).toList();
			var bindings = fixture.bindings();
			assertEquals(2, bindings.size());
			var prototypes = bindings.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,
					entry -> prototype(entry.getValue())));
			var firstDiscovery = fixture.discovery.scan();
			var firstSources = sources(firstDiscovery);
			for (var source : firstSources) assertTrue(bindings.values().stream().anyMatch(installedSource -> installedSource == source),
					"discovery and installed definition must reuse the same immutable compiled resource");
			for (int i = 0; i < 3; i++) {
				fixture.service.invalidate(Fixture.AIMC);
				var result = fixture.service.reconcileNow();
				assertEquals(1, result.unchanged());
				assertEquals(0, result.installedOrUpdated());
				assertSame(installed, fixture.bundle());
				for (var route : routes) assertSame(route, fixture.runtime.context().getRoute(route.getId()));
				assertEquals(bindings.keySet(), fixture.bindings().keySet());
				for (var entry : bindings.entrySet()) {
					var current = fixture.bindings().get(entry.getKey());
					assertSame(entry.getValue(), current);
					assertSame(prototypes.get(entry.getKey()), prototype(current));
				}
				assertReply(fixture.call(), "old/old");
				var nextSources = sources(fixture.discovery.scan());
				for (var source : firstSources) {
					var sameContent = nextSources.stream().filter(next -> next.contentIdentity().equals(source.contentIdentity())).findFirst().orElseThrow();
					assertSame(source, sameContent, "unchanged discovery must not recompile managed script bytes");
					assertSame(prototype(source), prototype(sameContent));
				}
			}
			assertEquals(3, fixture.commands.size());
		}
	}

	@Test
	void oldInflightOperationKeepsBothScriptsWhileReplacementDrainsAndSiblingRemainsLive() throws Exception {
		try (Fixture fixture = new Fixture()) {
			fixture.version("old");
			fixture.sibling("sibling");
			assertEquals(2, fixture.service.reconcileNow().installedOrUpdated());
			var old = fixture.bundle();
			var oldBindings = fixture.bindings();
			fixture.blockDevice.set(true);
			var request = fixture.caller.sendAsync(fixture.request("contract"), HttpResponse.BodyHandlers.ofString());
			assertTrue(fixture.deviceEntered.await(3, TimeUnit.SECONDS));
			assertEquals("old", fixture.commands.get(0).get("generation").getAsString());
			fixture.version("new");
			fixture.service.invalidate(Fixture.AIMC);
			var replacement = fixture.workers.submit(fixture.service::reconcileNow);
			awaitQuiesced(old);
			assertFalse(replacement.isDone(), "replacement must await the admitted old Operation");
			var metrics = fixture.workers.submit(fixture.runtime::resourceMetrics).get(1, TimeUnit.SECONDS);
			assertEquals(2L, metrics.get("managedBundles"));
			assertEquals(1L, metrics.get("stagedBundles"));
			assertEquals(6L, metrics.get("luaBindings"), "old, staged and independent sibling bindings remain visible during drain");
			assertTrue(metrics.get("inflightIngressExchanges") >= 1);
			for (var entry : oldBindings.entrySet()) assertSame(entry.getValue(), fixture.runtime.context().getRegistry().lookupByName(entry.getKey()));
			var unavailable = fixture.call();
			assertEquals(503, unavailable.statusCode());
			assertEquals(1, fixture.commands.size(), "quiescence must not send a command");
			assertReply(fixture.call("contractB"), "sibling/sibling");
			fixture.deviceRelease.countDown();
			assertReply(request.get(5, TimeUnit.SECONDS), "old/old");
			assertEquals(1, replacement.get(5, TimeUnit.SECONDS).installedOrUpdated());
			assertNotSame(old, fixture.bundle());
			for (var entry : oldBindings.entrySet()) {
				if (new String(entry.getValue().bytes(), StandardCharsets.UTF_8).contains("old"))
					assertNull(fixture.runtime.context().getRegistry().lookupByName(entry.getKey()), "old script retires only after the request drains");
				else assertSame(entry.getValue(), fixture.bindings().get(entry.getKey()), "sibling script binding is untouched");
			}
			assertReply(fixture.call(), "new/new");
			assertEquals(List.of("old", "sibling", "new"), fixture.commands.stream()
					.map(command -> command.get("generation").getAsString()).toList(), "each caller sends exactly once, with its own request script");
			assertEquals(4, fixture.bindings().size());
		}
	}

	@Test
	void overlappingReconciliationRejectsStaleCompiledUpdateBeforeNewerScriptInstalls() throws Exception {
		staleCompile(false);
	}

	@Test
	void overlappingReconciliationCannotResurrectRemovedLuaMapping() throws Exception {
		staleCompile(true);
	}

	@Test
	void invalidationDuringOldRequestDrainAllowsNewestLuaToConvergeWithoutReplay() throws Exception {
		supersededDrain(false);
	}

	@Test
	void removalDuringOldRequestDrainDoesNotRestoreOrRetainRetiredLuaOwnership() throws Exception {
		supersededDrain(true);
	}

	private void supersededDrain(boolean remove) throws Exception {
		try (Fixture fixture = new Fixture()) {
			fixture.version("old");
			fixture.sibling("sibling");
			fixture.service.reconcileNow();
			var old = fixture.bundle();
			fixture.blockDevice.set(true);
			var request = fixture.caller.sendAsync(fixture.request("contract"), HttpResponse.BodyHandlers.ofString());
			assertTrue(fixture.deviceEntered.await(3, TimeUnit.SECONDS));
			fixture.version("superseded");
			fixture.service.invalidate(Fixture.AIMC);
			var replacing = fixture.workers.submit(fixture.service::reconcileNow);
			awaitQuiesced(old);
			assertFalse(replacing.isDone());
			if (remove) fixture.models.remove(Fixture.AIMC);
			else fixture.version("newest");
			fixture.service.invalidate(Fixture.AIMC);
			var latest = fixture.workers.submit(fixture.service::reconcileNow);
			fixture.deviceRelease.countDown();
			assertReply(request.get(5, TimeUnit.SECONDS), "old/old");
			assertTrue(replacing.get(5, TimeUnit.SECONDS).superseded());
			var report = latest.get(5, TimeUnit.SECONDS);
			assertFalse(report.superseded());
			assertTrue(report.categories().isEmpty(), report.toString());
			assertEquals(0, old.resourceCount());
			if (remove) {
				assertNull(fixture.bundle());
				assertEquals(2, fixture.bindings().size(), "only the sibling retains script bindings");
				assertEquals(1, report.mappings().size());
				fixture.version("recreated");
				assertEquals(1, fixture.service.reconcileNow().installedOrUpdated(), "retired Active must not prevent recreation");
				assertReply(fixture.call(), "recreated/recreated");
			} else {
				assertEquals(1, report.installedOrUpdated(), "newest desired state must recover after superseded drain removed the old bundle");
				assertReply(fixture.call(), "newest/newest");
			}
			assertEquals(2, fixture.commands.size(), "only the old and post-convergence callers invoke the device");
			assertEquals(4, fixture.bindings().size());
		}
	}

	private void staleCompile(boolean remove) throws Exception {
		try (Fixture fixture = new Fixture()) {
			fixture.version("old");
			var compiled = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			var armed = new AtomicBoolean(false);
			var staleScan = new AtomicReference<RepositoryDiscovery.Scan>();
			// A scan compiles each mapping once; the stale pass is held after compiling the stale scripts.
			fixture.replaceService(() -> {
				var scan = fixture.discovery.scan();
				if (armed.getAndSet(false)) {
					staleScan.set(scan);
					compiled.countDown();
					await(release);
				}
				return scan;
			});
			fixture.service.reconcileNow();
			var oldBindings = fixture.bindings();
			armed.set(true);
			fixture.version("stale");
			var stale = fixture.workers.submit(fixture.service::reconcileNow);
			assertTrue(compiled.await(3, TimeUnit.SECONDS));
			if (remove) fixture.models.remove(Fixture.AIMC);
			else fixture.version("newest");
			fixture.service.invalidate(Fixture.AIMC);
			var latest = fixture.workers.submit(fixture.service::reconcileNow);
			try {
				release.countDown();
				assertTrue(stale.get(5, TimeUnit.SECONDS).superseded());
				var result = latest.get(5, TimeUnit.SECONDS);
				assertFalse(result.superseded());
				var bound = fixture.bindings().values();
				assertTrue(sources(staleScan.get()).stream().noneMatch(script -> bound.stream().anyMatch(source -> source == script)),
						"the stale compiled scripts are never bound");
				for (var name : oldBindings.keySet()) assertNull(fixture.runtime.context().getRegistry().lookupByName(name));
				if (remove) {
					assertEquals(1, result.removed());
					assertNull(fixture.bundle());
					assertEquals(0, fixture.bindings().size());
					assertEquals(404, fixture.call().statusCode());
					assertEquals(0, fixture.commands.size());
				} else {
					assertEquals(1, result.installedOrUpdated());
					assertReply(fixture.call(), "newest/newest");
					assertEquals(1, fixture.commands.size());
					assertEquals("newest", fixture.commands.get(0).get("generation").getAsString());
					assertEquals(2, fixture.bindings().size());
				}
			} finally { release.countDown(); }
		}
	}

	@Test
	void invalidRequestAndResponseLuaRetainLastGoodWithoutActionReplay() throws Exception {
		try (Fixture fixture = new Fixture()) {
			fixture.version("good");
			fixture.service.reconcileNow();
			var good = fixture.bundle();
			var bindings = fixture.bindings();
			assertReply(fixture.call(), "good/good");
			for (String field : List.of("Transformation", "ResponseTransformation")) {
				fixture.version("bad");
				fixture.script(field, "function aimc_main(sources) this is invalid Lua end");
				fixture.service.invalidate(Fixture.AIMC);
				var result = fixture.service.reconcileNow();
				assertEquals(MappingReconciliationService.State.STALE, result.mappings().get(0).state());
				assertEquals(0, result.installedOrUpdated());
				assertSame(good, fixture.bundle());
				assertEquals(bindings, fixture.bindings());
				assertEquals(field.equals("Transformation") ? 1 : 2, fixture.commands.size(), "invalid preflight never invokes/replays a command");
				assertReply(fixture.call(), "good/good");
			}
			assertEquals(3, fixture.commands.size());
			fixture.version("fixed");
			assertEquals(1, fixture.service.reconcileNow().installedOrUpdated());
			assertReply(fixture.call(), "fixed/fixed");
			assertEquals(4, fixture.commands.size());
		}
	}

	@Test
	void repeatedScriptChangesAndRemovalReachSteadyRoutesEndpointsAndBindings() throws Exception {
		try (Fixture fixture = new Fixture()) {
			fixture.version("warm");
			fixture.service.reconcileNow();
			assertReply(fixture.call(), "warm/warm");
			fixture.models.remove(Fixture.AIMC);
			assertEquals(1, fixture.service.reconcileNow().removed());
			int baselineRoutes = fixture.runtime.context().getRoutesSize();
			int baselineEndpoints = fixture.runtime.context().getEndpoints().size();
			int installedRoutes = -1, installedEndpoints = -1;
			for (int i = 0; i < 8; i++) {
				String version = "generation" + i;
				fixture.version(version);
				assertEquals(1, fixture.service.reconcileNow().installedOrUpdated());
				assertReply(fixture.call(), version + "/" + version);
				assertEquals(2, fixture.bindings().size());
				assertEquals(2, fixture.runtime.context().getEndpoints().stream().filter(LuaEndpoint.class::isInstance).count());
				if (i == 0) {
					installedRoutes = fixture.runtime.context().getRoutesSize();
					installedEndpoints = fixture.runtime.context().getEndpoints().size();
				} else {
					assertEquals(installedRoutes, fixture.runtime.context().getRoutesSize());
					assertEquals(installedEndpoints, fixture.runtime.context().getEndpoints().size());
				}
				if (i % 2 == 1) {
					fixture.models.remove(Fixture.AIMC);
					assertEquals(1, fixture.service.reconcileNow().removed());
					assertEquals(0, fixture.bindings().size());
					assertEquals(baselineRoutes, fixture.runtime.context().getRoutesSize());
					assertEquals(baselineEndpoints, fixture.runtime.context().getEndpoints().size());
					assertEquals(0, fixture.runtime.generationTrackingCount());
				}
			}
			assertEquals(9, fixture.commands.size(), "churn never replays a device action");
		}
	}

	private static Object prototype(LuaCompiledSource source) {
		try {
			var field = LuaCompiledSource.class.getDeclaredField("compiledScript");
			field.setAccessible(true);
			return field.get(source);
		} catch (ReflectiveOperationException failure) { throw new AssertionError(failure); }
	}

	private static List<LuaCompiledSource> sources(RepositoryDiscovery.Scan scan) {
		assertTrue(scan.complete());
		assertTrue(scan.failures().isEmpty(), scan.failures().toString());
		return scan.mappings().values().stream().flatMap(candidate -> candidate.desired().bundle().route()
				.endpoints().stream()).filter(LuaScript.class::isInstance).map(script -> ((LuaScript) script).source())
				.toList();
	}

	private static void assertReply(HttpResponse<String> response, String expected) {
		assertEquals(200, response.statusCode(), response.body());
		assertEquals(expected, JsonParser.parseString(OperationCalls.outputs(response.body())).getAsJsonObject().get("deviceReply").getAsString());
	}

	private static void awaitQuiesced(DmpComponent.ManagedBundle bundle) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
		while (bundle.active() && System.nanoTime() < deadline) Thread.sleep(10);
		assertFalse(bundle.active(), "old ingress did not quiesce");
	}

	private static void await(CountDownLatch latch) {
		try { if (!latch.await(8, TimeUnit.SECONDS)) throw new AssertionError("test latch deadline"); }
		catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
	}

	private static final class Runtime extends DmpComponent {
		Runtime() { super(Duration.ofSeconds(7)); }
		CamelContext context() { return camelContext; }
	}

	private static final class Fixture implements AutoCloseable {
		static final String AIMC = "contractAAS/submodels/AIMC";
		final Runtime runtime = new Runtime();
		final Map<String, String> models;
		final List<JsonObject> commands = new CopyOnWriteArrayList<>();
		final CountDownLatch deviceEntered = new CountDownLatch(1), deviceRelease = new CountDownLatch(1);
		final AtomicBoolean blockDevice = new AtomicBoolean();
		final AtomicReference<Throwable> peerFailure = new AtomicReference<>();
		final ExecutorService workers = Executors.newFixedThreadPool(2), deviceWorkers = Executors.newFixedThreadPool(2);
		final HttpClient caller = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
		final HttpServer device;
		final FakeAasRepository repository = new FakeAasRepository();
		final RepositoryDiscovery discovery;
		final ConnectorConfigurationFactory.RuntimeSettings settings;
		final int port;
		String bundleId;
		MappingReconciliationService service;

		Fixture() throws Exception {
			models = repository.models();
			device = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
			device.setExecutor(deviceWorkers);
			device.createContext("/plc/api/actions/Restart", exchange -> {
				try {
					var command = JsonParser.parseString(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)).getAsJsonObject();
					assertEquals(7, command.get("x").getAsInt(), "request Lua preserves the decoded request-local value");
					commands.add(command);
					if (blockDevice.getAndSet(false)) { deviceEntered.countDown(); await(deviceRelease); }
					respond(exchange, 200, "{\"deviceReply\":\"" + command.get("generation").getAsString() + "\"}");
				} catch (Throwable failure) { peerFailure.set(failure); exchange.close(); }
			});
			device.start();
			put(resource("aid.json").replace("http://192.168.0.60:8080/plc/api", "http://127.0.0.1:" + device.getAddress().getPort() + "/plc/api"));
			put(PublishedModels.publish(cci(), "contractAAS", "contract"));
			URI base = repository.uri();
			settings = new ConnectorConfigurationFactory.RuntimeSettings(base);
			discovery = new RepositoryDiscovery(new AasRepositoryClient(base, Duration.ofSeconds(2), Duration.ofSeconds(2), 100, 4, null), settings);
			try (ServerSocket socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
			runtime.startComponent();
			runtime.addRuntimeRoutes(new RouteBuilder() {
				@Override public void configure() {
					restConfiguration().component("jetty").host("127.0.0.1").port(port).inlineRoutes(false);
					// Managed production keeps its health/control surface on the shared Operation listener.
					rest("/r3-fixture-health").get().to("direct:r3-fixture-health");
					from("direct:r3-fixture-health").routeId("r3-fixture-health").setBody(constant("ready"));
				}
			});
			service = new MappingReconciliationService(runtime, discovery, 32, 16, 60_000);
		}

		void replaceService(MappingReconciliationService.ScanProvider scans) {
			service.close();
			service = new MappingReconciliationService(runtime, scans, 32, 16, 60_000);
		}

		void version(String version) throws Exception {
			// The Operation answers deviceReply: the action's output is the ResponseSource Restart,
			// the Operation the ResponseSink Restart.
			JsonObject aimc = JsonParser.parseString(resource("aimc.json")).getAsJsonObject();
			ResponseMappings.respond(aimc.getAsJsonArray("submodelElements").get(0).getAsJsonObject()
					.getAsJsonArray("value").get(0).getAsJsonObject(), "Restart", "Restart", responseScript(version));
			put(aimc.toString());
			script("Transformation", requestScript(version));
			script("ResponseTransformation", responseScript(version));
		}

		/** A second asset with its own AIMC and CCI, whose Operation has a path of its own. */
		void sibling(String version) throws Exception {
			String cci = "a/submodels/ControlComponentInstance", siblingCci = "b/submodels/ControlComponentInstance";
			put(PublishedModels.publish(cci().replace(cci, siblingCci), "contractBAS", "contractB"));
			String body = models.get(AIMC).replace("contractAAS", "contractBAS").replace(cci, siblingCci);
			body = body.replace(Base64.getEncoder().encodeToString(requestScript("old").getBytes(StandardCharsets.UTF_8)),
					Base64.getEncoder().encodeToString(requestScript(version).getBytes(StandardCharsets.UTF_8)));
			body = body.replace(Base64.getEncoder().encodeToString(responseScript("old").getBytes(StandardCharsets.UTF_8)),
					Base64.getEncoder().encodeToString(responseScript(version).getBytes(StandardCharsets.UTF_8)));
			put(body);
		}

		void script(String field, String source) {
			JsonObject model = JsonParser.parseString(models.get(AIMC)).getAsJsonObject();
			JsonArray fields = model.getAsJsonArray("submodelElements").get(0).getAsJsonObject().getAsJsonArray("value")
					.get(0).getAsJsonObject().getAsJsonArray("value");
			JsonObject blob = null;
			for (var value : fields) if (field.equals(value.getAsJsonObject().get("idShort").getAsString())) blob = value.getAsJsonObject();
			if (blob == null) {
				blob = new JsonObject(); blob.addProperty("idShort", field); blob.addProperty("modelType", "Blob");
				blob.addProperty("contentType", "text/plain"); fields.add(blob);
			}
			blob.addProperty("value", Base64.getEncoder().encodeToString(source.getBytes(StandardCharsets.UTF_8)));
			models.put(AIMC, model.toString());
		}

		DmpComponent.ManagedBundle bundle() {
			if (bundleId == null && !service.status().isEmpty()) {
				var candidate = discovery.scan().mappings().values().stream()
						.filter(value -> value.key().aimcId().equals(AIMC)).findFirst().orElseThrow();
				bundleId = candidate.desired().bundle().bundleId();
			}
			return bundleId == null ? null : runtime.managedBundle(bundleId);
		}

		Map<String, LuaCompiledSource> bindings() {
			return Map.copyOf(runtime.context().getRegistry().findByTypeWithName(LuaCompiledSource.class));
		}

		HttpRequest request(String owner) {
			// Each asset's CCI submodel: contract's a/…, the sibling contractB's b/….
			String cci = ("contractB".equals(owner) ? "b" : "a") + "/submodels/ControlComponentInstance";
			return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + OperationCalls.path(cci, "Restart")))
					.timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
					.POST(HttpRequest.BodyPublishers.ofString(OperationCalls.of("{\"x\":7}"))).build();
		}
		HttpResponse<String> call() throws Exception { return call("contract"); }
		HttpResponse<String> call(String owner) throws Exception { return caller.send(request(owner), HttpResponse.BodyHandlers.ofString()); }

		void put(String body) { models.put(JsonParser.parseString(body).getAsJsonObject().get("id").getAsString(), body); }

		@Override public void close() throws Exception {
			deviceRelease.countDown();
			service.close();
			workers.shutdownNow();
			assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
			try { runtime.stopComponent(); }
			finally { repository.close(); device.stop(0); deviceWorkers.shutdownNow(); }
			assertTrue(deviceWorkers.awaitTermination(5, TimeUnit.SECONDS));
			assertNull(peerFailure.get(), "loopback peer failure");
			assertEquals(0, bindings().size(), "context shutdown releases all managed script bindings");
		}
	}

	private static String requestScript(String version) {
		return "function aimc_main(sources) return { Restart = { x = sources.Restart.x, generation = '" + version + "' } } end";
	}
	private static String responseScript(String version) {
		return "function aimc_main(sources) return { Restart = { deviceReply = sources.Restart.deviceReply .. '/" + version + "' } } end";
	}
	/** The fixture CCI whose Restart Operation declares the output deviceReply. */
	private static String cci() throws Exception {
		JsonObject cci = JsonParser.parseString(resource("dependencies/0.json")).getAsJsonObject();
		ResponseMappings.outputs(ResponseMappings.operation(cci, "Restart"), "deviceReply");
		return cci.toString();
	}
	private static String resource(String name) throws Exception {
		try (var input = ManagedLuaReconciliationTest.class.getResourceAsStream("/runtime-model-contracts/http-operation/" + name)) {
			assertNotNull(input, name);
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
	private static void respond(HttpExchange exchange, int status, String body) throws java.io.IOException {
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().set("Content-Type", "application/json");
		exchange.sendResponseHeaders(status, bytes.length);
		exchange.getResponseBody().write(bytes);
		exchange.close();
	}
}
