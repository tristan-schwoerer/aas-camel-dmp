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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.List;
import java.util.Set;

import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.lua.configuration.LuaTransformerConfiguration;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

/** Exercises the first controlled install/remove API for ordinary bundles only. */
class ManagedOrdinaryBundleLifecycleTest {
	private static final class Runtime extends DmpComponent {
		Runtime() { this(java.time.Duration.ofSeconds(2)); }
		Runtime(java.time.Duration timeout) { super(timeout); }
		org.apache.camel.CamelContext context() { return camelContext; }
	}

	private static final class Source extends dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration {
		private final String uri;
		Source(String uri) { this.uri = uri; setSourceId("value"); }
		@Override public String getConnectionURI() { return uri; }
	}

	private static RoutesConfiguration paired(String routeId, String prefix) {
		RoutesConfiguration result = flowing(routeId, "direct:" + prefix + "-first");
		Source second = new Source("direct:" + prefix + "-second");
		second.setSourceId("extra");
		result.setDatasources(java.util.Map.of("in", result.getDatasources().get("in"), "second", second));
		RouteConfiguration route = new RouteConfiguration("execution-policy", List.of("in", "second"), List.of(), List.of("out"));
		route.setRouteId(routeId);
		result.setRoutes(List.of(route));
		return result;
	}

	@Test
	void successfulReplacementIsColdWhileSiblingReadinessAndCachedProducerRemainLive() throws Exception {
		Runtime runtime = new Runtime();
		var delivered = new java.util.concurrent.CopyOnWriteArrayList<Object>();
		try {
			runtime.startComponent();
			runtime.context().addRoutes(new org.apache.camel.builder.RouteBuilder() {
				@Override public void configure() {
					from("direct:lifecycle-output").process(exchange -> delivered.add(exchange.getMessage().getBody()));
				}
			});
			var first = install(runtime, "first", () -> paired("first", "first"));
			var sibling = install(runtime, "sibling", () -> paired("sibling", "sibling"));
			var siblingRoute = runtime.context().getRoute("sibling");
			var template = runtime.context().createProducerTemplate();
			try {
				template.sendBody("direct:first-first", 11);
				template.sendBody("direct:first-second", 12);
				template.sendBody("direct:sibling-first", 21);
				template.sendBody("direct:sibling-second", 22);
				assertEquals(List.of(11, 21), delivered);
				var candidate = runtime.stageBundle("first", runtime.nextBundleGeneration("first"), () -> paired("first", "first"));
				var replaced = runtime.replaceBundle(first, candidate);
				assertNotSame(first, replaced);
				assertSame(sibling, runtime.managedBundle("sibling"));
				assertSame(siblingRoute, runtime.context().getRoute("sibling"));
				template.sendBody("direct:first-first", 31);
				assertEquals(List.of(11, 21), delivered, "replacement must wait for a complete cold snapshot");
				template.sendBody("direct:sibling-first", 41);
				assertEquals(List.of(11, 21, 41), delivered, "sibling must retain readiness");
				template.sendBody("direct:first-second", 32);
				assertEquals(List.of(11, 21, 41, 31), delivered);
			} finally { template.stop(); }
		} finally { runtime.stopComponent(); }
	}

	private static final class Sink extends dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration {
		Sink() { super("out"); setSinkId("value"); }
		@Override public String getConnectionURI() { return "direct:lifecycle-output"; }
	}

	private static final class RetryCloseTransformer extends dk.aau.smartproductionlab.dmp.core.configuration.entity.DataTransformerConfiguration
			implements dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource {
		int closes;
		RetryCloseTransformer() { super("retry-close"); }
		@Override public String getConnectionURI() { return "direct:retry-close"; }
		@Override public void bind(org.apache.camel.CamelContext context) { }
		@Override public void close() {
			if (++closes == 1) throw new IllegalStateException("injected cleanup failure");
		}
	}

	@Test
	void failedCandidateCleanupRetainsInactiveOwnedHandleForExplicitRetry() {
		Runtime runtime = new Runtime();
		var resource = new RetryCloseTransformer();
		try {
			runtime.startComponent();
			int routes = runtime.context().getRoutesSize();
			int endpoints = runtime.context().getEndpoints().size();
			RoutesConfiguration candidate = flowing("broken", "missing-component:source");
			candidate.setTransformers(java.util.Map.of("retry", resource));
			assertThrows(IllegalStateException.class, () -> runtime.installBundle("owner", candidate));
			var cleanup = runtime.managedBundle("owner");
			assertFalse(cleanup.active());
			assertEquals(1, cleanup.resourceCount());
			assertTrue(cleanup.routeIds().isEmpty());
			assertThrows(IllegalStateException.class, () -> runtime.installBundle("owner", configuration("new")));
			runtime.removeBundle(cleanup);
			assertEquals(2, resource.closes);
			assertNull(runtime.managedBundle("owner"));
			assertEquals(routes, runtime.context().getRoutesSize());
			assertEquals(endpoints, runtime.context().getEndpoints().size());
		} finally { runtime.stopComponent(); }
	}

	private static final class StartupSource extends org.apache.camel.support.DefaultComponent {
		Runnable onStart = () -> { };
		boolean fail;
		@Override protected org.apache.camel.Endpoint createEndpoint(String uri, String remaining,
				java.util.Map<String, Object> parameters) {
			return new org.apache.camel.support.DefaultEndpoint(uri, this) {
				@Override public boolean isSingleton() { return true; }
				@Override public org.apache.camel.Producer createProducer() {
					return new org.apache.camel.support.DefaultProducer(this) {
						@Override public void process(org.apache.camel.Exchange exchange) { }
					};
				}
				@Override public org.apache.camel.Consumer createConsumer(org.apache.camel.Processor processor) {
					return new org.apache.camel.support.DefaultConsumer(this, processor) {
						@Override protected void doStart() throws Exception {
							super.doStart();
							onStart.run();
							if (fail) throw new IllegalStateException("consumer start failed");
							var retained = new org.apache.camel.support.DefaultExchange(getEndpoint().getCamelContext());
							retained.getMessage().setBody(42);
							getProcessor().process(retained);
						}
					};
				}
			};
		}
	}

	@Test
	void startupDeliveryIsFencedAndConsumerStartFailureCleansCandidateThenRestores() throws Exception {
		Runtime runtime = new Runtime();
		var outputCount = new java.util.concurrent.atomic.AtomicInteger();
		var source = new StartupSource();
		try {
			runtime.startComponent();
			runtime.context().addComponent("startup", source);
			runtime.context().addRoutes(new org.apache.camel.builder.RouteBuilder() {
				@Override public void configure() {
					from("direct:lifecycle-output").process(exchange -> outputCount.incrementAndGet());
				}
			});
			var old = install(runtime, "owner", () -> flowing("old", "startup:input"));
			assertEquals(0, outputCount.get(), "startup/retained delivery must not execute before activation");
			int baselineEndpoints = runtime.context().getEndpoints().size();
			int baselineRoutes = runtime.context().getRoutesSize();
			var candidate = runtime.stageBundle("owner", runtime.nextBundleGeneration("owner"), () -> flowing("new", "startup:input"));
			source.fail = true;
			source.onStart = () -> {
				// Only candidate fails; the previous definition's fresh restore succeeds.
				if (runtime.context().getRoute("old-source-in") != null) source.fail = false;
			};
			assertThrows(IllegalStateException.class, () -> runtime.replaceBundle(old, candidate));
			assertTrue(runtime.managedBundle("owner").active());
			assertEquals(Set.of("old", "old-source-in"), runtime.managedBundle("owner").routeIds());
			assertEquals(baselineEndpoints, runtime.context().getEndpoints().size());
			assertEquals(baselineRoutes, runtime.context().getRoutesSize());
			assertEquals(0, outputCount.get());
		} finally { runtime.stopComponent(); }
	}

	@Test
	void invalidationDuringConsumerStartNeverActivatesCandidate() throws Exception {
		Runtime runtime = new Runtime();
		var source = new StartupSource();
		try {
			runtime.startComponent();
			runtime.context().addComponent("startup", source);
			int endpoints = runtime.context().getEndpoints().size();
			var candidate = runtime.stageBundle("owner", runtime.nextBundleGeneration("owner"), () -> flowing("new", "startup:input"));
			source.onStart = () -> runtime.nextBundleGeneration("owner");
			assertThrows(IllegalStateException.class, () -> runtime.installBundle(candidate));
			assertNull(runtime.managedBundle("owner"));
			assertNull(runtime.context().getRoute("new"));
			assertEquals(endpoints, runtime.context().getEndpoints().size());
		} finally { runtime.stopComponent(); }
	}

	private static RoutesConfiguration flowing(String route, String input) {
		RouteConfiguration definition = new RouteConfiguration("execution-policy", List.of("in"), List.of(), List.of("out"));
		definition.setRouteId(route);
		return new RoutesConfiguration(java.util.Map.of("in", new Source(input)), java.util.Map.of(),
				java.util.Map.of("out", new Sink()), List.of(definition));
	}

	private static DmpComponent.ManagedBundle install(Runtime runtime, String owner,
			java.util.function.Supplier<RoutesConfiguration> factory) {
		return runtime.installBundle(runtime.stageBundle(owner, runtime.nextBundleGeneration(owner), factory));
	}

	@Test
	void stagedDefinitionsAreInactiveAndChurnReturnsExactEndpointAndRouteCounts() throws Exception {
		Runtime runtime = new Runtime();
		try {
			runtime.startComponent();
			int routes = runtime.context().getRoutesSize();
			int endpoints = runtime.context().getEndpoints().size();
			for (int i = 0; i < 8; i++) {
				String id = "churn-" + i;
				var stage = runtime.stageBundle(id, runtime.nextBundleGeneration(id), () -> configuration(id));
				assertNull(runtime.context().getRoute(id));
				assertEquals(routes, runtime.context().getRoutesSize());
				var installed = runtime.installBundle(stage);
				assertTrue(installed.active());
				assertEquals(stage.generation(), installed.generation());
				assertEquals(1, installed.resourceCount());
				runtime.removeBundle(installed);
				assertEquals(routes, runtime.context().getRoutesSize());
				assertEquals(endpoints, runtime.context().getEndpoints().size());
				assertEquals(0, runtime.generationTrackingCount(), "successful distinct-ID retirement must prune its fence");
			}
		} finally { runtime.stopComponent(); }
	}

	@Test
	void staleStagedWorkCannotUndoRemovalAndRecreation() {
		Runtime runtime = new Runtime();
		try {
			runtime.startComponent();
			var old = install(runtime, "owner", () -> configuration("old"));
			var obsolete = runtime.stageBundle("owner", runtime.nextBundleGeneration("owner"), () -> configuration("obsolete"));
			runtime.removeBundle(old);
			assertThrows(IllegalStateException.class, () -> runtime.installBundle(obsolete));
			assertNull(runtime.managedBundle("owner"));
			var recreated = install(runtime, "owner", () -> configuration("recreated"));
			assertTrue(recreated.generation() > obsolete.generation(), "recreated IDs must never reuse a pruned epoch");
			assertThrows(IllegalArgumentException.class, () -> runtime.installBundle(obsolete));
			assertSame(recreated, runtime.managedBundle("owner"));
		} finally { runtime.stopComponent(); }
	}

	@Test
	void staleDeletionEpochCannotQuiesceCurrentInstalledState() {
		Runtime runtime = new Runtime();
		try {
			runtime.startComponent();
			var installed = install(runtime, "owner", () -> configuration("installed"));
			long staleDelete = runtime.nextBundleGeneration("owner");
			long current = runtime.nextBundleGeneration("owner");
			assertThrows(IllegalStateException.class, () -> runtime.removeBundle(installed, staleDelete));
			assertSame(installed, runtime.managedBundle("owner"));
			assertTrue(installed.active());
			runtime.removeBundle(installed, current);
			assertNull(runtime.managedBundle("owner"));
			assertEquals(0, runtime.generationTrackingCount());
		} finally { runtime.stopComponent(); }
	}

	@Test
	void failedReplacementRestoresFreshPreviousDefinitionWithoutRestartingSibling() throws Exception {
		Runtime runtime = new Runtime();
		try {
			runtime.startComponent();
			var calls = new java.util.concurrent.atomic.AtomicInteger();
			var old = install(runtime, "owner", () -> { calls.incrementAndGet(); return configuration("old"); });
			var sibling = install(runtime, "sibling", () -> configuration("sibling"));
			var siblingRoute = runtime.context().getRoute("sibling");
			var stage = runtime.stageBundle("owner", runtime.nextBundleGeneration("owner"),
					() -> flowing("broken", "missing-component:source"));
			assertThrows(IllegalStateException.class, () -> runtime.replaceBundle(old, stage));
			var restored = runtime.managedBundle("owner");
			assertNotSame(old, restored);
			assertTrue(restored.active());
			assertEquals(Set.of("old"), restored.routeIds());
			assertEquals(2, calls.get());
			assertSame(sibling, runtime.managedBundle("sibling"));
			assertSame(siblingRoute, runtime.context().getRoute("sibling"));
			assertNull(runtime.context().getRoute("broken"));
		} finally { runtime.stopComponent(); }
	}

	@Test
	void failedRestoreLeavesNoInstalledStateAndExposesBothFailures() {
		Runtime runtime = new Runtime();
		try {
			runtime.startComponent();
			var calls = new java.util.concurrent.atomic.AtomicInteger();
			var old = install(runtime, "owner", () -> {
				if (calls.incrementAndGet() > 1) throw new IllegalStateException("restore failed");
				return configuration("old");
			});
			var stage = runtime.stageBundle("owner", runtime.nextBundleGeneration("owner"),
					() -> flowing("broken", "missing-component:source"));
			var failure = assertThrows(IllegalStateException.class, () -> runtime.replaceBundle(old, stage));
			assertTrue(failure.getCause().getSuppressed().length > 0);
			assertNull(runtime.managedBundle("owner"));
			assertNull(runtime.context().getRoute("old"));
		} finally { runtime.stopComponent(); }
	}

	@Test
	void failedForeignLuaBindDoesNotReleaseExistingOwners() throws Exception {
		Runtime first = new Runtime();
		Runtime second = new Runtime();
		LuaTransformerConfiguration lua = new LuaTransformerConfiguration("foreign-lua",
				LuaCompiledSource.fromBytes("function aimc_main(sources) return sources end".getBytes(StandardCharsets.UTF_8),
						"foreign", 256));
		try {
			first.context().start();
			second.context().start();
			var owner = first.installBundle("owner", configuration("foreign-owner", lua));
			var sibling = first.installBundle("sibling", configuration("foreign-sibling", lua));
			assertThrows(IllegalStateException.class,
					() -> second.installBundle("intruder", configuration("intruder", lua)));
			assertTrue(lua.isManagedSourceBound());
			assertTrue(owner.active());
			assertNull(second.managedBundle("intruder"));
			first.removeBundle(owner);
			assertTrue(lua.isManagedSourceBound(), "failed foreign binding must not decrement either legitimate owner");
			first.removeBundle(sibling);
			assertFalse(lua.isManagedSourceBound());
		} finally { first.stopComponent(); second.stopComponent(); }
	}

	@Test
	void replacementDrainsAcceptedWorkAndAFenceDuringTheDrainRestoresThePreviousBundle() throws Exception {
		Runtime runtime = new Runtime();
		var entered = new java.util.concurrent.CountDownLatch(1);
		var release = new java.util.concurrent.CountDownLatch(1);
		var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
		try {
			runtime.startComponent();
			runtime.context().addRoutes(new org.apache.camel.builder.RouteBuilder() {
				@Override public void configure() {
					from("direct:lifecycle-output").process(exchange -> {
						entered.countDown();
						if (!release.await(5, java.util.concurrent.TimeUnit.SECONDS)) throw new IllegalStateException("test deadline");
					});
				}
			});
			var old = install(runtime, "owner", () -> flowing("old", "direct:lifecycle-input"));
			var template = runtime.context().createProducerTemplate();
			try {
				var request = pool.submit(() -> template.sendBody("direct:lifecycle-input", 42));
				assertTrue(entered.await(2, java.util.concurrent.TimeUnit.SECONDS));
				var stage = runtime.stageBundle("owner", runtime.nextBundleGeneration("owner"), () -> configuration("new"));
				var replace = pool.submit(() -> runtime.replaceBundle(old, stage));
				awaitQuiescence(old);
				runtime.nextBundleGeneration("owner");
				assertFalse(replace.isDone());
				release.countDown();
				request.get(3, java.util.concurrent.TimeUnit.SECONDS);
				assertThrows(java.util.concurrent.ExecutionException.class,
						() -> replace.get(3, java.util.concurrent.TimeUnit.SECONDS), "the fenced replacement is not activated");
				assertNull(runtime.context().getRoute("new"));
				var restored = runtime.managedBundle("owner");
				assertNotNull(restored, "the mapping is not left without a bundle; the pass the fence queued replaces it");
				assertTrue(restored.active());
				assertNotNull(runtime.context().getRoute("old"));
			} finally { template.stop(); }
		} finally { release.countDown(); pool.shutdownNow(); runtime.stopComponent(); }
	}

	@Test
	void drainDeadlineRetainsQuiescedHandleAndBlocksReplacementUntilRetirementRetries() throws Exception {
		Runtime runtime = new Runtime(java.time.Duration.ofMillis(150));
		var entered = new java.util.concurrent.CountDownLatch(1);
		var release = new java.util.concurrent.CountDownLatch(1);
		var pool = java.util.concurrent.Executors.newSingleThreadExecutor();
		try {
			runtime.startComponent();
			runtime.context().addRoutes(new org.apache.camel.builder.RouteBuilder() {
				@Override public void configure() {
					from("direct:lifecycle-output").process(exchange -> { entered.countDown(); release.await(); });
				}
			});
			var old = install(runtime, "owner", () -> flowing("old", "direct:lifecycle-input"));
			var template = runtime.context().createProducerTemplate();
			try {
				var request = pool.submit(() -> template.sendBody("direct:lifecycle-input", 42));
				assertTrue(entered.await(2, java.util.concurrent.TimeUnit.SECONDS));
				long started = System.nanoTime();
				assertThrows(IllegalStateException.class, () -> runtime.removeBundle(old));
				assertTrue(java.time.Duration.ofNanos(System.nanoTime() - started).compareTo(java.time.Duration.ofSeconds(2)) < 0);
				assertSame(old, runtime.managedBundle("owner"));
				assertFalse(old.active());
				release.countDown();
				request.get(2, java.util.concurrent.TimeUnit.SECONDS);
				runtime.removeBundle(old);
				assertNull(runtime.managedBundle("owner"));
			} finally { template.stop(); }
		} finally { release.countDown(); pool.shutdownNow(); runtime.stopComponent(); }
	}

	private static void awaitQuiescence(DmpComponent.ManagedBundle bundle) throws Exception {
		long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
		while (bundle.active()) {
			if (System.nanoTime() >= deadline) throw new AssertionError("ingress did not quiesce");
			Thread.sleep(10);
		}
	}
	@Test
	void independentOrdinaryBundlesInstallRemoveAndReaddWithoutRestartingTheComponent() {
		DmpComponent dmp = new DmpComponent();
		try {
			dmp.startComponent();
			var first = dmp.installBundle("owner-A", configuration("mapping-A"));
			var second = dmp.installBundle("owner-B", configuration("mapping-B"));
			assertEquals(Set.of("mapping-A"), first.routeIds());
			assertEquals(Set.of("mapping-B"), second.routeIds());

			dmp.removeBundle(first);
			assertNull(dmp.managedBundle("owner-A"));
			org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
					() -> dmp.removeBundle(first));
			assertSame(second, dmp.managedBundle("owner-B"));

			var readded = dmp.installBundle("owner-A", configuration("mapping-A"));
			assertEquals(Set.of("mapping-A"), readded.routeIds());
			dmp.removeBundle(readded);
			assertSame(second, dmp.managedBundle("owner-B"));
			dmp.removeBundle(second);
			assertNull(dmp.managedBundle("owner-B"));
		} finally {
			dmp.stopComponent();
		}
	}

	@Test
	void managedLuaBindingLivesExactlyAsLongAsItsBundle() {
		DmpComponent dmp = new DmpComponent();
		LuaTransformerConfiguration lua = new LuaTransformerConfiguration("managed-lua",
				LuaCompiledSource.fromBytes("function aimc_main(sources) return sources end"
						.getBytes(StandardCharsets.UTF_8), "mapping:managed-lua", 256));
		try {
			dmp.startComponent();
			var bundle = dmp.installBundle("lua-owner", configuration("mapping-lua", lua));
			var sibling = dmp.installBundle("lua-owner-2", configuration("mapping-lua-2", lua));
			assertTrue(lua.isManagedSourceBound());
			dmp.removeBundle(bundle);
			assertTrue(lua.isManagedSourceBound());
			dmp.removeBundle(sibling);
			assertFalse(lua.isManagedSourceBound());
		} finally {
			dmp.stopComponent();
		}
	}

	@Test
	void componentStopRetiresManagedBundlesBeforeStoppingTheContext() {
		DmpComponent dmp = new DmpComponent();
		LuaTransformerConfiguration lua = new LuaTransformerConfiguration("managed-stop",
				LuaCompiledSource.fromBytes("function aimc_main(sources) return sources end"
						.getBytes(StandardCharsets.UTF_8), "mapping:managed-stop", 256));
		try {
			dmp.startComponent();
			dmp.installBundle("stop-owner", configuration("mapping-stop", lua));
			assertTrue(lua.isManagedSourceBound());
			dmp.stopComponent();
			assertFalse(lua.isManagedSourceBound());
			assertNull(dmp.managedBundle("stop-owner"));
		} finally {
			dmp.stopComponent();
		}
	}

	private static RoutesConfiguration configuration(String routeId) {
		return configuration(routeId, null);
	}

	private static RoutesConfiguration configuration(String routeId, LuaTransformerConfiguration lua) {
		RouteConfiguration route = new RouteConfiguration("execution-policy", List.of(), List.of(), List.of());
		if (lua != null) {
			route = new RouteConfiguration("execution-policy", List.of(), List.of("lua"), List.of());
		}
		route.setRouteId(routeId);
		return new RoutesConfiguration(java.util.Map.of(),
				lua == null ? java.util.Map.of() : java.util.Map.of("lua", lua),
				java.util.Map.of(), List.of(route));
	}
}
