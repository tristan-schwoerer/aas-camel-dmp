/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.MappingRoute;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource.AccountingEntry;
import dk.aau.smartproductionlab.dmp.lua.configuration.LuaScript;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource;
import org.junit.jupiter.api.Test;

class ManagedResourceAccountingTest {
	private static final byte[] SCRIPT = "function aimc_main(sources) return sources end".getBytes(StandardCharsets.UTF_8);
	private static final class Runtime extends DmpComponent implements AutoCloseable {
		Runtime() throws Exception { super(); camelContext.start(); }
		CamelContext context() { return camelContext; }
		@Override public void close() { stopComponent(); }
	}
	private static MappingRoute configuration(String id, Endpoint transformer) {
		return new MappingRoute(id, List.of(), transformer, List.of(), null);
	}
	/** Each build of the script binds its own {@code LuaBinding} of the (possibly shared) compiled source. */
	private static LuaScript lua(LuaCompiledSource source) {
		return new LuaScript(source);
	}
	private static LuaCompiledSource source() { return LuaCompiledSource.fromBytes(SCRIPT, "accounting", 256); }

	@Test
	void sharedCompiledSourceIsCountedOnceWithSharedThenExclusiveOwnerAttribution() throws Exception {
		try (var runtime = new Runtime()) {
			var source = source();
			var first = runtime.installBundle("first", configuration("first", lua(source)));
			var second = runtime.installBundle("second", configuration("second", lua(source)));
			assertEquals((long) SCRIPT.length, runtime.resourceMetrics().get("luaSourceRetainedBytes"));
			assertEquals(2L, runtime.resourceMetrics().get("luaBindings"));
			assertEquals((long) SCRIPT.length, runtime.ownerResourceMetrics(first).get("sharedLuaSourceRetainedBytes"));
			assertEquals(0L, runtime.ownerResourceMetrics(second).get("exclusiveLuaSourceRetainedBytes"));
			assertTrue(runtime.resourceMetrics().get("ownedEndpoints") > 0, "the ownership ledger remains separate");
			assertTrue(runtime.resourceMetrics().get("unaccountedEndpointLedgerEntries") > 0, "unsupported endpoints are not silently declared fully accounted");
			assertEquals(0L, runtime.resourceMetrics().get("mqttObservedOpenTcpSockets"));
			runtime.removeBundle(first);
			assertEquals(0L, runtime.ownerResourceMetrics(first).get("luaSourceRetainedBytes"));
			assertEquals(0L, runtime.ownerResourceMetrics(second).get("sharedLuaSourceRetainedBytes"));
			assertEquals((long) SCRIPT.length, runtime.ownerResourceMetrics(second).get("exclusiveLuaSourceRetainedBytes"));
			runtime.removeBundle(second);
			assertTrue(runtime.resourceMetrics().values().stream().allMatch(value -> value == 0));
		}
	}

	@Test
	void equalContentInDistinctAllocationsIsNotMistakenForSharedMemory() throws Exception {
		try (var runtime = new Runtime()) {
			var first = runtime.installBundle("first", configuration("first", lua(source())));
			var second = runtime.installBundle("second", configuration("second", lua(source())));
			assertEquals(2L * SCRIPT.length, runtime.resourceMetrics().get("luaSourceRetainedBytes"));
			assertEquals((long) SCRIPT.length, runtime.ownerResourceMetrics(first).get("exclusiveLuaSourceRetainedBytes"));
			assertEquals((long) SCRIPT.length, runtime.ownerResourceMetrics(second).get("exclusiveLuaSourceRetainedBytes"));
		}
	}

	@Test
	void stagedAbandonmentDropsOnlyItsAttributionAndDoesNotCreateSockets() throws Exception {
		try (var runtime = new Runtime()) {
			var source = source();
			var installed = runtime.installBundle("first", configuration("first", lua(source)));
			var staged = runtime.stageBundle("candidate", runtime.nextBundleGeneration("candidate"),
					configuration("candidate", lua(source)));
			assertEquals(1L, runtime.resourceMetrics().get("stagedBundles"));
			assertEquals((long) SCRIPT.length, runtime.ownerResourceMetrics(installed).get("sharedLuaSourceRetainedBytes"));
			staged.close();
			runtime.abandonUninstalledBundle("candidate");
			assertEquals((long) SCRIPT.length, runtime.ownerResourceMetrics(installed).get("exclusiveLuaSourceRetainedBytes"));
			assertEquals(1L, runtime.resourceMetrics().get("luaBindings"));
			assertEquals(0L, runtime.resourceMetrics().get("mqttClients"));
		}
	}

	private static final class AccountedEndpoint extends org.apache.camel.support.DefaultEndpoint
			implements ManagedRouteResource.AccountedEndpoint {
		volatile long clients;
		AccountedEndpoint(String uri, org.apache.camel.Component component) { super(uri, component); }
		@Override public boolean isSingleton() { return true; }
		@Override public org.apache.camel.Producer createProducer() {
			return new org.apache.camel.support.DefaultProducer(this) {
				@Override public void process(org.apache.camel.Exchange exchange) { }
			};
		}
		@Override public org.apache.camel.Consumer createConsumer(org.apache.camel.Processor processor) {
			throw new UnsupportedOperationException();
		}
		@Override public List<AccountingEntry> accounting() {
			return List.of(new AccountingEntry(this, Map.of("mqttClients", clients, "mqttObservedOpenTcpSockets", clients)));
		}
	}

	@Test
	void endpointSpiObservesOnlyExistingEndpointsAndDeduplicatesSharedTransportOwnership() throws Exception {
		try (var runtime = new Runtime()) {
			runtime.context().addComponent("accounted", new org.apache.camel.support.DefaultComponent() {
				@Override protected org.apache.camel.Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
					return new AccountedEndpoint(uri, this);
				}
			});
			var transform = Endpoint.of("accounted:shared");
			var staged = runtime.stageBundle("first", runtime.nextBundleGeneration("first"), configuration("first", transform));
			assertEquals(0L, runtime.resourceMetrics().get("mqttObservedOpenTcpSockets"));
			assertNull(runtime.context().hasEndpoint("accounted:shared"), "metrics must not create a staged endpoint");
			var first = runtime.installBundle(staged);
			var second = runtime.installBundle("second", configuration("second", transform));
			var endpoint = (AccountedEndpoint) runtime.context().hasEndpoint("accounted:shared");
			endpoint.clients = 1;
			assertEquals(1L, runtime.resourceMetrics().get("mqttClients"));
			assertEquals(1L, runtime.ownerResourceMetrics(first).get("mqttObservedOpenTcpSockets"));
			assertEquals(1L, runtime.ownerResourceMetrics(second).get("mqttObservedOpenTcpSockets"));
			runtime.removeBundle(first);
			assertSame(endpoint, runtime.context().hasEndpoint("accounted:shared"));
			assertEquals(1L, runtime.resourceMetrics().get("mqttObservedOpenTcpSockets"));
			runtime.removeBundle(second);
			assertNull(runtime.context().hasEndpoint("accounted:shared"));
			assertEquals(0L, runtime.resourceMetrics().get("mqttObservedOpenTcpSockets"));
		}
	}

	private static final class RetryingResource implements ManagedRouteResource {
		private int closes;
		private boolean unreadable;
		/** A transformation whose build owns this resource. */
		Endpoint endpoint() {
			return resources -> {
				resources.own(this);
				return "direct:accounting-retry";
			};
		}
		@Override public void bind(CamelContext context) { }
		@Override public Map<String, Long> metrics() {
			if (unreadable) throw new IllegalStateException("private-native-error");
			return Map.of("luaSourceRetainedBytes", 13L, "password-do-not-export", 99L);
		}
		@Override public void close() { if (++closes == 1) throw new IllegalStateException("retry cleanup"); }
	}

	@Test
	void unavailableAccountingIsFlaggedWithoutExposingNativeFailureDetails() throws Exception {
		try (var runtime = new Runtime()) {
			var resource = new RetryingResource();
			resource.unreadable = true;
			resource.closes = 1;
			var bundle = runtime.installBundle("unreadable", configuration("unreadable", resource.endpoint()));
			assertEquals(1L, runtime.resourceMetrics().get("accountingReadFailures"));
			assertEquals(1L, runtime.ownerResourceMetrics(bundle).get("accountingReadFailures"));
			assertFalse(runtime.resourceMetrics().toString().contains("private-native-error"));
		}
	}

	@Test
	void failedCleanupRetainsAttributionUntilSuccessfulRetryAndFiltersUnknownKeys() throws Exception {
		try (var runtime = new Runtime()) {
			var bundle = runtime.installBundle("retry", configuration("retry", new RetryingResource().endpoint()));
			assertThrows(IllegalStateException.class, () -> runtime.removeBundle(bundle));
			assertEquals(13L, runtime.ownerResourceMetrics(bundle).get("luaSourceRetainedBytes"));
			assertFalse(runtime.resourceMetrics().containsKey("password-do-not-export"));
			runtime.removeBundle(bundle);
			assertEquals(0L, runtime.ownerResourceMetrics(bundle).get("luaSourceRetainedBytes"));
			assertTrue(runtime.resourceMetrics().values().stream().allMatch(value -> value == 0));
		}
	}

	@Test
	void ownerAccountingRemainsReadableWhileTheLifecycleLockDrainsAnAcceptedRequest() throws Exception {
		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var workers = java.util.concurrent.Executors.newFixedThreadPool(3);
		try (var runtime = new Runtime()) {
			runtime.context().addRoutes(new org.apache.camel.builder.RouteBuilder() {
				@Override public void configure() {
					from("direct:accounting-blocker").process(exchange -> {
						entered.countDown();
						if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("test deadline");
					});
				}
			});
			var input = new Source("input", "value", Endpoint.of("direct:accounting-input"), Source.Delivery.OBSERVED, 0);
			var sink = new Sink("out", "value", Endpoint.of("direct:accounting-blocker"));
			var config = new MappingRoute("draining", List.of(input), lua(source()), List.of(sink), null);
			var bundle = runtime.installBundle("draining", config);
			try (var template = runtime.context().createProducerTemplate()) {
				try {
					var request = workers.submit(() -> template.sendBody("direct:accounting-input", 7));
					assertTrue(entered.await(2, TimeUnit.SECONDS));
					var removal = workers.submit(() -> runtime.removeBundle(bundle));
					org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(2)).until(() -> !bundle.active());
					var metrics = workers.submit(() -> runtime.ownerResourceMetrics(bundle)).get(500, TimeUnit.MILLISECONDS);
					assertEquals((long) SCRIPT.length, metrics.get("luaSourceRetainedBytes"));
					assertFalse(removal.isDone());
					release.countDown();
					request.get(3, TimeUnit.SECONDS);
					removal.get(3, TimeUnit.SECONDS);
				} finally { release.countDown(); }
			}
		} finally { release.countDown(); workers.shutdownNow(); assertTrue(workers.awaitTermination(3, TimeUnit.SECONDS)); }
	}
}
