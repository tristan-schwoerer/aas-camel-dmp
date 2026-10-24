/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.impl.health.DefaultHealthCheckRegistry;
import org.apache.camel.impl.health.RoutesHealthCheckRepository;
import org.apache.camel.health.HealthCheckRegistry;
import dk.aau.smartproductionlab.dmp.component.health.HealthCheckProcessor;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ManagedRuntimeStatusTest {
	@AfterEach void reset() { ManagedRuntimeStatus.reset(); }

	@Test
	void healthIsNotReadyUntilTheInitialManagedScanCompletes() throws Exception {
		ManagedRuntimeStatus.beginManaged();
		var outcome = new java.util.concurrent.atomic.AtomicReference<>(ManagedControlReadiness.Outcome.ASSIGNED);
		var probes = new java.util.concurrent.atomic.AtomicInteger();
		var observer = new ManagedControlReadiness(new ManagedControlReadiness.Probe() {
			@Override public ManagedControlReadiness.Outcome check() { probes.incrementAndGet(); return outcome.get(); }
			@Override public void close() { }
		}, System::nanoTime, 5000);
		ManagedRuntimeStatus.trackControlReadiness(observer);
		var context = new DefaultCamelContext();
		DefaultHealthCheckRegistry registry = new DefaultHealthCheckRegistry();
		registry.register(new RoutesHealthCheckRepository());
		context.getCamelContextExtension().addContextPlugin(HealthCheckRegistry.class, registry);
		context.start();
		try {
			var processor = new HealthCheckProcessor();
			Exchange starting = new DefaultExchange(context);
			processor.processHealthCheck(starting);
			assertEquals(503, starting.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
			assertEquals("starting", ManagedRuntimeStatus.asMap().get("state"));

			ManagedRuntimeStatus.publish(new MappingReconciliationService.Report(true, false, 0, 0, 0, List.of(), List.of()));
			assertFalse(ManagedRuntimeStatus.snapshot().ready(), "scan alone cannot publish readiness before the control route starts");
			ManagedRuntimeStatus.controlRouteStarted();
			assertFalse(ManagedRuntimeStatus.snapshot().ready(), "route configured/start does not stand in for own assignment");
			observer.poll();
			Exchange ready = new DefaultExchange(context);
			processor.processHealthCheck(ready);
			assertEquals(200, ready.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
			assertEquals("ready", ManagedRuntimeStatus.asMap().get("state"));
			outcome.set(ManagedControlReadiness.Outcome.UNAVAILABLE);
			observer.poll();
			Exchange down = new DefaultExchange(context);
			processor.processHealthCheck(down);
			assertEquals(503, down.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
			assertEquals(2, probes.get(), "health processor must never run a probe");
		} finally { observer.close(); context.stop(); }
	}

	@Test void dependencyOutageDegradesWithoutTakingTheProcessOutOfServiceAndDetailsRemainCredentialFree() {
		ManagedRuntimeStatus.beginManaged();
		ManagedRuntimeStatus.controlRouteStarted();
		ManagedRuntimeStatus.publish(new MappingReconciliationService.Report(false, false, 0, 0, 0, List.of(),
				List.of("repository-transient_failure")));
		assertFalse(ManagedRuntimeStatus.snapshot().ready(), "not ready before the initial scan completed");
		var key = new dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery.MappingKey(
				"private-aimc", "private-mapping", 0);
		ManagedRuntimeStatus.publish(new MappingReconciliationService.Report(true, false, 0, 0, 0,
				List.of(new MappingReconciliationService.MappingStatus(key, MappingReconciliationService.State.STALE,
						"new-digest", "old-digest", 7, "mapping-resolution-transient_failure", 3)),
				List.of("mapping-resolution-transient_failure")));
		assertEquals(ManagedRuntimeStatus.State.DEGRADED, ManagedRuntimeStatus.snapshot().state());
		assertTrue(ManagedRuntimeStatus.snapshot().ready(), "one unreachable dependency does not make the DMP unhealthy (SPEC §4.4)");
		ManagedRuntimeStatus.publish(new MappingReconciliationService.Report(false, false, 0, 0, 0, List.of(),
				List.of("repository-transient_failure")));
		assertTrue(ManagedRuntimeStatus.snapshot().ready(), "nor does a later pass the repository did not answer");
		ManagedRuntimeStatus.publish(new MappingReconciliationService.Report(true, false, 0, 0, 0,
				List.of(new MappingReconciliationService.MappingStatus(key, MappingReconciliationService.State.STALE,
						"new-digest", "old-digest", 7, "mapping-resolution-transient_failure", 3)),
				List.of("mapping-resolution-transient_failure")));
		var status = ManagedRuntimeStatus.asMap();
		assertEquals(false, status.get("converged"));
		var details = (java.util.Map<?, ?>) ((List<?>) status.get("mappings")).get(0);
		assertEquals(7L, details.get("generation"));
		assertEquals(3L, details.get("revision"));
		assertEquals("old-digest", details.get("installedFingerprint"));
		assertFalse(status.toString().contains("private-"));
	}

	@Test void mappingDetailsOfOneAimcNameItsMappingsAndTheirReasons() {
		ManagedRuntimeStatus.beginManaged();
		var key = new dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery.MappingKey("urn:aimc:a", "History", 2);
		var other = new dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery.MappingKey("urn:aimc:b", "Other", 0);
		ManagedRuntimeStatus.publish(new MappingReconciliationService.Report(true, false, 0, 0, 0, List.of(
				new MappingReconciliationService.MappingStatus(key, MappingReconciliationService.State.BLOCKED, "d", null, 0,
						"unsupported-connector-capability", 0, "", List.of(new MappingReconciliationService.Diagnostic(
								"Sinks/value/0", "TimeSeries segments need a configured InfluxDB"))),
				new MappingReconciliationService.MappingStatus(other, MappingReconciliationService.State.INSTALLED, "d", "d", 1, "", 1)),
				List.of()));
		var details = ManagedRuntimeStatus.mappings("urn:aimc:a");
		var mappings = (List<?>) details.get("mappings");
		assertEquals(1, mappings.size());
		var mapping = (java.util.Map<?, ?>) mappings.get(0);
		assertEquals("History", mapping.get("identity"));
		assertEquals(2, mapping.get("listIndex"));
		assertEquals("blocked", mapping.get("state"));
		assertEquals(List.of(java.util.Map.of("path", "Sinks/value/0", "message", "TimeSeries segments need a configured InfluxDB")),
				mapping.get("diagnostics"));
		assertFalse(ManagedRuntimeStatus.asMap().toString().contains("InfluxDB"), "the status stays without messages");
	}
}
