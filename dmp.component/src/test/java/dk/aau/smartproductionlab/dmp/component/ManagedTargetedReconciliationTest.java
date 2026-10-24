/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import static org.junit.jupiter.api.Assertions.*;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory.RuntimeSettings;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery;
import org.junit.jupiter.api.Test;

class ManagedTargetedReconciliationTest {
	@Test void aChangedAimcIsRefreshedAloneAndUnknownSubmodelsNeverTriggerAFullScan() throws Exception {
		String oldPort = System.getProperty("dmp.http.port");
		try (var fixture = new ManagedPackagedRuntimeIntegrationTest.Fixture()) {
			System.setProperty("dmp.http.port", Integer.toString(fixture.port));
			URI repositoryUri = fixture.repository.uri();
			var settings = ManagedRuntimeSettings.fromEnvironment(Map.of("DMP_MANAGED_AAS_REPOSITORY_URI", repositoryUri.toString(), "DMP_MANAGED_KAFKA_BROKERS", fixture.broker.bootstrap(),
					"DMP_MANAGED_CONTROL_TOPIC", fixture.topic, "DMP_MANAGED_CONTROL_GROUP_ID", fixture.group));
			var client = new AasRepositoryClient(repositoryUri, Duration.ofSeconds(2), Duration.ofSeconds(2), 100, 4, null);
			var runtimeSettings = new RuntimeSettings(repositoryUri);
			var discovery = new RepositoryDiscovery(client, runtimeSettings);
			@SuppressWarnings("resource") // this test owns the component lifecycle
			var component = new ManagedDmpRuntime(settings).component();
			component.startComponent();
			try (var service = new MappingReconciliationService(component, discovery, 16, 8, 3000)) {
				var reports = new AtomicInteger();
				service.setReportListener(report -> { if (report.complete() && !report.superseded()) reports.incrementAndGet(); });
				assertEquals(2, service.reconcileNow().installedOrUpdated());
				service.start();
				assertEquals(1, discovery.fullScanCount());
				long siblingReads = fixture.reads("contractBAS/submodels/AIMC");
				fixture.version("targeted");
				int before = reports.get();
				service.invalidate(ManagedPackagedRuntimeIntegrationTest.Fixture.AIMC);
				Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> reports.get() > before);
				assertEquals(1, discovery.targetedScanCount());
				assertEquals(1, discovery.fullScanCount());
				assertEquals(siblingReads, fixture.reads("contractBAS/submodels/AIMC"), "the sibling AIMC is not read again");
				assertEquals(2L, component.resourceMetrics().get("managedBundles"));
				assertEquals(4L, component.resourceMetrics().get("luaBindings"));
				int next = reports.get();
				service.invalidate("unknown-membership");
				Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> reports.get() > next);
				assertEquals(2, discovery.targetedScanCount());
				assertEquals(1, discovery.fullScanCount(), "an unknown submodel is read once, never with a full scan");
				Thread.sleep(1500);
				assertEquals(1, discovery.fullScanCount(), "there is no periodic repair");
				assertEquals(0, fixture.sends.get(), "refreshes are not ACTIONs");
			}
			finally { component.stopComponent(); }
		} finally {
			if (oldPort == null) System.clearProperty("dmp.http.port"); else System.setProperty("dmp.http.port", oldPort);
			ManagedRuntimeStatus.reset();
		}
	}
}
