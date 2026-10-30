/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.builder.RouteBuilder;

import dk.aau.smartproductionlab.dmp.aas.operation.OperationRoute;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.MappingRoute;
import dk.aau.smartproductionlab.dmp.core.plan.RoutePlan;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory.CompiledBundle;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory.DesiredBundle;
import dk.aau.smartproductionlab.dmp.management.compiler.EffectiveFingerprint;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.DesiredMapping;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.MappingPlan;
import dk.aau.smartproductionlab.dmp.management.model.WireModel;
import org.junit.jupiter.api.Test;

class MappingReconciliationServiceTest {
	private static final String BUNDLE = "managed-map-a";
	private static final RepositoryDiscovery.MappingKey KEY =
			new RepositoryDiscovery.MappingKey("urn:aimc", "mapping-a", 0);

	@Test
	void noOpStalePreservationConfirmedMissingStopAndMappingRemovalConvergeIndependently() {
		Runtime runtime = new Runtime();
		AtomicReference<RepositoryDiscovery.Scan> desired = new AtomicReference<>(scan(valid("first"), true));
		var service = service(runtime, desired::get);
		runtime.startComponent();
		try {
			var first = service.reconcileNow();
			assertEquals(1, first.installedOrUpdated());
			assertEquals(1, first.mappings().get(0).revision(), "the first installation is revision 1");
			var original = runtime.managedBundle(BUNDLE);
			assertNotNull(original);
			assertTrue(original.active());

			var unchanged = service.reconcileNow();
			assertEquals(1, unchanged.unchanged());
			assertSame(original, runtime.managedBundle(BUNDLE), "unchanged effective state does not restart routes");
			assertEquals(1, unchanged.mappings().get(0).revision(), "an unchanged mapping keeps its revision");

			service.invalidate("urn:dependency");
			var valueOnly = service.reconcileNow();
			assertEquals(1, valueOnly.unchanged(), "telemetry invalidation with the same effective fingerprint is a no-op");
			assertSame(original, runtime.managedBundle(BUNDLE));
			assertEquals(1, valueOnly.mappings().get(0).revision(), "a value-only invalidation does not count as a revision");

			desired.set(scan(invalid(AasRepositoryClient.Status.TRANSIENT_FAILURE), true));
			var stale = service.reconcileNow();
			assertEquals(MappingReconciliationService.State.STALE, stale.mappings().get(0).state());
			assertSame(original, runtime.managedBundle(BUNDLE), "transient dependency failure retains last-valid routes");
			assertEquals(1, stale.mappings().get(0).revision(), "a stale mapping still reports the running revision");

			desired.set(scan(valid("changed"), true));
			var changed = service.reconcileNow();
			assertEquals(1, changed.installedOrUpdated());
			var replacement = runtime.managedBundle(BUNDLE);
			assertNotSame(original, replacement);
			assertTrue(replacement.generation() > original.generation());
			assertEquals(2, changed.mappings().get(0).revision(), "a replacement is the next revision of the mapping");

			desired.set(scan(invalid(AasRepositoryClient.Status.NOT_FOUND), true));
			var missing = service.reconcileNow();
			assertEquals(1, missing.removed(), "confirmed required dependency deletion stops the stale mapping");
			assertNull(runtime.managedBundle(BUNDLE));
			assertEquals(MappingReconciliationService.State.BLOCKED, missing.mappings().get(0).state());

			desired.set(scan(null, true));
			var removed = service.reconcileNow();
			assertEquals(0, removed.mappings().size());
			assertNull(runtime.managedBundle(BUNDLE));

			desired.set(scan(valid("recreate"), true));
			var recreatedReport = service.reconcileNow();
			assertEquals(1, recreatedReport.mappings().get(0).revision(), "a removed and recreated mapping starts over");
			var recreated = runtime.managedBundle(BUNDLE);
			desired.set(scan(null, false));
			var incomplete = service.reconcileNow();
			assertFalse(incomplete.complete());
			assertSame(recreated, runtime.managedBundle(BUNDLE), "failed full scan never implies deletion");
		} finally {
			service.close();
			runtime.stopComponent();
		}
	}

	@Test
	void dependencyInvalidationFencesCandidateStagedFromAnOlderScan() {
		Runtime runtime = new Runtime();
		AtomicReference<MappingReconciliationService> serviceRef = new AtomicReference<>();
		AtomicBoolean invalidateDuringStage = new AtomicBoolean(true);
		var service = service(runtime, () -> scan(valid("first", () -> {
			if (invalidateDuringStage.getAndSet(false)) serviceRef.get().invalidate("urn:dependency");
		}), true));
		serviceRef.set(service);
		runtime.startComponent();
		try {
			var result = service.reconcileNow();
			assertTrue(result.superseded());
			assertNull(runtime.managedBundle(BUNDLE), "stale staged ingress never activates");
			assertEquals(0, runtime.generationTrackingCount(), "abandoned staged generations are pruned safely");
			assertEquals(0, runtime.routeCount());
		} finally {
			service.close();
			runtime.stopComponent();
		}
	}

	@Test
	void retryRecoversAfterInitialRepositoryFailure() throws Exception {
		Runtime runtime = new Runtime();
		java.util.concurrent.atomic.AtomicInteger scans = new java.util.concurrent.atomic.AtomicInteger();
		var service = service(runtime, () -> scans.getAndIncrement() == 0
				? new RepositoryDiscovery.Scan(false, "repository-transient_failure", Set.of(), Set.of(), Map.of(), Map.of(), List.of())
				: scan(valid("first"), true), 100);
		runtime.startComponent();
		try {
			service.start();
			long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
			while (runtime.managedBundle(BUNDLE) == null && System.nanoTime() < deadline) Thread.sleep(20);
			assertNotNull(runtime.managedBundle(BUNDLE), "the retried full scan discovers state after an initial outage");
			assertTrue(scans.get() >= 2);
		} finally {
			service.close();
			runtime.stopComponent();
		}
	}

	@Test
	void aTimeSeriesFieldWithConflictingTypesHoldsBackEveryMappingInvolved() {
		Runtime runtime = new Runtime();
		var longs = seriesCandidate("urn:aimc:a", "bundle-a", "station_state", "xs:long");
		var doubles = seriesCandidate("urn:aimc:b", "bundle-b", "station_state", "xs:double");
		var service = service(runtime, () -> scan(longs, doubles));
		runtime.startComponent();
		try {
			var report = service.reconcileNow();
			assertTrue(report.mappings().stream().allMatch(status -> status.state() == MappingReconciliationService.State.BLOCKED),
					String.valueOf(report.mappings()));
			assertTrue(report.categories().contains("timeseries-type-conflict"));
			assertNull(runtime.managedBundle("bundle-a"));
			var diagnostic = report.mappings().stream().filter(status -> status.key().aimcId().equals("urn:aimc:a"))
					.findFirst().orElseThrow().diagnostics().get(0);
			assertTrue(diagnostic.message().contains("station_state/Count is INTEGER (xs:long) here, but FLOAT (xs:double) in mapping"),
					diagnostic.message());
			assertTrue(diagnostic.message().contains("of AIMC urn:aimc:b"), diagnostic.message());
		} finally {
			service.close();
			runtime.stopComponent();
		}
	}

	@Test
	void aBlockedMappingExplainsEachFailureAtItsPath() {
		var failure = new AasRepositoryClient.Result<>(AasRepositoryClient.Status.NOT_FOUND, null, 404, "repository resource not found");
		var desired = new DesiredMapping("mapping-a", 0, null, List.of(failure), Set.of(), null, null, List.of("Sinks/value/1"));
		var issue = new ConnectorConfigurationFactory.Issue("MappingConfigurations/value/0/Sources/value/0/forms/mqv_qos",
				"mapping-a", "MQTT QoS must be between 0 and 2");
		var bundle = new DesiredBundle(desired, null, List.of(issue), fingerprint(plan(BUNDLE)));
		assertEquals(List.of(new MappingReconciliationService.Diagnostic("Sinks/value/1", "not found: repository resource not found"),
				new MappingReconciliationService.Diagnostic("Sources/value/0/forms/mqv_qos", "MQTT QoS must be between 0 and 2")),
				MappingReconciliationService.diagnostics(bundle));
		assertEquals("Sinks/value/1/forms", MappingReconciliationService.relativePath("/submodelElements/0/value/3/Sinks/value/1/forms"));
	}

	@Test
	void installationFailuresHaveAFixedCause() {
		assertEquals(InstallationCause.MULTIPLE_CONSUMERS_UNSUPPORTED, InstallationCause.of(new IllegalStateException("unable to install",
				new IllegalStateException("Multiple consumers for the same endpoint is not allowed: dmp-milo-client://user:secret@host"))));
		assertEquals(InstallationCause.CONNECTION_REFUSED, InstallationCause.of(new RuntimeException(new java.net.ConnectException("refused"))));
		assertEquals(InstallationCause.UNKNOWN_HOST, InstallationCause.of(new java.net.UnknownHostException("plc")));
		assertEquals(InstallationCause.UNKNOWN, InstallationCause.of(new IllegalStateException("something else")));
	}

	@Test
	void sameInfluxTypesOrOtherMeasurementsDoNotConflict() {
		Runtime runtime = new Runtime();
		var doubles = seriesCandidate("urn:aimc:a", "bundle-a", "station_state", "xs:double");
		var floats = seriesCandidate("urn:aimc:b", "bundle-b", "station_state", "xs:float");
		var elsewhere = seriesCandidate("urn:aimc:c", "bundle-c", "dosing", "xs:long");
		var service = service(runtime, () -> scan(doubles, floats, elsewhere));
		runtime.startComponent();
		try {
			var report = service.reconcileNow();
			assertFalse(report.categories().contains("timeseries-type-conflict"), String.valueOf(report.categories()));
			assertTrue(report.mappings().stream().allMatch(status -> status.state() == MappingReconciliationService.State.INSTALLED),
					String.valueOf(report.mappings()));
		} finally {
			service.close();
			runtime.stopComponent();
		}
	}

	@Test
	void duplicateOperationBindingsAreRejectedBeforeEitherBundleStarts() {
		Runtime runtime = new Runtime();
		var first = restCandidate("urn:aimc:a", "map-a", "bundle-a", "/operations/shared");
		var second = restCandidate("urn:aimc:b", "map-b", "bundle-b", "/operations/shared");
		Map<RepositoryDiscovery.MappingKey, RepositoryDiscovery.MappingCandidate> mappings = Map.of(
				first.key(), first, second.key(), second);
		var scan = new RepositoryDiscovery.Scan(true, "", Set.of("urn:aimc:a", "urn:aimc:b"), Set.of(),
				mappings, Map.of(), List.of());
		var service = service(runtime, () -> scan);
		runtime.startComponent();
		try {
			var report = service.reconcileNow();
			assertEquals(2, report.mappings().size());
			assertTrue(report.mappings().stream().allMatch(status -> status.state() == MappingReconciliationService.State.BLOCKED));
			assertTrue(report.categories().contains("rest-path-conflict"));
			var diagnostics = report.mappings().stream().filter(status -> status.key().mappingIdentity().equals("map-a"))
					.findFirst().orElseThrow().diagnostics();
			assertEquals(List.of(new MappingReconciliationService.Diagnostic("",
					"POST /operations/shared is also served by mapping map-b of AIMC urn:aimc:b")), diagnostics);
			assertNull(runtime.managedBundle("bundle-a"));
			assertNull(runtime.managedBundle("bundle-b"));
			assertEquals(0, runtime.routeCount());
		} finally {
			service.close();
			runtime.stopComponent();
		}
	}

	@Test
	void overflowRemainsBoundedAcrossRepeatedScansAndFreesCapacityAfterRemoval() {
		Runtime runtime = new Runtime();
		var first = candidate("urn:aimc:a", "a", 0, "bundle-a", "a", "dependency-a");
		var second = candidate("urn:aimc:b", "b", 0, "bundle-b", "b", "dependency-b");
		AtomicReference<RepositoryDiscovery.Scan> desired = new AtomicReference<>(scan(first, second));
		var service = new MappingReconciliationService(runtime, desired::get, 1, 8, 60_000);
		runtime.startComponent();
		try {
			for (int i = 0; i < 4; i++) {
				var report = service.reconcileNow();
				assertEquals(1, report.mappings().size(), "overflow does not grow tracking or become admitted on a later scan");
				assertEquals(1, service.overflowMappingCount());
				assertTrue(report.categories().contains("mapping-capacity-exceeded"));
				assertNotNull(runtime.managedBundle("bundle-a"));
				assertNull(runtime.managedBundle("bundle-b"));
			}
			desired.set(scan(second));
			var repaired = service.reconcileNow();
			assertEquals(1, repaired.removed());
			assertEquals(1, repaired.installedOrUpdated());
			assertEquals(0, service.overflowMappingCount());
			assertNotNull(runtime.managedBundle("bundle-b"));
		} finally { service.close(); runtime.stopComponent(); }
	}

	@Test
	void noSurvivingBundleAfterFailedReplacementCanInstallAgain() {
		Runtime runtime = new Runtime();
		AtomicReference<RepositoryDiscovery.Scan> desired = new AtomicReference<>(scan(valid("first"), true));
		var service = service(runtime, desired::get);
		runtime.startComponent();
		try {
			service.reconcileNow();
			runtime.failReplacementWithoutSurvivor = true;
			desired.set(scan(valid("changed"), true));
			var failed = service.reconcileNow();
			assertEquals(MappingReconciliationService.State.FAILED, failed.mappings().get(0).state());
			assertEquals(InstallationCause.UNKNOWN, failed.mappings().get(0).cause());
			assertNull(failed.mappings().get(0).installedFingerprint());
			assertNull(runtime.managedBundle(BUNDLE));
			assertEquals(1, service.reconcileNow().installedOrUpdated(), "retired handle must not poison a later valid installation");
			assertTrue(runtime.managedBundle(BUNDLE).active());
		} finally { service.close(); runtime.stopComponent(); }
	}

	@Test
	void failedRemovalRemainsVisibleUntilOwnedBundleIsRetired() {
		Runtime runtime = new Runtime();
		AtomicReference<RepositoryDiscovery.Scan> desired = new AtomicReference<>(scan(valid("first"), true));
		var service = service(runtime, desired::get);
		runtime.startComponent();
		try {
			service.reconcileNow();
			runtime.failRemoval = true;
			desired.set(scan(null, true));
			for (int i = 0; i < 2; i++) {
				var failed = service.reconcileNow();
				assertEquals(1, failed.mappings().size());
				assertEquals(MappingReconciliationService.State.STALE, failed.mappings().get(0).state());
				assertTrue(failed.categories().contains("bundle-removal-failed"));
				assertNotNull(runtime.managedBundle(BUNDLE));
			}
			runtime.failRemoval = false;
			assertEquals(1, service.reconcileNow().removed());
			assertTrue(service.status().isEmpty());
		} finally { runtime.failRemoval = false; service.close(); runtime.stopComponent(); }
	}

	@Test
	void shiftingNamedMappingListIndexPreservesItsBundleAndGeneration() {
		Runtime runtime = new Runtime();
		var before = candidate("urn:aimc", "named", 1, "named-bundle", "same", "dependency");
		var after = candidate("urn:aimc", "named", 0, "named-bundle", "same", "dependency");
		assertEquals(before.key(), after.key());
		AtomicReference<RepositoryDiscovery.Scan> desired = new AtomicReference<>(scan(before));
		var service = service(runtime, desired::get);
		runtime.startComponent();
		try {
			service.reconcileNow();
			var original = runtime.managedBundle("named-bundle");
			desired.set(scan(after));
			var report = service.reconcileNow();
			assertEquals(1, report.unchanged());
			assertEquals(0, report.removed());
			assertEquals(0, report.mappings().get(0).key().listIndex(), "list position stays current as diagnostic metadata");
			assertSame(original, runtime.managedBundle("named-bundle"));
		} finally { service.close(); runtime.stopComponent(); }
	}

	@Test
	void invalidationDuringFailedScanSchedulesBackoffInsteadOfImmediateRetry() {
		Runtime runtime = new Runtime();
		java.util.concurrent.atomic.AtomicInteger scans = new java.util.concurrent.atomic.AtomicInteger();
		var service = service(runtime, () -> {
			scans.incrementAndGet();
			return scan(null, false);
		});
		var worker = new ManualScheduler();
		try {
			service.invalidate("urn:dependency");
			service.start(worker);
			assertNull(worker.timer, "starting runs the pending scan; there is no periodic repair timer");
			worker.runWork();
			assertEquals(1, scans.get());
			assertTrue(worker.work.isEmpty(), "accepted-but-unapplied event must not bypass failure delay");
			assertEquals(60_000, worker.timer.delayMillis);
			assertEquals(1, service.consecutiveScanFailures());
			service.invalidate("urn:dependency");
			assertTrue(worker.work.isEmpty(), "more events during backoff preserve the repair obligation without retry storms");
			assertTrue(worker.timer.delayMillis > 50_000);
			worker.runTimer();
			worker.runWork();
			assertEquals(2, scans.get());
			assertEquals(120_000, worker.timer.delayMillis, "consecutive failures back off");
		} finally { service.close(); }
	}

	@Test
	void noisyAimcDoesNotDiscardIndependentCandidateFromSameScan() {
		Runtime runtime = new Runtime();
		var noisy = candidate("urn:noisy", "noisy", 0, "noisy-bundle", "old", "noisy-dependency");
		var healthy = candidate("urn:healthy", "healthy", 0, "healthy-bundle", "current", "healthy-dependency");
		AtomicReference<MappingReconciliationService> reference = new AtomicReference<>();
		var service = service(runtime, () -> {
			reference.get().invalidate("noisy-dependency");
			return scan(noisy, healthy);
		});
		reference.set(service);
		runtime.startComponent();
		try {
			var first = service.reconcileNow();
			assertTrue(first.superseded());
			assertEquals(1, first.installedOrUpdated());
			assertNull(runtime.managedBundle("noisy-bundle"));
			var installed = runtime.managedBundle("healthy-bundle");
			assertNotNull(installed);
			assertEquals(1, service.reconcileNow().unchanged());
			assertSame(installed, runtime.managedBundle("healthy-bundle"));
		} finally { service.close(); runtime.stopComponent(); }
	}

	@Test
	void dependencyInvalidationDuringStageDiscardsOnlyItsAimcAndReleasesAbandonedGeneration() {
		Runtime runtime = new Runtime();
		AtomicBoolean invalidateDuringStage = new AtomicBoolean(true);
		AtomicReference<MappingReconciliationService> reference = new AtomicReference<>();
		var noisy = candidate("urn:noisy", "noisy", 0, "noisy-bundle", "old", "noisy-dependency", () -> {
			if (invalidateDuringStage.getAndSet(false)) reference.get().invalidate("noisy-dependency");
		});
		var healthy = candidate("urn:healthy", "healthy", 0, "healthy-bundle", "current", "healthy-dependency");
		var service = service(runtime, () -> scan(noisy, healthy));
		reference.set(service);
		runtime.startComponent();
		try {
			var first = service.reconcileNow();
			assertTrue(first.superseded());
			assertEquals(1, first.installedOrUpdated());
			assertNull(runtime.managedBundle("noisy-bundle"));
			assertNotNull(runtime.managedBundle("healthy-bundle"));
			assertEquals(1, runtime.generationTrackingCount(), "discarded candidate generation is not retained");
			var next = service.reconcileNow();
			assertEquals(1, next.installedOrUpdated());
			assertEquals(1, next.unchanged());
		} finally { service.close(); runtime.stopComponent(); }
	}

	@Test
	void trackingOverflowFencesAllStagedWorkAndRetainsFullRepairObligation() {
		Runtime runtime = new Runtime();
		AtomicBoolean overflowDuringStage = new AtomicBoolean(true);
		AtomicReference<MappingReconciliationService> reference = new AtomicReference<>();
		Runnable overflow = () -> {
			if (overflowDuringStage.getAndSet(false)) {
				reference.get().invalidate("first-dependency");
				reference.get().invalidate("second-dependency");
			}
		};
		var first = candidate("urn:first", "first", 0, "first-bundle", "old", "first-dependency", overflow);
		var second = candidate("urn:second", "second", 0, "second-bundle", "current", "second-dependency", overflow);
		var service = new MappingReconciliationService(runtime, () -> scan(first, second), 8, 1, 60_000);
		reference.set(service);
		runtime.startComponent();
		try {
			var superseded = service.reconcileNow();
			assertTrue(superseded.superseded());
			assertEquals(0, superseded.installedOrUpdated());
			assertEquals(1, service.invalidationOverflowCount());
			assertEquals(0, runtime.generationTrackingCount());
			assertEquals(2, service.reconcileNow().installedOrUpdated(), "bounded tracking overflow cannot lose the repair");
		} finally { service.close(); runtime.stopComponent(); }
	}

	@Test
	void aRescanTickDuringAPendingPassLeavesThatPassToInstall() {
		Runtime runtime = new Runtime();
		AtomicReference<PollingControlRouteBuilder> polling = new AtomicReference<>();
		var service = service(runtime, () -> {
			polling.get().rescan();
			return scan(valid("first"), true);
		});
		polling.set(new PollingControlRouteBuilder(new PollingControlRouteBuilder.Settings(java.time.Duration.ofSeconds(1)), service));
		runtime.startComponent();
		try {
			service.invalidate(null);
			var pass = service.reconcileNow();
			assertFalse(pass.superseded(), "a tick while the pass runs does not supersede it");
			assertEquals(1, pass.installedOrUpdated());
			assertFalse(service.repairPending());
			polling.get().rescan();
			assertTrue(service.repairPending(), "the first tick after the pass rescans");
		} finally { service.close(); runtime.stopComponent(); }
	}

	@Test
	void newMembershipEventDuringEmptyScanRequiresRepairWithoutDiscardingUnrelatedWork() {
		Runtime runtime = new Runtime();
		AtomicReference<MappingReconciliationService> reference = new AtomicReference<>();
		AtomicBoolean eventDuringScan = new AtomicBoolean(true);
		var service = service(runtime, () -> {
			if (eventDuringScan.getAndSet(false)) reference.get().invalidate("urn:new-aimc");
			return scan();
		});
		reference.set(service);
		try {
			assertTrue(service.reconcileNow().superseded(), "unknown newly created membership still obliges a follow-up scan");
			assertFalse(service.reconcileNow().superseded());
		} finally { service.close(); }
	}

	@Test
	void successfulPassesScheduleNothingAndEventsRunTargetedPasses() {
		Runtime runtime = new Runtime();
		java.util.concurrent.atomic.AtomicInteger scans = new java.util.concurrent.atomic.AtomicInteger();
		var service = service(runtime, () -> { scans.incrementAndGet(); return scan(valid("first"), true); });
		var worker = new ManualScheduler();
		runtime.startComponent();
		try {
			service.reconcileNow();
			service.start(worker);
			assertTrue(worker.work.isEmpty(), "a completed startup scan is not repeated");
			assertNull(worker.timer, "no periodic repair");
			service.acceptEvent(AasControlEvent.parse("{\"type\":\"SM_UPDATED\",\"id\":\"urn:dependency\"}"));
			worker.runWork();
			assertEquals(2, scans.get());
			assertTrue(worker.work.isEmpty());
			assertNull(worker.timer);
		} finally { service.close(); runtime.stopComponent(); }
	}

	/** Distinct per route identity and Operation identity, like the compiled plans these fixtures stand in for. */
	private static EffectiveFingerprint fingerprint(RoutePlan plan) {
		return EffectiveFingerprint.ofValue(List.of(plan.routeId(),
				plan instanceof OperationRoute operation ? String.valueOf(operation.identity()) : ""));
	}

	private static RepositoryDiscovery.Scan scan(RepositoryDiscovery.MappingCandidate... candidates) {
		Map<RepositoryDiscovery.MappingKey, RepositoryDiscovery.MappingCandidate> mappings = new java.util.LinkedHashMap<>();
		Set<String> aimcs = new java.util.LinkedHashSet<>();
		for (var candidate : candidates) { mappings.put(candidate.key(), candidate); aimcs.add(candidate.key().aimcId()); }
		return new RepositoryDiscovery.Scan(true, "", aimcs, Set.of(), mappings, Map.of(), List.of());
	}

	private static RepositoryDiscovery.MappingCandidate candidate(String aimc, String identity, int index,
			String bundleId, String version, String dependency) {
		return candidate(aimc, identity, index, bundleId, version, dependency, () -> { });
	}

	/** A candidate whose plan runs {@code onStage} whenever it is staged. */
	private static RepositoryDiscovery.MappingCandidate candidate(String aimc, String identity, int index,
			String bundleId, String version, String dependency, Runnable onStage) {
		var configuration = staging(plan(bundleId + "-" + version), onStage);
		var fingerprint = fingerprint(configuration);
		var plan = new MappingPlan(identity, index, List.of(), List.of(), WireModel.Presence.absent(),
				WireModel.Presence.absent(), WireModel.Presence.absent(), WireModel.Presence.absent(),
				WireModel.Presence.absent(), List.of());
		var desired = new DesiredMapping(identity, index, plan, List.of(),
				Set.of(new MappingPlanCompiler.Dependency("submodel", dependency)));
		var bundle = new CompiledBundle(identity, index, bundleId, bundleId + "-" + version, configuration, fingerprint);
		var key = new RepositoryDiscovery.MappingKey(aimc, identity, index);
		return new RepositoryDiscovery.MappingCandidate(key, new DesiredBundle(desired, bundle, List.of(), fingerprint));
	}

	private static MappingReconciliationService service(Runtime runtime,
			MappingReconciliationService.ScanProvider scan) {
		return new MappingReconciliationService(runtime, scan, 32, 16, 60_000);
	}

	private static MappingReconciliationService service(Runtime runtime,
			MappingReconciliationService.ScanProvider scan, long retryMillis) {
		return new MappingReconciliationService(runtime, scan, 32, 16, retryMillis);
	}

	private static RepositoryDiscovery.Scan scan(DesiredBundle candidate, boolean complete) {
		Map<RepositoryDiscovery.MappingKey, RepositoryDiscovery.MappingCandidate> mappings = candidate == null
				? Map.of() : Map.of(KEY, new RepositoryDiscovery.MappingCandidate(KEY, candidate));
		return new RepositoryDiscovery.Scan(complete, complete ? "" : "repository-transient_failure",
				Set.of("urn:aimc"), Set.of(), mappings,
				candidate == null ? Map.of() : Map.of("urn:dependency", Set.of(KEY)), List.of());
	}

	private static DesiredBundle valid(String version) {
		return valid(version, () -> { });
	}

	/** A valid mapping whose plan runs {@code onStage} whenever it is staged. */
	private static DesiredBundle valid(String version, Runnable onStage) {
		String routeId = BUNDLE + "-" + version;
		RoutePlan configuration = staging(plan(routeId), onStage);
		var plan = new MappingPlan("mapping-a", 0, List.of(), List.of(), WireModel.Presence.absent(),
				WireModel.Presence.absent(), WireModel.Presence.absent(), WireModel.Presence.absent(),
				WireModel.Presence.absent(), List.of());
		var desired = new DesiredMapping("mapping-a", 0, plan, List.of(),
				Set.of(new MappingPlanCompiler.Dependency("submodel", "urn:dependency")));
		var fingerprint = fingerprint(configuration);
		var bundle = new CompiledBundle("mapping-a", 0, BUNDLE, routeId, configuration, fingerprint);
		return new DesiredBundle(desired, bundle, List.of(), fingerprint);
	}

	private static DesiredBundle invalid(AasRepositoryClient.Status status) {
		var failure = new AasRepositoryClient.Result<>(status, null, status == AasRepositoryClient.Status.NOT_FOUND ? 404 : 503,
				"safe test failure");
		var desired = new DesiredMapping("mapping-a", 0, null, List.of(failure),
				Set.of(new MappingPlanCompiler.Dependency("submodel", "urn:dependency")));
		var fingerprint = fingerprint(plan(BUNDLE));
		return new DesiredBundle(desired, null, List.of(), fingerprint);
	}

	private static RepositoryDiscovery.MappingCandidate restCandidate(String aimc, String identity,
			String bundleId, String restPath) {
		var command = new OperationRoute.Command(new Sink("command", null, Endpoint.of("mock:" + bundleId)), List.of(),
				false, Map.of(), List.of(), Map.of(), List.of(), null, Map.of());
		var caller = new OperationRoute.Caller(Map.of(), Map.of(), Map.of(), Map.of(), List.of(), Map.of(), null);
		var configuration = new OperationRoute(bundleId, new OperationRoute.Identity(restPath, "invoke", null, null),
				Endpoint.of("direct:request-" + bundleId), List.of(), command, caller, null);
		var fingerprint = fingerprint(configuration);
		var bundle = new CompiledBundle(identity, 0, bundleId, bundleId, configuration, fingerprint);
		var plan = new MappingPlan(identity, 0, List.of(), List.of(), WireModel.Presence.absent(),
				WireModel.Presence.absent(), WireModel.Presence.absent(), WireModel.Presence.absent(),
				WireModel.Presence.absent(), List.of());
		var desired = new DesiredMapping(identity, 0, plan, List.of(), Set.of());
		var desiredBundle = new DesiredBundle(desired, bundle, List.of(), fingerprint);
		var key = new RepositoryDiscovery.MappingKey(aimc, identity, 0);
		return new RepositoryDiscovery.MappingCandidate(key, desiredBundle);
	}

	/** A mapping writing the field Count of {@code measurement} as {@code valueType}. */
	private static RepositoryDiscovery.MappingCandidate seriesCandidate(String aimc, String bundleId, String measurement,
			String valueType) {
		var series = new dk.aau.smartproductionlab.dmp.influxdb2.configuration.InfluxDb2Endpoint(measurement, "ap2030", "Time",
				Map.of(), List.of(new dk.aau.smartproductionlab.dmp.influxdb2.RecordColumn("Count", valueType)), null, null, false);
		var configuration = new MappingRoute(bundleId, List.of(), null, List.of(new Sink("sink-" + bundleId, null, series)), null);
		var fingerprint = fingerprint(configuration);
		var bundle = new CompiledBundle(bundleId, 0, bundleId, bundleId, configuration, fingerprint);
		var plan = new MappingPlan(bundleId, 0, List.of(), List.of(), WireModel.Presence.absent(),
				WireModel.Presence.absent(), WireModel.Presence.absent(), WireModel.Presence.absent(),
				WireModel.Presence.absent(), List.of());
		var desired = new DesiredMapping(bundleId, 0, plan, List.of(), Set.of());
		var key = new RepositoryDiscovery.MappingKey(aimc, bundleId, 0);
		return new RepositoryDiscovery.MappingCandidate(key, new DesiredBundle(desired, bundle, List.of(), fingerprint));
	}

	/** A mapping without sources or sinks: staging it installs only its collector route. */
	private static MappingRoute plan(String routeId) {
		return new MappingRoute(routeId, List.of(), null, List.of(), null);
	}

	/** {@code plan}, running {@code onStage} each time a bundle builds it. */
	private static RoutePlan staging(RoutePlan plan, Runnable onStage) {
		return new RoutePlan() {
			@Override public String routeId() { return plan.routeId(); }
			@Override public void build(RouteBuilder builder) { onStage.run(); plan.build(builder); }
			@Override public List<Endpoint> endpoints() { return plan.endpoints(); }
		};
	}

	private static final class Runtime extends DmpComponent {
		boolean failReplacementWithoutSurvivor;
		boolean failRemoval;
		Runtime() { super(); }
		int routeCount() { return camelContext.getRoutesSize(); }
		@Override public synchronized ManagedBundle replaceBundle(ManagedBundle previous, StagedBundle staged) {
			if (failReplacementWithoutSurvivor) {
				failReplacementWithoutSurvivor = false;
				super.removeBundle(previous);
				staged.close();
				throw new IllegalStateException("candidate and restoration failed");
			}
			return super.replaceBundle(previous, staged);
		}
		@Override public synchronized void removeBundle(ManagedBundle bundle, long generation) {
			if (failRemoval) throw new IllegalStateException("cleanup failed");
			super.removeBundle(bundle, generation);
		}
	}

	private static final class ManualScheduler extends java.util.concurrent.AbstractExecutorService
			implements java.util.concurrent.ScheduledExecutorService {
		final java.util.ArrayDeque<Runnable> work = new java.util.ArrayDeque<>();
		Timer timer;
		boolean shutdown;
		void runTimer() { Timer next = timer; timer = null; next.run(); }
		void runWork() { work.removeFirst().run(); }
		@Override public void execute(Runnable command) { work.add(command); }
		@Override public java.util.concurrent.ScheduledFuture<?> schedule(Runnable command, long delay, java.util.concurrent.TimeUnit unit) {
			return timer = new Timer(command, unit.toMillis(delay));
		}
		@Override public <V> java.util.concurrent.ScheduledFuture<V> schedule(java.util.concurrent.Callable<V> command, long delay, java.util.concurrent.TimeUnit unit) { throw new UnsupportedOperationException(); }
		@Override public java.util.concurrent.ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initial, long period, java.util.concurrent.TimeUnit unit) { throw new UnsupportedOperationException(); }
		@Override public java.util.concurrent.ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initial, long delay, java.util.concurrent.TimeUnit unit) { throw new UnsupportedOperationException(); }
		@Override public void shutdown() { shutdown = true; }
		@Override public List<Runnable> shutdownNow() { shutdown = true; return List.of(); }
		@Override public boolean isShutdown() { return shutdown; }
		@Override public boolean isTerminated() { return shutdown; }
		@Override public boolean awaitTermination(long timeout, java.util.concurrent.TimeUnit unit) { return shutdown; }
	}
	private static final class Timer extends java.util.concurrent.FutureTask<Void> implements java.util.concurrent.ScheduledFuture<Void> {
		final long delayMillis;
		Timer(Runnable command, long delayMillis) { super(command, null); this.delayMillis = delayMillis; }
		@Override public long getDelay(java.util.concurrent.TimeUnit unit) { return unit.convert(delayMillis, java.util.concurrent.TimeUnit.MILLISECONDS); }
		@Override public int compareTo(java.util.concurrent.Delayed other) { return Long.compare(delayMillis, other.getDelay(java.util.concurrent.TimeUnit.MILLISECONDS)); }
	}
}
