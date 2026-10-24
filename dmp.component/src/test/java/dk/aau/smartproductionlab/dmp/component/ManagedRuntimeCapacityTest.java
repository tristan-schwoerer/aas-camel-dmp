/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory.RuntimeSettings;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/** Bounded local stress, not maximum-fleet or heap-isolation certification. */
@org.junit.jupiter.api.Tag("capacity")
class ManagedRuntimeCapacityTest {
	static java.util.stream.Stream<Arguments> capacities() {
		String configured = System.getProperty("dmp.stress.capacity");
		if (configured == null) return java.util.stream.Stream.of(Arguments.of(32, 16), Arguments.of(256, 128));
		int capacity = Integer.parseInt(configured);
		int desired = Integer.getInteger("dmp.stress.desiredMappings", capacity + Math.min(capacity, 1000));
		if (capacity < 1 || capacity > 8192 || desired <= capacity || desired > 10_000)
			throw new IllegalArgumentException("stress capacity must be 1..8192 with capacity < desiredMappings <=10000");
		return java.util.stream.Stream.of(Arguments.of(desired, capacity));
	}

	@ParameterizedTest @MethodSource("capacities") @org.junit.jupiter.api.Timeout(value = 20, unit = java.util.concurrent.TimeUnit.MINUTES)
	void desiredMappingsStayWithinCapacityAcrossBurstAndRepeatedReplacement(int desired, int capacity) throws Exception {
		long started = System.nanoTime();
		var phases = new java.util.ArrayList<Map<String, Object>>();
		String previousPort = System.getProperty("dmp.http.port");
		try (var fixture = new ManagedPackagedRuntimeIntegrationTest.Fixture()) {
			System.setProperty("dmp.http.port", Integer.toString(fixture.port));
			fixture.models.remove("contractBAS/submodels/AIMC");
			fixture.models.remove("contractBAS/submodels/ControlComponentInstance");
			for (int i = 1; i < desired; i++) fixture.unit("unit-" + i, "unit-" + i);
			System.out.println("DMP_STRESS_INPUT " + dk.aau.smartproductionlab.dmp.core.util.JsonCodec.toJson(Map.of(
					"desiredMappings", desired, "capacity", capacity, "modelPayloadBytes", fixture.models.values().stream()
							.mapToLong(value -> value.getBytes(StandardCharsets.UTF_8).length).sum())));
			var settings = ManagedRuntimeSettings.fromEnvironment(Map.of("DMP_MANAGED_AAS_REPOSITORY_URI", fixture.repository.uri().toString(),
					"DMP_MANAGED_KAFKA_BROKERS", fixture.broker.bootstrap(), "DMP_MANAGED_CONTROL_TOPIC", fixture.topic,
					"DMP_MANAGED_CONTROL_GROUP_ID", fixture.group, "DMP_MANAGED_MAX_MAPPINGS", Integer.toString(capacity),
					"DMP_MANAGED_MAX_INVALIDATIONS", "8"));
			var runtimeSettings = new RuntimeSettings(settings.repositoryUri());
			var discovery = new RepositoryDiscovery(new AasRepositoryClient(settings.repositoryUri(), Duration.ofSeconds(2),
					Duration.ofSeconds(2), 100, 100, null), runtimeSettings, 4, java.util.Set.of());
			@SuppressWarnings("resource") // closing the runtime would reconcile; this test owns the component lifecycle
			var component = new ManagedDmpRuntime(settings).component();
			component.startComponent();
			try (var service = new MappingReconciliationService(component, discovery, capacity, 8, 1000)) {
				long phaseStarted = System.nanoTime();
				var initial = service.reconcileNow();
				assertTrue(initial.complete(), initial.categories().toString());
				assertEquals(capacity, initial.installedOrUpdated(), initial.categories().toString());
				phases.add(sample("initial", phaseStarted, component, fixture));
				assertEquals(desired - capacity, service.overflowMappingCount());
				var baseline = component.resourceMetrics();
				assertEquals((long) capacity, baseline.get("managedBundles"));
				assertEquals(capacity * 2L, baseline.get("luaBindings"));
				assertEquals(0L, baseline.get("stagedBundles"));
				var process = component.processMetrics();
				assertTrue(process.get("heapUsedBytes") > 0);
				assertTrue(process.get("heapCommittedBytes") >= process.get("heapUsedBytes"));
				assertTrue(process.get("threadCount") >= process.get("daemonThreadCount"));
				assertEquals(capacity, service.status().size());
				for (int i = 0; i < 1000; i++) service.invalidate("unknown-" + i);
				assertTrue(service.invalidationOverflowCount() > 0);
				phaseStarted = System.nanoTime();
				assertEquals(0, service.reconcileNow().installedOrUpdated());
				phases.add(sample("burstRepair", phaseStarted, component, fixture));
				assertEquals(baseline, component.resourceMetrics(), "repair bursts must not grow ownership, fences or script bindings");
				for (int revision = 0; revision < 3; revision++) {
					String script = "function aimc_main(sources) return { Restart = { x = sources.Restart.x, generation = 'v0" + revision + "' } } end";
					for (var entry : fixture.models.entrySet()) if (entry.getKey().endsWith("/AIMC")) {
						JsonObject model = JsonParser.parseString(entry.getValue()).getAsJsonObject();
						for (var field : model.getAsJsonArray("submodelElements").get(0).getAsJsonObject().getAsJsonArray("value")
								.get(0).getAsJsonObject().getAsJsonArray("value")) {
							if ("Transformation".equals(field.getAsJsonObject().get("idShort").getAsString()))
								field.getAsJsonObject().addProperty("value", Base64.getEncoder().encodeToString(script.getBytes(StandardCharsets.UTF_8)));
						}
						entry.setValue(model.toString());
					}
					phaseStarted = System.nanoTime();
					assertEquals(capacity, service.reconcileNow().installedOrUpdated());
					phases.add(sample("replacement" + revision, phaseStarted, component, fixture));
					assertEquals(baseline, component.resourceMetrics(), "each replacement reaches the same owned-resource baseline");
					assertEquals(capacity, service.status().size());
					assertEquals(desired - capacity, service.overflowMappingCount());
				}
				assertEquals(0, fixture.sends.get(), "compilation, replacement and repair must not invoke ACTIONs");
				fixture.models.clear();
				phaseStarted = System.nanoTime();
				assertEquals(capacity, service.reconcileNow().removed());
				phases.add(sample("removal", phaseStarted, component, fixture));
				assertTrue(component.resourceMetrics().values().stream().allMatch(value -> value == 0), "confirmed removal releases every tracked owned resource");
				System.out.println("DMP_STRESS_METRICS " + dk.aau.smartproductionlab.dmp.core.util.JsonCodec.toJson(Map.of(
						"desiredMappings", desired, "capacity", capacity, "replacementWaves", 3, "phases", phases,
						"elapsedMillis", java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started),
						"baselineResources", baseline, "initialProcess", process, "finalProcess", component.processMetrics(),
						"finalResources", component.resourceMetrics(), "actionSends", fixture.sends.get())));
			} finally { component.stopComponent(); }
		} finally {
			if (previousPort == null) System.clearProperty("dmp.http.port"); else System.setProperty("dmp.http.port", previousPort);
			ManagedRuntimeStatus.reset();
		}
	}

	private static Map<String, Object> sample(String phase, long started,
			dk.aau.smartproductionlab.dmp.core.component.DmpComponent component,
			ManagedPackagedRuntimeIntegrationTest.Fixture fixture) {
		var sample = Map.<String, Object>of("phase", phase,
				"elapsedMillis", java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started),
				"process", component.processMetrics(),
				"cumulativeRepositoryRequests", fixture.repository.requests().size());
		System.out.println("DMP_STRESS_PHASE " + dk.aau.smartproductionlab.dmp.core.util.JsonCodec.toJson(sample));
		return sample;
	}
}
