/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import dk.aau.smartproductionlab.dmp.core.execution.DmpExecutors;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory.RuntimeSettings;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery;
import dk.aau.smartproductionlab.dmp.paho.mqtt5.component.DmpPahoMqtt5Component;
import dk.aau.smartproductionlab.dmp.paho.testing.Mqtt5TestBroker;

/**
 * E4's fleet (EVALUATION.md): N assets, each with one mapping from MQTT to AAS Properties (the {@code fan-in-out}
 * contract: two sources, two sinks), installed through discovery and reconciliation and driven with one update
 * per source. The DMP's threads must not grow with N: the mappings share one MQTT connection and the workers.
 * Size it with {@code -Ddmp.stress.mqttMappings} (default 1000).
 */
@Tag("capacity")
class ManagedMqttCapacityTest {
	private static final int MAPPINGS = Integer.getInteger("dmp.stress.mqttMappings", 1000);
	/** Threads the DMP may add for any N: the workers, the scheduler, the MQTT and Kafka clients and Camel's own. */
	private static final int THREAD_BUDGET = DmpExecutors.WORKER_THREADS + 60;

	@Test @Timeout(value = 20, unit = TimeUnit.MINUTES)
	void aThousandMqttMappingsShareTheirThreads() throws Exception {
		var threads = ManagementFactory.getThreadMXBean();
		String previousPort = System.getProperty("dmp.http.port");
		try (var fixture = new ManagedPackagedRuntimeIntegrationTest.Fixture(); var mqtt = new Mqtt5TestBroker()) {
			System.setProperty("dmp.http.port", Integer.toString(fixture.port));
			fixture.models.clear();
			for (int i = 0; i < MAPPINGS; i++) unit(fixture, i, mqtt.port());
			// The sinks' $value writes succeed; they are counted as the repository's non-GET requests.
			fixture.repository.override("value-writes", request -> request.endsWith("/$value") ? new FakeAasRepository.Response(200, "{}") : null);
			var settings = ManagedRuntimeSettings.fromEnvironment(Map.of("DMP_MANAGED_AAS_REPOSITORY_URI", fixture.repository.uri().toString(),
					"DMP_MANAGED_KAFKA_BROKERS", fixture.broker.bootstrap(), "DMP_MANAGED_CONTROL_TOPIC", fixture.topic,
					"DMP_MANAGED_CONTROL_GROUP_ID", fixture.group, "DMP_MANAGED_MAX_MAPPINGS", Integer.toString(MAPPINGS)));
			var runtimeSettings = new RuntimeSettings(settings.repositoryUri());
			var discovery = new RepositoryDiscovery(new AasRepositoryClient(settings.repositoryUri(), Duration.ofSeconds(5),
					Duration.ofSeconds(5), 100, 100, null), runtimeSettings, 4, java.util.Set.of());
			@SuppressWarnings("resource") // closing the runtime would reconcile; this test owns the component lifecycle
			var component = new ManagedDmpRuntime(settings).component();
			component.startComponent();
			int baseline = threads.getThreadCount();
			try (var service = new MappingReconciliationService(component, discovery, MAPPINGS, 8, 1000)) {
				long started = System.nanoTime();
				var installed = service.reconcileNow();
				assertTrue(installed.complete(), installed.categories().toString());
				assertEquals(MAPPINGS, installed.installedOrUpdated(), installed.categories().toString());
				long installMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
				var mqttSessions = component.runtimeContext().getComponent(DmpPahoMqtt5Component.SCHEME, DmpPahoMqtt5Component.class);
				assertEquals(1, mqttSessions.sessionCount(), "one MQTT connection for every mapping of the broker");
				assertEquals(1, mqtt.clientIds.size(), "one MQTT connection for every mapping of the broker");
				int afterInstall = threads.getThreadCount();
				assertTrue(afterInstall - baseline <= THREAD_BUDGET,
						MAPPINGS + " mappings added " + (afterInstall - baseline) + " threads, more than " + THREAD_BUDGET);

				// Every source delivers once (the gate waits for both), then State again: two writes per evaluation.
				int writesBefore = fixture.repository.writes();
				started = System.nanoTime();
				for (int i = 0; i < MAPPINGS; i++) mqtt.publish("unit" + i + "/state", "\"001\"");
				for (int i = 0; i < MAPPINGS; i++) mqtt.publish("unit" + i + "/mode", "true");
				awaitWrites(fixture, writesBefore + 2 * MAPPINGS);
				long firstValueMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
				started = System.nanoTime();
				for (int i = 0; i < MAPPINGS; i++) mqtt.publish("unit" + i + "/state", "\"002\"");
				awaitWrites(fixture, writesBefore + 4 * MAPPINGS);
				long updateMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
				int underLoad = threads.getPeakThreadCount();
				assertTrue(underLoad - baseline <= THREAD_BUDGET,
						MAPPINGS + " evaluating mappings peaked at " + (underLoad - baseline) + " threads, more than " + THREAD_BUDGET);

				fixture.models.clear();
				assertEquals(MAPPINGS, service.reconcileNow().removed());
				assertEquals(0, mqttSessions.sessionCount(), "removing the last mapping closes the connection");
				System.out.println("DMP_STRESS_MQTT " + JsonCodec.toJson(Map.of("mappings", MAPPINGS,
						"installMillis", installMillis, "firstValueMillis", firstValueMillis, "updateMillis", updateMillis,
						"baselineThreads", baseline, "threadsAfterInstall", afterInstall, "peakThreads", underLoad,
						"threadsAfterRemoval", threads.getThreadCount(), "process", component.processMetrics())));
			} finally { component.stopComponent(); }
		} finally {
			if (previousPort == null) System.clearProperty("dmp.http.port"); else System.setProperty("dmp.http.port", previousPort);
			ManagedRuntimeStatus.reset();
		}
	}

	/** Asset {@code unit<i>}: the fan-in-out AIMC, its own AID on topics {@code unit<i>/state} and {@code unit<i>/mode}, its KPI submodel. */
	private static void unit(ManagedPackagedRuntimeIntegrationTest.Fixture fixture, int i, int brokerPort) throws Exception {
		String id = "unit" + i;
		for (String file : new String[] { "aimc.json", "aid.json", "dependencies/0.json" }) {
			String model = resource(file).replace("a/submodels/AID", id + "/submodels/AID")
					.replace("contractAAS/", id + "/").replace("mqtt://192.168.0.104:1883/base/", "mqtt://127.0.0.1:" + brokerPort + "/" + id + "/");
			fixture.put(PublishedModels.publish(model, id, id));
		}
	}

	private static void awaitWrites(ManagedPackagedRuntimeIntegrationTest.Fixture fixture, int expected) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
		while (fixture.repository.writes() < expected) {
			if (System.nanoTime() > deadline)
				throw new AssertionError("only " + fixture.repository.writes() + " of " + expected + " AAS writes arrived");
			Thread.sleep(20);
		}
	}

	private static String resource(String file) throws Exception {
		try (var input = ManagedMqttCapacityTest.class.getResourceAsStream("/runtime-model-contracts/fan-in-out/" + file)) {
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
