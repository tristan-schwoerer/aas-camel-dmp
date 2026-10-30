/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;

import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import dk.aau.smartproductionlab.dmp.management.repository.PollingControlRouteBuilder;
import dk.aau.smartproductionlab.dmp.management.repository.PublishedModels;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonParser;

/**
 * A repository whose events the DMP does not read (e.g. BaSyx Go): a mapping authored, changed or removed while the
 * DMP runs is picked up by the next full rescan (DMP_MANAGED_EVENT_TRANSPORT=poll).
 */
class ManagedPollingRuntimeTest {
	private String oldHealthPort;

	@AfterEach void restore() {
		if (oldHealthPort == null) System.clearProperty("dmp.http.port");
		else System.setProperty("dmp.http.port", oldHealthPort);
		ManagedRuntimeStatus.reset();
	}

	@Test
	void aMappingAuthoredAndRemovedWhileTheDmpRunsIsPickedUpByTheNextRescan() throws Exception {
		oldHealthPort = System.getProperty("dmp.http.port");
		try (ServerSocket socket = new ServerSocket(0)) { System.setProperty("dmp.http.port", Integer.toString(socket.getLocalPort())); }
		try (var repository = new FakeAasRepository()) {
			repository.put(resource("http-operation/aid.json"));
			repository.put(PublishedModels.publish(resource("http-operation/dependencies/0.json"), "contractAAS", "contract"));
			var settings = new ManagedRuntimeSettings(repository.uri(), null, null, null, Set.of(), 16, 16, 4, 30_000,
					Duration.ofSeconds(2), Duration.ofSeconds(2), 50, 4, null, new PollingControlRouteBuilder.Settings(Duration.ofSeconds(1)));
			try (var runtime = new ManagedDmpRuntime(settings)) {
				runtime.start();
				assertEquals(ManagedRuntimeStatus.State.READY, ManagedRuntimeStatus.snapshot().state(), ManagedRuntimeStatus.asMap().toString());
				assertEquals(0, ManagedRuntimeStatus.snapshot().installedMappings());

				String aimc = resource("http-operation/aimc.json");
				repository.put(aimc);
				Awaitility.await().atMost(Duration.ofSeconds(8))
						.until(() -> ManagedRuntimeStatus.snapshot().installedMappings() == 1);

				repository.remove(JsonParser.parseString(aimc).getAsJsonObject().get("id").getAsString());
				Awaitility.await().atMost(Duration.ofSeconds(8))
						.until(() -> ManagedRuntimeStatus.snapshot().installedMappings() == 0);
			}
		}
	}

	private static String resource(String path) throws Exception {
		try (var input = ManagedPollingRuntimeTest.class.getResourceAsStream("/runtime-model-contracts/" + path)) {
			assertNotNull(input, "fixture " + path);
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
