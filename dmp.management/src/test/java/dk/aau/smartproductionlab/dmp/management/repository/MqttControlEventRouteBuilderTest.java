/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.aas.event.AasEventComponent;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.datatypes.MqttTopicFilter;
import dk.aau.smartproductionlab.dmp.paho.testing.Mqtt5TestBroker;
import org.junit.jupiter.api.Test;

class MqttControlEventRouteBuilderTest {
	private static final String ID = "https://example.org/aas/a/submodels/Variables";

	@Test
	void controlUsesTwoPersistentSessionsAndTheValueFeedStartsCleanOnlyForListeningRoutes() throws Exception {
		try (var broker = new Mqtt5TestBroker(); var context = new DefaultCamelContext()) {
			var reconciliation = reconciliation();
			var routes = new MqttControlEventRouteBuilder(new MqttControlEventRouteBuilder.Settings(URI.create(broker.uri()),
					"dmp-test", null, null, "+", 3600), reconciliation);
			context.addRoutes(routes);
			context.start();
			await(routes::consumerReady);
			assertEquals(Set.of("sm-repository/+/submodels/+", "sm-repository/+/submodels/+/submodelElements/#"), broker.subscriptions);
			assertEquals(List.of(false, false), broker.connects, "both control clients resume persistent sessions");
			Thread.sleep(300);
			assertFalse(broker.clientIds.contains("dmp-test-values"), "no event-sourced route, no value feed");

			String element = "sm-repository/sm-repo/submodels/" + AasRepositoryClient.base64UrlIdentifier(ID) + "/submodelElements/Status.State/value/updated";
			String property = "{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:int\",\"value\":\"3\"}";
			assertEquals(1, broker.publish(element, property));
			await(() -> reconciliation.acceptedInvalidationCount() == 1);

			context.addRoutes(new RouteBuilder() {
				@Override public void configure() { from(AasEventComponent.uri(ID, "Status.State")).to("mock:values"); }
			});
			await(() -> broker.subscriptions.contains("sm-repository/+/submodels/#"));
			assertTrue(broker.clientIds.contains("dmp-test-values"));
			assertTrue(broker.connects.get(broker.clientIds.indexOf("dmp-test-values")), "the value feed starts clean: no backlog");
			MockEndpoint values = context.getEndpoint("mock:values", MockEndpoint.class);
			values.expectedMessageCount(1);
			assertEquals(2, broker.publish(element, property), "the control element client and the independent value client");
			values.assertIsSatisfied(5000);
			assertTrue(values.getExchanges().get(0).getMessage().getBody(String.class).contains("\"value\":\"3\""));
			await(() -> reconciliation.acceptedInvalidationCount() == 2);
			reconciliation.close();
		}
	}

	@Test
	void controlSubscriptionsMatchRepositoryTopicsOnlyNeverDeviceTelemetry() {
		var topics = new MqttControlEventRouteBuilder.Settings(URI.create("tcp://broker:1883"), "dmp", null, null, "+", 60).topics();
		for (String repository : List.of("sm-repository/sm-repo/submodels/created",
				"sm-repository/sm-repo/submodels/c2Q/submodelElements/Status.State/updated",
				"sm-repository/sm-repo/submodels/c2Q/submodelElements/Status.State/value/updated",
				"sm-repository/sm-repo/submodels/c2Q/submodelElements/patched"))
			assertTrue(topics.stream().anyMatch(filter -> MqttTopicFilter.of(filter).matches(MqttTopic.of(repository))), repository);
		// Telemetry on the same broker is for the data plane's own clients, never the control channel.
		for (String telemetry : List.of("base/state", "stations/stoppering/PackML/State", "spBv1.0/group/DDATA/edge",
				"aas-repository/aas-repo/shells/created"))
			assertFalse(topics.stream().anyMatch(filter -> MqttTopicFilter.of(filter).matches(MqttTopic.of(telemetry))), telemetry);
	}

	private static MappingReconciliationService reconciliation() {
		return new MappingReconciliationService(new DmpComponent(),
				() -> new RepositoryDiscovery.Scan(true, "", Set.of(), Set.of(), Map.of(), Map.of(), List.of()), 8, 8, 1000);
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + 10_000_000_000L;
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() > deadline) throw new AssertionError("condition not reached");
			Thread.sleep(20);
		}
	}
}
