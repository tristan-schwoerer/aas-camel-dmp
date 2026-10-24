/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import dk.aau.smartproductionlab.dmp.aas.configuration.AasEventSource;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import org.junit.jupiter.api.Test;

class AasEventHubTest extends CamelTestSupport {
	private static final String SUBMODEL = "https://example.org/aas/a/submodels/Variables";
	private final Source source = new Source("state", "State",
			new AasEventSource(SUBMODEL, "Status.State", "Property", "http://127.0.0.1:1/unused"), Source.Delivery.OBSERVED, 0);

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override public void configure() {
				from(source.endpoint().uri(null))
						.process(source.endpoint().normalizer(source))
						.filter(exchange -> Boolean.TRUE.equals(exchange.getProperty(Source.CONTRIBUTION_PROPERTY)))
						.to("mock:values");
				// many listeners, one of them slow
				for (int i = 0; i < LISTENERS; i++) from(AasEventComponent.uri(LOAD, "Many.P" + i)).to("mock:many");
				from(AasEventComponent.uri(LOAD, "Slow.State"))
						.process(exchange -> SLOW.await(10, java.util.concurrent.TimeUnit.SECONDS)).to("mock:slow");
			}
		};
	}

	/** The submodel of the many listeners. */
	private static final String LOAD = "https://example.org/aas/a/submodels/Load";
	private static final int LISTENERS = 200;
	private final java.util.concurrent.CountDownLatch SLOW = new java.util.concurrent.CountDownLatch(1);

	private AasEventHub hub() { return AasEventComponent.hub(context); }

	private static String update(String path, String value) {
		return "{\"type\":\"SME_UPDATED\",\"id\":\"" + LOAD + "\",\"smElementPath\":\"" + path + "\","
				+ "\"smElement\":{\"modelType\":\"Property\",\"idShort\":\"x\",\"valueType\":\"xs:string\",\"value\":\"" + value + "\"}}";
	}

	@Test
	void listenersShareTheWorkersAndASlowOneHoldsNeitherTheFeedNorTheOthers() throws Exception {
		MockEndpoint many = getMockEndpoint("mock:many");
		MockEndpoint slow = getMockEndpoint("mock:slow");
		many.expectedMessageCount(LISTENERS);
		// more events than a listener may hold, while it is busy
		long started = System.nanoTime();
		for (int i = 0; i < 1100; i++) hub().publish(update("Slow.State", "v" + i), Map.of());
		assertTrue(System.nanoTime() - started < java.util.concurrent.TimeUnit.SECONDS.toNanos(2),
				"publish only hands the events over, also to a full listener");
		for (int i = 0; i < LISTENERS; i++) hub().publish(update("Many.P" + i, "fast"), Map.of());
		many.assertIsSatisfied(5000);
		int threads = java.lang.management.ManagementFactory.getThreadMXBean().getThreadCount();
		assertTrue(threads < LISTENERS, threads + " threads for " + LISTENERS + " listeners that each had an event");
		assertEquals(0, slow.getReceivedCounter(), "the slow listener is still held");
		SLOW.countDown();
		org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10)).until(() -> slow.getReceivedCounter() > 0
				&& String.valueOf(slow.getExchanges().get(slow.getExchanges().size() - 1).getMessage().getBody()).contains("\"v1099\""));
		assertTrue(slow.getReceivedCounter() <= 1025, "a full listener keeps the newest event: " + slow.getReceivedCounter());
	}

	@Test
	void deliversTheWatchedPropertyFromItsOwnAnAncestorsAndAWholeSubmodelEvent() throws Exception {
		MockEndpoint values = getMockEndpoint("mock:values");
		values.expectedBodiesReceived("RUNNING", "STOPPED", "IDLE");
		assertTrue(hub().listensTo(SUBMODEL));
		hub().publish("{\"type\":\"SME_UPDATED\",\"id\":\"" + SUBMODEL + "\",\"smElementPath\":\"Status.State\","
				+ "\"smElement\":{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:string\",\"value\":\"RUNNING\"}}", Map.of());
		hub().publish("{\"type\":\"SME_UPDATED\",\"id\":\"" + SUBMODEL + "\",\"smElementPath\":\"Status\","
				+ "\"smElement\":{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Status\",\"value\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:string\",\"value\":\"STOPPED\"}]}}", Map.of());
		hub().publish("{\"type\":\"SM_UPDATED\",\"id\":\"" + SUBMODEL + "\",\"submodel\":{\"modelType\":\"Submodel\",\"id\":\"" + SUBMODEL
				+ "\",\"submodelElements\":[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Status\",\"value\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:string\",\"value\":\"IDLE\"}]}]}}", Map.of());
		values.assertIsSatisfied(5000);
	}

	@Test
	void ignoresOtherElementsOtherSubmodelsAndDeletions() throws Exception {
		MockEndpoint values = getMockEndpoint("mock:values");
		values.expectedMessageCount(0);
		hub().publish("{\"type\":\"SME_UPDATED\",\"id\":\"" + SUBMODEL + "\",\"smElementPath\":\"Status.Mode\","
				+ "\"smElement\":{\"modelType\":\"Property\",\"idShort\":\"Mode\",\"valueType\":\"xs:string\",\"value\":\"x\"}}", Map.of());
		hub().publish("{\"type\":\"SME_UPDATED\",\"id\":\"urn:other\",\"smElementPath\":\"Status.State\","
				+ "\"smElement\":{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:string\",\"value\":\"x\"}}", Map.of());
		hub().publish("{\"type\":\"SME_DELETED\",\"id\":\"" + SUBMODEL + "\",\"smElementPath\":\"Status.State\"}", Map.of());
		hub().publish("not json", Map.of());
		values.assertIsSatisfied(500);
		assertFalse(hub().listensTo("urn:other"));
	}

	@Test
	void stoppingTheRouteUnregistersItsListener() throws Exception {
		context.getRouteController().stopRoute(context.getRoutes().get(0).getRouteId());
		assertFalse(hub().listensTo(SUBMODEL));
	}
}
