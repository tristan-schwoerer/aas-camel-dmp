/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.configuration.route.execution.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.component.seda.SedaEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.Evaluation;
import dk.aau.smartproductionlab.dmp.core.plan.MappingRoute;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * An observed {@code Cached} source with an initial read (SPEC §6): read once when its route starts, so the mapping
 * need not wait for its first update; an update that arrived first is kept. One context for the class: each initial
 * read waits (up to the read timeout) for the value its test hands it on a {@code seda:} queue.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CachedSourceInitialReadTest extends CamelTestSupport {

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() {
				add("held", "direct:triggerHeld", "direct:cachedHeld", "seda:readHeld", "mock:held");
				add("updateFirst", "direct:triggerUpdate", "direct:cachedUpdate", "seda:readUpdate", "mock:updateFirst");
			}

			private void add(String routeId, String trigger, String cached, String read, String sink) {
				new MappingRoute(routeId, List.of(
						new Source("trigger", "Trigger", Endpoint.of(trigger), Source.Delivery.OBSERVED, 0),
						new Source("cached", "Cached", Endpoint.of(cached), Source.Delivery.OBSERVED, 0)
								.withEvaluation(Evaluation.CACHED).withInitialRead(Endpoint.of(read))),
						null, List.of(new Sink("out", null, Endpoint.of(sink))), null).build(this);
			}
		};
	}

	@Test
	void aTriggerTheGateHeldEvaluatesOnceTheInitialReadArrivesAndTheSourceIsReadOnlyOnce() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:held");
		out.expectedMessageCount(1);
		template.sendBody("direct:triggerHeld", "go");
		Thread.sleep(200);
		assertEquals(0, out.getReceivedCounter(), "the Cached source has no value yet");

		template.sendBody("seda:readHeld", "SEEDED");
		out.assertIsSatisfied(3000);
		assertEquals(Map.of("Trigger", "go", "Cached", "SEEDED"), out.getExchanges().get(0).getMessage().getBody());

		template.sendBody("seda:readHeld", "NOT READ");
		Thread.sleep(300);
		assertEquals(1, context.getEndpoint("seda:readHeld", SedaEndpoint.class).getCurrentQueueSize(),
				"the initial read happens once");
	}

	@Test
	void anUpdateThatArrivesBeforeTheInitialReadIsKept() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:updateFirst");
		out.expectedMessageCount(1);
		template.sendBody("direct:cachedUpdate", "NEWER");
		template.sendBody("seda:readUpdate", "OLDER");
		SedaEndpoint read = context.getEndpoint("seda:readUpdate", SedaEndpoint.class);
		for (int i = 0; i < 50 && read.getCurrentQueueSize() > 0; i++) Thread.sleep(20);
		Thread.sleep(200);

		template.sendBody("direct:triggerUpdate", "go");
		out.assertIsSatisfied(3000);
		assertEquals("NEWER", ((Map<?, ?>) out.getExchanges().get(0).getMessage().getBody()).get("Cached"));
	}
}
