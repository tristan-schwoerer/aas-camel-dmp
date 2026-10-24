/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.configuration.route.execution.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import dk.aau.smartproductionlab.dmp.core.util.TransformerMeta;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.Evaluation;
import dk.aau.smartproductionlab.dmp.core.plan.MappingRoute;
import dk.aau.smartproductionlab.dmp.core.plan.Response;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.junit.jupiter.api.Test;

/** An ordinary mapping's response half: after its sinks, also when they failed, before the next evaluation. */
class OrdinaryResponseRouteTest extends CamelTestSupport {

	private final AtomicReference<Map<?, ?>> responseMeta = new AtomicReference<>();
	private final AtomicReference<Object> requestSources = new AtomicReference<>();

	private static Source source(String id, String sourceId, String uri, Evaluation evaluation) {
		return new Source(id, sourceId, Endpoint.of(uri), evaluation == Evaluation.READ_AT_EVALUATION
				? Source.Delivery.READ : Source.Delivery.OBSERVED, 0).withEvaluation(evaluation);
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() {
				// the request Lua passes the value to the device; the device rejects "reject"
				from("direct:requestLua").process(exchange -> exchange.getMessage().setBody(
						Map.of("Setpoint", ((Map<?, ?>) exchange.getMessage().getBody()).get("Value"))));
				from("direct:device").process(exchange -> {
					if ("reject".equals(exchange.getMessage().getBody()))
						throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
								"DEVICE_REJECTED", "the device rejected the write", Map.of());
				}).to("mock:device");
				// the response Lua logs what was written and the outcome
				from("direct:responseLua").process(exchange -> {
					responseMeta.set(exchange.getProperty(TransformerMeta.PROPERTY, Map.class));
					requestSources.set(exchange.getProperty(TransformerMeta.REQUEST_SOURCES));
					Map<?, ?> invocation = (Map<?, ?>) responseMeta.get().get("invocation");
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("outcome", invocation.get("outcome"));
					row.put("written", ((Map<?, ?>) exchange.getMessage().getBody()).get("Written"));
					exchange.getMessage().setBody(Map.of("Log", row));
				});

				// a second mapping: a trigger, and a reading read once per evaluation
				from("direct:readingLua").process(exchange -> exchange.getMessage().setBody(
						Map.of("Setpoint", ((Map<?, ?>) exchange.getMessage().getBody()).get("Reading"))));

				Sink setpoint = new Sink("setpoint", "Setpoint", Endpoint.of("direct:device"));
				Response response = new Response(Endpoint.of("direct:responseLua"),
						List.of(new Response.ResponseSource("Written", "Setpoint", null)),
						List.of(new Sink("log", "Log", Endpoint.of("mock:log"))));
				new MappingRoute("logged", List.of(source("value", "Value", "direct:in", Evaluation.ON_UPDATE)),
						Endpoint.of("direct:requestLua"), List.of(setpoint), response).build(this);
				new MappingRoute("reads", List.of(source("go", "Go", "direct:go", Evaluation.ON_UPDATE),
						source("reading", "Reading", "seda:reading", Evaluation.READ_AT_EVALUATION)),
						Endpoint.of("direct:readingLua"), List.of(setpoint), response).build(this);
			}
		};
	}

	@Test
	void theResponseProjectsTheWrittenValueAndIsWrittenBeforeTheEvaluationEnds() throws Exception {
		MockEndpoint device = getMockEndpoint("mock:device");
		MockEndpoint log = getMockEndpoint("mock:log");
		log.expectedMessageCount(1);
		template.sendBody("direct:in", 21);
		// the response is written inside the evaluation, so once it arrived the device has been written
		log.assertIsSatisfied(2000);
		assertEquals(1, device.getReceivedCounter());
		assertEquals(Map.of("outcome", "Succeeded", "written", 21), log.getExchanges().get(0).getMessage().getBody());
		Map<?, ?> invocation = (Map<?, ?>) responseMeta.get().get("invocation");
		assertTrue(invocation.containsKey("id") && invocation.containsKey("started") && invocation.containsKey("durationMs"),
				String.valueOf(invocation));
		assertEquals(Map.of("Log", Map.of("kind", "mock")), responseMeta.get().get("sinks"));
		assertEquals(Map.of("Value", 21), requestSources.get(), "the response sees what the request transformation saw");
	}

	@Test
	void aSourceReadAtEvaluationIsReadOnceForEachEvaluation() throws Exception {
		MockEndpoint device = getMockEndpoint("mock:device");
		MockEndpoint log = getMockEndpoint("mock:log");
		log.expectedMessageCount(1);
		template.sendBody("seda:reading", 7);
		template.sendBody("direct:go", "now");
		log.assertIsSatisfied(2000);
		assertEquals(7, device.getExchanges().get(0).getMessage().getBody());
		assertEquals(Map.of("Go", "now", "Reading", 7), requestSources.get());
		assertEquals("Succeeded", ((Map<?, ?>) responseMeta.get().get("invocation")).get("outcome"));

		// nothing to read: the evaluation fails, and its response says why
		log.reset();
		log.expectedMessageCount(1);
		template.sendBody("seda:reading", null);
		template.sendBody("direct:go", "again");
		log.assertIsSatisfied(2000);
		assertEquals(1, device.getReceivedCounter());
		assertEquals("Failed", log.getExchanges().get(0).getMessage().getBody(Map.class).get("outcome"));
		Map<?, ?> error = (Map<?, ?>) ((Map<?, ?>) responseMeta.get().get("invocation")).get("error");
		assertEquals("DMP_SOURCE_READ_FAILED", error.get("code"));
	}

	@Test
	void aFailedEvaluationStillRunsItsResponseAndLeavesTheSourceAlone() throws Exception {
		MockEndpoint log = getMockEndpoint("mock:log");
		log.expectedMessageCount(1);
		// the source only accepts its value; the evaluation fails later, on a worker
		template.sendBody("direct:in", "reject");
		log.assertIsSatisfied(2000);
		Map<String, Object> row = new LinkedHashMap<>();
		row.put("outcome", "Failed");
		row.put("written", null);
		assertEquals(row, log.getExchanges().get(0).getMessage().getBody(), "a failed evaluation has no ResponseSources");
		Map<?, ?> error = (Map<?, ?>) ((Map<?, ?>) responseMeta.get().get("invocation")).get("error");
		assertEquals("DEVICE_REJECTED", error.get("code"), "the sink's own error: " + error);
		assertEquals("REMOTE", error.get("category"));
	}
}
