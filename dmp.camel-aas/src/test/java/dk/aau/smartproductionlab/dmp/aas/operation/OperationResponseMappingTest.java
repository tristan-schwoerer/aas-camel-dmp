package dk.aau.smartproductionlab.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.support.DefaultExchange;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.AffordanceValidation;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.ResponseProjectionProcessor;
import dk.aau.smartproductionlab.dmp.core.plan.Response.ResponseSource;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.ResponseSinkFanOutProcessor;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.PayloadValidationException;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

class OperationResponseMappingTest {
	private static final Gson GSON = new Gson();

	@Test
	void responseSourcesProjectDifferentMembersUnderAuthoredIds() throws Exception {
		List<ResponseSource> bindings = List.of(
				new ResponseSource("State", null, "$.Machine.State"),
				new ResponseSource("Audit", null, "$.Machine.Audit"));
		Map<String, Object> projected = ResponseProjectionProcessor.project(
				Map.of("Machine", Map.of("State", "READY", "Audit", Map.of("code", 7))), bindings);
		assertEquals("READY", projected.get("State"));
		assertEquals(Map.of("code", 7), projected.get("Audit"));
	}

	@Test
	void projectionDistinguishesMissingPointerFromPresentNull() {
		Map<String, Object> reply = new java.util.LinkedHashMap<>();
		reply.put("optional", null);
		var bindings = List.of(new ResponseSource("Optional", null, "/optional"));
		var projected = ResponseProjectionProcessor.project(reply, bindings);
		assertTrue(projected.containsKey("Optional"));
		assertNull(projected.get("Optional"));
		PayloadValidationException failure = assertThrows(PayloadValidationException.class,
				() -> ResponseProjectionProcessor.project(Map.of(), bindings));
		assertEquals(PayloadValidationException.Stage.MAPPING, failure.getStage());
	}

	@Test
	void wholeAndRootProjectionPreserveNormalizedScalarArrayAndNullValues() {
		for (Object reply : java.util.Arrays.asList("READY", List.of(1L, 2L), null, JsonPointer.JSON_NULL)) {
			Object expected = reply == JsonPointer.JSON_NULL ? null : reply;
			for (ResponseSource binding : List.of(new ResponseSource("Whole", null, null),
					new ResponseSource("Whole", null, "$"))) {
				var projected = ResponseProjectionProcessor.project(reply, List.of(binding));
				assertTrue(projected.containsKey("Whole"));
				assertEquals(expected, projected.get("Whole"));
			}
		}
	}

	@Test
	void projectionRejectsDuplicateSourceIdAfterANullContribution() {
		Map<String, Object> reply = new java.util.LinkedHashMap<>();
		reply.put("optional", null);
		reply.put("other", "READY");
		PayloadValidationException failure = assertThrows(PayloadValidationException.class,
				() -> ResponseProjectionProcessor.project(reply, List.of(
						new ResponseSource("Duplicate", null, "/optional"),
						new ResponseSource("Duplicate", null, "/other"))));
		assertEquals(PayloadValidationException.Stage.MAPPING, failure.getStage());
		assertTrue(failure.getMessage().contains("duplicate ResponseSource id Duplicate"));
	}

	@Test
	void replyValidationDoesNotGuessAnEnvelopeFromTheActionMember() throws Exception {
		var validation = new AffordanceValidation(Map.of("type", "object",
				"required", List.of("Invoke", "sibling"), "properties", Map.of(
						"Invoke", Map.of("type", "object"), "sibling", Map.of("type", "integer"))),
				"normalized device reply", AffordanceValidation.Mode.ON);
		try (var context = new DefaultCamelContext()) {
			Exchange exchange = new DefaultExchange(context);
			Map<String, Object> reply = Map.of("Invoke", Map.of("x", 1L), "sibling", 2L);
			exchange.getMessage().setBody(reply);
			new OperationReplyValidationProcessor("Invoke", validation).process(exchange);
			assertEquals(reply, exchange.getMessage().getBody());
		}
	}

	@Test
	void invalidCorrelatedReplyCarriesStableErrorCodeAndStatus() {
		AffordanceValidation validation = new AffordanceValidation(
				Map.of("type", "object", "required", List.of("state"),
						"properties", Map.of("state", Map.of("type", "string"))), "test output",
					AffordanceValidation.Mode.ON);
		PayloadValidationException failure = assertThrows(PayloadValidationException.class,
				() -> validation.correlatedResponse(Map.of("other", 1)));
		assertEquals(PayloadValidationException.Stage.RESPONSE, failure.getStage());
		assertFalse(failure.getViolations().isEmpty());
	}

	@Test
	void projectionProcessorReplacesReplyWithSourceIdMap() throws Exception {
		try (CamelContext context = new DefaultCamelContext()) {
			Exchange exchange = new DefaultExchange(context);
			exchange.getMessage().setBody(Map.of("State", "READY"));
			new ResponseProjectionProcessor(List.of(
					new ResponseSource("MachineState", null, "$.State"))).process(exchange);
			assertEquals(Map.of("MachineState", "READY"), exchange.getMessage().getBody());
		}
	}

	@Test
	void responseSinkBranchesReceiveTheirOwnLuaValues() throws Exception {
		DefaultCamelContext context = new DefaultCamelContext();
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:state").to("mock:state");
				from("direct:audit").to("mock:audit");
			}
		});
		context.start();
		try {
			MockEndpoint state = context.getEndpoint("mock:state", MockEndpoint.class);
			MockEndpoint audit = context.getEndpoint("mock:audit", MockEndpoint.class);
			state.expectedBodiesReceived("READY");
			audit.expectedMessageCount(1);
			var exchange = new DefaultExchange(context);
			exchange.getMessage().setBody(Map.of("CurrentState", "READY",
					"Audit", Map.of("state", "READY", "code", 7L)));
			try (var processor = new ResponseSinkFanOutProcessor(context, "invocation-1", List.of(
					new ResponseSinkFanOutProcessor.Target("CurrentState", "direct:state", null, null, null, null, false),
					new ResponseSinkFanOutProcessor.Target("Audit", "direct:audit", null, null, null, null, false)))) {
				dispatch(context, processor, processor.prepareJobs(exchange));
				MockEndpoint.assertIsSatisfied(2_000, java.util.concurrent.TimeUnit.MILLISECONDS,
						state, audit);
				assertEquals(Map.of("state", "READY", "code", 7L),
						audit.getExchanges().get(0).getMessage().getBody());
			}
		} finally {
			context.stop();
		}
	}

	@Test
	void callerOperationVariablesUseCallerDeclarationOrder() {
		Map<String, Object> reply = new java.util.LinkedHashMap<>();
		reply.put("State", "READY");
		reply.put("Outcome", "OK");
		reply.put("TimeStamp", "later");
		var variables = OperationPayloads.toOperationVariables(reply,
				Map.of("State", "string", "Outcome", "string"), List.of("Outcome", "State"));
		var first = GSON.fromJson(GSON.toJson(variables.get(0)), Map.class);
		var firstValue = (Map<?, ?>) first.get("value");
		assertEquals("Outcome", firstValue.get("idShort"));
		assertEquals(2, variables.size());
	}

	@Test
	void responseMemberSinksMergeIntoOneValidatedDevicePayload() throws Exception {
		DefaultCamelContext context = new DefaultCamelContext();
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:device").to("mock:device");
			}
		});
		context.start();
		try {
			MockEndpoint device = context.getEndpoint("mock:device", MockEndpoint.class);
			device.expectedMessageCount(1);
			device.expectedMessageCount(1);
			var exchange = new DefaultExchange(context);
			exchange.getMessage().setBody(Map.of("XResult", 1, "YResult", 2));
			Map<String, Object> schema = Map.of("type", "object", "required", List.of("X", "Y"),
					"properties", Map.of("X", Map.of("type", "number"), "Y", Map.of("type", "number")));
			try (var processor = new ResponseSinkFanOutProcessor(context, "invocation-2", List.of(
					new ResponseSinkFanOutProcessor.Target("XResult", "direct:device", "move", "$.X", schema,
							Map.of("type", "number"), true),
					new ResponseSinkFanOutProcessor.Target("YResult", "direct:device", "move", "$.Y", schema,
							Map.of("type", "number"), true)))) {
				dispatch(context, processor, processor.prepareJobs(exchange));
				MockEndpoint.assertIsSatisfied(2_000, java.util.concurrent.TimeUnit.MILLISECONDS, device);
				assertEquals(Map.of("X", 1, "Y", 2), device.getExchanges().get(0).getMessage().getBody());
			}
		} finally {
			context.stop();
		}
	}

	@Test
	void missingMappedMemberRejectsResponseGroupEvenWithoutSchemaRequirements() throws Exception {
		DefaultCamelContext context = new DefaultCamelContext();
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:device").to("mock:device");
			}
		});
		context.start();
		try {
			MockEndpoint device = context.getEndpoint("mock:device", MockEndpoint.class);
			device.expectedMessageCount(0);
			var exchange = new DefaultExchange(context);
			exchange.getMessage().setBody(Map.of("XResult", 1));
			try (var processor = new ResponseSinkFanOutProcessor(context, "invocation-3", List.of(
					new ResponseSinkFanOutProcessor.Target("XResult", "direct:device", "move", "$.X", null, null, false),
					new ResponseSinkFanOutProcessor.Target("YResult", "direct:device", "move", "$.Y", null, null, false)))) {
				dispatch(context, processor, processor.prepareJobs(exchange));
				MockEndpoint.assertIsSatisfied(2_000, java.util.concurrent.TimeUnit.MILLISECONDS, device);
			}
		} finally {
			context.stop();
		}
	}

	@Test
	void responseJsonRetainsNullMembersAndExactIntegralNumbers() throws Exception {
		DefaultCamelContext context = new DefaultCamelContext();
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:device").to("mock:device");
			}
		});
		context.start();
		try {
			MockEndpoint device = context.getEndpoint("mock:device", MockEndpoint.class);
			device.expectedMessageCount(1);
			var exchange = new DefaultExchange(context);
			exchange.getMessage().setBody(Map.of("Payload", Map.of(
					"count", 2147483648L, "optional", JsonPointer.JSON_NULL)));
			try (var processor = new ResponseSinkFanOutProcessor(context, "invocation-4", List.of(
					new ResponseSinkFanOutProcessor.Target("Payload", "direct:device", null, null, null, null, false)))) {
				dispatch(context, processor, processor.prepareJobs(exchange));
				MockEndpoint.assertIsSatisfied(2_000, java.util.concurrent.TimeUnit.MILLISECONDS, device);
				assertEquals(dk.aau.smartproductionlab.dmp.core.util.JsonCodec.parseJson(
						"{\"count\":2147483648,\"optional\":null}"),
						device.getExchanges().get(0).getMessage().getBody());
			}
		} finally {
			context.stop();
		}
	}

	/** Sends the jobs through the processor's detached response-write route steps (Wire Tap, write route). */
	private static void dispatch(org.apache.camel.CamelContext context, ResponseSinkFanOutProcessor processor,
			java.util.List<ResponseSinkFanOutProcessor.DispatchJob> jobs) throws Exception {
		String id = "response-writes-" + System.identityHashCode(processor);
		if (context.getRoute(id + "-dispatch") == null) context.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				processor.configureDispatch(from("direct:" + id + "-dispatch").routeId(id + "-dispatch"), "direct:" + id + "-write");
				processor.configureWrite(from("direct:" + id + "-write").routeId(id + "-write"));
			}
		});
		try (var template = context.createProducerTemplate()) {
			template.sendBody("direct:" + id + "-dispatch", jobs);
		}
	}
}
