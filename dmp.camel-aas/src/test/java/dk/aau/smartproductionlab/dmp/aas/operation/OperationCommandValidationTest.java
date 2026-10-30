package dk.aau.smartproductionlab.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.AffordanceValidation;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.PayloadAssembler;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.PayloadValidationException;
import org.junit.jupiter.api.Test;

class OperationCommandValidationTest {
	@Test
	void commandPayloadValidationIsProtocolNeutral() {
		var schema = Map.of("type", "object", "required", List.of("Uuid", "Mode"),
				"properties", Map.of("Uuid", Map.of("type", "string"),
						"Mode", Map.of("type", "string")));
		var guard = new AffordanceValidation(schema, "declared action input", AffordanceValidation.Mode.ON);
		assertThrows(PayloadValidationException.class, () -> guard.outgoing(Map.of("Uuid", "a")));
		guard.outgoing(Map.of("Uuid", "a", "Mode", "RUN"));
	}

	@Test
	void memberContributionsAssembleOneNamedDevicePayload() {
		Object payload = PayloadAssembler.assemble(Map.of("First", "unit-7", "Second", 2),
				List.of("First", "Second"), Map.of("First", "$.input.Uuid", "Second", "$.input.Speed"));
		assertEquals(Map.of("input", Map.of("Uuid", "unit-7", "Speed", 2)), payload);
	}

	@Test
	void actionAssemblyRejectsMissingAuthoredSinkIds() {
		assertThrows(IllegalArgumentException.class, () -> OperationPayloads.assembleCommand(
				Map.of("Other", Map.of("x", 1)), List.of("Expected"),
				Map.of("Expected", "$.X")));
	}

	@Test
	void actionAssemblyUsesExactlyTheAuthoredSinkValue() {
		assertEquals(Map.of("x", 1), OperationPayloads.assembleCommand(
				Map.of("Expected", Map.of("x", 1), "Other", Map.of("wrong", true)),
				List.of("Expected"), Map.of("Expected", "$")));
	}

	@Test
	void actionWholeAndMemberOverlapMergesWithoutUnwrappingPayloadAgain() {
		Map<String, Object> whole = Map.of("sinkId", Map.of("x", 1), "sibling", "keep");
		assertEquals(whole, OperationPayloads.assembleCommand(Map.of("sinkId", whole),
				List.of("sinkId"), Map.of()));
		assertEquals(whole, OperationPayloads.assembleCommand(Map.of("sinkId", whole),
				List.of("sinkId"), Map.of("sinkId", "$")));
		assertEquals(whole, OperationPayloads.assembleCommand(Map.of("sinkId", whole, "Member", 1),
				List.of("sinkId", "Member"), Map.of("Member", "$.sinkId.x")));
		assertThrows(IllegalArgumentException.class, () -> OperationPayloads.assembleCommand(
				Map.of("sinkId", whole, "Member", 2), List.of("sinkId", "Member"),
				Map.of("Member", "$.sinkId.x")));
	}

	@Test
	void actionArrayMembersUseTheSharedPresenceAwareAssembler() {
		assertEquals(Map.of("items", List.of("first", "second")), OperationPayloads.assembleCommand(
				Map.of("First", "first", "Second", "second"), List.of("Second", "First"),
				Map.of("First", "$.items[0]", "Second", "$.items[1]")));
		assertThrows(IllegalArgumentException.class, () -> OperationPayloads.assembleCommand(
				Map.of("First", "first"), List.of("First", "Second"),
				Map.of("First", "$.items[0]", "Second", "$.items[1]")));
	}

	@Test
	void actionSinkIdsAreExactStringsNotStringifiedForeignKeys() {
		assertThrows(IllegalArgumentException.class, () -> OperationPayloads.assembleCommand(
				Map.of(1, Map.of("x", 1)), List.of("1"), Map.of()));
	}

	@Test
	void invalidActionGroupFailsBeforeAnyDeviceInvocation() throws Exception {
		DefaultCamelContext camel = new DefaultCamelContext();
		AtomicInteger invocations = new AtomicInteger();
		AtomicReference<Object> command = new AtomicReference<>();
		camel.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				from("direct:group-device").process(exchange -> {
					invocations.incrementAndGet();
					command.set(exchange.getMessage().getBody());
					exchange.getMessage().setBody(Map.of("Done", true));
				});
			}
		});
		camel.start();
		try {
			OperationContext context = OperationContext.builder("Move", "direct:group-device")
				.callerInputSourcePaths(Map.of("Invoke", "$"))
				.callerReplySinkId("Reply")
				.commandSinkIds(List.of("Whole", "Member"))
				.commandSinkValuePaths(Map.of("Member", "$.member"))
				.build();
			try (SyncOperationProcessor processor = new SyncOperationProcessor(camel, context,
					new AffordanceValidation(null, "command"), new AffordanceValidation(null, "reply"),
					List.of(), List.of())) {
				commandRoute(camel, processor, List.of());
				var template = camel.createProducerTemplate();
				assertThrows(Exception.class, () -> template.requestBody("direct:command",
						Map.of("Whole", Map.of("member", "a"))));
				assertThrows(Exception.class, () -> template.requestBody("direct:command",
						Map.of("Whole", Map.of("member", "a"), "Member", "b")));
				assertEquals(0, invocations.get());
				template.requestBody("direct:command", Map.of("Whole", Map.of("member", "a"), "Member", "a"));
				assertEquals(1, invocations.get());
				assertEquals(Map.of("member", "a"), command.get());
			}
		} finally {
			camel.stop();
		}
	}

	@Test
	void requestValueIsKeyedByTheAuthoredOperationSourceId() throws Exception {
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody(Delegation.body(Map.of("Position", Map.of("x", 1L))));
		new OperationRequestProcessor(Map.of(), Map.of("Invoke", "$")).process(exchange);
		Map<?, ?> actual = exchange.getMessage().getBody(Map.class);
		assertEquals(Map.of("Position", Map.of("x", 1L)), actual.get("Invoke"));
	}

	@Test
	void actionRunsRequestTransformersInOrderBeforeOneInvocation() throws Exception {
		DefaultCamelContext camel = new DefaultCamelContext();
		AtomicReference<Object> command = new AtomicReference<>();
		AtomicInteger requestTransformations = new AtomicInteger();
		AtomicInteger invocations = new AtomicInteger();
		camel.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:request-one").process(exchange -> requestTransformations.incrementAndGet()).setBody()
						.constant(Map.of("First", Map.of("v", 1), "Second", Map.of("v", 2)));
				from("direct:request-two").process(exchange -> requestTransformations.incrementAndGet());
				from("direct:device").process(exchange -> {
					invocations.incrementAndGet();
					command.set(exchange.getMessage().getBody());
					exchange.getMessage().setBody(Map.of("Done", true));
				});
			}
		});
		camel.start();
		try {
			OperationContext context = OperationContext.builder("Move", "direct:device")
				.responseTypes(Map.of("Done", "boolean"))
				.callerResponseTypes(Map.of("Done", "boolean"))
				.callerOutputOrder(List.of("Done"))
				.callerInputSourcePaths(Map.of("Invoke", "$"))
				.callerReplySinkId("Invoke")
				.commandSinkIds(List.of("First", "Second"))
				.commandSinkValuePaths(Map.of("First", "$.X", "Second", "$.Y"))
				.build();
			var exchange = new DefaultExchange(camel);
			exchange.getMessage().setBody("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Invoke\",\"valueType\":\"xs:string\",\"value\":\"\"}}]");
			try (var processor = new SyncOperationProcessor(camel, context,
					new AffordanceValidation(null, "command"),
					new AffordanceValidation(null, "reply"), List.of(), List.of())) {
				operationRoute(camel, processor, List.of("direct:request-one", "direct:request-two"));
				camel.createProducerTemplate().send("direct:operation", exchange);
			}

			assertEquals(Map.of("X", Map.of("v", 1), "Y", Map.of("v", 2)), command.get());
			assertEquals(2, requestTransformations.get(), "every request transformer must run in order");
			assertEquals(1, invocations.get(), "grouped member sinks must make one device invocation");
			assertEquals(true, exchange.getMessage().getBody() instanceof List<?>,
					"BaSyx OperationVariable[] callers must receive the declared array envelope");
		} finally {
			camel.stop();
		}
	}

	/** The bridge's command steps: request result → command → device → accepted reply. */
	private static void commandRoute(DefaultCamelContext camel, SyncOperationProcessor processor,
			List<String> requestTransformers) throws Exception {
		camel.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				var route = from("direct:command").process(processor::keepRequestResult).process(processor::prepareCommand);
				processor.configureInvoke(route);
				route.process(processor::acceptReply);
			}
		});
	}

	/** The bridge's one-shot operation flow, as ProtocolBridgeRouteCreator wires it. */
	private static void operationRoute(DefaultCamelContext camel, SyncOperationProcessor processor,
			List<String> requestTransformers) throws Exception {
		camel.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				var route = from("direct:operation").process(processor::prepareRequest);
				for (String transformer : requestTransformers) route.to(transformer);
				route.process(processor::keepRequestResult);
				route.process(processor::prepareCommand);
				processor.configureInvoke(route);
				route.process(processor::acceptReply)
						.process(processor::collectResponseSources)
						.process(processor::prepareResponseWrites)
						.process(processor::completeReply);
			}
		});
	}
}
