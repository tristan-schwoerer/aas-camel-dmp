package dk.aau.smartproductionlab.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import org.junit.jupiter.api.Test;

/**
 * The reply leg must honour the declared types exactly as the request leg does.
 *
 * <p>The request leg already stringifies a correlation Uuid before it reaches
 * the device (native stations validate {@code Uuid} against a string schema),
 * so the reply leg has to agree: a device echoing {@code "1"} for a
 * {@code xs:string} Uuid must get {@code "1"} back, not {@code 1.0}.
 */
class OperationReplyProcessorTest {

	/** Field order carries no meaning in a JSON reply, so compare structure. */
	private static void assertJson(String expected, Object actual) {
		assertEquals(JsonCodec.parseJson(expected), envelope(actual));
	}

	/** The reply as the caller receives it: OperationVariables in AAS JSON, any other value as JSON. */
	private static Object envelope(Object body) {
		return JsonCodec.parseJson(OperationPayloads.encodeReply(body));
	}

	private static Exchange exchangeWith(Object body) {
		DefaultCamelContext context = new DefaultCamelContext();
		Exchange exchange = new DefaultExchange(context);
		exchange.getMessage().setBody(body);
		return exchange;
	}

	/** The ResponseSink selecting the Operation: its SinkId keys the caller's outputs. */
	private static final String OUTPUTS = "Outputs";

	/** A response mapping whose Operation declares {@code types} as its outputs, in that order. */
	private static OperationContext context(Map<String, String> types) {
		return OperationContext.builder("Occupy", "mock:cmd")
				.responseLuaUri("res.lua")
				.callerResponseTypes(types)
				.callerOutputOrder(List.copyOf(types.keySet()))
				.callerInputSourcePaths(Map.of("Occupy", "$"))
				.callerReplySinkId(OUTPUTS)
				.commandSinkIds(List.of("Occupy"))
				.explicitResponseMapping(true)
				.build();
	}

	/** The response Lua's result: the outputs under the Operation's SinkId. */
	private static Map<String, Object> outputs(Map<String, ?> outputs) {
		Map<String, Object> result = new LinkedHashMap<>();
		result.put(OUTPUTS, outputs);
		return result;
	}

	@Test
	void aNumericLookingUuidComesBackAsAString() {
		// what the station actually sends back after a stringified Uuid
		Exchange exchange = exchangeWith(outputs(Map.of("State", "SUCCESS", "Uuid", "1",
				"TimeStamp", "2026-09-30T07:43:32.365Z")));

		Map<String, String> types = new LinkedHashMap<>();
		types.put("Uuid", "xs:string");
		types.put("State", "xs:string");
		new OperationReplyProcessor(context(types)).process(exchange);

		assertJson("{\"State\":\"SUCCESS\",\"Uuid\":\"1\"}", outputs(exchange));
		assertEquals("1", String.valueOf(outputs(exchange).get("Uuid")),
				"a numeric-looking string Uuid must not come back as a double");
	}

	@Test
	void anIntegralNumberStaysIntegral() {
		Exchange exchange = exchangeWith(outputs(Map.of("count", 858383)));

		new OperationReplyProcessor(context(Map.of("count", "integer"))).process(exchange);

		assertEquals(858383L, ((Number) outputs(exchange).get("count")).longValue(),
				"an integral count must not become 858383.0");
	}

	@Test
	void aGenuineFractionKeepsItsFractionalForm() {
		Exchange exchange = exchangeWith(outputs(Map.of("v", 103.5585973)));

		new OperationReplyProcessor(context(Map.of("v", "number"))).process(exchange);

		assertEquals(103.5585973, ((Number) outputs(exchange).get("v")).doubleValue());
	}

	@Test
	void wholeCommandPayloadIsNotUnwrappedByItsOwnMemberName() {
		Map<String, Object> command = OperationPayloads.assembleCommand(
				Map.of("Expected", Map.of("Expected", "inner", "sibling", 1L)),
				List.of("Expected"), Map.of());
		assertEquals(Map.of("Expected", "inner", "sibling", 1L), command);
	}

	@Test
	void withoutAResponseMappingTheDeviceReplyIsNeverMatchedByName() {
		// The reply has exactly the declared output names; it still is no caller output.
		OperationContext ctx = OperationContext.builder("Occupy", "mock:cmd")
				.callerResponseTypes(Map.of("Uuid", "xs:string", "State", "xs:string"))
				.callerOutputOrder(List.of("Uuid", "State"))
				.callerInputSourcePaths(Map.of("Occupy", "$"))
				.commandSinkIds(List.of("Occupy"))
				.build();
		Exchange exchange = exchangeWith(Map.of("Uuid", "1", "State", "SUCCESS"));
		new OperationReplyProcessor(ctx).process(exchange);
		assertEquals(List.of(), exchange.getMessage().getBody());
		assertEquals(200, exchange.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
	}

	@Test
	void anInvokeOnlyOperationAnswersWithNoOutputs() {
		// A response mapping that only fans out (no ResponseSink selects the Operation).
		OperationContext ctx = OperationContext.builder("Halt", "mock:cmd")
				.responseLuaUri("res.lua")
				.callerInputSourcePaths(Map.of("Halt", "$"))
				.commandSinkIds(List.of("Halt"))
				.explicitResponseMapping(true)
				.build();
		Exchange exchange = exchangeWith(Map.of("LastHalt", "SUCCESS"));
		new OperationReplyProcessor(ctx).process(exchange);
		assertEquals(List.of(), exchange.getMessage().getBody());
	}

	@Test
	void callerOutputsAreOnlyTheDeclaredVariables() {
		Exchange exchange = exchangeWith(Map.of(OUTPUTS, Map.of("State", "SUCCESS", "TimeStamp", "t"),
				"Audit", "a ResponseSink value, not a caller output"));
		new OperationReplyProcessor(context(Map.of("State", "xs:string"))).process(exchange);
		assertEquals(Map.of("State", "SUCCESS"), outputs(exchange));
	}

	@Test
	void multipleResponseSinksAssembleOneCallerOperationReply() {
		OperationContext context = OperationContext.builder("Occupy", "mock:cmd")
				.responseLuaUri("res.lua")
				.callerOutputOrder(List.of("Position"))
				.callerInputSourcePaths(Map.of("Occupy", "$"))
				.callerReplyBindings(Map.of("x-sink", "$.Position.x", "y-sink", "$.Position.y"))
				.commandSinkIds(List.of("Occupy"))
				.explicitResponseMapping(true)
				.build();
		Exchange exchange = exchangeWith(Map.of("x-sink", 1L, "y-sink", 2L));
		new OperationReplyProcessor(context).process(exchange);
		assertEquals("{Position={x=1, y=2}}", outputs(exchange).toString(), "both sinks fill the one Position output");
	}

	@Test
	void variableResponseSinkOfAnOptionalOutputMayBeOmitted() {
		Map<String, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement> outputs = new java.util.LinkedHashMap<>();
		outputs.put("State", OptionalQualifierTest.property("State", null));
		outputs.put("Outcome", OptionalQualifierTest.property("Outcome", "true"));
		OperationContext context = OperationContext.builder("Occupy", "mock:cmd")
				.responseLuaUri("res.lua")
				.callerOutputOrder(List.copyOf(outputs.keySet()))
				.callerOutputTemplates(outputs)
				.callerInputSourcePaths(Map.of("Occupy", "$"))
				.callerReplyBindings(Map.of("state-sink", "$.State", "outcome-sink", "$.Outcome"))
				.commandSinkIds(List.of("Occupy"))
				.explicitResponseMapping(true)
				.build();
		Exchange answered = exchangeWith(Map.of("state-sink", "SUCCESS"));
		new OperationReplyProcessor(context).process(answered);
		assertEquals(Map.of("State", "SUCCESS"), outputs(answered));
		OperationValidationException missing = assertThrows(OperationValidationException.class,
				() -> new OperationReplyProcessor(context).process(exchangeWith(Map.of("outcome-sink", true))));
		assertTrue(missing.getMessage().contains("State"), missing.getMessage());
	}

	/** A response mapping whose Operation declares the output templates {@code outputs}. */
	private static OperationContext withTemplates(Map<String, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement> outputs) {
		return OperationContext.builder("Occupy", "mock:cmd")
				.responseLuaUri("res.lua")
				.callerOutputOrder(List.copyOf(outputs.keySet()))
				.callerOutputTemplates(outputs)
				.callerInputSourcePaths(Map.of("Occupy", "$"))
				.callerReplySinkId(OUTPUTS)
				.commandSinkIds(List.of("Occupy"))
				.explicitResponseMapping(true)
				.build();
	}

	@Test
	void everyOutputNotMarkedOptionalMustBeAnswered() {
		Map<String, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement> outputs = new LinkedHashMap<>();
		outputs.put("State", OptionalQualifierTest.property("State", null));
		outputs.put("Outcome", OptionalQualifierTest.property("Outcome", "true"));
		Exchange answered = exchangeWith(outputs(Map.of("State", "SUCCESS")));
		new OperationReplyProcessor(withTemplates(outputs)).process(answered);
		assertEquals(Map.of("State", "SUCCESS"), outputs(answered), "the optional Outcome may be absent");

		// A Lua typo leaves out the required State: that fails instead of answering without it.
		OperationValidationException failure = assertThrows(OperationValidationException.class,
				() -> new OperationReplyProcessor(withTemplates(outputs))
						.process(exchangeWith(outputs(Map.of("Stat", "SUCCESS", "Outcome", 0L)))));
		assertEquals("DMP_RESPONSE_MAPPING_FAILED", failure.getCode());
	}

	@Test
	void callerOutputsMustBeAnObjectKeyedByOutputIdShort() {
		for (Object value : java.util.Arrays.asList("SUCCESS", List.of("SUCCESS"), null)) {
			Map<String, Object> result = new LinkedHashMap<>();
			result.put(OUTPUTS, value);
			OperationValidationException failure = assertThrows(OperationValidationException.class,
					() -> new OperationReplyProcessor(context(Map.of("State", "xs:string")))
							.process(exchangeWith(result)));
			assertEquals("DMP_RESPONSE_MAPPING_FAILED", failure.getCode());
		}
	}

	@Test
	void aMissingOperationSinkIdFails() {
		OperationValidationException failure = assertThrows(OperationValidationException.class,
				() -> new OperationReplyProcessor(context(Map.of("State", "xs:string")))
						.process(exchangeWith(Map.of("Audit", "ignored"))));
		assertEquals("DMP_RESPONSE_MAPPING_FAILED", failure.getCode());
	}

	@Test
	void basyxUuidInoutputPrecedesOutputsAndKeepsLexicalStringAndNull() {
		Map<String, Object> reply = new LinkedHashMap<>();
		reply.put("State", null);
		reply.put("Uuid", "00000000-0000-0000-0000-000000000001");
		Map<String, String> types = new LinkedHashMap<>();
		types.put("Uuid", "xs:string");
		types.put("State", "xs:string");
		Exchange exchange = exchangeWith(outputs(reply));
		new OperationReplyProcessor(context(types)).process(exchange);
		assertJson("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Uuid\","
				+ "\"valueType\":\"xs:string\",\"value\":\"00000000-0000-0000-0000-000000000001\"}},"
				+ "{\"value\":{\"modelType\":\"Property\",\"idShort\":\"State\","
				+ "\"valueType\":\"xs:string\"}}]",
				exchange.getMessage().getBody());
	}

	@Test
	void declaredOutputTypesStillApply() {
		Exchange exchange = exchangeWith(outputs(Map.of("State", 1)));

		new OperationReplyProcessor(context(Map.of("State", "string"))).process(exchange);

		assertEquals(Map.of("State", "1"), outputs(exchange),
				"a declared xs:string output must be stringified in the reply too");
	}

	@Test
	void theSharedCodecKeepsTheNumericContractInOnePlace() {
		assertEquals("{\"a\":1}", dk.aau.smartproductionlab.dmp.core.util
				.JsonCodec.toJson(dk.aau.smartproductionlab.dmp.core.util
						.JsonCodec.parseJson("{\"a\":1}")));
		assertEquals(List.of(1L, 2L), dk.aau.smartproductionlab.dmp.core.util
				.JsonCodec.parseJson("{\"a\":[1,2]}") instanceof Map
						? ((Map<?, ?>) dk.aau.smartproductionlab.dmp.core.util
								.JsonCodec.parseJson("{\"a\":[1,2]}")).get("a")
						: null);
	}

	private static OperationContext separateTypes(Map<String, String> callerTypes, List<String> order) {
		return OperationContext.builder("Invoke", "mock:cmd")
				.responseLuaUri("res.lua")
				.responseTypes(Map.of("Count", "integer", "DeviceFlag", "string"))
				.callerResponseTypes(callerTypes)
				.callerOutputOrder(order)
				.callerInputSourcePaths(Map.of("Request", "$"))
				.callerReplySinkId("Reply")
				.commandSinkIds(List.of("Command"))
				.explicitResponseMapping(true)
				.build();
	}

	@Test
	void callerTypesAndOrderAreIndependentOfSameNameAndDifferentlyNamedDeviceOutputs() {
		Map<String, String> callerTypes = Map.of("Count", "string", "Display", "number", "Flag", "boolean");
		OperationContext ctx = separateTypes(callerTypes, List.of("Flag", "Count", "Display"));
		Exchange exchange = exchangeWith(Map.of("Reply",
				Map.of("Count", "001", "Display", "1.5", "Flag", "true", "DeviceFlag", "not caller output")));
		new OperationReplyProcessor(ctx).process(exchange);
		assertEquals(List.of(
				Map.of("value", Map.of("modelType", "Property", "idShort", "Flag", "valueType", "xs:boolean", "value", "true")),
				Map.of("value", Map.of("modelType", "Property", "idShort", "Count", "valueType", "xs:string", "value", "001")),
				Map.of("value", Map.of("modelType", "Property", "idShort", "Display", "valueType", "xs:double", "value", "1.5"))),
				envelope(exchange.getMessage().getBody()));
		assertEquals(Map.of("Count", "integer", "DeviceFlag", "string"), ctx.responseTypes);
		assertEquals(callerTypes, ctx.callerResponseTypes);
	}

	@Test
	void explicitEmptyCallerTypeMapDoesNotFallBackToDeviceTypes() {
		Exchange exchange = exchangeWith(Map.of("Reply", Map.of("Count", "001")));
		new OperationReplyProcessor(separateTypes(Map.of(), List.of("Count"))).process(exchange);
		assertEquals(Map.of("Count", "001"), outputs(exchange));
	}

	/** The reply's output and InOut variables, decoded as BaSyx would: idShort to typed value. */
	static Map<String, Object> outputs(Exchange exchange) {
		return OperationPayloads.flattenOperationVariables(OperationPayloads.encodeReply(exchange.getMessage().getBody()));
	}

}
