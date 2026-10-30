package dk.aau.smartproductionlab.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.lua.configuration.LuaBinding;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource;
import org.junit.jupiter.api.Test;

class OperationRequestProcessorTest {
	@Test
	void sameNameOnlyPreservesItsObjectLevelThroughLua() throws Exception {
		assertRequestThroughLua(false);
	}

	@Test
	void sameNameAndSiblingPreserveTheirObjectLevelsThroughLua() throws Exception {
		assertRequestThroughLua(true);
	}

	private void assertRequestThroughLua(boolean siblings) throws Exception {
		for (String sourceId : List.of("Invoke", "body", "Body")) {
			Map<String, Object> caller = new LinkedHashMap<>();
			caller.put(sourceId, Map.of("x", 1L));
			if (siblings) caller.put("sibling", "2");
			String request = Delegation.body(caller);
			AtomicInteger sends = new AtomicInteger();
			AtomicReference<Object> boundary = new AtomicReference<>();
			AtomicReference<Object> command = new AtomicReference<>();
			try (var camel = new DefaultCamelContext()) {
				var lua = new LuaBinding(LuaCompiledSource.fromBytes(
						("function aimc_main(sources) return { Command = sources['" + sourceId + "'] } end")
								.getBytes(StandardCharsets.UTF_8), "request-collision", 4096));
				lua.bind(camel);
				try {
					camel.addRoutes(new RouteBuilder() {
						@Override public void configure() {
							from("direct:request-boundary").process(new OperationRequestProcessor(Map.of(), Map.of(sourceId, "$")))
									.process(exchange -> boundary.set(JsonCodec.copyValue(exchange.getMessage().getBody())))
									.to(lua.uri())
									.process(exchange -> exchange.getMessage().setBody(OperationPayloads.assembleCommand(
											exchange.getMessage().getBody(), List.of("Command"), Map.of())))
									.to("direct:device");
							from("direct:device").process(exchange -> {
								sends.incrementAndGet();
								command.set(JsonCodec.copyValue(exchange.getMessage().getBody()));
							});
						}
					});
					camel.start();
					try (var producer = camel.createProducerTemplate()) {
						var result = producer.request("direct:request-boundary",
								exchange -> exchange.getMessage().setBody(request));
						assertNull(result.getException());
					}
					assertEquals(Map.of(sourceId, caller), boundary.get(), "external boundary must add exactly one envelope");
					assertEquals(caller, command.get(), "Lua must see the complete caller object, not its same-name member");
					assertEquals(1, sends.get(), "the operation must send exactly once");
				} finally {
					camel.stop();
					lua.close();
				}
			}
		}
	}

	@Test
	void aBodyOtherThanTheDelegatedVariablesIsTheCallersError() throws Exception {
		try (var camel = new DefaultCamelContext()) {
			for (Object value : java.util.Arrays.asList("READY", Map.of("Invoke", 1L), List.of(1L, 2L), null)) {
				var exchange = new DefaultExchange(camel);
				exchange.getMessage().setBody(JsonCodec.toJson(value));
				var failure = assertThrows(OperationValidationException.class,
						() -> new OperationRequestProcessor(Map.of(), Map.of("Invoke", "$")).process(exchange));
				assertEquals(400, failure.getStatus(), String.valueOf(value));
			}
		}
	}

	@Test
	void basyxUuidInoutputRetainsItsExactLexicalValueAtTheRequestBoundary() throws Exception {
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Uuid\",\"valueType\":\"xs:string\",\"value\":\"00000000-0000-0000-0000-000000000001\"}}]");
		new OperationRequestProcessor(Map.of(), Map.of("Invoke", "$")).process(exchange);
		assertEquals(Map.of("Invoke", Map.of("Uuid", "00000000-0000-0000-0000-000000000001")),
				exchange.getMessage().getBody());
	}

	@Test
	void multipleOperationVariableSourcesProjectFromOneCallerRequest() throws Exception {
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody(Delegation.body(Map.of("Position", Map.of("x", 1L, "y", 2L))));
		new OperationRequestProcessor(Map.of(), Map.of("source-x", "$.Position.x", "source-y", "$.Position.y"))
				.process(exchange);
		assertEquals(Map.of("source-x", 1L, "source-y", 2L), exchange.getMessage().getBody());
	}

	@Test
	void wholeOperationSourceTakesAllVariables() throws Exception {
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody(Delegation.body(Map.of("At", "now")));
		new OperationRequestProcessor(Map.of(), Map.of("Halt", "$")).process(exchange);
		assertEquals(Map.of("Halt", Map.of("At", "now")), exchange.getMessage().getBody());
	}

	@Test
	void omittedOptionalInputGivesItsSourceNoValue() throws Exception {
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody(Delegation.body(Map.of("Speed", "3")));
		new OperationRequestProcessor(Map.of("Speed", OptionalQualifierTest.property("Speed", null),
						"Ramp", OptionalQualifierTest.property("Ramp", "true")),
				Map.of("speed", "$.Speed", "ramp", "$.Ramp")).process(exchange);
		assertEquals(Map.of("speed", "3"), exchange.getMessage().getBody());
	}
}
