package dk.aau.smartproductionlab.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

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
	void directSameNameOnlyPreservesItsObjectLevelThroughLua() throws Exception {
		assertRequestThroughLua(false, false);
	}

	@Test
	void directSameNameAndSiblingPreserveTheirObjectLevelsThroughLua() throws Exception {
		assertRequestThroughLua(false, true);
	}

	@Test
	void basyxSameNameOnlyPreservesItsObjectLevelThroughLua() throws Exception {
		assertRequestThroughLua(true, false);
	}

	@Test
	void basyxSameNameAndSiblingPreserveTheirObjectLevelsThroughLua() throws Exception {
		assertRequestThroughLua(true, true);
	}

	private void assertRequestThroughLua(boolean basyx, boolean siblings) throws Exception {
		for (String sourceId : List.of("Invoke", "body", "Body")) {
			Map<String, Object> caller = new LinkedHashMap<>();
			// A BaSyx delegation carries scalar Property values; a plain REST caller may send any JSON tree.
			caller.put(sourceId, basyx ? "1" : Map.of("x", 1L));
			if (siblings) caller.put("sibling", basyx ? "2" : 2L);
			Object request = basyx ? caller.entrySet().stream().map(entry -> Map.of("value", Map.of("modelType", "Property",
					"idShort", entry.getKey(), "valueType", "xs:string", "value", entry.getValue()))).toList() : caller;
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
								exchange -> exchange.getMessage().setBody(JsonCodec.toJson(request)));
						assertNull(result.getException());
						assertEquals(basyx ? true : null, result.getProperty(OperationPayloads.BASYX_DELEGATION_BODY));
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
	void externalScalarArrayAndNullValuesAlsoGetExactlyOneEnvelope() throws Exception {
		try (var camel = new DefaultCamelContext()) {
			for (Object value : java.util.Arrays.asList("READY", "001",
					"[{\"value\":{\"idShort\":\"Invoke\",\"value\":1}}]", List.of(1L, 2L), null)) {
				var exchange = new DefaultExchange(camel);
				exchange.getMessage().setBody(JsonCodec.toJson(value));
				new OperationRequestProcessor(Map.of(), Map.of("Invoke", "$")).process(exchange);
				Map<String, Object> expected = new LinkedHashMap<>();
				expected.put("Invoke", value);
				assertEquals(expected, exchange.getMessage().getBody());
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
		assertEquals(true, exchange.getProperty(OperationPayloads.BASYX_DELEGATION_BODY));
	}

	@Test
	void multipleOperationVariableSourcesProjectFromOneCallerRequest() throws Exception {
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody("{\"Position\":{\"x\":1,\"y\":2}}");
		new OperationRequestProcessor(Map.of(), Map.of("source-x", "$.Position.x", "source-y", "$.Position.y"))
				.process(exchange);
		assertEquals(Map.of("source-x", 1L, "source-y", 2L), exchange.getMessage().getBody());
	}

	@Test
	void wholeOperationSourceTakesTheRequestAsItIs() throws Exception {
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody("\"now\"");
		new OperationRequestProcessor(Map.of(), Map.of("Halt", "$")).process(exchange);
		assertEquals(Map.of("Halt", "now"), exchange.getMessage().getBody(),
				"a request that is not an object reaches its whole-Operation source as it is");
	}

	@Test
	void omittedOptionalInputGivesItsSourceNoValue() throws Exception {
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody("{\"Speed\":3}");
		new OperationRequestProcessor(Map.of("Speed", OptionalQualifierTest.property("Speed", null),
						"Ramp", OptionalQualifierTest.property("Ramp", "true")),
				Map.of("speed", "$.Speed", "ramp", "$.Ramp")).process(exchange);
		assertEquals(Map.of("speed", 3L), exchange.getMessage().getBody());
	}
}
