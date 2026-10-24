package dk.aau.smartproductionlab.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.Response;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.lua.configuration.LuaBinding;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource;
import org.junit.jupiter.api.Test;

class OperationCallerTypeRouteTest {
	@Test
	void directCallerUsesCciTypesAfterDeviceNormalizationAndOneResponseLua() throws Exception {
		assertCallerReply(false);
	}

	@Test
	void basyxCallerUsesCciNamesTypesAndOrderAfterDeviceNormalizationAndOneResponseLua() throws Exception {
		assertCallerReply(true);
	}

	private void assertCallerReply(boolean basyx) throws Exception {
		AtomicInteger sends = new AtomicInteger();
		AtomicInteger responseTransforms = new AtomicInteger();
		try (var camel = new DefaultCamelContext()) {
			var lua = new LuaBinding(LuaCompiledSource.fromBytes((
					"function aimc_main(sources) "
					+ "assert(type(sources.Device.Count) == 'number') "
					+ "return { Reply = { Count = string.format('%03d', sources.Device.Count), "
					+ "Display = sources.Device.Count / 2, Flag = sources.Device.DeviceFlag == 'true' } } end")
					.getBytes(StandardCharsets.UTF_8), "caller-types", 4096));
			lua.bind(camel);
			try {
				camel.addRoutes(new RouteBuilder() {
					@Override public void configure() throws Exception {
						int port;
						try (var probe = new java.net.ServerSocket(0)) { port = probe.getLocalPort(); }
						restConfiguration().component("jetty").host("localhost").port(port).inlineRoutes(false);
						var command = new OperationRoute.Command(new Sink("command", null, Endpoint.of("direct:types-device")),
								List.of(new OperationRoute.Command.Member("Command", null)), false, Map.of(), List.of(),
								Map.of("Count", "integer", "DeviceFlag", "string"), List.of(), null, Map.of());
						var caller = new OperationRoute.Caller(Map.of("Request", "$"), Map.of(), Map.of(),
								Map.of("Count", "xs:string", "Display", "xs:double", "Flag", "xs:boolean"),
								List.of("Flag", "Count", "Display"), Map.of(), "Reply");
						new OperationRoute("caller-types", new OperationRoute.Identity("/caller-types", "Invoke", null, null),
								Endpoint.of("direct:types-request"), List.of(), command, caller,
								new Response(Endpoint.of("direct:types-response"),
										List.of(new Response.ResponseSource("Device", null, null)), List.of())).build(this);
						from("direct:types-request").setBody().constant(Map.of("Command", Map.of("argument", 1L)));
						from("direct:types-device").process(exchange -> {
							sends.incrementAndGet();
							// Simulated connector reply already normalized to the AID's device types.
							exchange.getMessage().setBody(Map.of("Count", 7L, "DeviceFlag", "true"));
						});
						from("direct:types-response").process(exchange -> responseTransforms.incrementAndGet())
								.to(lua.uri());
					}
				});
				camel.start();
				try (var producer = camel.createProducerTemplate()) {
					var result = producer.request("direct:caller-types", exchange -> exchange.getMessage().setBody(
							basyx ? "[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"argument\",\"valueType\":\"xs:int\",\"value\":\"1\"}}]" : "{\"argument\":1}"));
					assertNull(result.getException());
					assertEquals(200, result.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
					Object expected = basyx ? List.of(
							Map.of("value", Map.of("modelType", "Property", "idShort", "Flag", "valueType", "xs:boolean", "value", "true")),
							Map.of("value", Map.of("modelType", "Property", "idShort", "Count", "valueType", "xs:string", "value", "007")),
							Map.of("value", Map.of("modelType", "Property", "idShort", "Display", "valueType", "xs:double", "value", "3.5")))
							: Map.of("Count", "007", "Display", 3.5, "Flag", true);
					assertEquals(expected, JsonCodec.parseJson(result.getMessage().getBody(String.class)));
				}
				assertEquals(1, sends.get());
				assertEquals(1, responseTransforms.get());
			} finally {
				camel.stop();
				lua.close();
			}
		}
	}
}
