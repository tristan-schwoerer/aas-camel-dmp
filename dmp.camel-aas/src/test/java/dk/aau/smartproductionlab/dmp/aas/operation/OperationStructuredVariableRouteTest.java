package dk.aau.smartproductionlab.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements;
import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementList;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultOperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultProperty;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementList;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.Response;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.lua.configuration.LuaBinding;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource;
import org.junit.jupiter.api.Test;

/** Operation variables declared as collections and lists, nested, in all three categories. */
class OperationStructuredVariableRouteTest {

	private static Property property(String idShort, String valueType) {
		return new DefaultProperty.Builder().idShort(idShort).valueType(Aas.xsd(valueType)).build();
	}

	private static SubmodelElementCollection collection(String idShort, SubmodelElement... children) {
		return new DefaultSubmodelElementCollection.Builder().idShort(idShort).value(List.of(children)).build();
	}

	private static SubmodelElementList intList(String idShort) {
		return new DefaultSubmodelElementList.Builder().idShort(idShort).typeValueListElement(AasSubmodelElements.PROPERTY)
				.valueTypeListElement(Aas.xsd("xs:int")).build();
	}

	// Input: Target {x, y} and Codes [int]; InOut: Plan [{id, speed}]; output: Result {ok, codes [int]}.
	private static final SubmodelElement TARGET = collection("Target", property("x", "xs:double"), property("y", "xs:double"));
	private static final SubmodelElement CODES = intList("Codes");
	private static final SubmodelElement PLAN = new DefaultSubmodelElementList.Builder().idShort("Plan")
			.typeValueListElement(AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION)
			.value(List.of(collection(null, property("id", "xs:string"), property("speed", "xs:int")))).build();
	private static final SubmodelElement RESULT = collection("Result", property("ok", "xs:boolean"), intList("codes"));

	private static final String REQUEST_LUA = "function aimc_main(sources) local r = sources.Request "
			+ "assert(r.Target.x == 1.5 and r.Target.y == 2, 'typed collection members') "
			+ "assert(#r.Codes == 2 and r.Codes[2] == 4, 'typed Property list') "
			+ "assert(r.Plan[1].id == 'a' and r.Plan[1].speed == 5, 'typed list of collections') "
			+ "return { Command = { sum = r.Target.x + r.Target.y, speed = r.Plan[1].speed } } end";

	private static final String RESPONSE_LUA = "function aimc_main(sources) local d = sources.Device "
			+ "return { Reply = { Plan = { { id = 'a', speed = d.speed + 1 } }, "
			+ "Result = { ok = true, codes = { 7, 8 }, undeclared = 1 } } } end";

	@Test
	void basyxCallerSendsAndReceivesDeclaredStructures() throws Exception {
		String request = Aas.writeList(List.of(
				variable(collection("Target", value(property("x", "xs:double"), "1.5"), value(property("y", "xs:double"), "2"))),
				variable(new DefaultSubmodelElementList.Builder().idShort("Codes").typeValueListElement(AasSubmodelElements.PROPERTY)
						.valueTypeListElement(Aas.xsd("xs:int"))
						.value(List.of(value(property(null, "xs:int"), "3"), value(property(null, "xs:int"), "4"))).build()),
				variable(new DefaultSubmodelElementList.Builder().idShort("Plan")
						.typeValueListElement(AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION)
						.value(List.of(collection(null, value(property("id", "xs:string"), "a"),
								value(property("speed", "xs:int"), "5")))).build())));
		AtomicReference<Object> command = new AtomicReference<>();
		String reply = invoke(request, command, RESPONSE_LUA);

		assertEquals(Map.of("sum", 3.5, "speed", 5L), command.get());
		List<OperationVariable> variables = Aas.readList(reply, OperationVariable.class);
		assertEquals(List.of("Plan", "Result"), variables.stream().map(v -> v.getValue().getIdShort()).toList());

		var plan = assertInstanceOf(SubmodelElementList.class, variables.get(0).getValue());
		assertEquals(AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION, plan.getTypeValueListElement());
		var step = assertInstanceOf(SubmodelElementCollection.class, plan.getValue().get(0));
		assertNull(step.getIdShort(), "AASd-120: list members have no idShort");
		assertEquals(Map.of("id", "a", "speed", "6"), values(step));
		assertEquals("xs:int", Aas.xsd(((Property) step.getValue().get(1)).getValueType()));

		var result = assertInstanceOf(SubmodelElementCollection.class, variables.get(1).getValue());
		assertEquals(List.of("ok", "codes"), result.getValue().stream().map(SubmodelElement::getIdShort).toList(),
				"undeclared members are dropped, declared order is kept");
		assertEquals("true", ((Property) result.getValue().get(0)).getValue());
		var codes = assertInstanceOf(SubmodelElementList.class, result.getValue().get(1));
		assertEquals("xs:int", Aas.xsd(codes.getValueTypeListElement()));
		assertEquals(List.of("7", "8"), codes.getValue().stream().map(item -> ((Property) item).getValue()).toList());
	}

	@Test
	void directCallerReceivesTypedNestedJson() throws Exception {
		AtomicReference<Object> command = new AtomicReference<>();
		String reply = invoke("{\"Target\":{\"x\":1.5,\"y\":2},\"Codes\":[3,4],\"Plan\":[{\"id\":\"a\",\"speed\":5}]}",
				command, RESPONSE_LUA);
		assertEquals(Map.of("sum", 3.5, "speed", 5L), command.get());
		assertEquals(Map.of("Plan", List.of(Map.of("id", "a", "speed", 6L)),
				"Result", Map.of("ok", true, "codes", List.of(7L, 8L), "undeclared", 1L)), JsonCodec.parseJson(reply));
	}

	@Test
	void replyThatContradictsTheDeclaredShapeFails() throws Exception {
		String basyx = basyxRequest();
		String scalarForList = "function aimc_main(sources) return { Reply = { Plan = 'not a list' } } end";
		var reply = JsonCodec.parseJson(invoke(basyx, new AtomicReference<>(), scalarForList, 500));
		assertEquals("DMP_RESPONSE_MAPPING_FAILED", ((Map<?, ?>) reply).get("code"));
	}


	private static String basyxRequest() {
		return Aas.writeList(List.of(variable(collection("Target", value(property("x", "xs:double"), "1.5"),
				value(property("y", "xs:double"), "2"))), variable(new DefaultSubmodelElementList.Builder().idShort("Codes")
						.typeValueListElement(AasSubmodelElements.PROPERTY).valueTypeListElement(Aas.xsd("xs:int"))
						.value(List.of(value(property(null, "xs:int"), "3"), value(property(null, "xs:int"), "4"))).build()),
				variable(new DefaultSubmodelElementList.Builder().idShort("Plan")
						.typeValueListElement(AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION)
						.value(List.of(collection(null, value(property("id", "xs:string"), "a"),
								value(property("speed", "xs:int"), "5")))).build())));
	}

	private static OperationVariable variable(SubmodelElement value) {
		return new DefaultOperationVariable.Builder().value(value).build();
	}

	private static Property value(Property property, String value) {
		property.setValue(value);
		return property;
	}

	private static Map<String, String> values(SubmodelElementCollection collection) {
		Map<String, String> out = new java.util.LinkedHashMap<>();
		for (SubmodelElement child : collection.getValue()) out.put(child.getIdShort(), ((Property) child).getValue());
		return out;
	}

	private static String invoke(String body, AtomicReference<Object> command, String responseLua) throws Exception {
		return invoke(body, command, responseLua, 200);
	}

	private static String invoke(String body, AtomicReference<Object> command, String responseLua, int status)
			throws Exception {
		try (var camel = new DefaultCamelContext()) {
			var request = lua(camel, "structured-request", REQUEST_LUA);
			var response = lua(camel, "structured-response", responseLua);
			try {
				camel.addRoutes(new RouteBuilder() {
					@Override public void configure() throws Exception {
						int port;
						try (var probe = new java.net.ServerSocket(0)) { port = probe.getLocalPort(); }
						restConfiguration().component("jetty").host("localhost").port(port).inlineRoutes(false);
						var caller = new OperationRoute.Caller(Map.of("Request", "$"),
								Map.of("Target", Aas.write(TARGET), "Codes", Aas.write(CODES), "Plan", Aas.write(PLAN)),
								Map.of("Plan", Aas.write(PLAN), "Result", Aas.write(RESULT)), Map.of(),
								List.of("Plan", "Result"), Map.of(), "Reply");
						new OperationRoute("structured",
								new OperationRoute.Identity("/structured", "Move", "urn:cci", "Skills.Move.operation"),
								Endpoint.of("direct:structured-request"), List.of(),
								ProtocolBridgeRouteCreatorTest.command("command", "direct:structured-device",
										new OperationRoute.Command.Member("Command", null)),
								caller, new Response(Endpoint.of("direct:structured-response"),
										List.of(new Response.ResponseSource("Device", null, null)), List.of())).build(this);
						from("direct:structured-request").to(request.uri());
						from("direct:structured-device").process(exchange -> {
							command.set(exchange.getMessage().getBody());
							exchange.getMessage().setBody(Map.of("speed", 5L));
						});
						from("direct:structured-response").to(response.uri());
					}
				});
				camel.start();
				try (var producer = camel.createProducerTemplate()) {
					var result = producer.request("direct:structured",
							exchange -> exchange.getMessage().setBody(body));
					assertNull(result.getException());
					assertEquals(status, result.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
					return result.getMessage().getBody(String.class);
				}
			} finally {
				camel.stop();
				request.close();
				response.close();
			}
		}
	}

	private static LuaBinding lua(DefaultCamelContext camel, String id, String source) throws Exception {
		var lua = new LuaBinding(LuaCompiledSource.fromBytes(source.getBytes(StandardCharsets.UTF_8), id, 4096));
		lua.bind(camel);
		return lua;
	}
}
