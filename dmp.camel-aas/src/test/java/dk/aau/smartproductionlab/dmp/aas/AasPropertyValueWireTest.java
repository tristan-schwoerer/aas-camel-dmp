package dk.aau.smartproductionlab.dmp.aas;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

/**
 * A Property's $value goes on the wire as its lexical form in a JSON string, as BaSyx requires, also inside a
 * collection (BaSyx 2.0 answers a JSON number with a 500, checked 2026-10-24).
 */
class AasPropertyValueWireTest {
	private static final String SUBMODEL = "urn:example:sm:Variables";

	@Test
	void scalarsAreWrittenAsJsonStrings() throws Exception {
		List<String> bodies = new CopyOnWriteArrayList<>();
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/", exchange -> {
			bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
			exchange.sendResponseHeaders(204, -1);
			exchange.close();
		});
		server.start();
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			context.addComponent("aas", new AASComponent());
			context.start();
			String base = "aas:http://127.0.0.1:" + server.getAddress().getPort() + "/submodels/"
					+ Base64.getUrlEncoder().withoutPadding().encodeToString(SUBMODEL.getBytes(StandardCharsets.UTF_8));
			AASEndpoint weight = context.getEndpoint(base + "?propertyPath=Weight", AASEndpoint.class);
			for (Object value : new Object[] { 2.9842, 3.0, 7L, 0.0001, true, "IDLE" }) weight.setPropertyValue(value);
			AASEndpoint whole = context.getEndpoint(base + "?propertyPath=Weight&valuePath=Weight", AASEndpoint.class);
			whole.setPropertyValue(Map.of("Weight", 1.5));
			AASEndpoint group = context.getEndpoint(base + "?propertyPath=Location&elementType=SubmodelElementCollection",
					AASEndpoint.class);
			group.setPropertyValue(Map.of("x", 1.5));
		} finally {
			server.stop(0);
		}
		assertEquals(List.of("\"2.9842\"", "\"3\"", "\"7\"", "\"0.0001\"", "\"true\"", "\"IDLE\"", "\"1.5\"",
				"{\"x\":\"1.5\"}"), bodies);
	}

	@Test
	void collectionLeavesAreTextAtEveryDepthAndAPropertyTakesStructureAsJsonText() {
		Map<String, Object> location = new java.util.LinkedHashMap<>();
		location.put("x", 7.5);
		location.put("ok", false);
		location.put("inner", Map.of("n", 3L));
		location.put("names", List.of("a", 2L));
		assertEquals("{\"x\":\"7.5\",\"ok\":\"false\",\"inner\":{\"n\":\"3\"},\"names\":[\"a\",\"2\"]}",
				dk.aau.smartproductionlab.dmp.core.util.JsonCodec.toJson(AASEndpoint.wireValue(location, "SubmodelElementCollection")));
		assertEquals("[1,2]", AASEndpoint.wireValue(List.of(1L, 2L), "Property"));
	}

	@Test
	void aListItemIsAddressedEncodedAndEveryRequestCarriesTheAuthorization() throws Exception {
		List<String> seen = new CopyOnWriteArrayList<>();
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/", exchange -> {
			seen.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getRawPath() + " "
					+ exchange.getRequestHeaders().getFirst("Authorization"));
			byte[] element = "{\"modelType\":\"Property\",\"valueType\":\"xs:int\",\"value\":\"4\"}".getBytes(StandardCharsets.UTF_8);
			if (exchange.getRequestMethod().equals("GET")) {
				exchange.sendResponseHeaders(200, element.length);
				exchange.getResponseBody().write(element);
			} else {
				exchange.getRequestBody().readAllBytes();
				exchange.sendResponseHeaders(204, -1);
			}
			exchange.close();
		});
		server.start();
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			context.addComponent("aas", new AASComponent());
			context.start();
			dk.aau.smartproductionlab.dmp.aas.http.AasHttp.of(context).authorize("Bearer secret");
			String uri = "aas:http://127.0.0.1:" + server.getAddress().getPort() + "/submodels/sm?propertyPath=Station.Items[0]";
			context.getEndpoint(uri, AASEndpoint.class).setPropertyValue(5L);
			var consumer = new AASPollingConsumer(context.getEndpoint(uri, AASEndpoint.class));
			consumer.start();
			assertEquals(4L, consumer.receive(2000).getMessage().getBody());
			consumer.stop();
		} finally {
			server.stop(0);
		}
		assertEquals(List.of("PATCH /submodels/sm/submodel-elements/Station.Items%5B0%5D/$value Bearer secret",
				"GET /submodels/sm/submodel-elements/Station.Items%5B0%5D Bearer secret"), seen);
	}
}
