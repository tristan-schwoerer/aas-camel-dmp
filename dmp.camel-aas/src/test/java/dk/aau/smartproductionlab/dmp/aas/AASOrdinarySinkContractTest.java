package dk.aau.smartproductionlab.dmp.aas;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.aas.configuration.AasElementSink;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.FanOut;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

class AASOrdinarySinkContractTest {
	@Test
	void authoredSinkIdAndConnectorSelectorWriteEachDecodedValueExactlyOnce() throws Exception {
		List<String> writes = new CopyOnWriteArrayList<>();
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/submodels/test/submodel-elements/Value/$value", request -> {
			assertEquals("PATCH", request.getRequestMethod());
			writes.add(new String(request.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
			request.sendResponseHeaders(204, -1);
			request.close();
		});
		server.start();
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			Sink sink = new Sink("internal-sink-id", "AuthoredId", new AasElementSink(
					"http://127.0.0.1:" + server.getAddress().getPort() + "/submodels/test",
					"Value", "Property", "AuthoredId")).withValuePath("AuthoredId");
			context.addRoutes(new RouteBuilder() {
				@Override
				public void configure() {
					FanOut.apply(from("direct:ordinary"), List.of(sink), new String[0],
							new String[] {sink.endpoint().uri(null)}, "ordinary");
				}
			});
			context.start();
			try (var producer = context.createProducerTemplate()) {
				for (Object value : List.of("\"authored quotes\"", Map.of("AuthoredId", 7L, "sibling", 9L))) {
					producer.sendBody("direct:ordinary", Map.of("AuthoredId", value));
					// A Property's value is text: a structured value is written as its JSON text.
					Object text = value instanceof Map<?, ?> ? JsonCodec.toJson(value) : value;
					assertEquals(JsonCodec.toJson(text), writes.get(writes.size() - 1));
				}
				producer.sendBody("direct:ordinary", Collections.singletonMap("AuthoredId", null));
				assertEquals(3, writes.size());
				assertEquals("null", writes.get(2));
			}
		} finally {
			server.stop(0);
		}
	}
}
