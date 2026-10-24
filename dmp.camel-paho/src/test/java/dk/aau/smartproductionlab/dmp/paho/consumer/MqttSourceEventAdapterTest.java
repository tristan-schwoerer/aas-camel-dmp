package dk.aau.smartproductionlab.dmp.paho.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SourceEventAdapter;
import dk.aau.smartproductionlab.dmp.paho.configuration.MqttDataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;
import org.junit.jupiter.api.Test;

class MqttSourceEventAdapterTest {
	@Test
	void connectorDecoderIsDiscoverableAtRuntime() {
		assertTrue(ServiceLoader.load(SourceEventAdapter.class).stream()
				.map(ServiceLoader.Provider::get)
				.anyMatch(adapter -> "mqtt".equals(adapter.sourceKind())));
	}

	@Test
	void decodesDeclaredJsonPayloadBytesIntoJavaValues() throws Exception {
		var context = new DefaultCamelContext();
		var source = new MqttDataSourceConfiguration();
		source.setSchema(Map.of("type", "object"));
		var exchange = new DefaultExchange(context);
		exchange.getMessage().setBody("{\"count\":7}".getBytes(StandardCharsets.UTF_8));

		new MqttSourceEventAdapter().normalizer(source).process(exchange);

		assertEquals(Map.of("count", 7L), exchange.getMessage().getBody());
		assertTrue(Boolean.TRUE.equals(exchange.getProperty(SourceEventAdapter.NORMALIZED_VALUE_PROPERTY)));
	}

	@Test
	void scalarSchemasDecodeJsonAndAStringTakesBareText() throws Exception {
		var context = new DefaultCamelContext();
		Object[][] cases = { { "number", "21.5", 21.5 }, { "integer", "7", 7L }, { "boolean", "true", true },
				{ "string", "RUNNING", "RUNNING" }, { "string", "\"READY\"", "READY" } };
		for (Object[] example : cases) {
			var source = new MqttDataSourceConfiguration();
			source.setSchema(Map.of("type", example[0]));
			var exchange = new DefaultExchange(context);
			exchange.getMessage().setBody(((String) example[1]).getBytes(StandardCharsets.UTF_8));
			new MqttSourceEventAdapter().normalizer(source).process(exchange);
			assertEquals(example[2], exchange.getMessage().getBody(), example[0] + " " + example[1]);
		}
	}

	@Test
	void preservesOpaqueJsonLookingTextWhenSchemaSaysString() throws Exception {
		var context = new DefaultCamelContext();
		var source = new MqttDataSourceConfiguration();
		source.setSchema(Map.of("type", "string"));
		var exchange = new DefaultExchange(context);
		String text = "{\"opaque\":true}";
		exchange.getMessage().setBody(text);

		new MqttSourceEventAdapter().normalizer(source).process(exchange);

		assertEquals(text, exchange.getMessage().getBody());
		assertInstanceOf(String.class, exchange.getMessage().getBody());
	}

	@Test
	void jsonContentTypeDecodesScalarJsonStringButRetainsItsStringType() throws Exception {
		var context = new DefaultCamelContext();
		var source = new MqttDataSourceConfiguration();
		source.setSchema(Map.of("type", "string"));
		var exchange = new DefaultExchange(context);
		exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
		exchange.getMessage().setBody("\"READY\"");

		new MqttSourceEventAdapter().normalizer(source).process(exchange);

		assertEquals("READY", exchange.getMessage().getBody());
		assertInstanceOf(String.class, exchange.getMessage().getBody());
	}

	@Test
	void declaredJsonNullIsAContributionRatherThanAbsence() throws Exception {
		var context = new DefaultCamelContext();
		var source = new MqttDataSourceConfiguration();
		var exchange = new DefaultExchange(context);
		exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
		exchange.getMessage().setBody("null".getBytes(StandardCharsets.UTF_8));

		new MqttSourceEventAdapter().normalizer(source).process(exchange);

		assertEquals(JsonPointer.JSON_NULL, exchange.getMessage().getBody());
		assertTrue(Boolean.TRUE.equals(exchange.getProperty(SourceEventAdapter.CONTRIBUTION_PROPERTY)));
	}
}
