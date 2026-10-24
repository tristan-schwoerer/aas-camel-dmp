package dk.aau.smartproductionlab.dmp.opcua.canonical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import dk.aau.smartproductionlab.dmp.opcua.configuration.OpcUaEndpoint;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.junit.jupiter.api.Test;

class OpcUaSourceEventAdapterTest {
	/** A monitored node's source; its endpoint normalizes what the subscription delivers. */
	private static Source source(String node) {
		return new Source("source", "State", OpcUaEndpoint.source("127.0.0.1", 4840, "", node, null, null, Map.of()),
				Source.Delivery.OBSERVED, 0);
	}

	@Test
	void aSampleContributesItsValueOnly() throws Exception {
		Source source = source("ns=2;s=Machine.State");
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody(new DataValue(new Variant("READY")));

		source.endpoint().normalizer(source).process(exchange);

		assertEquals("READY", exchange.getMessage().getBody(), "the value only, as an MQTT or HTTP source delivers it");
		assertTrue(Boolean.TRUE.equals(exchange.getProperty(Source.NORMALIZED_VALUE_PROPERTY)));
		assertTrue(Boolean.TRUE.equals(exchange.getProperty(Source.CONTRIBUTION_PROPERTY)));
	}

	@Test
	void anEventNotificationIsAlreadyItsSelectedFields() throws Exception {
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody(Map.of("ToState", "Halted", "Message", "calibration done"));

		Source source = source("ns=2;s=Machine.Events");
		source.endpoint().normalizer(source).process(exchange);

		assertEquals(Map.of("ToState", "Halted", "Message", "calibration done"), exchange.getMessage().getBody());
		assertTrue(Boolean.TRUE.equals(exchange.getProperty(Source.NORMALIZED_VALUE_PROPERTY)));
		assertTrue(Boolean.TRUE.equals(exchange.getProperty(Source.CONTRIBUTION_PROPERTY)));
	}

	@Test
	void badStatusDoesNotContributeToLatestSourceState() throws Exception {
		Source source = source("ns=2;s=Machine.State");
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody(new DataValue(new Variant("BAD"), StatusCode.BAD));

		source.endpoint().normalizer(source).process(exchange);

		assertEquals(Boolean.FALSE, exchange.getProperty(Source.CONTRIBUTION_PROPERTY));
		assertInstanceOf(DataValue.class, exchange.getMessage().getBody());
	}
}
