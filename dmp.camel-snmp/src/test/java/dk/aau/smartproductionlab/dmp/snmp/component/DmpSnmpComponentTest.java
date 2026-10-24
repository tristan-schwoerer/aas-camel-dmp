/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.snmp.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.snmp.testing.SnmpTestAgent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.snmp4j.smi.Counter32;
import org.snmp4j.smi.Gauge32;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.OctetString;

class DmpSnmpComponentTest {
	private SnmpTestAgent agent;
	private CamelContext camel;

	@BeforeEach void start() throws Exception {
		agent = new SnmpTestAgent()
				.put("1.3.6.1.2.1.1.1.0", new OctetString("test device"), false)
				.put("1.3.6.1.4.1.9999.1.0", new Integer32(21), true)
				.put("1.3.6.1.4.1.9999.2.0", new Gauge32(4000000000L), true)
				.put("1.3.6.1.4.1.9999.3.0", new Counter32(7), false);
		camel = new DefaultCamelContext();
		camel.start();
	}

	@AfterEach void stop() throws Exception {
		camel.stop();
		agent.close();
	}

	private String uri(String oid) {
		return DmpSnmpComponent.uri("127.0.0.1", agent.port(), oid, "SNMPv2c", "public");
	}

	private Object read(String oid) {
		ConsumerTemplate consumer = camel.createConsumerTemplate();
		return consumer.receiveBody(uri(oid), 2000);
	}

	@Test void aBoundCommunityReachesTheAgentAndStaysOutOfTheUri() throws Exception {
		var source = new dk.aau.smartproductionlab.dmp.snmp.configuration.SnmpDataSourceConfiguration("source", "127.0.0.1",
				agent.port(), "1.3.6.1.2.1.1.1.0", "SNMPv2c", "pr1v&te#");
		source.bind(camel);
		assertTrue(!source.getConnectionURI().contains("pr1v"), source.getConnectionURI());
		assertEquals("test device", camel.createConsumerTemplate().receiveBody(source.getConnectionURI(), 2000));
		assertEquals("pr1v&te#", agent.lastCommunity());
		source.close();
	}

	@Test void aGetReadsTheObjectAsJson() {
		assertEquals("test device", read("1.3.6.1.2.1.1.1.0"));
		assertEquals(21, read("1.3.6.1.4.1.9999.1.0"));
		assertEquals(4000000000L, read("1.3.6.1.4.1.9999.2.0"));
		assertEquals(7L, read("1.3.6.1.4.1.9999.3.0"));
	}

	@Test void aSetWritesTheObjectAsItsOwnType() {
		camel.createProducerTemplate().sendBody(uri("1.3.6.1.4.1.9999.1.0"), 42L);
		assertEquals(new Integer32(42), agent.get("1.3.6.1.4.1.9999.1.0"));
		camel.createProducerTemplate().sendBody(uri("1.3.6.1.4.1.9999.2.0"), 5);
		assertEquals(new Gauge32(5), agent.get("1.3.6.1.4.1.9999.2.0"));
	}

	@Test void rejectionsAndMissingObjectsRaise() {
		var notWritable = assertThrows(CamelExecutionException.class,
				() -> camel.createProducerTemplate().sendBody(uri("1.3.6.1.4.1.9999.3.0"), 8));
		assertEquals("SNMP_ERROR", assertInstanceOf(ConnectorInvocationException.class, notWritable.getCause()).getConnectorCode());
		var outOfRange = assertThrows(CamelExecutionException.class,
				() -> camel.createProducerTemplate().sendBody(uri("1.3.6.1.4.1.9999.1.0"), 1.5));
		assertEquals("SNMP_VALUE_MISMATCH", assertInstanceOf(ConnectorInvocationException.class, outOfRange.getCause()).getConnectorCode());
		var missing = assertThrows(ConnectorInvocationException.class, () -> read("1.3.6.1.4.1.9999.404.0"));
		assertEquals("SNMP_NO_SUCH_OBJECT", missing.getConnectorCode());
	}
}
