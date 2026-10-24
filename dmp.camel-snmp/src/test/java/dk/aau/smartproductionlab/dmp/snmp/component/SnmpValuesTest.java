/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.snmp.component;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.snmp4j.smi.OctetString;

/** An octet string is text when it is printable UTF-8, else its hex form; text is written as UTF-8. */
class SnmpValuesTest {
	@Test
	void printableUtf8IsTextAndBinaryIsHex() {
		assertEquals("Café Ø 22 °C", SnmpValues.json(new OctetString("Café Ø 22 °C".getBytes(StandardCharsets.UTF_8))));
		assertEquals("line1\nline2", SnmpValues.json(new OctetString("line1\nline2".getBytes(StandardCharsets.UTF_8))));
		assertEquals("00:1a:2b", SnmpValues.json(new OctetString(new byte[] { 0x00, 0x1a, 0x2b })), "a MAC address stays hex");
		assertEquals("c3:28", SnmpValues.json(new OctetString(new byte[] { (byte) 0xc3, 0x28 })), "invalid UTF-8 stays hex");
	}

	@Test
	void textIsWrittenAsUtf8() {
		var written = (OctetString) SnmpValues.variable("Café", new OctetString("old"));
		assertArrayEquals("Café".getBytes(StandardCharsets.UTF_8), written.getValue());
	}
}
