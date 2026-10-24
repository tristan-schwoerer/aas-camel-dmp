/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.compiler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;

import org.junit.jupiter.api.Test;

/** The DMP's credentials file: by endpoint, overridden by scheme reference; strict, and never printed. */
class DmpCredentialsTest {
	private static final DmpCredentials CREDENTIALS = DmpCredentials.parse("""
			{"endpoints": {"mqtt://Broker": {"username": "dmp", "password": "s3cret"}},
			 "schemes": {"urn:aid#interface_mqtt/basic_sc": {"username": "station", "password": "other"}}}""");

	@Test
	void anEndpointMatchesWithItsDefaultPortAndASchemeReferenceOverridesIt() {
		assertEquals("dmp", CREDENTIALS.find("urn:aid#interface_mqtt/other_sc", URI.create("mqtt://broker:1883/site/")).orElseThrow().username());
		assertEquals("station", CREDENTIALS.find("urn:aid#interface_mqtt/basic_sc", URI.create("mqtt://broker:1883")).orElseThrow().username());
		assertTrue(CREDENTIALS.find(null, URI.create("mqtt://broker:1884")).isEmpty());
	}

	@Test
	void aMalformedFileIsAnErrorThatQuotesNoSecret() {
		for (String json : new String[] { "{\"endpoints\": {\"mqtt://b:1\": {\"passwd\": \"s3cret\"}}}",
				"{\"endpoint\": {}}", "{\"endpoints\": {\"mqtt://user:s3cret@b:1\": {}}}", "not json s3cret" }) {
			var failure = assertThrows(IllegalArgumentException.class, () -> DmpCredentials.parse(json));
			assertFalse(failure.getMessage().contains("s3cret"), failure.getMessage());
		}
		assertFalse(CREDENTIALS.toString().contains("s3cret"));
		assertFalse(CREDENTIALS.find(null, URI.create("mqtt://broker")).orElseThrow().toString().contains("s3cret"));
	}
}
