/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.opcua.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** An OPC UA endpoint URI names its credentials, never carries them, and is the same for the same configuration. */
class OpcuaConnectionUriTest {
	@Test
	void credentialsAreReferencesAndOptionsAreInNameOrder() {
		Map<String, String> options = new HashMap<>(Map.of("samplingInterval", "50", "allowedSecurityPolicies", "None"));
		var source = new OpcuaDataSourceConfiguration("source", "127.0.0.1", 4840, "", "ns=1;s=Level", "dmp", "p@ss&w#rd", options);
		String uri = source.getConnectionURI();
		assertFalse(uri.contains("p@ss") || uri.contains("dmp:"), uri);
		assertTrue(uri.contains("&username=#bean:") && uri.contains("&password=#bean:") && uri.contains("&clientId=dmp-"), uri);
		assertTrue(uri.indexOf("allowedSecurityPolicies") < uri.indexOf("requestTimeout=30000")
				&& uri.indexOf("requestTimeout") < uri.indexOf("samplingInterval"), uri);
		var same = new OpcuaDataSinkConfiguration("sink", "127.0.0.1", 4840, "", "ns=1;s=Level", "dmp", "p@ss&w#rd",
				new HashMap<>(Map.of("allowedSecurityPolicies", "None", "samplingInterval", "50")));
		assertEquals(clientId(uri), clientId(same.getConnectionURI()), "one session per server and user");
		var other = new OpcuaDataSinkConfiguration("sink", "127.0.0.1", 4840, "", "ns=1;s=Level", "dmp", "another",
				new HashMap<>(Map.of("allowedSecurityPolicies", "None")));
		assertFalse(clientId(uri).equals(clientId(other.getConnectionURI())), "another password is another session");
	}

	private static String clientId(String uri) {
		return uri.substring(uri.indexOf("clientId=") + 9).split("&")[0];
	}
}
