/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.opcua.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;

/** An OPC UA endpoint URI names its credentials, never carries them, and is the same for the same configuration. */
class OpcuaConnectionUriTest {
	/** Resources that are only named, never bound: the URI is all this test reads. */
	private static final BundleResources RESOURCES = new BundleResources() {
		@Override public <T extends ManagedRouteResource> T own(T resource) { return resource; }
	};

	@Test
	void credentialsAreReferencesAndOptionsAreInNameOrder() {
		Map<String, String> options = new HashMap<>(Map.of("samplingInterval", "50", "allowedSecurityPolicies", "None"));
		var source = OpcUaEndpoint.source("127.0.0.1", 4840, "", "ns=1;s=Level", "dmp", "p@ss&w#rd", options);
		String uri = source.uri(RESOURCES);
		assertFalse(uri.contains("p@ss") || uri.contains("dmp:"), uri);
		assertTrue(uri.contains("&username=#bean:") && uri.contains("&password=#bean:") && uri.contains("&clientId=dmp-"), uri);
		assertTrue(uri.indexOf("allowedSecurityPolicies") < uri.indexOf("requestTimeout=30000")
				&& uri.indexOf("requestTimeout") < uri.indexOf("samplingInterval"), uri);
		var same = OpcUaEndpoint.sink("127.0.0.1", 4840, "", "ns=1;s=Level", "dmp", "p@ss&w#rd",
				new HashMap<>(Map.of("allowedSecurityPolicies", "None", "samplingInterval", "50")));
		assertEquals(clientId(uri), clientId(same.uri(RESOURCES)), "one session per server and user");
		var other = OpcUaEndpoint.sink("127.0.0.1", 4840, "", "ns=1;s=Level", "dmp", "another",
				new HashMap<>(Map.of("allowedSecurityPolicies", "None")));
		assertFalse(clientId(uri).equals(clientId(other.uri(RESOURCES))), "another password is another session");
		assertFalse(source.toString().contains("p@ss"), source.toString());
	}

	private static String clientId(String uri) {
		return uri.substring(uri.indexOf("clientId=") + 9).split("&")[0];
	}
}
