/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.paho.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dk.aau.smartproductionlab.dmp.paho.mqtt5.component.DmpPahoMqtt5Endpoint;
import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.core.security.Credentials;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class MqttCredentialBindingTest {
	private static final String SECRET = "pa ss&word#1";

	/** What a bundle binds in {@code context}, closed when it retires. */
	private static final class Bundle implements BundleResources {
		private final DefaultCamelContext context;
		private final List<ManagedRouteResource> owned = new ArrayList<>();
		Bundle(DefaultCamelContext context) { this.context = context; }
		@Override public <T extends ManagedRouteResource> T own(T resource) {
			try { resource.bind(context); } catch (Exception e) { throw new IllegalStateException(e); }
			owned.add(resource);
			return resource;
		}
		void retire() throws Exception { for (var resource : owned) resource.close(); }
	}

	@Test
	void sourceAndSinkUrisNameTheCredentialsWhileTheEndpointsReceiveThem() throws Exception {
		var source = new MqttSubscription("broker", 1883, "dmp-source", "base/state", new Credentials("alice", SECRET), null);
		var sink = new MqttPublication("broker", 1883, "base/command", "dmp-sink", new Credentials("alice", SECRET), null, null);
		assertFalse(source.toString().contains(SECRET) || sink.toString().contains(SECRET), "a plan never prints them");
		try (var context = new DefaultCamelContext()) {
			context.start();
			var bundle = new Bundle(context);
			String[] uris = { source.uri(bundle), sink.uri(bundle) };
			for (String uri : uris) {
				assertFalse(uri.contains(SECRET), uri);
				assertFalse(uri.contains("alice"), uri);
				assertTrue(uri.contains("password=#bean:dmp-credential-"), uri);
			}
			for (String uri : uris) {
				var endpoint = context.getEndpoint(uri, DmpPahoMqtt5Endpoint.class);
				assertEquals("alice", endpoint.getUserName());
				assertEquals(SECRET, endpoint.getPassword());
				assertFalse(endpoint.getEndpointUri().contains(SECRET), "the registered endpoint keeps the reference");
			}
			bundle.retire();
			assertTrue(context.getRegistry().findByTypeWithName(String.class).keySet().stream()
					.noneMatch(name -> name.startsWith("dmp-credential-")), "retiring the bundle unbinds the credentials");
		}
	}

	@Test
	void withoutCredentialsNothingIsBoundOrReferenced() throws Exception {
		var source = new MqttSubscription("broker", 1883, "dmp-source", "base/state", null, null);
		try (var context = new DefaultCamelContext()) {
			assertFalse(source.uri(new Bundle(context)).contains("userName"));
			assertTrue(context.getRegistry().findByTypeWithName(String.class).isEmpty());
		}
	}
}
