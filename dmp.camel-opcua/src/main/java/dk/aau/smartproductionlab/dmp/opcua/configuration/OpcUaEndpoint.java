/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.opcua.configuration;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.camel.Processor;
import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import dk.aau.smartproductionlab.dmp.core.security.BoundCredential;
import dk.aau.smartproductionlab.dmp.core.security.Credentials;
import dk.aau.smartproductionlab.dmp.opcua.canonical.OpcUaSourceEventAdapter;

/**
 * An OPC UA node on {@code opc.tcp://host:port/servicePath}: a variable read, monitored or written, a notifier
 * whose events are monitored, or an Object whose Method is called. {@code options} are {@code dmp-milo-client}
 * endpoint options.
 *
 * @see <a href="https://camel.apache.org/components/4.x/milo-client-component.html">Apache Camel - OPC UA Client</a>
 */
public record OpcUaEndpoint(String host, int port, String servicePath, String node, Credentials credentials,
		Map<String, String> options) implements Endpoint {

	public OpcUaEndpoint {
		options = Map.copyOf(options);
	}

	/** A source's node: no security policy, Read, Write and Call give up after 30 s, published every second. */
	public static OpcUaEndpoint source(String host, int port, String servicePath, String node, String username,
			String password, Map<String, String> options) {
		Map<String, String> all = defaults(options);
		all.putIfAbsent("requestedPublishingInterval", "1000");
		return new OpcUaEndpoint(host, port, servicePath, node, credentials(username, password), all);
	}

	/** A sink's or an action's node: no security policy, Write and Call give up after 30 s. */
	public static OpcUaEndpoint sink(String host, int port, String servicePath, String node, String username,
			String password, Map<String, String> options) {
		return new OpcUaEndpoint(host, port, servicePath, node, credentials(username, password), defaults(options));
	}

	private static Map<String, String> defaults(Map<String, String> options) {
		Map<String, String> all = new LinkedHashMap<>(options);
		all.putIfAbsent("allowedSecurityPolicies", "None");
		// Read, Write and Call give up after 30 s, as HTTP and MQTT actions do (Milo's default is 60 s).
		all.putIfAbsent("requestTimeout", "30000");
		return all;
	}

	private static Credentials credentials(String username, String password) {
		return username != null && password != null ? new Credentials(username, password) : null;
	}

	@Override
	public String uri(BundleResources resources) {
		return connectionUri(host, port, servicePath, node, credentials == null ? null : credentials.bind(resources), options);
	}

	/** A monitored item delivers DataValues and event field maps; a sample contributes its value only. */
	@Override
	public Processor normalizer(Source source) {
		return new OpcUaSourceEventAdapter().normalizer(source);
	}

	/** A node named by an AID browse path (starting with {@code /}) is the {@code browsePath} option, else {@code node}. */
	static String nodeParameter(String node) {
		return node != null && node.startsWith("/") ? "browsePath" : "node";
	}

	/**
	 * A {@code dmp-milo-client} URI. Credentials are only named ({@code username}/{@code password} references to
	 * the registry, SPEC §11), and the connection is keyed by the credentials' {@link BoundCredential#connectionKey()}
	 * ({@code clientId}), so endpoints of one server and user share a session; options are in name order, so equal
	 * configurations give equal URIs.
	 */
	static String connectionUri(String host, int port, String path, String node, BoundCredential credential,
			Map<String, String> configuration) {
		StringBuilder uri = new StringBuilder("dmp-milo-client:opc.tcp://").append(host).append(':').append(port).append('/')
				.append(path).append('?').append(nodeParameter(node)).append("=RAW(").append(node).append(')');
		new TreeMap<>(configuration).forEach((name, value) -> {
			if (value != null && !value.isEmpty()) uri.append('&').append(name).append('=').append(value);
		});
		if (credential != null) uri.append("&username=").append(credential.userNameReference()).append("&password=")
				.append(credential.passwordReference()).append("&clientId=").append(credential.connectionKey());
		return uri.toString();
	}
}
