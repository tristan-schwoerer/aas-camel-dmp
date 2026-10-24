/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.plc4x.component;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.camel.Endpoint;
import org.apache.camel.component.plc4x.Plc4XComponent;
import org.apache.camel.util.PropertiesHelper;

/**
 * {@code dmp-plc4x:<driver>://<host>:<port>?tag.<name>=<address>&<driver options>}: camel-plc4x
 * with acknowledged writes and checked reads (see {@link DmpPlc4xEndpoint}). URIs take the same
 * tag parameters as {@code plc4x:}; every other parameter that is not a Camel endpoint option is a
 * PLC4X driver option and becomes part of the connection string.
 *
 * <p>Endpoints with the same connection string (one device, different tags) share one connection
 * ({@link SharedConnection}), open while any of them is started.
 */
@org.apache.camel.spi.annotations.Component("dmp-plc4x")
public class DmpPlc4xComponent extends Plc4XComponent {
	private final Map<String, SharedConnection> connections = new java.util.HashMap<>();

	private static final Set<String> CAMEL_OPTIONS = Set.of("autoReconnect", "lazyStartProducer",
			"bridgeErrorHandler", "exceptionHandler", "exchangePattern");

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		Map<String, String> tags = new LinkedHashMap<>();
		@SuppressWarnings("unchecked")
		Map<String, Object> declared = getAndRemoveOrResolveReferenceParameter(parameters, "tags", Map.class);
		if (declared != null) declared.forEach((name, address) -> tags.put(name, String.valueOf(address)));
		PropertiesHelper.extractProperties(parameters, "tag.").forEach((name, address) -> tags.put(name, String.valueOf(address)));

		StringBuilder connection = new StringBuilder(remaining);
		String separator = remaining.contains("?") ? "&" : "?";
		for (var option : new LinkedHashMap<>(parameters).entrySet()) {
			if (CAMEL_OPTIONS.contains(option.getKey())) continue;
			connection.append(separator).append(URLEncoder.encode(option.getKey(), StandardCharsets.UTF_8))
					.append('=').append(URLEncoder.encode(String.valueOf(option.getValue()), StandardCharsets.UTF_8));
			separator = "&";
			parameters.remove(option.getKey());
		}

		DmpPlc4xEndpoint endpoint = new DmpPlc4xEndpoint(uri, this, connection.toString());
		endpoint.setTags(tags);
		setProperties(endpoint, parameters);
		return endpoint;
	}

	/** The device's connection, held by {@code owner} until {@link #release}. */
	synchronized SharedConnection acquire(String connectionString, Object owner) {
		SharedConnection shared = connections.computeIfAbsent(connectionString, SharedConnection::new);
		shared.owners.add(owner);
		return shared;
	}

	/** Closes the device's connection once its last owner released it. */
	synchronized void release(SharedConnection shared, Object owner) {
		shared.owners.remove(owner);
		if (!shared.owners.isEmpty()) return;
		connections.remove(shared.connectionString, shared);
		shared.close();
	}

	/** Open device connections (for tests and accounting). */
	synchronized int openConnections() {
		return (int) connections.values().stream().filter(SharedConnection::isOpen).count();
	}

	/**
	 * One device's connection. Requests take its lock, so a device answers one request of the DMP at a time
	 * whichever endpoint sends it (a Modbus device serves one transaction at a time anyway).
	 */
	static final class SharedConnection {
		final String connectionString;
		private final java.util.Set<Object> owners = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
		private org.apache.plc4x.java.api.PlcConnection connection;

		SharedConnection(String connectionString) { this.connectionString = connectionString; }

		/** The open connection, (re)connecting when it is closed or lost. */
		synchronized org.apache.plc4x.java.api.PlcConnection connected(org.apache.plc4x.java.api.PlcDriverManager drivers)
				throws org.apache.plc4x.java.api.exceptions.PlcConnectionException {
			if (connection != null && connection.isConnected()) return connection;
			close();
			connection = drivers.getConnectionManager().getConnection(connectionString);
			return connection;
		}

		synchronized boolean isOpen() { return connection != null && connection.isConnected(); }

		synchronized void close() {
			if (connection == null) return;
			try { connection.close(); } catch (Exception ignored) { /* the device went away */ }
			connection = null;
		}
	}
}
