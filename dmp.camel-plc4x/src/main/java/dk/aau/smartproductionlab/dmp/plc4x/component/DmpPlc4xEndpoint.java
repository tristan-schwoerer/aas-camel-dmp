/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.plc4x.component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.camel.PollingConsumer;
import org.apache.camel.Producer;
import org.apache.camel.component.plc4x.Plc4XEndpoint;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.api.messages.PlcReadResponse;
import org.apache.plc4x.java.api.messages.PlcWriteResponse;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;

/**
 * A camel-plc4x endpoint whose producer and polling consumer report every
 * device outcome. Upstream logs a write it could not send and answers a failed
 * read with an empty body; here both raise a {@link ConnectorInvocationException}.
 * The producer takes the decoded value of a single-tag endpoint (or a value per
 * tag) instead of upstream's {@code {tag: {address: value}}} body. Endpoints of one
 * device share its connection (upstream opens one per endpoint, so per tag).
 */
public class DmpPlc4xEndpoint extends Plc4XEndpoint {
	static final long REQUEST_TIMEOUT_MS = 5000;
	private final String connectionString;
	private final DmpPlc4xComponent component;
	private DmpPlc4xComponent.SharedConnection shared;

	DmpPlc4xEndpoint(String endpointUri, DmpPlc4xComponent component, String connectionString) {
		super(endpointUri, component);
		this.component = component;
		this.connectionString = connectionString;
	}

	/** The PLC4X connection string: driver, address and driver options. */
	@Override
	public String getUri() { return connectionString; }

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		synchronized (this) { shared = component.acquire(connectionString, this); }
	}

	@Override
	public void doStop() throws Exception {
		DmpPlc4xComponent.SharedConnection released;
		synchronized (this) {
			released = shared;
			shared = null;
			connection = null; // the device's connection is not this endpoint's to close
		}
		if (released != null) component.release(released, this);
		super.doStop();
	}

	private synchronized DmpPlc4xComponent.SharedConnection shared() {
		if (shared == null) throw new IllegalStateException("the PLC4X endpoint is not started");
		return shared;
	}

	@Override
	public void setupConnection() throws PlcConnectionException {
		connection = shared().connected(plcDriverManager);
	}

	@Override
	public void reconnectIfNeeded() throws PlcConnectionException {
		connection = shared().connected(plcDriverManager);
	}

	@Override
	public Producer createProducer() { return new DmpPlc4xProducer(this); }

	@Override
	public PollingConsumer createPollingConsumer() { return new DmpPlc4xPollingConsumer(this); }

	/** The values to write, by tag: the body itself for a single-tag endpoint, else a map keyed by tag. */
	Map<String, Object> valuesByTag(Object body) {
		Map<String, String> tags = getTags();
		if (tags == null || tags.isEmpty()) throw configuration("a PLC4X endpoint needs at least one tag");
		if (body instanceof Map<?, ?> map && !map.isEmpty() && tags.keySet().containsAll(map.keySet())) {
			Map<String, Object> values = new LinkedHashMap<>();
			map.forEach((tag, value) -> values.put(String.valueOf(tag), value));
			return values;
		}
		if (tags.size() != 1) throw configuration("a PLC4X write to several tags needs a value per tag");
		Map<String, Object> values = new LinkedHashMap<>();
		values.put(tags.keySet().iterator().next(), body);
		return values;
	}

	/** Writes and waits for the device to acknowledge every tag. */
	void write(Map<String, Object> values) {
		Map<String, Map<String, Object>> request = new LinkedHashMap<>();
		values.forEach((tag, value) -> {
			String address = getTags().get(tag);
			if (address == null) throw configuration("unknown PLC4X tag " + tag);
			Map<String, Object> addressed = new LinkedHashMap<>();
			addressed.put(address, value);
			request.put(tag, addressed);
		});
		PlcWriteResponse response = await(() -> buildPlcWriteRequest(request), "PLC4X_WRITE");
		for (String tag : values.keySet())
			if (response.getResponseCode(tag) != PlcResponseCode.OK)
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
						"PLC4X_WRITE_REJECTED", "the device rejected the tag write",
						Map.of("tag", tag, "responseCode", String.valueOf(response.getResponseCode(tag))));
	}

	/** Reads every tag; a single-tag endpoint yields the value itself. */
	Object read() {
		PlcReadResponse response = await(this::buildPlcReadRequest, "PLC4X_READ");
		Map<String, Object> values = new LinkedHashMap<>();
		for (String tag : getTags().keySet()) {
			if (response.getResponseCode(tag) != PlcResponseCode.OK)
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
						"PLC4X_READ_REJECTED", "the device rejected the tag read",
						Map.of("tag", tag, "responseCode", String.valueOf(response.getResponseCode(tag))));
			values.put(tag, response.getObject(tag));
		}
		return values.size() == 1 ? values.values().iterator().next() : values;
	}

	private interface Request<T extends org.apache.plc4x.java.api.messages.PlcResponse> {
		org.apache.plc4x.java.api.messages.PlcRequest build() throws Exception;
	}

	/** One request at a time per device: the shared connection's lock is held until the answer arrived. */
	@SuppressWarnings("unchecked")
	private <T extends org.apache.plc4x.java.api.messages.PlcResponse> T await(Request<T> request, String code) {
		DmpPlc4xComponent.SharedConnection device = shared();
		try {
			synchronized (device) {
				connection = device.connected(plcDriverManager);
				if (!isConnected()) throw new IllegalStateException("not connected");
				return (T) request.build().execute().get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			}
		} catch (TimeoutException timeout) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.TIMEOUT,
					code + "_TIMEOUT", "the device did not answer in time", Map.of());
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw transport(code, interrupted);
		} catch (ConnectorInvocationException typed) {
			throw typed;
		} catch (ExecutionException failed) {
			throw transport(code, failed.getCause() != null ? failed.getCause() : failed);
		} catch (Exception failure) {
			throw transport(code, failure);
		}
	}

	private static ConnectorInvocationException transport(String code, Throwable failure) {
		return new ConnectorInvocationException(ConnectorInvocationException.Category.TRANSPORT,
				code + "_FAILED", "the device could not be reached", Map.of("cause", causalMessage(failure)));
	}

	/** The failure and its causes, outermost first: PLC4X wraps the socket error in a connection exception. */
	private static String causalMessage(Throwable failure) {
		StringBuilder text = new StringBuilder();
		for (Throwable current = failure; current != null; current = current.getCause()) {
			if (!text.isEmpty()) text.append(" <- ");
			text.append(current.getClass().getSimpleName()).append(": ").append(current.getMessage());
		}
		return text.toString();
	}

	private static ConnectorInvocationException configuration(String message) {
		return new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
				"PLC4X_CONFIGURATION", message, Map.of());
	}
}
