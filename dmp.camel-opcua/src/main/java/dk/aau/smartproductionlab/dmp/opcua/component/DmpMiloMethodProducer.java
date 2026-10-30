/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.opcua.component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.camel.Exchange;
import org.apache.camel.component.milo.client.MiloClientConnection;
import org.apache.camel.support.DefaultProducer;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.structured.CallMethodResult;
import org.eclipse.milo.opcua.stack.core.types.structured.CallMethodRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.CallRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.CallResponse;

/**
 * One UA {@code Call} of the endpoint's Method on its owning Object. The body
 * is the positional input arguments ({@code Variant[]}); the reply body is the
 * {@code CallMethodResult}. Its status is the caller's to judge; a Call that
 * cannot be made or times out raises a {@link ConnectorInvocationException}.
 */
public final class DmpMiloMethodProducer extends DefaultProducer {
	/** Optional header: how long to wait for the Call, in milliseconds (default 30 s). */
	public static final String CALL_TIMEOUT_HEADER = "DmpOpcUaCallTimeout";
	private static final long DEFAULT_TIMEOUT_MS = 30_000;
	private final DmpMiloClientEndpoint endpoint;
	private MiloClientConnection connection;

	DmpMiloMethodProducer(DmpMiloClientEndpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		connection = endpoint.createConnection();
	}

	@Override
	protected void doStop() throws Exception {
		if (connection != null) endpoint.releaseConnection(connection);
		connection = null;
		super.doStop();
	}

	private OpcUaClient resolvedFor;
	private NodeId[] resolved;

	/** The Object and Method NodeIds, resolved once per session: a browse path is a TranslateBrowsePaths call. */
	private synchronized NodeId[] target(OpcUaClient client) throws UaException {
		if (resolvedFor != client) {
			resolved = new NodeId[] { OpcUaBrowsePaths.resolve(client, endpoint.getNode()),
					OpcUaBrowsePaths.resolve(client, endpoint.getMethod()) };
			resolvedFor = client;
		}
		return resolved;
	}

	/**
	 * The Call as {@link OpcUaClient#callAsync} makes it, but with {@code timeoutMs} as its timeout hint: the client's
	 * requestTimeout (30 s, shared by every endpoint of the session) would otherwise end a longer call first.
	 */
	private static CompletableFuture<CallResponse> call(OpcUaClient client, CallMethodRequest request, long timeoutMs) {
		return client.getSessionAsync().thenCompose(session -> client.sendRequestAsync(new CallRequest(
				client.newRequestHeader(session.getAuthenticationToken(), Unsigned.uint(timeoutMs)), new CallMethodRequest[] { request })))
				.thenApply(CallResponse.class::cast);
	}

	@Override
	public void process(Exchange exchange) {
		Variant[] pending = arguments(exchange.getMessage().getBody());
		Long timeout = exchange.getMessage().getHeader(CALL_TIMEOUT_HEADER, Long.class);
		long timeoutMs = timeout == null || timeout <= 0 ? DEFAULT_TIMEOUT_MS : timeout;
		try {
			var client = connection == null ? null : connection.getOpcUaClient();
			if (client == null) throw new IllegalStateException("OPC UA client is not connected");
			Variant[] arguments = encoded(pending, client);
			NodeId[] target = target(client);
			var object = target[0];
			var method = target[1];
			CallResponse response = call(client, new CallMethodRequest(object, method, arguments), timeoutMs)
					.get(timeoutMs, TimeUnit.MILLISECONDS);
			if (response == null || response.getResults() == null || response.getResults().length != 1)
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
						"METHOD_RESULT_INVALID", "OPC UA Method returned an unreadable result", Map.of());
			CallMethodResult result = response.getResults()[0];
			// Structured outputs are decoded with the server's DataTypes (SPEC §5.2).
			exchange.getMessage().setBody(result.getOutputArguments() == null ? result
					: new CallMethodResult(result.getStatusCode(), result.getInputArgumentResults(),
							result.getInputArgumentDiagnosticInfos(),
							(Variant[]) OpcUaStructures.decoded(result.getOutputArguments(), client)));
		} catch (TimeoutException timeoutFailure) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.TIMEOUT,
					"METHOD_CALL_TIMEOUT", "OPC UA Method invocation timed out", Map.of("timeoutMillis", timeoutMs));
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw transport(interrupted);
		} catch (ConnectorInvocationException typed) {
			throw typed;
		} catch (ExecutionException | RuntimeException | org.eclipse.milo.opcua.stack.core.UaException failure) {
			throw transport(failure);
		}
	}

	private static Variant[] arguments(Object body) {
		if (body == null) return new Variant[0];
		if (body instanceof Variant[] variants) return variants;
		if (body instanceof List<?> values) return values.stream()
				.map(value -> value instanceof Variant variant ? variant : new Variant(value)).toArray(Variant[]::new);
		throw new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
				"METHOD_ARGUMENTS_INVALID", "OPC UA Method arguments must be Variant[] or a list", Map.of());
	}

	/** Input arguments of a structured, enumerated or other non-simple DataType, encoded for the server. */
	private static Variant[] encoded(Variant[] arguments, org.eclipse.milo.opcua.sdk.client.OpcUaClient client) {
		Variant[] encoded = arguments.clone();
		for (int i = 0; i < encoded.length; i++) {
			if (!(encoded[i] != null && encoded[i].getValue() instanceof OpcUaStructures.Pending pending)) continue;
			try {
				encoded[i] = new Variant(OpcUaStructures.encode(pending.value(), NodeId.parse(pending.dataType()), client));
			} catch (IllegalArgumentException | org.eclipse.milo.opcua.stack.core.UaException mismatch) {
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
						"METHOD_ARGUMENT_MISMATCH", "OPC UA Method argument does not fit its DataType",
						Map.of("argument", i, "cause", String.valueOf(mismatch.getMessage())));
			}
		}
		return encoded;
	}

	private static ConnectorInvocationException transport(Exception failure) {
		return new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
				"METHOD_CALL_FAILED", "OPC UA Method invocation failed", Map.of("cause", String.valueOf(failure.getMessage())));
	}
}
