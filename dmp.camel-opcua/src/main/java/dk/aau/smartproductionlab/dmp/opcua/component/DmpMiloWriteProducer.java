/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.opcua.component;

import java.util.List;
import java.util.Map;

import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.component.milo.MiloConstants;
import org.apache.camel.component.milo.client.MiloClientConnection;
import org.apache.camel.component.milo.client.MiloClientProducer;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;

/**
 * A Write that waits for the server's answer and fails on a Bad status. camel-milo 4.22
 * either does not wait for a write or, when asked to ({@code await}), puts the returned
 * StatusCode in the body without looking at it, so a rejected write (e.g.
 * {@code Bad_NotWritable}, {@code Bad_TypeMismatch}) goes unnoticed.
 *
 * <p>A scalar value is written as the variable's built-in DataType (read once from the
 * server): decoded JSON numbers are Java longs and doubles, which an Int32 or Float
 * variable rejects. The body keeps the written value.
 */
final class DmpMiloWriteProducer extends MiloClientProducer {
	private final DmpMiloClientEndpoint endpoint;
	private MiloClientConnection connection;
	private volatile NodeId dataType;

	DmpMiloWriteProducer(DmpMiloClientEndpoint endpoint) {
		super(endpoint, true);
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

	@Override
	public boolean process(Exchange exchange, AsyncCallback callback) {
		List<?> reads = exchange.getMessage().getHeader(MiloConstants.HEADER_NODE_IDS, List.class);
		if (reads != null) {
			// A Read: browse paths in the node list are resolved to their NodeIds first.
			try {
				exchange.getMessage().setHeader(MiloConstants.HEADER_NODE_IDS, resolved(reads));
			} catch (Exception unresolved) {
				exchange.setException(unresolved);
				callback.done(true);
				return true;
			}
			return super.process(exchange, done -> {
				// A structure read is decoded with the server's DataTypes (SPEC §5.2).
				OpcUaClient client = connection == null ? null : connection.getOpcUaClient();
				if (client != null && exchange.getException() == null) {
					Object result = exchange.getMessage().getBody();
					exchange.getMessage().setBody(OpcUaStructures.decoded(result, client));
					Map<String, Object> schema = readSchema(result, client);
					if (schema != null)
						exchange.getMessage().setHeader(dk.aau.smartproductionlab.dmp.core.configuration.route.core.SourceEventAdapter.DEVICE_SCHEMA_HEADER, schema);
				}
				callback.done(done);
			});
		}
		Object written = exchange.getMessage().getBody();
		try {
			exchange.getMessage().setBody(typed(written));
		} catch (ConnectorInvocationException invalid) {
			exchange.setException(invalid);
			callback.done(true);
			return true;
		}
		if (endpoint.getBrowsePath() != null) {
			// camel-milo writes the NodeId it parsed when the producer was created; a browse path is resolved here.
			try {
				OpcUaClient client = client();
				Object value = exchange.getMessage().getBody();
				StatusCode status = client.writeValues(List.of(endpoint.node(client)),
						List.of(DataValue.valueOnly(value instanceof Variant variant ? variant : new Variant(value)))).get(0);
				exchange.getMessage().setBody(status);
			} catch (Exception failure) {
				exchange.setException(failure);
			}
			finish(exchange, written);
			callback.done(true);
			return true;
		}
		return super.process(exchange, done -> {
			finish(exchange, written);
			callback.done(done);
		});
	}

	/** The read variable's own type (SourceEventAdapter#DEVICE_SCHEMA_HEADER); null for a pending or multi-node Read. */
	private static Map<String, Object> readSchema(Object result, OpcUaClient client) {
		return result instanceof List<?> values && values.size() == 1 ? OpcUaStructures.deviceSchema(values.get(0), client) : null;
	}

	/** A Bad write status or a failure becomes a typed invocation error; the body is the written value again. */
	private static void finish(Exchange exchange, Object written) {
		Object answer = exchange.getMessage().getBody();
		if (exchange.getException() == null && answer instanceof StatusCode status && !status.isGood()) {
			exchange.setException(new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
					"OPCUA_WRITE_REJECTED", "OPC UA server rejected the write",
					Map.of("status", Long.toHexString(status.getValue()))));
		} else if (exchange.getException() != null && !(exchange.getException() instanceof ConnectorInvocationException)) {
			exchange.setException(new ConnectorInvocationException(ConnectorInvocationException.Category.TRANSPORT,
					"OPCUA_WRITE_FAILED", "OPC UA write failed", Map.of()));
		}
		exchange.getMessage().setBody(written);
	}

	/** The Read's node list with each browse path replaced by its NodeId. */
	private List<String> resolved(List<?> nodes) throws Exception {
		List<String> result = new java.util.ArrayList<>();
		for (Object node : nodes) {
			String text = String.valueOf(node);
			if (!OpcUaBrowsePaths.isBrowsePath(text)) result.add(text);
			// The endpoint's own path is resolved once per session; any other once per Read.
			else if (text.equals(endpoint.getBrowsePath())) result.add(endpoint.node(client()).toParseableString());
			else result.add(OpcUaBrowsePaths.resolve(client(), text).toParseableString());
		}
		return result;
	}

	private OpcUaClient client() {
		OpcUaClient client = connection == null ? null : connection.getOpcUaClient();
		if (client == null) throw new IllegalStateException("OPC UA client is not connected");
		return client;
	}

	/**
	 * The value as a Variant of the variable's DataType: a scalar of a simple built-in type
	 * converted here, anything else (structures, enumerations, arrays, other built-in types)
	 * encoded with the server's DataTypes. A value of an unknown DataType passes unchanged.
	 */
	private Object typed(Object value) {
		if (value instanceof Variant variant && (variant.getValue() instanceof Map<?, ?> || variant.getValue() instanceof List<?>))
			value = variant.getValue();
		NodeId type = dataType();
		if (type != null && value != null && !(value instanceof Variant)
				&& (!OpcUaStructures.simpleBuiltin(type) || value instanceof Map<?, ?> || value instanceof List<?>)) {
			try {
				return new Variant(OpcUaStructures.encode(value, type, client()));
			} catch (IllegalArgumentException | org.eclipse.milo.opcua.stack.core.UaException mismatch) {
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
						"OPCUA_VALUE_MISMATCH", "value does not fit the variable's DataType",
						Map.of("cause", String.valueOf(mismatch.getMessage())));
			}
		}
		if (!(value instanceof Number || value instanceof Boolean || value instanceof String)) return value;
		if (type == null || type.getNamespaceIndex().intValue() != 0 || !(type.getIdentifier() instanceof Number id)) return value;
		try {
			Object converted = switch (id.intValue()) {
				case 1 -> value instanceof Boolean flag ? flag : value;
				case 2 -> number(value) == null ? value : (byte) range(exactLong(number(value)), Byte.MIN_VALUE, Byte.MAX_VALUE);
				case 3 -> number(value) == null ? value : Unsigned.ubyte(range(exactLong(number(value)), 0, 0xFF));
				case 4 -> number(value) == null ? value : (short) range(exactLong(number(value)), Short.MIN_VALUE, Short.MAX_VALUE);
				case 5 -> number(value) == null ? value : Unsigned.ushort((int) range(exactLong(number(value)), 0, 0xFFFF));
				case 6 -> number(value) == null ? value : Math.toIntExact(exactLong(number(value)));
				case 7 -> number(value) == null ? value : Unsigned.uint(range(exactLong(number(value)), 0, 0xFFFFFFFFL));
				case 8 -> number(value) == null ? value : exactLong(number(value));
				case 9 -> number(value) == null ? value : Unsigned.ulong(unsigned64(number(value)));
				case 10 -> number(value) == null ? value : number(value).floatValue();
				case 11 -> number(value) == null ? value : number(value).doubleValue();
				case 12 -> String.valueOf(value);
				default -> value;
			};
			return new Variant(converted);
		} catch (ArithmeticException outOfRange) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
					"OPCUA_VALUE_OUT_OF_RANGE", "value does not fit the variable's DataType", Map.of());
		}
	}

	private static Number number(Object value) {
		if (!(value instanceof Number number)) return null;
		if ((number instanceof Double || number instanceof Float) && number.doubleValue() != Math.rint(number.doubleValue())) return null;
		return number;
	}

	/**
	 * The number as a long, or ArithmeticException when it is not one: a double beyond the range would saturate
	 * and a BigInteger would wrap, writing a different value than Lua returned.
	 */
	static long exactLong(Number number) {
		if (number instanceof java.math.BigInteger big) return big.longValueExact();
		if (number instanceof java.math.BigDecimal decimal) return decimal.toBigIntegerExact().longValueExact();
		if (number instanceof Double || number instanceof Float) {
			double value = number.doubleValue();
			if (!(value >= -0x1p63 && value < 0x1p63)) throw new ArithmeticException("out of range");
			return (long) value;
		}
		return number.longValue();
	}

	/** The number as an unsigned 64-bit integer (0 to 2^64-1), or ArithmeticException. */
	static java.math.BigInteger unsigned64(Number number) {
		java.math.BigInteger value = number instanceof java.math.BigInteger big ? big
				: number instanceof java.math.BigDecimal decimal ? decimal.toBigIntegerExact()
				: number instanceof Double || number instanceof Float ? new java.math.BigDecimal(number.doubleValue()).toBigIntegerExact()
				: java.math.BigInteger.valueOf(number.longValue());
		if (value.signum() < 0 || value.bitLength() > 64) throw new ArithmeticException("out of range");
		return value;
	}

	private static long range(long value, long min, long max) {
		if (value < min || value > max) throw new ArithmeticException("out of range");
		return value;
	}

	/** The variable's DataType, read once; {@code null} while the client is not connected. */
	private NodeId dataType() {
		NodeId known = dataType;
		if (known != null) return known;
		OpcUaClient client = connection == null ? null : connection.getOpcUaClient();
		if (client == null) return null;
		try {
			NodeId node = endpoint.node(client);
			DataValue read = client.read(0, TimestampsToReturn.Neither, List.of(new ReadValueId(node,
					AttributeId.DataType.uid(), null, QualifiedName.NULL_VALUE))).getResults()[0];
			if (read.getValue() != null && read.getValue().getValue() instanceof NodeId type) dataType = type;
		} catch (Exception unknown) {
			return null;
		}
		return dataType;
	}
}
