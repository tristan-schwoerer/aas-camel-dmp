/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas.operation;

import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultOperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultProperty;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.PayloadAssembler;
import java.math.BigInteger;
import java.util.Map;



/**
 * Shared payload helpers for operation delegation (DMP v3, ADR-022): Lua
 * blobs return {@code { <SinkId>: <payload> }} (AIMC contract) — unwrap the
 * action key before building the named command or answering the AAS Operation
 * caller. Connector modules own transport encoding and correlation.
 *
 * @author aas-camel-dmp
 */
public final class OperationPayloads {

	/** Exchange property marking a failed delegation (writeback skipped). */
	public static final String OPERATION_DELEGATION_FAILED = "operationDelegationFailed";

	private OperationPayloads() {
	}

	/**
	 * Deterministic value parse per a DECLARED JSON-schema type (the AID
	 * action's embedded input/output DataSchema), or a caller CCI primitive
	 * {@code xs:*} valueType. The declared type — not a
	 * payload convention — decides the wire representation:
	 * <ul>
	 * <li>{@code string}: stringified (integral numbers keep their plain
	 * form, {@code 1.0 -> "1"});</li>
	 * <li>{@code integer}: integral value or parseable string → Long;</li>
	 * <li>{@code number}/{@code float}/{@code double}: numeric value or
	 * parseable string → Double;</li>
	 * <li>{@code boolean}: boolean or parseable string → Boolean;</li>
	 * <li>anything else (object/array/unknown): passed through unchanged.</li>
	 * </ul>
	 */
	public static Object coerceTyped(Object raw, String jsonType) {
		if (raw == null) {
			return null;
		}
		if (raw == JsonPointer.JSON_NULL) return raw;
		String type = jsonType == null ? "" : jsonType.trim().toLowerCase(java.util.Locale.ROOT);
		if (type.startsWith("xs:")) {
			type = switch (type.substring(3)) {
			case "string", "normalizedstring", "token", "anyuri" -> "string";
			case "byte", "short", "int", "integer", "long", "unsignedbyte", "unsignedshort", "unsignedint", "unsignedlong" -> "integer";
			case "float", "double", "decimal" -> "number";
			case "boolean" -> "boolean";
			default -> type;
			};
		}
		switch (type) {
			case "string":
				if (raw instanceof String s) {
					return s;
				}
				if (raw instanceof Number n) {
					return numericString(n);
				}
				return String.valueOf(raw);
			case "integer":
			case "int":
			case "long":
				if (raw instanceof Number n) {
					try {
						BigInteger value = new BigInteger(numericString(n));
						return value.bitLength() < 63 ? value.longValue() : value;
					} catch (NumberFormatException ignored) {
						return raw;
					}
				}
				if (raw instanceof String s) {
					try {
						BigInteger value = new BigInteger(s.trim());
						return value.bitLength() < 63 ? value.longValue() : value;
					} catch (NumberFormatException ignored) {
						return raw;
					}
				}
				return raw;
			case "number":
			case "float":
			case "double":
				if (raw instanceof Number n) {
					return n.doubleValue();
				}
				if (raw instanceof String s) {
					try {
						return Double.parseDouble(s.trim());
					} catch (NumberFormatException ignored) {
						return raw;
					}
				}
				return raw;
			case "boolean":
			case "bool":
				if (raw instanceof Boolean b) {
					return b;
				}
				if (raw instanceof String s) {
					if (s.equalsIgnoreCase("true")) {
						return Boolean.TRUE;
					}
					if (s.equalsIgnoreCase("false")) {
						return Boolean.FALSE;
					}
				}
				return raw;
			default:
				return raw;
		}
	}

	/**
	 * Coerces every DECLARED parameter of a command map in place (the
	 * declared type decides the wire representation — see
	 * {@link #coerceTyped}). Fields without a declaration pass through.
	 */
	public static void coerceFields(Map<String, Object> command, java.util.Map<String, String> types) {
		if (command == null || types == null || types.isEmpty()) {
			return;
		}
		for (Map.Entry<String, String> e : types.entrySet()) {
			if (command.containsKey(e.getKey())) {
				command.put(e.getKey(), coerceTyped(command.get(e.getKey()), e.getValue()));
			}
		}
	}

	/**
	 * The BaSyx value type for a DECLARED JSON-schema type (reply envelope
	 * building): string → xs:string, integer → xs:integer, number →
	 * xs:double, boolean → xs:boolean; unknown → xs:string (BaSyx's trivial
	 * cast answers a string). Supported authored CCI {@code xs:*} aliases are
	 * preserved rather than replaced by their JSON-category defaults.
	 */
	public static String basyxValueType(String jsonType) {
		String type = jsonType == null ? "" : jsonType.trim().toLowerCase(java.util.Locale.ROOT);
		if (java.util.Set.of("xs:string", "xs:anyuri", "xs:boolean",
				"xs:byte", "xs:short", "xs:int", "xs:integer", "xs:long", "xs:unsignedbyte", "xs:unsignedshort",
				"xs:unsignedint", "xs:unsignedlong", "xs:float", "xs:double", "xs:decimal").contains(type)) {
			return jsonType.trim();
		}
		switch (type) {
			case "integer":
			case "int":
			case "long":
				return "xs:integer";
			case "number":
			case "float":
			case "double":
				return "xs:double";
			case "boolean":
			case "bool":
				return "xs:boolean";
			default:
				return "xs:string";
		}
	}

	/** True for the BaSyx OperationVariable envelope, not arbitrary JSON arrays. */
	public static boolean isOperationVariables(Object body) {
		return operationVariables(body) != null;
	}

	/** Flattens a delegated OperationVariable[] into the {@code {idShort: value}} request tree. */
	public static Map<String, Object> flattenOperationVariables(Object body) {
		return flattenOperationVariables(body, java.util.Map.of());
	}

	/**
	 * Flattens a delegated OperationVariable[] into the {@code {idShort: value}}
	 * request tree: Properties become typed scalars, collections maps and lists
	 * lists. Declared variables ({@code templates}) type the values.
	 */
	public static Map<String, Object> flattenOperationVariables(Object body,
			java.util.Map<String, SubmodelElement> templates) {
		java.util.List<OperationVariable> variables = operationVariables(body);
		if (variables == null) throw new IllegalArgumentException("BaSyx Operation request is not an OperationVariable array");
		Map<String, Object> flat = new java.util.LinkedHashMap<>();
		for (OperationVariable variable : variables) {
			SubmodelElement element = variable.getValue();
			flat.put(element.getIdShort(), OperationVariableValues.coerce(templates.get(element.getIdShort()),
					OperationVariableValues.toValue(element)));
		}
		return flat;
	}

	/** The OperationVariables of a delegated body, or null when it is not that envelope. */
	private static java.util.List<OperationVariable> operationVariables(Object body) {
		String json = operationBodyJson(body);
		if (json == null || !json.stripLeading().startsWith("[")) return null;
		java.util.List<OperationVariable> variables;
		try { variables = Aas.readList(json, OperationVariable.class); }
		catch (IllegalArgumentException notAnEnvelope) { return null; }
		for (OperationVariable variable : variables)
			if (variable.getValue() == null || variable.getValue().getIdShort() == null
					|| !OperationVariableValues.supported(variable.getValue())) return null;
		return variables;
	}

	private static String operationBodyJson(Object body) {
		if (body instanceof String text) return text;
		if (body instanceof byte[] bytes) return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
		if (body instanceof java.io.InputStream stream) {
			try { return new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8); }
			catch (java.io.IOException e) { throw new IllegalArgumentException("could not read BaSyx Operation request", e); }
		}
		return body instanceof java.util.List<?> ? JsonCodec.toJson(body) : null;
	}

	/** Maps a flat reply into the BaSyx OperationVariable[] envelope. */
	public static java.util.List<OperationVariable> toOperationVariables(Map<String, Object> map) {
		return toOperationVariables(map, java.util.Map.of());
	}

	/** Maps a flat reply into OperationVariables using declared reply types. */
	public static java.util.List<OperationVariable> toOperationVariables(
			Map<String, Object> map, java.util.Map<String, String> declaredTypes) {
		return toOperationVariables(map, declaredTypes, java.util.List.of());
	}

	/** Maps a flat reply to caller OperationVariables in the CCI-declared order. */
	public static java.util.List<OperationVariable> toOperationVariables(
			Map<String, Object> map, java.util.Map<String, String> declaredTypes,
			java.util.List<String> callerOrder) {
		return toOperationVariables(map, declaredTypes, callerOrder, java.util.Map.of());
	}

	/**
	 * Maps a flat reply to caller OperationVariables in the CCI-declared order.
	 * A declared variable ({@code templates}) is rebuilt with its declared
	 * structure; other names become Properties of their declared scalar type.
	 */
	public static java.util.List<OperationVariable> toOperationVariables(
			Map<String, Object> map, java.util.Map<String, String> declaredTypes,
			java.util.List<String> callerOrder, java.util.Map<String, SubmodelElement> templates) {
		if (map == null) {
			return java.util.List.of();
		}
		java.util.Map<String, String> types = declaredTypes == null ? java.util.Map.of() : declaredTypes;
		java.util.List<OperationVariable> variables = new java.util.ArrayList<>();
		java.util.LinkedHashSet<String> order = new java.util.LinkedHashSet<>();
		if (callerOrder != null && !callerOrder.isEmpty()) order.addAll(callerOrder);
		else order.addAll(map.keySet());
		for (String name : order) {
			if (!map.containsKey(name)) continue;
			Object rawValue = map.get(name);
			SubmodelElement template = templates == null ? null : templates.get(name);
			if (template != null) {
				variables.add(new DefaultOperationVariable.Builder()
						.value(OperationVariableValues.toElement(template, rawValue)).build());
				continue;
			}
			String type = types.getOrDefault(name, "string");
			Object value = rawValue == null || rawValue == JsonPointer.JSON_NULL ? null : coerceTyped(rawValue, type);
			variables.add(new DefaultOperationVariable.Builder().value(new DefaultProperty.Builder().idShort(name)
					.valueType(Aas.xsd(basyxValueType(type))).value(value == null ? null : String.valueOf(value)).build()).build());
		}
		return java.util.List.copyOf(variables);
	}

	/** Encodes a caller reply: OperationVariables as AAS JSON, any other decoded value as plain JSON. */
	public static String encodeReply(Object body) {
		if (body instanceof java.util.List<?> list && !list.isEmpty() && list.stream().allMatch(OperationVariable.class::isInstance))
			return Aas.writeList(list);
		return JsonCodec.toJson(body);
	}

	/** Builds one declared affordance payload from the Lua values keyed by SinkId. */
	public static Map<String, Object> assembleCommand(Object transformed,
			java.util.List<String> sinkIds, Map<String, String> valuePaths) {
		if (sinkIds == null || sinkIds.isEmpty()) throw new IllegalArgumentException("an action command needs its SinkIds");
		Object payload = PayloadAssembler.assemble(keyedValues(transformed), sinkIds, valuePaths);
		if (!(payload instanceof Map<?, ?> map)) {
			throw new IllegalArgumentException("whole-affordance command must be an object");
		}
		Map<String, Object> command = new java.util.LinkedHashMap<>();
		map.forEach((key, value) -> command.put((String) key, value));
		return command;
	}

	/** Parses a Lua result as an authored SinkId map without guessing missing keys. */
	public static Map<String, Object> keyedValues(Object payload) {
		return keyedValues(payload, "request Lua");
	}

	/** The SinkId-keyed values of a Lua result; {@code origin} names the Lua in errors. */
	public static Map<String, Object> keyedValues(Object payload, String origin) {
		if (!(payload instanceof Map<?, ?> map)) {
			throw new IllegalArgumentException(origin + " did not return a SinkId object");
		}
		Map<String, Object> values = new java.util.LinkedHashMap<>();
		map.forEach((key, value) -> {
			if (!(key instanceof String sinkId)) {
				throw new IllegalArgumentException(origin + " SinkId keys must be strings");
			}
			values.put(sinkId, value);
		});
		return values;
	}

	/** Requires Lua to emit the authored caller-reply SinkId; never return the whole SinkId map by accident. */
	public static Object requireSinkValue(Object payload, String sinkId) {
		if (!(payload instanceof Map<?, ?> map) || !map.containsKey(sinkId)) {
			throw new OperationValidationException("DMP_RESPONSE_MAPPING_FAILED", 500,
					"Response Lua omitted caller reply SinkId " + sinkId, java.util.List.of());
		}
		return map.get(sinkId);
	}

	/** Restricts a device reply to the caller Operation's declared output variables. */
	public static Map<String, Object> selectCallerOutputs(Map<String, Object> values,
			java.util.List<String> callerOrder) {
		if (values == null) return null;
		if (callerOrder == null || callerOrder.isEmpty()) return new java.util.LinkedHashMap<>();
		Map<String, Object> selected = new java.util.LinkedHashMap<>();
		for (String name : callerOrder) {
			if (values.containsKey(name)) selected.put(name, values.get(name));
		}
		return selected;
	}

	private static String numericString(Number number) {
		String text = number.toString();
		try {
			java.math.BigDecimal decimal = new java.math.BigDecimal(text);
			if (decimal.stripTrailingZeros().scale() <= 0) {
				return decimal.toBigIntegerExact().toString();
			}
			return decimal.toPlainString();
		} catch (NumberFormatException | ArithmeticException ignored) {
			return text;
		}
	}
}
