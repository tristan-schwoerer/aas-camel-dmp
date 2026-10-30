/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.aas.operation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;

/**
 * The body BaSyx delegates an Operation call with: its input and InOut variables as an {@code OperationVariable[]}.
 * Strings, longs, doubles and booleans become typed Properties, maps collections and lists lists.
 */
public final class Delegation {
	private Delegation() { }

	/** A JSON object of variables (idShort → value) as a delegation body. */
	public static String of(String json) {
		Object value = JsonCodec.parseJson(json);
		if (!(value instanceof Map<?, ?> map)) throw new IllegalArgumentException("variables must be a JSON object: " + json);
		Map<String, Object> values = new LinkedHashMap<>();
		map.forEach((key, member) -> values.put(String.valueOf(key), member));
		return body(values);
	}

	/** A reply's output and InOut variables as one JSON object (idShort → typed value); other bodies as they are. */
	public static String outputs(String reply) {
		if (reply == null || !reply.stripLeading().startsWith("[")) return reply;
		return JsonCodec.toJson(OperationPayloads.flattenOperationVariables(reply));
	}

	/** The AAS API {@code Result} of a failed call reduced to its first message's code and text. */
	public static Map<String, Object> error(String result) {
		Map<?, ?> message = (Map<?, ?>) ((List<?>) ((Map<?, ?>) JsonCodec.parseJson(result)).get("messages")).get(0);
		return Map.of("code", message.get("code"), "error", message.get("text"));
	}

	/** The variables {@code values} (idShort → value) as a delegation body. */
	public static String body(Map<String, ?> values) {
		List<Object> variables = new ArrayList<>();
		values.forEach((idShort, value) -> variables.add(Map.of("value", element(idShort, value))));
		return JsonCodec.toJson(variables);
	}

	private static Map<String, Object> element(String idShort, Object value) {
		Map<String, Object> element = new LinkedHashMap<>();
		if (value instanceof Map<?, ?> members) {
			element.put("modelType", "SubmodelElementCollection");
			if (idShort != null) element.put("idShort", idShort);
			List<Object> children = new ArrayList<>();
			members.forEach((key, member) -> children.add(element(String.valueOf(key), member)));
			element.put("value", children);
			return element;
		}
		if (value instanceof List<?> items) {
			element.put("modelType", "SubmodelElementList");
			if (idShort != null) element.put("idShort", idShort);
			element.put("typeValueListElement", "Property");
			if (!items.isEmpty()) element.put("valueTypeListElement", valueType(items.get(0)));
			List<Object> children = new ArrayList<>();
			for (Object item : items) children.add(element(null, item));
			element.put("value", children);
			return element;
		}
		element.put("modelType", "Property");
		if (idShort != null) element.put("idShort", idShort);
		element.put("valueType", valueType(value));
		if (value != null) element.put("value", String.valueOf(value));
		return element;
	}

	private static String valueType(Object value) {
		if (value instanceof Boolean) return "xs:boolean";
		if (value instanceof Integer || value instanceof Long) return "xs:long";
		if (value instanceof Number) return "xs:double";
		return "xs:string";
	}
}
