package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;

/**
 * Protocol-neutral assembly of required SinkId contributions into one payload. The contributions are shared, not
 * copied: only the containers the assembly creates around them are new.
 */
public final class PayloadAssembler {
	private static final Object ABSENT = new Object();

	private PayloadAssembler() {}

	public static Object assemble(Map<String, Object> sinkValues, List<String> sinkIds,
			Map<String, String> valuePaths) {
		if (sinkIds == null || sinkIds.isEmpty()) return new LinkedHashMap<>(sinkValues);
		Object result = ABSENT;
		for (String sinkId : sinkIds) {
			if (!sinkValues.containsKey(sinkId)) {
				throw new IllegalArgumentException("mapping result has no value for SinkId " + sinkId);
			}
			Object value = sinkValues.get(sinkId);
			String path = valuePaths == null ? null : valuePaths.get(sinkId);
			result = merge(result, contribution(JsonPointer.parse(path).tokens(), 0, value));
		}
		return materialize(result);
	}

	private static Object contribution(List<String> tokens, int offset, Object value) {
		if (offset == tokens.size()) return value;
		String token = tokens.get(offset);
		Object child = contribution(tokens, offset + 1, value);
		if (token.startsWith("[") && token.endsWith("]")) {
			int index = Integer.parseInt(token.substring(1, token.length() - 1));
			if (index < 0) throw new IllegalArgumentException("negative JSON array index: " + token);
			List<Object> array = new Assembled.Array();
			// Padding has no authored value and must not conflict with another contribution.
			while (array.size() <= index) array.add(ABSENT);
			array.set(index, child);
			return array;
		}
		Map<String, Object> object = new Assembled.Object();
		object.put(token, child);
		return object;
	}

	private static Object merge(Object existing, Object incoming) {
		if (existing == ABSENT) return incoming;
		if (incoming == ABSENT) return existing;
		if (existing instanceof Map<?, ?> left && incoming instanceof Map<?, ?> right) {
			Map<String, Object> result = new Assembled.Object();
			left.forEach((key, value) -> result.put((String) key, value));
			right.forEach((key, value) -> result.put((String) key,
					result.containsKey(key) ? merge(result.get(key), value) : value));
			return result;
		}
		if (existing instanceof List<?> left && incoming instanceof List<?> right) {
			List<Object> result = new Assembled.Array();
			for (int i = 0; i < Math.max(left.size(), right.size()); i++) {
				result.add(merge(i < left.size() ? left.get(i) : ABSENT,
						i < right.size() ? right.get(i) : ABSENT));
			}
			return result;
		}
		Object left = existing == JsonPointer.JSON_NULL ? null : existing;
		Object right = incoming == JsonPointer.JSON_NULL ? null : incoming;
		if (!Objects.deepEquals(left, right)) throw new IllegalArgumentException("conflicting payload contributions");
		return existing;
	}

	/** Replaces padding with null in the containers the assembly created; contributions are left as they are. */
	private static Object materialize(Object value) {
		if (value == ABSENT) return null;
		if (value instanceof Assembled.Object map) {
			Map<String, Object> result = new LinkedHashMap<>();
			map.forEach((key, member) -> result.put((String) key, materialize(member)));
			return result;
		}
		if (value instanceof Assembled.Array list) {
			List<Object> result = new ArrayList<>(list.size());
			list.forEach(member -> result.add(materialize(member)));
			return result;
		}
		return value;
	}

	/** The containers an assembly creates, told apart from the contributions inside them. */
	private static final class Assembled {
		private static final class Object extends LinkedHashMap<String, java.lang.Object> { }
		private static final class Array extends ArrayList<java.lang.Object> { }
	}
}
