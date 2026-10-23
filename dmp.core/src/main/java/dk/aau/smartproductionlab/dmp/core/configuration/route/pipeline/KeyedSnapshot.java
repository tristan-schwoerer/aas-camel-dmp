package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * An immutable, id-keyed payload — the single unit that flows between the two
 * halves of a mapping route.
 *
 * <p>It is deliberately shared by BOTH directions so the route reads as one
 * symmetric pipeline:
 *
 * <pre>
 *   fan-in   N source contributions ──▶ {@code KeyedSnapshot} keyed by SourceId
 *            (one decode per connector, one merge, gate on completeness)   ──▶ transform
 *   fan-out  transform result         ──▶ {@code KeyedSnapshot} keyed by SinkId
 *            (decoded value tree, then N independent payloads)
 * </pre>
 *
 * <p>Both halves therefore obey the same three rules: the shared value is
 * materialised exactly once, it is never mutated afterwards, and every
 * per-entity payload is derived from that one immutable instance. A sink can
 * never observe a sibling's output, and a source can never observe another's.
 *
 * <p>Text is always text: this class never guesses that a String is JSON.
 * Decoding belongs to a connector that knows the wire content type. A transform
 * may return a scalar or an array, so {@link #root()} carries the whole value.
 */
public final class KeyedSnapshot {


	private final Object root;

	private KeyedSnapshot(Object root) {
		this.root = root;
	}

	/** A snapshot over an already-decoded Java value tree, frozen ({@link JsonCodec#freeze}): frozen members are shared. */
	public static KeyedSnapshot of(Object root) {
		return new KeyedSnapshot(JsonCodec.freeze(root));
	}

	/** The empty keyed snapshot. */
	public static KeyedSnapshot empty() {
		return new KeyedSnapshot(new LinkedHashMap<String, Object>());
	}

	/**
	 * Takes an existing value tree into a snapshot without parsing strings.
	 */
	public static KeyedSnapshot parse(Object body) {
		return body == null ? empty() : of(body);
	}

	/** The whole parsed value, whether keyed or not. */
	public Object root() {
		return root;
	}

	/** True when the value is a JSON object and therefore addressable by id. */
	public boolean isKeyed() {
		return root instanceof Map;
	}

	/** The value stored under {@code id}, or {@code null} when absent. */
	public Object value(String id) {
		if (!(root instanceof Map<?, ?> map)) {
			return null;
		}
		return map.get(id);
	}

	/** Whether {@code id} is present — a stored {@code null} counts as present. */
	public boolean has(String id) {
		return root instanceof Map<?, ?> map && map.containsKey(id);
	}

	@SuppressWarnings("unchecked")
	public Set<String> ids() {
		return root instanceof Map<?, ?> map
				? Collections.unmodifiableSet(((Map<String, Object>) map).keySet())
				: Collections.emptySet();
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> asMap() {
		return root instanceof Map<?, ?> map
				? Collections.unmodifiableMap((Map<String, Object>) map)
				: Collections.emptyMap();
	}

	/** A defensive, ordered copy of the keyed values (empty when not keyed). */
	public Map<String, Object> copyValues() {
		return new LinkedHashMap<>(asMap());
	}

	@Override
	public String toString() {
		return isKeyed() ? "KeyedSnapshot" + ids() : "KeyedSnapshot[" + root + "]";
	}
}
