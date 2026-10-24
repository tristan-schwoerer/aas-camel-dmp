/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas.event;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Hands repository element events from the control-plane consumer to event-sourced AAS Property routes in the
 * same Camel context, so value changes reach routes without polling or a second broker subscription.
 *
 * <p>Events use the BaSyx Kafka shape ({@code type}, {@code id}, {@code smElementPath}, {@code smElement},
 * {@code submodel}); the MQTT consumer converts its topic and payload into that shape first. An event is parsed
 * once and the same tree is handed to every listener it concerns, which only read it.
 */
public final class AasEventHub {
	/** One subscribed element. */
	interface Listener {
		String submodelId();
		String idShortPath();
		void deliver(JsonObject event, Map<String, Object> headers);
	}

	private final Map<String, Set<Listener>> listeners = new ConcurrentHashMap<>();
	private volatile java.util.function.Consumer<Boolean> presence = ignored -> { };

	synchronized void register(Listener listener) {
		boolean first = listeners.isEmpty();
		listeners.computeIfAbsent(listener.submodelId(), ignored -> new CopyOnWriteArraySet<>()).add(listener);
		if (first) presence.accept(true);
	}

	synchronized void unregister(Listener listener) {
		listeners.computeIfPresent(listener.submodelId(), (id, set) -> { set.remove(listener); return set.isEmpty() ? null : set; });
		if (listeners.isEmpty()) presence.accept(false);
	}

	/** True while any route listens; the value feed runs only then. */
	public boolean hasListeners() { return !listeners.isEmpty(); }

	/**
	 * Told when the first route starts listening ({@code true}) and when the last one stops ({@code false}).
	 * The callback must not block.
	 */
	public synchronized void onPresenceChanged(java.util.function.Consumer<Boolean> callback) {
		presence = callback == null ? ignored -> { } : callback;
		presence.accept(hasListeners());
	}

	/** True when some route listens to an element of this submodel. */
	public boolean listensTo(String submodelId) {
		return submodelId != null && listeners.containsKey(submodelId);
	}

	/**
	 * Delivers an event to every listener it can concern: the element itself, an ancestor of it (an ancestor
	 * update carries the subtree), or a whole-submodel update.
	 */
	public void publish(String event, Map<String, Object> headers) {
		try {
			JsonElement parsed = JsonParser.parseString(event);
			if (parsed.isJsonObject()) publish(parsed.getAsJsonObject(), headers);
		} catch (RuntimeException malformed) { /* not an event */ }
	}

	/** Delivers a parsed event; the listeners share {@code object} and must not modify it. */
	public void publish(JsonObject object, Map<String, Object> headers) {
		if (object == null) return;
		String id = text(object, "id");
		Set<Listener> candidates = id == null ? null : listeners.get(id);
		if (candidates == null) return;
		String type = text(object, "type");
		String path = text(object, "smElementPath");
		for (Listener listener : candidates) {
			boolean concerned = switch (type == null ? "" : type) {
				case "SME_CREATED", "SME_UPDATED" -> path != null && within(listener.idShortPath(), path);
				case "SM_CREATED", "SM_UPDATED" -> true;
				default -> false;
			};
			if (concerned) listener.deliver(object, headers);
		}
	}

	private static boolean within(String path, String ancestor) {
		return path.equals(ancestor) || path.startsWith(ancestor + ".") || path.startsWith(ancestor + "[");
	}

	private static String text(JsonObject object, String name) {
		JsonElement value = object.get(name);
		return value == null || !value.isJsonPrimitive() ? null : value.getAsString();
	}
}
