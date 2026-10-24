/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Supplier;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;

/**
 * Safe identity extraction for repository invalidation events; raw event bodies are never logged.
 *
 * <p>Only the identity ({@link #type()}, {@link #resourceId()}, {@link #elementPath()}) is read up front, by a
 * streaming pass that skips the submodel or element the event carries, because most events are dropped on it.
 * Everything else is derived on first use, once, and then shared: {@link #json()} is the event in the BaSyx
 * Kafka shape ({@code type}, {@code id}, {@code smElementPath}, {@code smElement}, {@code submodel}),
 * {@link #element()} and {@link #submodel()} what it carries, {@link #semanticIds()} the semanticId key values of
 * its submodel (null when it carries none).
 *
 * <p>An event of a type the DMP does not know, and a malformed one, is {@link #ignored()}: it cannot concern a
 * mapping it does not describe. One without the identity of what changed asks for a {@link #fullRescan()}.
 */
public final class AasControlEvent {
	private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(AasControlEvent.class);

	private final String type, resourceId, elementPath;
	private final boolean fullRescan, ignored;
	private final Lazy<JsonObject> event;
	private final Lazy<SubmodelElement> element;
	private final Lazy<Submodel> submodel;
	private final Lazy<Set<String>> semanticIds;

	private AasControlEvent(String type, String resourceId, String elementPath, boolean fullRescan, boolean ignored,
			Supplier<JsonObject> event) {
		this.type = type;
		this.resourceId = resourceId;
		this.elementPath = elementPath;
		this.fullRescan = fullRescan;
		this.ignored = ignored;
		this.event = new Lazy<>(event);
		this.element = new Lazy<>(() -> read(this.event.get(), "smElement", SubmodelElement.class));
		this.submodel = new Lazy<>(() -> read(this.event.get(), "submodel", Submodel.class));
		this.semanticIds = new Lazy<>(() -> this.event.get() == null ? null : semanticIds(this.event.get().get("submodel")));
	}

	public String type() { return type; }
	public String resourceId() { return resourceId; }
	public String elementPath() { return elementPath; }
	public boolean fullRescan() { return fullRescan; }
	/** An event the DMP does not know or cannot read: it changes nothing. */
	public boolean ignored() { return ignored; }
	/** The element an element event carries, or null. */
	public SubmodelElement element() { return identified() ? element.get() : null; }
	/** The submodel a submodel event carries, or null. */
	public Submodel submodel() { return identified() ? submodel.get() : null; }
	public Set<String> semanticIds() { return identified() ? semanticIds.get() : null; }
	/** The event in the BaSyx Kafka shape, parsed once and shared; null when it has no identity. Not to be modified. */
	public JsonObject json() { return identified() ? event.get() : null; }

	private boolean identified() { return !fullRescan && !ignored; }

	@Override public String toString() {
		return "AasControlEvent[type=" + type + ", resourceId=" + resourceId + ", elementPath=" + elementPath
				+ ", fullRescan=" + fullRescan + ", ignored=" + ignored + "]";
	}

	/**
	 * A BaSyx Kafka event, possibly wrapped. The identity is read by streaming over the top-level members; the
	 * whole event is parsed only when something asks for more.
	 */
	public static AasControlEvent parse(Object body) {
		String text;
		try {
			text = body instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8)
					: body instanceof String string ? string : JsonCodec.toJson(body);
		} catch (RuntimeException unreadable) {
			return malformed();
		}
		try (com.google.gson.stream.JsonReader reader = new com.google.gson.stream.JsonReader(new java.io.StringReader(text))) {
			String type = null, submodelId = null, aasId = null, id = null, path = null;
			boolean wrapped = false;
			reader.beginObject();
			while (reader.hasNext()) {
				String name = reader.nextName();
				boolean identity = switch (name) {
				case "type", "submodelId", "aasId", "id", "smElementPath" -> reader.peek() == com.google.gson.stream.JsonToken.STRING;
				default -> false;
				};
				if (!identity) {
					wrapped |= java.util.List.of("event", "payload", "data", "notification").contains(name);
					reader.skipValue();
					continue;
				}
				String value = reader.nextString();
				switch (name) {
				case "type" -> type = value;
				case "submodelId" -> submodelId = value;
				case "aasId" -> aasId = value;
				case "id" -> id = value;
				default -> path = value;
				}
			}
			reader.endObject();
			if (type == null && wrapped) return parseWrapped(text);
			String resourceId = submodelId != null && !submodelId.isBlank() ? submodelId
					: aasId != null && !aasId.isBlank() ? aasId : id;
			return of(type, resourceId, path, () -> eventObject(JsonParser.parseString(text), 0));
		} catch (java.io.IOException | RuntimeException unreadable) {
			return malformed();
		}
	}

	/** An event inside a wrapper object: parsed whole, as there is no identity at the top. */
	private static AasControlEvent parseWrapped(String text) {
		JsonObject event = eventObject(JsonParser.parseString(text), 0);
		if (event == null) return malformed();
		return of(text(event, "type"), first(event, "submodelId", "aasId", "id"), text(event, "smElementPath"), () -> event);
	}

	/**
	 * A BaSyx submodel-repository MQTT event. Its topic carries the event kind and, for element events, the
	 * submodel ID and element path ({@code sm-repository/<repo>/submodels/<base64url id>/submodelElements/<path>/updated});
	 * its payload is the submodel or element. A value write ({@code .../submodelElements/<path>/value/updated}, BaSyx
	 * since 2026-07) reads as an {@code SME_UPDATED} of that element, like the plain {@code /updated} older versions send.
	 * An element payload is parsed only when something asks for it.
	 */
	public static AasControlEvent parseMqtt(String topic, byte[] payload) {
		try {
			String[] levels = topic.split("/");
			if (levels.length < 4 || !"sm-repository".equals(levels[0]) || !"submodels".equals(levels[2])) return ignored("unknown");
			String action = levels[levels.length - 1];
			if (levels.length == 4) {
				// Submodel events name the submodel only in the payload.
				String type = "SM_" + action.toUpperCase(java.util.Locale.ROOT);
				JsonElement body;
				// A submodel changed, but which one cannot be read: everything is looked at again.
				try { body = payload == null || payload.length == 0 ? null : JsonParser.parseString(new String(payload, StandardCharsets.UTF_8)); }
				catch (RuntimeException unreadable) { return rescan(type); }
				if (body == null || !body.isJsonObject()) return rescan(type);
				String id = text(body.getAsJsonObject(), "id");
				return of(type, id, null, () -> {
					JsonObject event = new JsonObject();
					event.addProperty("type", type);
					event.addProperty("id", id);
					event.add("submodel", body);
					return event;
				});
			}
			if (levels.length < 6 || !"submodelElements".equals(levels[4])) return ignored("unknown");
			String id = new String(Base64.getUrlDecoder().decode(levels[3]), StandardCharsets.UTF_8);
			if (levels.length == 6 && "patched".equals(action)) {
				// A bulk element patch: which elements changed is only in the payload.
				return of("SME_PATCHED", id, null, () -> event("SME_PATCHED", id, null, null));
			}
			boolean attachment = levels.length >= 8 && "attachment".equals(levels[levels.length - 2]);
			boolean value = levels.length >= 8 && "value".equals(levels[levels.length - 2]) && "updated".equals(action);
			String path = String.join("/", java.util.Arrays.copyOfRange(levels, 5, levels.length - (attachment || value ? 2 : 1)));
			String type = attachment || value ? "SME_UPDATED" : "SME_" + action.toUpperCase(java.util.Locale.ROOT);
			return of(type, id, path, () -> event(type, id, path,
					attachment || payload == null || payload.length == 0 ? null : JsonParser.parseString(new String(payload, StandardCharsets.UTF_8))));
		} catch (RuntimeException unreadable) {
			return malformed();
		}
	}

	private static JsonObject event(String type, String id, String path, JsonElement element) {
		JsonObject event = new JsonObject();
		event.addProperty("type", type);
		event.addProperty("id", id);
		if (path != null) event.addProperty("smElementPath", path);
		if (element != null) event.add("smElement", element);
		return event;
	}

	private static AasControlEvent of(String type, String id, String path, Supplier<JsonObject> event) {
		if (!switch (type == null ? "" : type) {
		case "SM_CREATED", "SM_UPDATED", "SM_DELETED", "SME_CREATED", "SME_UPDATED", "SME_DELETED", "SME_PATCHED",
				"AAS_CREATED", "AAS_UPDATED", "AAS_DELETED" -> true;
		default -> false;
		}) return ignored(type == null ? "unknown" : type);
		return id == null || id.isBlank() ? rescan(type) : new AasControlEvent(type, id, path, false, false, event);
	}

	private static AasControlEvent rescan(String type) {
		return new AasControlEvent(type, null, null, true, false, () -> null);
	}

	private static AasControlEvent ignored(String type) {
		return new AasControlEvent(type, null, null, false, true, () -> null);
	}

	private static AasControlEvent malformed() {
		LOGGER.warn("ignored a repository event that is not a readable JSON object");
		return ignored("malformed");
	}

	/** {@code member} of the event, as AAS4J reads it; null when absent or unreadable. */
	private static <T> T read(JsonObject event, String member, Class<T> type) {
		if (event == null || !(event.get(member) instanceof JsonObject value)) return null;
		try { return Aas.read(value.toString(), type); }
		catch (RuntimeException unreadable) { return null; }
	}

	private static Set<String> semanticIds(JsonElement submodel) {
		if (submodel == null || !submodel.isJsonObject()) return null;
		Set<String> ids = new LinkedHashSet<>();
		JsonElement semantic = submodel.getAsJsonObject().get("semanticId");
		if (semantic != null && semantic.isJsonObject() && semantic.getAsJsonObject().get("keys") instanceof JsonArray keys)
			for (JsonElement key : keys) if (key.isJsonObject() && text(key.getAsJsonObject(), "value") != null)
				ids.add(text(key.getAsJsonObject(), "value"));
		return Set.copyOf(ids);
	}

	private static JsonObject eventObject(JsonElement value, int depth) {
		if (depth > 4 || value == null || !value.isJsonObject()) return null;
		JsonObject object = value.getAsJsonObject();
		if (object.has("type")) return object;
		for (String wrapper : new String[] { "event", "payload", "data", "notification" }) {
			if (object.has(wrapper)) {
				JsonObject nested = eventObject(object.get(wrapper), depth + 1);
				if (nested != null) return nested;
			}
		}
		return null;
	}

	private static String first(JsonObject event, String... names) {
		for (String name : names) {
			String value = text(event, name);
			if (value != null && !value.isBlank()) return value;
		}
		return null;
	}

	private static String text(JsonObject object, String name) {
		JsonElement value = object.get(name);
		return value == null || value.isJsonNull() || !value.isJsonPrimitive() ? null : value.getAsString();
	}

	/** Computed once, on first use; a payload that fails to parse reads as absent. */
	private static final class Lazy<T> implements Supplier<T> {
		private Supplier<T> source;
		private T value;
		Lazy(Supplier<T> source) { this.source = source; }
		@Override public synchronized T get() {
			if (source != null) {
				try { value = source.get(); }
				catch (RuntimeException malformed) { value = null; }
				source = null;
			}
			return value;
		}
	}
}
