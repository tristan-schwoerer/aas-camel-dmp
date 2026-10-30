/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas.event;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import dk.aau.smartproductionlab.dmp.aas.AasValues;
import dk.aau.smartproductionlab.dmp.aas.http.AasHttp;
import dk.aau.smartproductionlab.dmp.aas.configuration.AasEventSource;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Converts a BaSyx submodel event into an ordinary mapping source
 * contribution: the new value of the watched Property, collection or list.
 * An event on the element itself, on an ancestor or on the whole submodel
 * carries it; an event on a member of a watched collection or list carries
 * only that member (or, for a removed member, nothing), so the element is read again. The processor is
 * transport-neutral: the filter contract is the source's {@link AasEventSource}.
 */
public final class AasElementEventProcessor implements Processor {

	static final String MATCH_PROPERTY = "dmp.aasEventMatch";
	static final String EVENT_ID_HEADER = "AasEvent.EventId";
	static final String EVENT_TYPE_HEADER = "AasEvent.EventType";
	static final String SUBMODEL_ID_HEADER = "AasEvent.SubmodelId";
	static final String ELEMENT_PATH_HEADER = "AasEvent.ElementPath";
	static final String VERSION_HEADER = "AasEvent.Version";
	static final String EVENT_TIME_HEADER = "AasEvent.Timestamp";

	private static final Gson GSON = new Gson();
	private static final Pattern INDEXED_SEGMENT = Pattern.compile("^(.+?)(?:\\[(\\d+)\\])?$");

	private static final java.time.Duration ENRICHMENT_TIMEOUT = java.time.Duration.ofSeconds(5);

	private final AasEventSource source;
	private final Set<String> seenEvents = ConcurrentHashMap.newKeySet();

	public AasElementEventProcessor(AasEventSource source) {
		this.source = source;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		exchange.setProperty(MATCH_PROPERTY, false);
		JsonObject event = eventObject(exchange.getMessage().getBody());
		if (event == null || !matches(event)) {
			return;
		}

		String eventType = text(event, "type");
		String submodelId = text(event, "submodelId");
		if (submodelId == null) {
			submodelId = text(event, "id");
		}
		String elementPath = text(event, "smElementPath");
		String eventId = eventIdentity(exchange, event);
		if (eventId != null && seenEvents.contains(eventId)) {
			return;
		}
		exchange.getMessage().setHeader(EVENT_ID_HEADER, eventId);
		exchange.getMessage().setHeader(EVENT_TYPE_HEADER, eventType);
		exchange.getMessage().setHeader(SUBMODEL_ID_HEADER, submodelId);
		exchange.getMessage().setHeader(ELEMENT_PATH_HEADER, elementPath);
		exchange.getMessage().setHeader(VERSION_HEADER, text(event, "version"));
		exchange.getMessage().setHeader(EVENT_TIME_HEADER,
				text(event, "timestamp") != null ? text(event, "timestamp") : text(event, "eventTime"));

		String watched = source.idShortPath();
		String expectedType = String.valueOf(source.modelType());
		JsonElement value = null;
		if (elementPath != null && watched.equals(elementPath.replace('/', '.'))) {
			value = propertyValue(event);
		} else if (elementPath != null && event.get("smElement") != null
				&& watched.startsWith(elementPath.replace('/', '.'))) {
			String ancestor = elementPath.replace('/', '.');
			String rest = watched.substring(ancestor.length());
			if (rest.startsWith(".")) {
				value = propertyValueFromSubmodel(event.get("smElement"), rest.substring(1), expectedType);
			}
		}
		// A member of a watched collection or list changed: the event carries only the member, so the element is
		// read below.
		if (value == null && ("SM_UPDATED".equals(eventType) || "SM_CREATED".equals(eventType))) {
			value = propertyValueFromSubmodel(event.get("submodel"), watched, expectedType);
		}
		if (value == null) {
			value = readCurrentValue(exchange.getContext());
		}
		if (value == null) {
			throw new IllegalStateException("AAS element event matched but contained no value and enrichment failed");
		}
		if (eventId != null) {
			seenEvents.add(eventId);
			if (seenEvents.size() > 10000) {
				seenEvents.clear();
			}
		}

		// The element's value, typed by its valueType as polled reads deliver it (SPEC §5.3).
		Object decoded = AasValues.elementValue(toJava(value));
		exchange.getMessage().setBody(decoded);
		exchange.setProperty(MATCH_PROPERTY, true);
		exchange.setProperty(Source.CONTRIBUTION_PROPERTY, true);
	}

	private String eventIdentity(Exchange exchange, JsonObject event) {
		String eventId = text(event, "eventId");
		if (eventId != null && !eventId.isBlank()) {
			return eventId;
		}
		Object topic = exchange.getMessage().getHeader("kafka.TOPIC");
		Object partition = exchange.getMessage().getHeader("kafka.PARTITION");
		Object offset = exchange.getMessage().getHeader("kafka.OFFSET");
		if (topic != null && partition != null && offset != null) {
			return topic + "/" + partition + "/" + offset;
		}
		return null;
	}

	private boolean matches(JsonObject event) {
		String expectedSubmodel = source.submodelId();
		String actualSubmodel = text(event, "submodelId");
		if (actualSubmodel == null) {
			actualSubmodel = text(event, "id");
		}
		if (!expectedSubmodel.equals(actualSubmodel)) {
			return false;
		}
		String type = text(event, "type");
		if ("SM_UPDATED".equals(type) || "SM_CREATED".equals(type)) {
			return true;
		}
		boolean deleted = "SME_DELETED".equals(type);
		if (!deleted && !"SME_CREATED".equals(type) && !"SME_UPDATED".equals(type)) {
			return false;
		}
		String expectedPath = source.idShortPath();
		String actualPath = text(event, "smElementPath");
		if (actualPath == null) {
			return false;
		}
		actualPath = actualPath.replace('/', '.');
		if (deleted) {
			// A member of a watched collection or list was removed: the element is read again.
			return actualPath.startsWith(expectedPath + ".") || actualPath.startsWith(expectedPath + "[");
		}
		if (!expectedPath.equals(actualPath)) {
			// An update of an ancestor carries the watched element in its subtree; one of a member of a watched
			// collection or list changes its value.
			return expectedPath.startsWith(actualPath + ".") || expectedPath.startsWith(actualPath + "[")
					|| actualPath.startsWith(expectedPath + ".") || actualPath.startsWith(expectedPath + "[");
		}
		JsonElement element = event.get("smElement");
		return source.modelType() == null || element == null || !element.isJsonObject()
				|| source.modelType().equals(text(element.getAsJsonObject(), "modelType"));
	}

	private JsonElement propertyValue(JsonObject event) {
		JsonElement element = event.get("smElement");
		if (element != null && element.isJsonObject()) {
			JsonObject property = element.getAsJsonObject();
			String expectedType = String.valueOf(source.modelType());
			if (!expectedType.equals(text(property, "modelType"))) {
				return null;
			}
			if (!"Property".equals(expectedType) || property.has("value")) {
				return property;
			}
		}
		return null;
	}

	private JsonElement propertyValueFromSubmodel(JsonElement submodel, String path,
			String expectedType) {
		JsonElement current = submodel;
		for (String segment : path.split("\\.")) {
			Matcher matcher = INDEXED_SEGMENT.matcher(segment);
			if (!matcher.matches()) {
				return null;
			}
			current = findChild(current, matcher.group(1));
			if (current == null) {
				return null;
			}
			if (matcher.group(2) != null) {
				if (!current.isJsonObject() || !current.getAsJsonObject().has("value")
						|| !current.getAsJsonObject().get("value").isJsonArray()) {
					return null;
				}
				int index = Integer.parseInt(matcher.group(2));
				JsonElement values = current.getAsJsonObject().get("value");
				if (index >= values.getAsJsonArray().size()) {
					return null;
				}
				current = values.getAsJsonArray().get(index);
			}
		}
		if (current == null || !current.isJsonObject()) {
			return null;
		}
		JsonObject element = current.getAsJsonObject();
		if (!expectedType.equals(text(element, "modelType")) || !hasValue(element)) {
			return null;
		}
		return element;
	}

	private JsonElement findChild(JsonElement parent, String idShort) {
		if (parent == null || !parent.isJsonObject()) {
			return null;
		}
		JsonObject object = parent.getAsJsonObject();
		for (String key : new String[] {"submodelElements", "value", "statements"}) {
			JsonElement children = object.get(key);
			if (children == null || !children.isJsonArray()) {
				continue;
			}
			for (JsonElement child : children.getAsJsonArray()) {
				if (child.isJsonObject() && idShort.equals(text(child.getAsJsonObject(), "idShort"))) {
					return child;
				}
			}
		}
		return null;
	}

	private JsonElement readCurrentValue(org.apache.camel.CamelContext context) throws Exception {
		String readUrl = source.readUrl();
		if (readUrl == null || readUrl.isBlank()) {
			return null;
		}
		JsonElement body = JsonParser.parseString(AasHttp.of(context).get(readUrl, ENRICHMENT_TIMEOUT));
		return body.isJsonObject() && hasValue(body.getAsJsonObject()) ? body : null;
	}

	/**
	 * Whether {@code element} has a value to deliver. An empty collection or list has: aas4j leaves out an empty
	 * {@code value}, and {@link AasValues#elementValue} reads it as {@code {}} or {@code []}.
	 */
	private static boolean hasValue(JsonObject element) {
		String modelType = text(element, "modelType");
		return element.has("value") || "SubmodelElementCollection".equals(modelType) || "SubmodelElementList".equals(modelType);
	}

	private static JsonObject eventObject(Object body) {
		if (body == null) {
			return null;
		}
		JsonElement parsed;
		try {
			parsed = body instanceof JsonObject shared ? shared : body instanceof String
					? JsonParser.parseString((String) body) : GSON.toJsonTree(body);
		} catch (RuntimeException e) {
			return null;
		}
		if (!parsed.isJsonObject()) {
			return null;
		}
		JsonObject object = parsed.getAsJsonObject();
		JsonElement wrapped = object.get("event");
		return wrapped != null && wrapped.isJsonObject() ? wrapped.getAsJsonObject() : object;
	}

	private static String text(JsonObject object, String name) {
		JsonElement value = object.get(name);
		return value == null || value.isJsonNull() ? null : value.getAsString();
	}

	private static Object toJava(JsonElement value) {
		if (value.isJsonNull()) {
			return JsonPointer.JSON_NULL;
		}
		if (value.isJsonObject()) {
			Map<String, Object> object = new LinkedHashMap<>();
			value.getAsJsonObject().entrySet().forEach(entry ->
					object.put(entry.getKey(), toJava(entry.getValue())));
			return object;
		}
		if (value.isJsonArray()) {
			List<Object> array = new ArrayList<>();
			value.getAsJsonArray().forEach(item -> array.add(toJava(item)));
			return array;
		}
		if (value.getAsJsonPrimitive().isBoolean()) {
			return value.getAsBoolean();
		}
		if (value.getAsJsonPrimitive().isNumber()) {
			String lexical = value.getAsString();
			try {
				return new BigInteger(lexical);
			} catch (NumberFormatException ignored) {
				return new BigDecimal(lexical);
			}
		}
		return value.getAsString();
	}
}
