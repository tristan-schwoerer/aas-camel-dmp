/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.aas.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.eclipse.digitaltwin.aas4j.v3.dataformat.core.DeserializationException;
import org.eclipse.digitaltwin.aas4j.v3.dataformat.core.SerializationException;
import org.eclipse.digitaltwin.aas4j.v3.dataformat.json.JsonDeserializer;
import org.eclipse.digitaltwin.aas4j.v3.dataformat.json.JsonSerializer;
import org.eclipse.digitaltwin.aas4j.v3.model.AnnotatedRelationshipElement;
import org.eclipse.digitaltwin.aas4j.v3.model.BasicEventElement;
import org.eclipse.digitaltwin.aas4j.v3.model.Blob;
import org.eclipse.digitaltwin.aas4j.v3.model.Capability;
import org.eclipse.digitaltwin.aas4j.v3.model.DataTypeDefXsd;
import org.eclipse.digitaltwin.aas4j.v3.model.Entity;
import org.eclipse.digitaltwin.aas4j.v3.model.File;
import org.eclipse.digitaltwin.aas4j.v3.model.HasSemantics;
import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.KeyTypes;
import org.eclipse.digitaltwin.aas4j.v3.model.MultiLanguageProperty;
import org.eclipse.digitaltwin.aas4j.v3.model.Operation;
import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.Range;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.ReferenceElement;
import org.eclipse.digitaltwin.aas4j.v3.model.ReferenceTypes;
import org.eclipse.digitaltwin.aas4j.v3.model.RelationshipElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementList;

import com.google.common.base.CaseFormat;

/**
 * Compact read access to the AAS4J metamodel in the vocabulary of the AAS
 * JSON format: model and key types are their serialized names
 * ({@code SubmodelElementCollection}), value types their XSD names
 * ({@code xs:string}).
 */
public final class Aas {
	private static final JsonDeserializer DESERIALIZER = new JsonDeserializer();
	private static final JsonSerializer SERIALIZER = new JsonSerializer();

	private Aas() { }

	/** Deserializes one AAS JSON value; malformed or metamodel-invalid input is an {@link IllegalArgumentException}. */
	public static <T> T read(String json, Class<T> type) {
		try { return DESERIALIZER.read(json, type); }
		catch (DeserializationException | RuntimeException invalid) {
			throw new IllegalArgumentException("invalid AAS JSON for " + type.getSimpleName(), invalid);
		}
	}

	/** Serializes a list of AAS4J objects to an AAS JSON array. */
	public static String writeList(java.util.Collection<?> values) {
		try { return SERIALIZER.writeList(values); }
		catch (SerializationException failure) { throw new IllegalStateException("AAS serialization failed", failure); }
	}

	public static <T> List<T> readList(String json, Class<T> type) {
		try { return DESERIALIZER.readList(json, type); }
		catch (DeserializationException | RuntimeException invalid) {
			throw new IllegalArgumentException("invalid AAS JSON list of " + type.getSimpleName(), invalid);
		}
	}

	/** Serializes an AAS4J object to its AAS JSON form. */
	public static String write(Object value) {
		try { return SERIALIZER.write(value); }
		catch (SerializationException failure) { throw new IllegalStateException("AAS serialization failed", failure); }
	}

	/** The serialized {@code modelType} of an element, e.g. {@code Property}. */
	public static String modelType(SubmodelElement element) {
		if (element instanceof Property) return "Property";
		if (element instanceof MultiLanguageProperty) return "MultiLanguageProperty";
		if (element instanceof Range) return "Range";
		if (element instanceof Blob) return "Blob";
		if (element instanceof File) return "File";
		if (element instanceof ReferenceElement) return "ReferenceElement";
		if (element instanceof AnnotatedRelationshipElement) return "AnnotatedRelationshipElement";
		if (element instanceof RelationshipElement) return "RelationshipElement";
		if (element instanceof SubmodelElementCollection) return "SubmodelElementCollection";
		if (element instanceof SubmodelElementList) return "SubmodelElementList";
		if (element instanceof Entity) return "Entity";
		if (element instanceof BasicEventElement) return "BasicEventElement";
		if (element instanceof Operation) return "Operation";
		if (element instanceof Capability) return "Capability";
		throw new IllegalArgumentException("unsupported AAS element " + element.getClass().getName());
	}

	/** The serialized key type, e.g. {@code Submodel} or {@code SubmodelElementCollection}. */
	public static String keyType(Key key) {
		return key.getType() == null ? null : CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, key.getType().name());
	}

	public static KeyTypes keyType(String serialized) {
		return KeyTypes.valueOf(CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, serialized));
	}

	public static boolean isModelReference(Reference reference) {
		return reference != null && reference.getType() == ReferenceTypes.MODEL_REFERENCE;
	}

	private static final java.util.Map<DataTypeDefXsd, String> XSD = java.util.Map.ofEntries(
			java.util.Map.entry(DataTypeDefXsd.ANY_URI, "xs:anyURI"), java.util.Map.entry(DataTypeDefXsd.BASE64BINARY, "xs:base64Binary"),
			java.util.Map.entry(DataTypeDefXsd.BOOLEAN, "xs:boolean"), java.util.Map.entry(DataTypeDefXsd.BYTE, "xs:byte"),
			java.util.Map.entry(DataTypeDefXsd.DATE, "xs:date"), java.util.Map.entry(DataTypeDefXsd.DATE_TIME, "xs:dateTime"),
			java.util.Map.entry(DataTypeDefXsd.DECIMAL, "xs:decimal"), java.util.Map.entry(DataTypeDefXsd.DOUBLE, "xs:double"),
			java.util.Map.entry(DataTypeDefXsd.DURATION, "xs:duration"), java.util.Map.entry(DataTypeDefXsd.FLOAT, "xs:float"),
			java.util.Map.entry(DataTypeDefXsd.GDAY, "xs:gDay"), java.util.Map.entry(DataTypeDefXsd.GMONTH, "xs:gMonth"),
			java.util.Map.entry(DataTypeDefXsd.GMONTH_DAY, "xs:gMonthDay"), java.util.Map.entry(DataTypeDefXsd.GYEAR, "xs:gYear"),
			java.util.Map.entry(DataTypeDefXsd.GYEAR_MONTH, "xs:gYearMonth"), java.util.Map.entry(DataTypeDefXsd.HEX_BINARY, "xs:hexBinary"),
			java.util.Map.entry(DataTypeDefXsd.INT, "xs:int"), java.util.Map.entry(DataTypeDefXsd.INTEGER, "xs:integer"),
			java.util.Map.entry(DataTypeDefXsd.LONG, "xs:long"), java.util.Map.entry(DataTypeDefXsd.NEGATIVE_INTEGER, "xs:negativeInteger"),
			java.util.Map.entry(DataTypeDefXsd.NON_NEGATIVE_INTEGER, "xs:nonNegativeInteger"),
			java.util.Map.entry(DataTypeDefXsd.NON_POSITIVE_INTEGER, "xs:nonPositiveInteger"),
			java.util.Map.entry(DataTypeDefXsd.POSITIVE_INTEGER, "xs:positiveInteger"), java.util.Map.entry(DataTypeDefXsd.SHORT, "xs:short"),
			java.util.Map.entry(DataTypeDefXsd.STRING, "xs:string"), java.util.Map.entry(DataTypeDefXsd.TIME, "xs:time"),
			java.util.Map.entry(DataTypeDefXsd.UNSIGNED_BYTE, "xs:unsignedByte"), java.util.Map.entry(DataTypeDefXsd.UNSIGNED_INT, "xs:unsignedInt"),
			java.util.Map.entry(DataTypeDefXsd.UNSIGNED_LONG, "xs:unsignedLong"), java.util.Map.entry(DataTypeDefXsd.UNSIGNED_SHORT, "xs:unsignedShort"));

	/** The XSD name of a value type, e.g. {@code xs:string}, or null. */
	public static String xsd(DataTypeDefXsd type) {
		return type == null ? null : XSD.get(type);
	}

	/** The value type with the given XSD name, e.g. {@code xs:int}. */
	public static DataTypeDefXsd xsd(String name) {
		return XSD.entrySet().stream().filter(entry -> entry.getValue().equals(name)).map(java.util.Map.Entry::getKey)
				.findFirst().orElseThrow(() -> new IllegalArgumentException("unknown XSD value type " + name));
	}

	/** Direct children in serialized order: collection/list values and Operation variable values. */
	public static List<SubmodelElement> children(SubmodelElement element) {
		if (element instanceof SubmodelElementCollection collection) return nonNull(collection.getValue());
		if (element instanceof SubmodelElementList list) return nonNull(list.getValue());
		if (element instanceof Entity entity) return nonNull(entity.getStatements());
		if (element instanceof Operation operation) {
			List<SubmodelElement> values = new ArrayList<>();
			for (List<OperationVariable> variables : List.of(nonNull(operation.getInputVariables()),
					nonNull(operation.getOutputVariables()), nonNull(operation.getInoutputVariables())))
				for (OperationVariable variable : variables) if (variable.getValue() != null) values.add(variable.getValue());
			return List.copyOf(values);
		}
		return List.of();
	}

	public static List<SubmodelElement> descendants(SubmodelElement element) {
		List<SubmodelElement> out = new ArrayList<>();
		for (SubmodelElement child : children(element)) { out.add(child); out.addAll(descendants(child)); }
		return out;
	}

	/** The single direct child with the given idShort, or null. */
	public static SubmodelElement child(SubmodelElement parent, String idShort) {
		return children(parent).stream().filter(child -> idShort.equals(child.getIdShort())).findFirst().orElse(null);
	}

	public static boolean hasSemantic(HasSemantics element, String semantic) {
		return element != null && element.getSemanticId() != null && nonNull(element.getSemanticId().getKeys()).stream()
				.anyMatch(key -> semantic.equals(key.getValue()));
	}

	/** True when the semantic ID is exactly one key with the given value. */
	public static boolean hasExactSemantic(HasSemantics element, String semantic) {
		return element != null && element.getSemanticId() != null && nonNull(element.getSemanticId().getKeys()).size() == 1
				&& semantic.equals(element.getSemanticId().getKeys().get(0).getValue());
	}

	/** Lexical value of a Property, or null. */
	public static String value(SubmodelElement element) {
		return element instanceof Property property ? property.getValue() : null;
	}


	/**
	 * An element's structure without its values, at every depth: Property, File and Blob values, Range bounds
	 * and MultiLanguageProperty texts are dropped (an MLP keeps its languages, which are structure), and a list
	 * keeps one item of each distinct structure, since its length is a value; everything else, references and
	 * qualifiers included, stays. Two elements that differ only in values have equal
	 * structures.
	 */
	public static SubmodelElement withoutValues(SubmodelElement element) {
		if (element == null) return null;
		com.google.gson.JsonElement json = com.google.gson.JsonParser.parseString(write(element));
		stripValues(json);
		return read(json.toString(), SubmodelElement.class);
	}

	private static void stripValues(com.google.gson.JsonElement json) {
		if (json == null || !json.isJsonObject()) return;
		com.google.gson.JsonObject element = json.getAsJsonObject();
		com.google.gson.JsonElement type = element.get("modelType");
		switch (type == null || !type.isJsonPrimitive() ? "" : type.getAsString()) {
		case "Property", "File", "Blob" -> element.remove("value");
		case "Range" -> { element.remove("min"); element.remove("max"); }
		case "MultiLanguageProperty" -> {
			if (element.get("value") instanceof com.google.gson.JsonArray texts)
				for (com.google.gson.JsonElement text : texts)
					if (text.isJsonObject()) text.getAsJsonObject().addProperty("text", "");
		}
		case "SubmodelElementCollection" -> each(element.get("value"), Aas::stripValues);
		case "SubmodelElementList" -> {
			// How many items a list holds is its value: a sink that appends must not change the structure.
			if (element.get("value") instanceof com.google.gson.JsonArray items) {
				java.util.Set<String> shapes = new java.util.LinkedHashSet<>();
				for (com.google.gson.JsonElement item : items) {
					stripValues(item);
					shapes.add(item.toString());
				}
				com.google.gson.JsonArray distinct = new com.google.gson.JsonArray();
				shapes.forEach(shape -> distinct.add(com.google.gson.JsonParser.parseString(shape)));
				element.add("value", distinct);
			}
		}
		case "Entity" -> each(element.get("statements"), Aas::stripValues);
		case "AnnotatedRelationshipElement" -> each(element.get("annotations"), Aas::stripValues);
		case "Operation" -> {
			for (String variables : new String[] { "inputVariables", "outputVariables", "inoutputVariables" })
				each(element.get(variables), variable -> {
					if (variable.isJsonObject()) stripValues(variable.getAsJsonObject().get("value"));
				});
		}
		default -> { }
		}
	}

	private static void each(com.google.gson.JsonElement array, java.util.function.Consumer<com.google.gson.JsonElement> action) {
		if (array != null && array.isJsonArray()) array.getAsJsonArray().forEach(action);
	}

	public static List<Key> keys(Reference reference) {
		return reference == null ? List.of() : nonNull(reference.getKeys());
	}

	public static <T> List<T> nonNull(List<T> values) {
		return values == null ? List.of() : values.stream().filter(Objects::nonNull).toList();
	}
}
