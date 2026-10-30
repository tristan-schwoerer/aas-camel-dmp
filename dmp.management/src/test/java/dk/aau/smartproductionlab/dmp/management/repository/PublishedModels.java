/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import dk.aau.smartproductionlab.dmp.management.compiler.OperationPaths;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/**
 * Turns an authored model capture into what aas-model publishes: {@code {aas_id}} and {@code {aas_id_short}}
 * resolved ({@code id_injector}), and every Operation given its {@code invocationDelegation} URL
 * ({@code invocation_authoring}): {@code http://dmp:8080} and the Operation's AAS API invoke path,
 * {@code /submodels/{base64url(submodel id)}/submodel-elements/{idShortPath}/invoke}.
 */
public final class PublishedModels {
	public static final String INVOCATION_ORIGIN = "http://dmp:8080";

	private PublishedModels() { }

	public static String publish(String submodelJson, String aasId, String aasIdShort) {
		JsonElement model = JsonParser.parseString(submodelJson);
		String submodelId = model.isJsonObject() && model.getAsJsonObject().has("id")
				? model.getAsJsonObject().get("id").getAsString().replace("{aas_id}", aasId) : null;
		return resolve(model, aasId, aasIdShort, submodelId, null).toString();
	}

	/** {@code path}: the idShortPath of {@code value} below its submodel, null at the submodel itself. */
	private static JsonElement resolve(JsonElement value, String aasId, String aasIdShort, String submodelId, String path) {
		if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isString())
			return new JsonPrimitive(value.getAsString().replace("{aas_id}", aasId).replace("{aas_id_short}", aasIdShort));
		if (value.isJsonArray()) {
			JsonArray array = new JsonArray();
			for (JsonElement item : value.getAsJsonArray()) array.add(resolve(item, aasId, aasIdShort, submodelId, path));
			return array;
		}
		if (!value.isJsonObject()) return value;
		JsonObject object = value.getAsJsonObject();
		boolean element = object.has("idShort") && object.get("idShort").isJsonPrimitive() && !"Submodel".equals(text(object, "modelType"));
		String here = element ? (path == null ? "" : path + ".") + object.get("idShort").getAsString() : path;
		JsonObject result = new JsonObject();
		for (var entry : object.entrySet()) {
			// Elements nest in "value" (collections) and "submodelElements" (the submodel); variables keep their path.
			boolean children = "value".equals(entry.getKey()) || "submodelElements".equals(entry.getKey());
			result.add(entry.getKey(), resolve(entry.getValue(), aasId, aasIdShort, submodelId, children ? here : path));
		}
		if ("Operation".equals(text(object, "modelType")) && submodelId != null) delegate(result, submodelId, here);
		return result;
	}

	private static void delegate(JsonObject operation, String submodelId, String idShortPath) {
		String url = INVOCATION_ORIGIN + OperationPaths.invokePath(submodelId, idShortPath);
		JsonArray qualifiers = operation.has("qualifiers") ? operation.getAsJsonArray("qualifiers") : new JsonArray();
		for (JsonElement qualifier : qualifiers) {
			JsonObject object = qualifier.getAsJsonObject();
			if (!"invocationDelegation".equals(text(object, "type"))) continue;
			object.addProperty("value", object.get("value").getAsString().replace(INVOCATION_ORIGIN + "{operation_path}", url));
			return;
		}
		JsonObject qualifier = new JsonObject();
		qualifier.addProperty("kind", "ConceptQualifier");
		qualifier.addProperty("type", "invocationDelegation");
		qualifier.addProperty("valueType", "xs:string");
		qualifier.addProperty("value", url);
		qualifiers.add(qualifier);
		operation.add("qualifiers", qualifiers);
	}

	private static String text(JsonObject object, String name) {
		JsonElement value = object.get(name);
		return value == null || !value.isJsonPrimitive() ? null : value.getAsString();
	}
}
