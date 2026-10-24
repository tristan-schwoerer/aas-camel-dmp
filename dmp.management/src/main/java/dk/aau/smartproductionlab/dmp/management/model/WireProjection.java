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
package dk.aau.smartproductionlab.dmp.management.model;

import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import static dk.aau.smartproductionlab.dmp.aas.model.Aas.child;
import static dk.aau.smartproductionlab.dmp.aas.model.Aas.children;
import static dk.aau.smartproductionlab.dmp.aas.model.Aas.descendants;
import static dk.aau.smartproductionlab.dmp.aas.model.Aas.hasExactSemantic;
import static dk.aau.smartproductionlab.dmp.aas.model.Aas.hasSemantic;
import static dk.aau.smartproductionlab.dmp.aas.model.Aas.nonNull;
import static dk.aau.smartproductionlab.dmp.management.model.WireModel.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements;
import org.eclipse.digitaltwin.aas4j.v3.model.File;
import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.Qualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.Range;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.ReferenceElement;
import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementList;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;

/** Strict AIMC and AID projection of AAS4J submodels into the mapping/interface domain view. */
public final class WireProjection {
	public static final String AIMC = "https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel";
	private static final String AIMC_MAPPING_LIST = "https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/1/0/MappingConfigurations";
	/** A source's Evaluation (DMP extension): whether it triggers its mapping and how its value is obtained. */
	private static final String AIMC_EVALUATION = "https://smartproductionlab.aau.dk/aimc/Evaluation/1/0";
	/** AID 1.1 and 1.2 share the interface structure the DMP reads (1.2 adds protocols and optional fields). */
	private static final java.util.List<String> AID = java.util.List.of(
			"https://admin-shell.io/idta/AssetInterfacesDescription/1/1/Submodel",
			"https://admin-shell.io/idta/AssetInterfacesDescription/1/2/Submodel");
	private static final String AID_INTERFACE = "https://admin-shell.io/idta/AssetInterfacesDescription/1/0/Interface";
	private static final Map<String,ProtocolFamily> PROTOCOL_BINDINGS=Map.of(
			"http://www.w3.org/2011/mqtt",ProtocolFamily.MQTT,
			"http://www.w3.org/2011/http",ProtocolFamily.HTTP,
			"http://www.w3.org/2011/modbus",ProtocolFamily.MODBUS,
			"http://opcfoundation.org/UA/WoT-Binding/",ProtocolFamily.OPCUA,
			"http://www.w3.org/2022/bacnet",ProtocolFamily.BACNET,
			"https://admin-shell.io/idta/AssetInterfacesDescription/1/1/IO-Link",ProtocolFamily.IOLINK,
			"https://admin-shell.io/idta/AssetInterfacesDescription/1/2/SNMP",ProtocolFamily.SNMP);
	private static final String AID_ENDPOINT = "https://admin-shell.io/idta/AssetInterfacesDescription/1/0/EndpointMetadata";
	private static final String AID_INTERACTION = "https://admin-shell.io/idta/AssetInterfacesDescription/1/0/InteractionMetadata";
	private static final String WOT_FORM = "https://www.w3.org/2019/wot/td#hasForm";
	private static final String WOT_PROPERTY = "https://www.w3.org/2019/wot/td#PropertyAffordance";
	private static final String WOT_ACTION = "https://www.w3.org/2019/wot/td#ActionAffordance";
	private static final String WOT_EVENT = "https://www.w3.org/2019/wot/td#EventAffordance";
	private static final String WOT_INPUT = "https://www.w3.org/2019/wot/td#hasInputSchema";
	private static final String WOT_OUTPUT = "https://www.w3.org/2019/wot/td#hasOutputSchema";
	/** An event's {@code data}: the schema of each notification (WoT TD 1.1 §5.3.1.5). */
	private static final String WOT_NOTIFICATION = "https://www.w3.org/2019/wot/td#hasNotificationSchema";
	private static final Set<String> MAPPING_FIELDS = Set.of("Sources","Sinks","DefaultPollingInterval","Transformation",
			"ResponseSources","ResponseSinks","ResponseTransformation");
	/** ExecutionSemantics/TriggerSourceIds gave way to each Source's Evaluation (2026-10-23). */
	private static final Set<String> RETIRED_ID_SHORTS = Set.of("executionpolicy","writedelegation","luamode","transformengine","jsonata",
			"querylanguage","executionsemantics");
	private static final List<String> BINDING_PREFIXES = List.of("mqv_","uav_","modv_","bacv_","iolv_","canv_","snmpv_");
	private WireProjection() { }

	public static boolean isAimc(Submodel submodel) { return hasExactSemantic(submodel, AIMC); }

	/**
	 * Fails closed on a referenced submodel that carries retired declarations or
	 * structurally incomplete elements anywhere in its tree.
	 */
	public static void validate(Submodel submodel) {
		List<SubmodelElement> roots = nonNull(submodel.getSubmodelElements());
		uniqueChildren(roots, "/submodelElements");
		for (int i = 0; i < roots.size(); i++) validateElement(roots.get(i), "/submodelElements/" + i);
	}

	/** {@link #validate(Submodel)} for one consumed element subtree. */
	public static void validate(SubmodelElement element, String path) {
		validateElement(element, path);
	}
	public static boolean isAid(Submodel submodel) { return AID.stream().anyMatch(semantic -> hasExactSemantic(submodel, semantic)); }

	/** Projects every mapping; the first invalid mapping fails the whole AIMC. */
	public static Aimc aimc(Submodel submodel) {
		SubmodelElementList list = mappingList(submodel);
		List<Mapping> mappings = new ArrayList<>();
		Set<String> authored = new LinkedHashSet<>();
		List<SubmodelElement> values = nonNull(list.getValue());
		for (int i = 0; i < values.size(); i++) {
			Mapping mapping = mapping(values.get(i), i);
			if (mapping.authoredId().isPresent() && !authored.add(mapping.authoredId().get()))
				throw error(mappingPath(i) + "/idShort", "duplicate authored mapping identity " + mapping.authoredId().get());
			mappings.add(mapping);
		}
		return new Aimc(submodel, mappings);
	}

	/** Projects valid mappings independently while retaining rejected entries for desired-state isolation. */
	public static Aimc aimcIsolated(Submodel submodel) {
		SubmodelElementList list = mappingList(submodel);
		List<SubmodelElement> values = nonNull(list.getValue());
		Map<String,Integer> identityCounts = new LinkedHashMap<>();
		for (SubmodelElement value : values)
			if (value.getIdShort() != null && !value.getIdShort().isBlank()) identityCounts.merge(value.getIdShort(), 1, Integer::sum);
		List<Mapping> accepted = new ArrayList<>();
		List<MappingFailure> rejected = new ArrayList<>();
		for (int i = 0; i < values.size(); i++) {
			SubmodelElement raw = values.get(i);
			String authored = raw.getIdShort();
			String identity = authored != null && !authored.isBlank() ? authored : "mapping_" + i;
			try {
				if (authored != null && identityCounts.getOrDefault(authored, 0) > 1)
					throw error(mappingPath(i) + "/idShort", "duplicate authored mapping identity " + authored);
				accepted.add(mapping(raw, i));
			} catch (WireProjectionException failure) {
				rejected.add(new MappingFailure(identity, i, failure.pointer(), failure.reason(),
						rejectedReferences(raw), raw));
			}
		}
		return new Aimc(submodel, accepted, rejected);
	}

	private static SubmodelElementList mappingList(Submodel submodel) {
		if (!hasExactSemantic(submodel, AIMC)) throw error("/semanticId", "unsupported semantic version; expected " + AIMC);
		List<SubmodelElement> roots = nonNull(submodel.getSubmodelElements());
		uniqueChildren(roots, "/submodelElements");
		List<Integer> lists = new ArrayList<>();
		for (int i = 0; i < roots.size(); i++) {
			if ("MappingConfigurations".equals(roots.get(i).getIdShort())) lists.add(i);
			else validateElement(roots.get(i), "/submodelElements/" + i);
		}
		if (lists.size() != 1) throw error("/submodelElements", "expected exactly one MappingConfigurations list");
		String path = "/submodelElements/" + lists.get(0);
		if (!(roots.get(lists.get(0)) instanceof SubmodelElementList list))
			throw error(path, "MappingConfigurations must be SubmodelElementList");
		validateListType(list, AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION, path);
		if (!hasExactSemantic(list, AIMC_MAPPING_LIST)) throw error(path + "/semanticId", "unexpected MappingConfigurations semanticId");
		rejectRetiredSemantics(list, path);
		return list;
	}

	private static String mappingPath(int index) { return "/submodelElements/0/value/" + index; }

	private static Mapping mapping(SubmodelElement raw, int i) {
		String path = mappingPath(i);
		if (!(raw instanceof SubmodelElementCollection mapping)) throw error(path, "mapping must be SubmodelElementCollection");
		validateElement(mapping, path);
		for (SubmodelElement child : nonNull(mapping.getValue()))
			if (!MAPPING_FIELDS.contains(child.getIdShort() == null ? "" : child.getIdShort()))
				throw error(path, "unsupported mapping configuration field");
		Optional<String> id = Optional.ofNullable(mapping.getIdShort());
		if (id.isPresent() && id.get().isBlank()) throw error(path + "/idShort", "authored mapping identity must not be blank");
		List<Endpoint> sources = endpoints(requiredList(mapping, "Sources", i), "Source", i);
		List<Endpoint> sinks = endpoints(requiredList(mapping, "Sinks", i), "Sink", i);
		Presence<String> polling = scalarField(mapping, "DefaultPollingInterval", i);
		Presence<Blob> transformation = blobField(mapping, "Transformation", i);
		Presence<List<Endpoint>> responseSources = optionalEndpoints(mapping, "ResponseSources", "Source", i);
		Presence<List<Endpoint>> responseSinks = optionalEndpoints(mapping, "ResponseSinks", "Sink", i);
		Presence<Blob> responseTransformation = blobField(mapping, "ResponseTransformation", i);
		return new Mapping(id, i, sources, sinks, polling, transformation, responseSources, responseSinks,
				responseTransformation, mapping);
	}

	private static List<RejectedReference> rejectedReferences(SubmodelElement mapping) {
		List<RejectedReference> result = new ArrayList<>();
		List<SubmodelElement> nodes = new ArrayList<>(List.of(mapping));
		nodes.addAll(descendants(mapping));
		for (SubmodelElement node : nodes)
			if (node instanceof ReferenceElement reference && reference.getValue() != null
					&& ("Source".equals(node.getIdShort()) || "Sink".equals(node.getIdShort())))
				result.add(new RejectedReference(node.getIdShort(), reference.getValue()));
		return List.copyOf(result);
	}

	/** Rejects retired declarations and structurally incomplete elements anywhere below {@code element}. */
	private static void validateElement(SubmodelElement element, String path) {
		String idShort = element.getIdShort();
		if (idShort != null && RETIRED_ID_SHORTS.contains(idShort.toLowerCase(java.util.Locale.ROOT)))
			throw error(path + "/idShort", "retired declaration is unsupported");
		rejectRetiredSemantics(element, path);
		for (Qualifier qualifier : nonNull(element.getQualifiers()))
			if ("writedelegation".equalsIgnoreCase(qualifier.getType())) throw error(path + "/qualifiers", "retired writeDelegation qualifier");
		if (element instanceof Property property && property.getValueType() == null) throw error(path + "/valueType", "missing required field");
		if (element instanceof Range range && range.getValueType() == null) throw error(path + "/valueType", "missing required field");
		if (element instanceof File file && (file.getContentType() == null || file.getContentType().isBlank()))
			throw error(path + "/contentType", "missing required field");
		if (element instanceof SubmodelElementCollection) uniqueChildren(children(element), path + "/value");
		List<SubmodelElement> children = children(element);
		for (int i = 0; i < children.size(); i++) validateElement(children.get(i), path + "/value/" + i);
	}

	private static void rejectRetiredSemantics(SubmodelElement element, String path) {
		List<Reference> references = new ArrayList<>();
		if (element.getSemanticId() != null) references.add(element.getSemanticId());
		references.addAll(nonNull(element.getSupplementalSemanticIds()));
		for (Reference reference : references) for (Key key : nonNull(reference.getKeys())) {
			String value = key.getValue() == null ? "" : key.getValue().toLowerCase(java.util.Locale.ROOT);
			if (value.startsWith("https://example.org/jsonata/") || value.startsWith("https://admin-shell.io/aas-camel-dmp/transform/jsonata")
					|| value.startsWith("urn:aas-camel-dmp:jsonata"))
				throw error(path + "/semanticId", "retired JSONata transform");
		}
	}

	/** Projects an AID submodel into its interfaces, forms, schemas and affordances. */
	public static Aid aid(Submodel sm) {
		if (!isAid(sm)) throw error("/semanticId", "expected AID 1.1 or 1.2 semantic identity");
		validate(sm);
		List<Interface> interfaces = new ArrayList<>();
		for (SubmodelElement e : nonNull(sm.getSubmodelElements())) if (hasSemantic(e, AID_INTERFACE)) {
			if (e.getIdShort() == null || e.getIdShort().isBlank()) throw error("/submodelElements", "AID interface idShort is required");
			List<String> supplements = nonNull(e.getSupplementalSemanticIds()).stream()
					.flatMap(reference -> nonNull(reference.getKeys()).stream()).map(Key::getValue).toList();
			List<ProtocolFamily> families = supplements.stream().map(PROTOCOL_BINDINGS::get)
					.filter(java.util.Objects::nonNull).distinct().toList();
			if (families.isEmpty()) throw error("/submodelElements/" + e.getIdShort() + "/supplementalSemanticIds", "no supported protocol binding");
			String basePath = "/submodelElements/" + e.getIdShort();
			List<SubmodelElement> nodes = descendants(e);
			List<Form> forms = new ArrayList<>();
			for (SubmodelElement node : nodes) if (hasSemantic(node, WOT_FORM)) forms.addAll(forms(node, basePath + "/forms"));
			List<DataSchema> schemas = nodes.stream().filter(x -> "DataSchema".equals(x.getIdShort()) || "dataSchema".equals(x.getIdShort())
					|| hasSemantic(x, WOT_INPUT) || hasSemantic(x, WOT_OUTPUT) || hasSemantic(x, WOT_NOTIFICATION))
					.map(x -> dataSchema(x, basePath + "/" + x.getIdShort())).toList();
			List<Affordance> properties = affordances(nodes, WOT_PROPERTY, "property", basePath);
			List<Affordance> actions = affordances(nodes, WOT_ACTION, "action", basePath);
			List<Affordance> events = affordances(nodes, WOT_EVENT, "event", basePath);
			Presence<SubmodelElement> endpoint = nodes.stream().filter(x -> hasSemantic(x, AID_ENDPOINT)).findFirst()
					.map(Presence::of).orElseGet(Presence::absent);
			Presence<SubmodelElement> interaction = nodes.stream().filter(x -> hasSemantic(x, AID_INTERACTION)).findFirst()
					.map(Presence::of).orElseGet(Presence::absent);
			Presence<DataSchema> inputSchema = actions.isEmpty() ? Presence.absent() : actions.get(0).input();
			Presence<DataSchema> outputSchema = actions.isEmpty() ? Presence.absent() : actions.get(0).output();
			interfaces.add(new Interface(AID_INTERFACE, e.getIdShort(), supplements, families, e, forms, schemas, properties,
					actions, events, endpoint, interaction, inputSchema, outputSchema, Presence.absent(), Presence.absent()));
		}
		if (interfaces.isEmpty()) throw error("/submodelElements", "no AID interfaces found");
		return new Aid(sm, interfaces);
	}

	/** A form node either is one inline form (it has an {@code href}) or lists several forms. */
	private static List<Form> forms(SubmodelElement node, String path) {
		if (child(node, "href") != null) return List.of(form(node, path));
		List<Form> result = new ArrayList<>();
		for (SubmodelElement raw : children(node)) result.add(form(raw, path));
		return result;
	}

	private static List<Affordance> affordances(List<SubmodelElement> nodes, String semantic, String kind, String path) {
		List<Affordance> result = new ArrayList<>();
		Set<SubmodelElement> projected = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
		String container = switch (kind) { case "property" -> "properties"; case "action" -> "actions"; default -> "events"; };
		for (SubmodelElement element : nodes) if (hasSemantic(element, semantic)) {
			List<SubmodelElement> candidates = container.equals(element.getIdShort()) ? children(element) : List.of(element);
			for (SubmodelElement candidate : candidates) {
				if (!projected.add(candidate)) continue;
				if (candidate != element && candidate.getIdShort() == null) throw error(path + "/" + kind, "AID affordance has no idShort");
				result.add(affordance(candidate, kind, path));
			}
		}
		return List.copyOf(result);
	}

	private static Affordance affordance(SubmodelElement element, String kind, String path) {
		List<SubmodelElement> nodes = descendants(element);
		List<Form> forms = new ArrayList<>();
		for (SubmodelElement node : nodes) if (hasSemantic(node, WOT_FORM)) forms.addAll(forms(node, path + "/" + kind + "/forms"));
		Presence<DataSchema> schema = nodes.stream().filter(x -> "DataSchema".equals(x.getIdShort()) || "dataSchema".equals(x.getIdShort()))
				.findFirst().map(x -> Presence.of(dataSchema(x, path + "/" + kind + "/schema"))).orElseGet(Presence::absent);
		if (!schema.present() && "event".equals(kind))
			schema = nodes.stream().filter(x -> hasSemantic(x, WOT_NOTIFICATION)).findFirst()
					.map(x -> Presence.of(dataSchema(x, path + "/" + kind + "/data"))).orElseGet(Presence::absent);
		if (!schema.present() && "property".equals(kind)
				&& (hasSemantic(element, "https://www.w3.org/2019/wot/json-schema#propertyName") || element instanceof SubmodelElementCollection))
			schema = Presence.of(dataSchema(element, path + "/" + kind + "/schema"));
		Presence<DataSchema> input = nodes.stream().filter(x -> hasSemantic(x, WOT_INPUT)).findFirst()
				.map(x -> Presence.of(dataSchema(x, path + "/" + kind + "/input"))).orElseGet(Presence::absent);
		Presence<DataSchema> output = nodes.stream().filter(x -> hasSemantic(x, WOT_OUTPUT)).findFirst()
				.map(x -> Presence.of(dataSchema(x, path + "/" + kind + "/output"))).orElseGet(Presence::absent);
		Presence<String> idShort = element.getIdShort() == null ? Presence.absent() : Presence.of(element.getIdShort());
		Presence<Reference> semanticId = element.getSemanticId() == null ? Presence.absent() : Presence.of(element.getSemanticId());
		return new Affordance(kind, idShort, semanticId, forms, schema, input, output, element);
	}

	/** An authored string Property; a missing child or a null value is absent. */
	private static Presence<String> stringField(SubmodelElement parent, String name, String path) {
		SubmodelElement child = directChild(parent, name, path);
		if (child == null) return Presence.absent();
		if (!(child instanceof Property property)) throw error(path + "/" + name, "must be Property");
		return property.getValue() == null ? Presence.absent() : Presence.of(property.getValue());
	}

	private static SubmodelElement directChild(SubmodelElement parent, String name, String path) {
		List<SubmodelElement> matches = children(parent).stream().filter(x -> name.equals(x.getIdShort())).toList();
		if (matches.size() > 1) throw error(path + "/" + name, "duplicate consumed child");
		return matches.isEmpty() ? null : matches.get(0);
	}

	private static Form form(SubmodelElement e, String p) {
		if (!(e instanceof SubmodelElementCollection)) throw error(p, "form must be SubmodelElementCollection");
		Map<String,String> mqtt = new LinkedHashMap<>();
		Map<String,String> binding = new LinkedHashMap<>();
		for (SubmodelElement child : children(e)) {
			String name = child.getIdShort();
			// Structured binding terms (bacv_hasDataType, iolv_payloadMapping, …) are no lexical values;
			// the compiler rejects the ones its connector does not represent.
			if (name == null || BINDING_PREFIXES.stream().noneMatch(name::startsWith) || !(child instanceof Property property)) continue;
			if (property.getValue() == null) continue;
			binding.put(name, property.getValue());
			if (name.startsWith("mqv_")) mqtt.put(name, property.getValue());
		}
		return new Form(stringField(e, "href", p), stringField(e, "contentType", p), operations(e, p),
				stringField(e, "htv_methodName", p), mqtt, stringField(e, "mqv_responseTopic", p), binding, e);
	}

	/** The form's {@code op} (hctl:hasOperationType): a list of operation types, lower-cased. */
	private static List<String> operations(SubmodelElement form, String p) {
		SubmodelElement op = directChild(form, "op", p);
		if (op == null) return List.of();
		if (!(op instanceof SubmodelElementList list))
			throw error(p + "/op", "op must be a list of WoT operation types (hctl:hasOperationType)");
		List<String> result = new ArrayList<>();
		for (SubmodelElement member : children(list)) {
			if (!(member instanceof Property property) || property.getValue() == null || property.getValue().isBlank())
				throw error(p + "/op", "op lists operation types as string Properties");
			result.add(property.getValue().trim().toLowerCase(java.util.Locale.ROOT));
		}
		return List.copyOf(result);
	}

	private static DataSchema dataSchema(SubmodelElement e, String p) {
		if (e instanceof Property property) {
			String valueType = Aas.xsd(property.getValueType());
			return new DataSchema(valueType == null ? Presence.absent() : Presence.of(valueType), Presence.absent(),
					Presence.absent(), Presence.absent(), Map.of(), Presence.absent(), Presence.absent(), Map.of(), e);
		}
		if (!(e instanceof SubmodelElementCollection)) throw error(p, "schema must be a collection or typed Property");
		Map<String,DataSchema> properties = new LinkedHashMap<>();
		SubmodelElement props = directChild(e, "properties", p);
		if (props != null && !(props instanceof SubmodelElementCollection))
			throw error(p + "/properties", "schema properties must be SubmodelElementCollection");
		if (props != null) for (SubmodelElement property : children(props)) {
			String name = property.getIdShort();
			if (name == null || name.isBlank()) throw error(p + "/properties", "schema property needs idShort");
			if (properties.putIfAbsent(name, dataSchema(property, p + "/properties/" + name)) != null)
				throw error(p + "/properties/" + name, "duplicate schema property");
		}
		SubmodelElement items = directChild(e, "items", p);
		Presence<DataSchema> item = items == null ? Presence.absent() : Presence.of(dataSchema(items, p + "/items"));
		Presence<List<String>> required = stringArrayField(e, "required", p);
		for (SubmodelElement child : children(e)) {
			String term = child.getIdShort();
			if (term != null && UNSUPPORTED_SCHEMA_TERMS.contains(term))
				throw error(p + "/" + term, "the DMP does not support the schema term " + term);
		}
		String declaredType = e instanceof SubmodelElementCollection && directChild(e, "type", p) instanceof Property t ? t.getValue() : null;
		if (declaredType != null && !SCHEMA_TYPES.contains(declaredType))
			throw error(p + "/type", "unsupported schema type " + declaredType + " (expected one of " + SCHEMA_TYPES + ")");
		Map<String,Object> constraints = new LinkedHashMap<>();
		// AID writes bounds as Ranges (minimum/maximum as min_max, …) and const as a typed Property.
		range(e, "min_max", "minimum", "maximum", false, constraints, p);
		range(e, "lengthRange", "minLength", "maxLength", true, constraints, p);
		range(e, "itemsRange", "minItems", "maxItems", true, constraints, p);
		if (directChild(e, "const", p) instanceof Property constant && constant.getValue() != null)
			constraints.put("const", typedConstant(constant.getValue(), declaredType, p));
		for (String key : List.of("minimum","maximum","exclusiveMinimum","exclusiveMaximum","multipleOf","minLength","maxLength",
				"pattern","minItems","maxItems","uniqueItems","enum","format","contentEncoding","contentMediaType","valueFormat")) {
			SubmodelElement c = directChild(e, key, p);
			if (c == null) continue;
			if ("enum".equals(key)) {
				if (!(c instanceof SubmodelElementList enumeration)) throw error(p + "/enum", "enum must be a SubmodelElementList");
				List<Object> values = new ArrayList<>();
				for (SubmodelElement member : nonNull(enumeration.getValue())) {
					if (!(member instanceof Property property) || property.getValue() == null)
						throw error(p + "/enum", "enum members must be valued Properties");
					values.add(property.getValue());
				}
				constraints.put(key, List.copyOf(values));
				continue;
			}
			if (!(c instanceof Property property)) throw error(p + "/" + key, "schema constraint must be Property");
			String text = property.getValue();
			if (text == null) continue;
			try {
				constraints.put(key, switch (key) {
					case "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf" -> {
						double number = Double.parseDouble(text);
						if (!Double.isFinite(number)) throw new IllegalArgumentException();
						yield JsonCodec.normalizeNumbers(number);
					}
					case "minLength", "maxLength", "minItems", "maxItems" -> {
						long number = Long.parseLong(text);
						if (number < 0) throw new IllegalArgumentException();
						yield number;
					}
					case "uniqueItems" -> {
						if (!text.equalsIgnoreCase("true") && !text.equalsIgnoreCase("false")) throw new IllegalArgumentException();
						yield Boolean.valueOf(text);
					}
					default -> text;
				});
			} catch (IllegalArgumentException invalid) { throw error(p + "/" + key, "invalid typed schema constraint"); }
		}
		// oneOf (jsonschema:oneOf): alternative data schemas, each a collection like this one.
		List<DataSchema> oneOf = new ArrayList<>();
		SubmodelElement alternatives = directChild(e, "oneOf", p);
		if (alternatives != null) {
			if (!(alternatives instanceof SubmodelElementList list) || nonNull(list.getValue()).isEmpty())
				throw error(p + "/oneOf", "oneOf must be a non-empty SubmodelElementList of data schemas");
			List<SubmodelElement> branches = nonNull(list.getValue());
			for (int i = 0; i < branches.size(); i++) {
				if (!(branches.get(i) instanceof SubmodelElementCollection))
					throw error(p + "/oneOf/" + i, "a oneOf alternative must be a data schema collection");
				oneOf.add(dataSchema(branches.get(i), p + "/oneOf/" + i));
			}
		}
		return new DataSchema(stringField(e, "valueType", p), stringField(e, "contentType", p), stringField(e, "valueFormat", p),
				stringField(e, "type", p), properties, item, required, constraints, oneOf, e);
	}

	/** JSON-schema terms whose meaning the DMP cannot honour; an AID using them is rejected, not misread. */
	private static final java.util.Set<String> UNSUPPORTED_SCHEMA_TERMS = java.util.Set.of("anyOf", "allOf", "not",
			"if", "then", "else", "$ref", "prefixItems", "additionalProperties", "patternProperties");
	private static final List<String> SCHEMA_TYPES = List.of("boolean", "integer", "number", "string", "object", "array", "null");

	/** An AID Range ({@code min_max}, {@code lengthRange}, {@code itemsRange}) as its two JSON-schema bounds. */
	private static void range(SubmodelElement schema, String term, String lower, String upper, boolean count,
			Map<String,Object> constraints, String p) {
		SubmodelElement element = directChild(schema, term, p);
		if (element == null) return;
		if (!(element instanceof Range range)) throw error(p + "/" + term, term + " must be a Range");
		try {
			if (range.getMin() != null && !range.getMin().isBlank()) constraints.put(lower, bound(range.getMin(), count));
			if (range.getMax() != null && !range.getMax().isBlank()) constraints.put(upper, bound(range.getMax(), count));
		} catch (IllegalArgumentException invalid) {
			throw error(p + "/" + term, "invalid " + term + " bound");
		}
	}

	private static Object bound(String text, boolean count) {
		if (count) {
			long value = Long.parseLong(text.trim());
			if (value < 0) throw new IllegalArgumentException();
			return value;
		}
		double value = Double.parseDouble(text.trim());
		if (!Double.isFinite(value)) throw new IllegalArgumentException();
		return JsonCodec.normalizeNumbers(value);
	}

	/** A const as the JSON value of the schema's type (AID keeps it as text). */
	private static Object typedConstant(String text, String type, String p) {
		try {
			return switch (type == null ? "" : type) {
				case "integer", "number" -> JsonCodec.normalizeNumbers(Double.parseDouble(text.trim()));
				case "boolean" -> {
					if (!text.equalsIgnoreCase("true") && !text.equalsIgnoreCase("false")) throw new IllegalArgumentException();
					yield Boolean.valueOf(text.trim());
				}
				default -> text;
			};
		} catch (IllegalArgumentException invalid) {
			throw error(p + "/const", "const does not fit the schema type " + type);
		}
	}

	private static List<SubmodelElement> requiredList(SubmodelElementCollection mapping, String id, int index) {
		SubmodelElement found = directChild(mapping, id, mappingPath(index));
		if (found == null) throw error(mappingPath(index), "missing " + id);
		if (!(found instanceof SubmodelElementList list)) throw error(mappingPath(index) + "/" + id, id + " must be SubmodelElementList");
		validateListType(list, AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION, mappingPath(index) + "/" + id);
		return nonNull(list.getValue());
	}

	private static void validateListType(SubmodelElementList list, AasSubmodelElements expected, String path) {
		if (list.getTypeValueListElement() != expected) throw error(path + "/typeValueListElement", "expected " + expected);
	}

	private static List<Endpoint> endpoints(List<SubmodelElement> endpoints, String role, int index) {
		Set<String> ids = new LinkedHashSet<>();
		List<Endpoint> result = new ArrayList<>();
		String identifier = role + "Id";
		for (int i = 0; i < endpoints.size(); i++) {
			String path = mappingPath(index) + "/" + role + "s/value/" + i;
			if (!(endpoints.get(i) instanceof SubmodelElementCollection endpoint)) throw error(path, role + " must be SubmodelElementCollection");
			List<SubmodelElement> fields = nonNull(endpoint.getValue());
			List<ReferenceElement> references = fields.stream().filter(ReferenceElement.class::isInstance).map(ReferenceElement.class::cast).toList();
			if (references.size() != 1) throw error(path, "expected exactly one reference element");
			ReferenceElement reference = references.get(0);
			if (!role.equals(reference.getIdShort())) throw error(path, "reference field must be the declared " + role);
			if (reference.getValue() == null) throw error(path, "missing reference value");
			if (reference.getValue().getType() == null || nonNull(reference.getValue().getKeys()).isEmpty()
					|| nonNull(reference.getValue().getKeys()).stream().anyMatch(key -> key.getType() == null
							|| key.getValue() == null || key.getValue().isBlank()))
				throw error(path + "/" + role, "reference requires a type and typed nonblank keys");
			SubmodelElement idElement = fields.stream().filter(e -> identifier.equals(e.getIdShort())).findFirst().orElse(null);
			if (idElement == null) throw error(path, "missing " + identifier);
			if (!(idElement instanceof Property idProperty) || idProperty.getValue() == null || idProperty.getValue().isBlank())
				throw error(path + "/" + identifier, "must be a non-empty string Property");
			for (SubmodelElement field : fields)
				if (!Set.of(role, identifier, "PollingInterval", "Evaluation").contains(field.getIdShort() == null ? "" : field.getIdShort()))
					throw error(path, "unsupported endpoint field '" + field.getIdShort() + "'");
			if (!ids.add(idProperty.getValue())) throw error(path + "/" + identifier, "duplicate authored " + identifier + " " + idProperty.getValue());
			Presence<String> polling = Presence.absent();
			SubmodelElement pollingElement = directChild(endpoint, "PollingInterval", path);
			if (pollingElement != null) {
				if (!role.equals("Source")) throw error(path + "/PollingInterval", "PollingInterval is a source-only field");
				if (!(pollingElement instanceof Property pollingProperty)) throw error(path + "/PollingInterval", "must be a Property with a value");
				polling = Presence.of(pollingProperty.getValue());
			}
			result.add(new Endpoint(idProperty.getValue(), reference.getValue(), polling, evaluation(endpoint, role, path), endpoint));
		}
		return List.copyOf(result);
	}

	private static Presence<List<Endpoint>> optionalEndpoints(SubmodelElementCollection mapping, String id, String role, int index) {
		SubmodelElement list = directChild(mapping, id, mappingPath(index));
		if (list == null) return Presence.absent();
		if (!(list instanceof SubmodelElementList endpoints)) throw error(mappingPath(index) + "/" + id, id + " must be SubmodelElementList");
		validateListType(endpoints, AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION, mappingPath(index) + "/" + id);
		return Presence.of(endpoints(nonNull(endpoints.getValue()), role, index));
	}

	private static Presence<String> scalarField(SubmodelElementCollection mapping, String name, int index) {
		SubmodelElement e = directChild(mapping, name, mappingPath(index));
		if (e == null) return Presence.absent();
		if (!(e instanceof Property property)) throw error(mappingPath(index) + "/" + name, "must be Property");
		if (property.getValue() == null) throw error(mappingPath(index) + "/" + name + "/value", "missing consumed value");
		return Presence.of(property.getValue());
	}

	private static Presence<Blob> blobField(SubmodelElementCollection mapping, String name, int index) {
		SubmodelElement e = directChild(mapping, name, mappingPath(index));
		if (e == null) return Presence.absent();
		if (!(e instanceof org.eclipse.digitaltwin.aas4j.v3.model.Blob blob)) throw error(mappingPath(index) + "/" + name, "must be Blob");
		if (blob.getValue() == null) return Presence.of(null);
		if (blob.getContentType() == null || blob.getContentType().isBlank())
			throw error(mappingPath(index) + "/" + name + "/contentType", "missing required field");
		return Presence.of(new Blob(blob.getValue(), blob.getContentType()));
	}

	/** A Source's {@code Evaluation/Mode}; which modes the mapping allows is the compiler's to judge. */
	private static Presence<String> evaluation(SubmodelElementCollection endpoint, String role, String path) {
		SubmodelElement element = directChild(endpoint, "Evaluation", path);
		if (element == null) return Presence.absent();
		String at = path + "/Evaluation";
		if (!role.equals("Source")) throw error(at, "Evaluation is a source-only field");
		if (!(element instanceof SubmodelElementCollection evaluation)) throw error(at, "must be SubmodelElementCollection");
		if (!hasExactSemantic(evaluation, AIMC_EVALUATION)) throw error(at + "/semanticId", "expected " + AIMC_EVALUATION);
		for (SubmodelElement child : nonNull(evaluation.getValue()))
			if (!"Mode".equals(child.getIdShort())) throw error(at, "unsupported evaluation field '" + child.getIdShort() + "'");
		SubmodelElement mode = directChild(evaluation, "Mode", at);
		if (!(mode instanceof Property property) || property.getValue() == null)
			throw error(at + "/Mode", "must be a Property with a value");
		if (dk.aau.smartproductionlab.dmp.core.plan.Evaluation.ofAuthored(property.getValue()) == null)
			throw error(at + "/Mode", "expected OnUpdate, OnChange, Cached or ReadAtEvaluation");
		return Presence.of(property.getValue());
	}

	private static Presence<List<String>> stringArrayField(SubmodelElement parent, String name, String path) {
		SubmodelElement e = directChild(parent, name, path);
		if (e == null) return Presence.absent();
		if (!(e instanceof SubmodelElementList list)) throw error(path + "/" + name, "must be SubmodelElementList");
		return stringArrayElement(list, path + "/" + name);
	}

	private static Presence<List<String>> stringArrayElement(SubmodelElementList list, String path) {
		List<String> values = new ArrayList<>();
		List<SubmodelElement> members = nonNull(list.getValue());
		for (int i = 0; i < members.size(); i++) {
			if (!(members.get(i) instanceof Property property) || property.getValue() == null)
				throw error(path + "/value/" + i, "expected string Property");
			values.add(property.getValue());
		}
		return Presence.of(List.copyOf(values));
	}

	private static void uniqueChildren(List<SubmodelElement> children, String path) {
		Set<String> ids = new LinkedHashSet<>();
		for (SubmodelElement child : children)
			if (child.getIdShort() != null && !ids.add(child.getIdShort())) throw error(path, "duplicate child idShort");
	}

	private static WireProjectionException error(String pointer, String message) {
		return new WireProjectionException(pointer.isEmpty() ? "/" : pointer, message);
	}
}
