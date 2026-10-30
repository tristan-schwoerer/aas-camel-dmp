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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection;

/**
 * Immutable AIMC/AID domain view over AAS4J metamodel objects. The metamodel
 * itself (submodels, elements, references, qualifiers) is AAS4J; these records
 * only carry the mapping and interface semantics derived from it.
 */
public final class WireModel {
	private WireModel() { }
	public enum ProtocolFamily { MQTT, HTTP, MODBUS, OPCUA, BACNET, IOLINK, SNMP }

	/** Distinguishes an authored optional AIMC/AID field from its absence. */
	public record Presence<T>(boolean present, T value) {
		public static <T> Presence<T> absent() { return new Presence<>(false, null); }
		public static <T> Presence<T> of(T value) { return new Presence<>(true, value); }
	}

	public record Endpoint(String id, Reference reference, Presence<String> pollingInterval, Presence<String> evaluation,
			SubmodelElementCollection element) {
		public Endpoint(String id, Reference reference, SubmodelElementCollection element) {
			this(id, reference, Presence.absent(), Presence.absent(), element);
		}
		public Endpoint(String id, Reference reference, Presence<String> pollingInterval, SubmodelElementCollection element) {
			this(id, reference, pollingInterval, Presence.absent(), element);
		}
	}
	public record Blob(byte[] bytes, String contentType) {
		public Blob { bytes = bytes.clone(); }
		@Override public byte[] bytes() { return bytes.clone(); }
		@Override public boolean equals(Object other) {
			return other instanceof Blob blob && java.util.Arrays.equals(bytes, blob.bytes)
					&& java.util.Objects.equals(contentType, blob.contentType);
		}
		@Override public int hashCode() { return 31 * java.util.Arrays.hashCode(bytes) + java.util.Objects.hashCode(contentType); }
	}
	/** {@code invocationTimeout}: the authored InvocationTimeout (an xs:duration), a DMP extension. */
	public record Mapping(Optional<String> authoredId, int listIndex, List<Endpoint> sources, List<Endpoint> sinks,
			Presence<String> defaultPollingInterval, Presence<Blob> transformation,
			Presence<List<Endpoint>> responseSources, Presence<List<Endpoint>> responseSinks,
			Presence<Blob> responseTransformation, SubmodelElementCollection element, Presence<String> invocationTimeout) {
		public Mapping(Optional<String> authoredId, int listIndex, List<Endpoint> sources, List<Endpoint> sinks,
				Presence<String> defaultPollingInterval, Presence<Blob> transformation,
				Presence<List<Endpoint>> responseSources, Presence<List<Endpoint>> responseSinks,
				Presence<Blob> responseTransformation, SubmodelElementCollection element) {
			this(authoredId, listIndex, sources, sinks, defaultPollingInterval, transformation, responseSources, responseSinks,
					responseTransformation, element, Presence.absent());
		}
		public Mapping { authoredId = authoredId == null ? Optional.empty() : authoredId;
			sources = List.copyOf(sources); sinks = List.copyOf(sinks);
			if (responseSources.present() && responseSources.value() != null) responseSources = Presence.of(List.copyOf(responseSources.value()));
			if (responseSinks.present() && responseSinks.value() != null) responseSinks = Presence.of(List.copyOf(responseSinks.value())); }
	}
	public record RejectedReference(String role, Reference reference) { }
	public record MappingFailure(String identity, int listIndex, String pointer, String message,
			List<RejectedReference> references, SubmodelElement rawMapping) {
		public MappingFailure { references = List.copyOf(references); }
		@Override public String toString() { return "MappingFailure[identity=" + identity + ", listIndex=" + listIndex + ", pointer=" + pointer + "]"; }
	}
	public record Aimc(Submodel submodel, List<Mapping> mappings, List<MappingFailure> mappingFailures) {
		public Aimc(Submodel submodel, List<Mapping> mappings) { this(submodel, mappings, List.of()); }
		public Aimc { mappings = List.copyOf(mappings); mappingFailures = List.copyOf(mappingFailures); }
	}
	public record Aid(Submodel submodel, List<Interface> interfaces) {
		public Aid { interfaces = List.copyOf(interfaces); }
	}
	public record Interface(String familySemanticId, String idShort, List<String> supplementalSemanticIds,
			List<ProtocolFamily> protocolFamilies, SubmodelElement element, List<Form> forms,
			List<DataSchema> schemas, List<Affordance> properties, List<Affordance> actions, List<Affordance> events,
			Presence<SubmodelElement> endpointMetadata, Presence<SubmodelElement> interactionMetadata,
			Presence<DataSchema> actionInputSchema, Presence<DataSchema> actionOutputSchema,
			Presence<OperationVariable> actionInput, Presence<OperationVariable> actionOutput) {
		public Interface { supplementalSemanticIds = List.copyOf(supplementalSemanticIds); protocolFamilies = List.copyOf(protocolFamilies); forms = List.copyOf(forms); schemas = List.copyOf(schemas); properties = List.copyOf(properties); actions = List.copyOf(actions); events = List.copyOf(events); }
	}
	public record Affordance(String kind, Presence<String> idShort, Presence<Reference> semanticId,
			List<Form> forms, Presence<DataSchema> schema, Presence<DataSchema> input,
			Presence<DataSchema> output, SubmodelElement element) {
		public Affordance { forms = List.copyOf(forms); }
	}
	/**
	 * One AID form; protocol binding terms ({@code mqv_*}, {@code uav_*}, …) keep their authored lexical values.
	 * {@code operations} is the authored {@code op} list, lower-cased; empty when {@code op} is absent, in which
	 * case the affordance's default operations apply (aas-model docs/aid-extension.md §3.4).
	 */
	public record Form(Presence<String> href, Presence<String> contentType, List<String> operations,
			Presence<String> httpMethod, Map<String, String> mqttTerms, Presence<String> responseTopic,
			Map<String, String> bindingTerms, SubmodelElement element) {
		public Form { operations = List.copyOf(operations); mqttTerms = immutableMap(mqttTerms); bindingTerms = immutableMap(bindingTerms); }
	}
	/** A WoT data schema; typed constraint values are String, Number, Boolean or a List of enum members. */
	/** A TD DataSchema; {@code oneOf} holds its alternative schemas (empty: none). */
	public record DataSchema(Presence<String> valueType, Presence<String> contentType,
			Presence<String> valueFormat, Presence<String> type, Map<String, DataSchema> properties,
			Presence<DataSchema> items, Presence<List<String>> required, Map<String, Object> constraints,
			List<DataSchema> oneOf, SubmodelElement element) {
		public DataSchema { properties = immutableMap(properties); constraints = immutableMap(constraints);
			oneOf = oneOf == null ? List.of() : List.copyOf(oneOf);
			if (required.present() && required.value() != null) required = Presence.of(List.copyOf(required.value())); }

		public DataSchema(Presence<String> valueType, Presence<String> contentType,
				Presence<String> valueFormat, Presence<String> type, Map<String, DataSchema> properties,
				Presence<DataSchema> items, Presence<List<String>> required, Map<String, Object> constraints,
				SubmodelElement element) {
			this(valueType, contentType, valueFormat, type, properties, items, required, constraints, List.of(), element);
		}
	}

	private static <K,V> Map<K,V> immutableMap(Map<K,V> source) {
		return java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(source));
	}
}
