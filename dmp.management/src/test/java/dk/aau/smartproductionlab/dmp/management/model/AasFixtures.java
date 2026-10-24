/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.model;

import java.util.Arrays;
import java.util.List;

import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.Operation;
import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.Qualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.QualifierKind;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.ReferenceTypes;
import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultKey;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultOperation;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultOperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultProperty;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultQualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultReference;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodel;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementCollection;

/** Compact AAS4J builders for compiler and resolver tests. */
public final class AasFixtures {
	private AasFixtures() { }

	public static Property property(String idShort, String valueType, String value) {
		return new DefaultProperty.Builder().idShort(idShort).valueType(Aas.xsd(valueType)).value(value).build();
	}

	/** An {@code xs:string} Property. */
	public static Property property(String idShort, String value) {
		return property(idShort, "xs:string", value);
	}

	public static SubmodelElementCollection collection(String idShort, SubmodelElement... children) {
		return new DefaultSubmodelElementCollection.Builder().idShort(idShort).value(Arrays.asList(children)).build();
	}

	/**
	 * {@code endpointMetadata} with the security AID demands of an interface without any: a {@code nosec_sc} in
	 * its {@code securityDefinitions}, referenced from {@code security} (AID 1.1 §2.25). Metadata that selects a
	 * scheme already is returned as it is.
	 */
	public static SubmodelElementCollection nosec(SubmodelElement endpointMetadata, String interfaceIdShort) {
		if (Aas.child(endpointMetadata, "security") != null) return (SubmodelElementCollection) endpointMetadata;
		java.util.List<SubmodelElement> children = new java.util.ArrayList<>(Aas.children(endpointMetadata));
		children.add(securityList(schemeReference(interfaceIdShort, "nosec_sc")));
		children.add(collection("securityDefinitions", collection("nosec_sc", property("scheme", "nosec"))));
		return collection("EndpointMetadata", children.toArray(SubmodelElement[]::new));
	}

	/** A {@code security} list referencing the given schemes. */
	public static SubmodelElement securityList(org.eclipse.digitaltwin.aas4j.v3.model.Reference... schemes) {
		return new org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementList.Builder().idShort("security")
				.typeValueListElement(org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements.REFERENCE_ELEMENT)
				.value(Arrays.stream(schemes).map(scheme -> (SubmodelElement) new org.eclipse.digitaltwin.aas4j.v3.model.impl
						.DefaultReferenceElement.Builder().value(scheme).build()).toList()).build();
	}

	/** The reference to scheme {@code scheme} of interface {@code interfaceIdShort} in the AID {@code urn:aid}. */
	public static org.eclipse.digitaltwin.aas4j.v3.model.Reference schemeReference(String interfaceIdShort, String scheme) {
		return reference("Submodel", "urn:aid", "SubmodelElementCollection", interfaceIdShort, "SubmodelElementCollection",
				"EndpointMetadata", "SubmodelElementCollection", "securityDefinitions", "SubmodelElementCollection", scheme);
	}

	public static OperationVariable variable(SubmodelElement value) {
		return new DefaultOperationVariable.Builder().value(value).build();
	}

	public static Operation operation(String idShort, List<OperationVariable> inputs, List<OperationVariable> outputs,
			List<OperationVariable> inoutputs) {
		return new DefaultOperation.Builder().idShort(idShort).inputVariables(inputs).outputVariables(outputs)
				.inoutputVariables(inoutputs).build();
	}

	/** The BaSyx invocationDelegation qualifier pointing an Operation at {@code url}. */
	public static Qualifier delegation(String url) {
		return new DefaultQualifier.Builder().type("invocationDelegation").valueType(Aas.xsd("xs:string")).value(url)
				.kind(QualifierKind.CONCEPT_QUALIFIER).build();
	}

	public static Submodel submodel(String id, String idShort, SubmodelElement... elements) {
		return new DefaultSubmodel.Builder().id(id).idShort(idShort).submodelElements(Arrays.asList(elements)).build();
	}

	/** A ModelReference from alternating key types and values, e.g. {@code "Submodel", "urn:x", "Property", "State"}. */
	public static Reference reference(String... typesAndValues) {
		List<Key> keys = new java.util.ArrayList<>();
		for (int i = 0; i < typesAndValues.length; i += 2)
			keys.add(new DefaultKey.Builder().type(Aas.keyType(typesAndValues[i])).value(typesAndValues[i + 1]).build());
		return new DefaultReference.Builder().type(ReferenceTypes.MODEL_REFERENCE).keys(keys).build();
	}
}
