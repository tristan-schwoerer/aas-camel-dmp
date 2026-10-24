/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.compiler;

import static dk.aau.smartproductionlab.dmp.management.model.AasFixtures.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.List;

import org.eclipse.digitaltwin.aas4j.v3.model.Operation;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultOperation;
import org.junit.jupiter.api.Test;

import dk.aau.smartproductionlab.dmp.management.model.WireModel;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;

/** What an AAS element target's fingerprint counts: its structure, never its values (SPEC §4.4, §5.3). */
class EffectiveFingerprintTest {
	private static final ConnectorConfigurationFactory.RuntimeSettings RUNTIME =
			new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://repository"));

	@Test
	void aCollectionsMemberValuesAreNotConfiguration() {
		var before = fingerprint(collection("Location", property("x", "xs:double", "1.0"), property("y", "xs:double", "2.0")), null);
		var moved = fingerprint(collection("Location", property("x", "xs:double", "7.5"), property("y", "xs:double", "2.0")), null);
		var retyped = fingerprint(collection("Location", property("x", "xs:int", "1"), property("y", "xs:double", "2.0")), null);
		assertTrue(before.sameEffectiveConfiguration(moved), "a sink writing the collection does not reinstall its mapping");
		assertFalse(before.sameEffectiveConfiguration(retyped), "a member's valueType is structure");
	}

	@Test
	void theOperationOfAVariableSourceCounts() {
		SubmodelElement uuid = property("Uuid", "xs:string", "");
		var original = fingerprint(uuid, operation("http://dmp:8080/operations/a/Occupy", uuid));
		var moved = fingerprint(uuid, operation("http://dmp:8080/operations/a/Reserve", uuid));
		var widened = fingerprint(uuid, operation("http://dmp:8080/operations/a/Occupy", uuid, property("Priority", "xs:int", "")));
		assertFalse(original.sameEffectiveConfiguration(moved), "the delegation URL is the bridge's REST path");
		assertFalse(original.sameEffectiveConfiguration(widened), "the Operation's variables shape the call");
	}

	/** A plan whose one Source targets {@code element}, a variable of {@code operation} when it is given. */
	private static EffectiveFingerprint fingerprint(SubmodelElement element, Operation operation) {
		var resolved = new AasReferenceResolver.ResolvedElement(submodel("urn:data", "Data"), element, operation,
				operation == null ? null : "Skills.Occupy");
		var source = new MappingPlanCompiler.EndpointPlan("source", reference("Submodel", "urn:data", "Property", element.getIdShort()),
				new MappingPlanCompiler.DirectTarget(resolved));
		var plan = new MappingPlanCompiler.MappingPlan("mapping", 0, List.of(source), List.of(), WireModel.Presence.absent(),
				WireModel.Presence.absent(), WireModel.Presence.absent(), WireModel.Presence.absent(), WireModel.Presence.absent(), List.of());
		return EffectiveFingerprint.of(plan, RUNTIME);
	}

	private static Operation operation(String delegationUrl, SubmodelElement... inputs) {
		return new DefaultOperation.Builder().idShort("Occupy").qualifiers(List.of(delegation(delegationUrl)))
				.inputVariables(java.util.Arrays.stream(inputs).map(input -> variable(input)).toList()).build();
	}
}
