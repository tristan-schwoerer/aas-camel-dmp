package dk.aau.smartproductionlab.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.eclipse.digitaltwin.aas4j.v3.model.DataTypeDefXsd;
import org.eclipse.digitaltwin.aas4j.v3.model.KeyTypes;
import org.eclipse.digitaltwin.aas4j.v3.model.Qualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.QualifierKind;
import org.eclipse.digitaltwin.aas4j.v3.model.ReferenceTypes;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultKey;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultProperty;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultQualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultReference;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementList;
import org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements;
import org.junit.jupiter.api.Test;

/** Optional marks which Operation variable values may be absent at invocation; everything else is required. */
class OptionalQualifierTest {

	static Qualifier optionalQualifier(String value) {
		return new DefaultQualifier.Builder().type(OptionalQualifier.TYPE).kind(QualifierKind.CONCEPT_QUALIFIER)
				.valueType(DataTypeDefXsd.BOOLEAN).value(value)
				.semanticId(new DefaultReference.Builder().type(ReferenceTypes.EXTERNAL_REFERENCE)
						.keys(new DefaultKey.Builder().type(KeyTypes.GLOBAL_REFERENCE).value(OptionalQualifier.SEMANTIC_ID).build())
						.build())
				.build();
	}

	static SubmodelElement property(String idShort, String optional) {
		var builder = new DefaultProperty.Builder().idShort(idShort).valueType(DataTypeDefXsd.STRING);
		if (optional != null) builder.qualifiers(List.of(optionalQualifier(optional)));
		return builder.build();
	}

	private static RuntimeException missing(String path) {
		return new IllegalStateException(path);
	}

	@Test
	void aVariableIsRequiredUnlessMarkedOptional() {
		assertFalse(OptionalQualifier.optional(property("State", null)));
		assertFalse(OptionalQualifier.optional(property("State", "false")));
		assertTrue(OptionalQualifier.optional(property("Outcome", "true")));
		assertTrue(OptionalQualifier.optional(property("Outcome", "1")));
	}

	@Test
	void theQualifierIsRecognisedBySemanticIdNotByType() {
		var unrelated = new DefaultProperty.Builder().idShort("Outcome").valueType(DataTypeDefXsd.STRING)
				.qualifiers(List.of(new DefaultQualifier.Builder().type("Optional").valueType(DataTypeDefXsd.BOOLEAN)
						.value("true").build())).build();
		assertFalse(OptionalQualifier.optional(unrelated), "a qualifier without the semantic ID is someone else's");
	}

	@Test
	void anUnreadableValueOrAMarkedListMemberIsAnAuthoringError() {
		assertEquals(List.of("Outcome: the Optional qualifier needs an xs:boolean value, got yes"),
				OptionalQualifier.problems(property("Outcome", "yes")));
		var list = new DefaultSubmodelElementList.Builder().idShort("Samples")
				.typeValueListElement(AasSubmodelElements.PROPERTY).valueTypeListElement(DataTypeDefXsd.STRING)
				.value(List.of(property(null, "true"))).build();
		assertEquals(List.of("Samples/items: list members share one template; mark the list itself Optional"),
				OptionalQualifier.problems(list));
	}

	@Test
	void requiredValuesAndRequiredCollectionMembersMustBePresent() {
		var result = new DefaultSubmodelElementCollection.Builder().idShort("Result")
				.value(List.of(property("State", null), property("Detail", "true"))).build();
		OptionalQualifier.requirePresent(result, true, Map.of("State", "SUCCESS"), "Result", OptionalQualifierTest::missing);
		var failure = assertThrows(IllegalStateException.class, () -> OptionalQualifier.requirePresent(result, true,
				Map.of("Detail", "x"), "Result", OptionalQualifierTest::missing));
		assertEquals("Result/State", failure.getMessage());
		assertThrows(IllegalStateException.class,
				() -> OptionalQualifier.requirePresent(property("State", null), true, null, "State", OptionalQualifierTest::missing));
		OptionalQualifier.requirePresent(property("Outcome", "true"), false, null, "Outcome", OptionalQualifierTest::missing);
	}

	@Test
	void theCallerMustGiveEveryRequiredInputBeforeTheDeviceIsCalled() throws Exception {
		Map<String, SubmodelElement> inputs = new LinkedHashMap<>();
		inputs.put("Uuid", property("Uuid", null));
		inputs.put("Volume", property("Volume", "true"));
		var processor = new OperationRequestProcessor(inputs, Map.of("Dispensing", "$"));
		Exchange given = new DefaultExchange(new DefaultCamelContext());
		given.getMessage().setBody(Delegation.body(Map.of("Uuid", "u-1")));
		processor.process(given);
		assertEquals(Map.of("Dispensing", Map.of("Uuid", "u-1")), given.getMessage().getBody());

		Exchange omitted = new DefaultExchange(new DefaultCamelContext());
		omitted.getMessage().setBody(Delegation.body(Map.of("Volume", 1.5)));
		var failure = assertThrows(OperationValidationException.class, () -> processor.process(omitted));
		assertEquals("DMP_OPERATION_VALIDATION_FAILED", failure.getCode());
		assertEquals(400, failure.getStatus());
		assertEquals(List.of("Uuid"), failure.getViolations());
	}
}
