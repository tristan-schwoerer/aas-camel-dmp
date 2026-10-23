package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;
import org.junit.jupiter.api.Test;

class PayloadAssemblerTest {
	@Test
	void compatibleWholeAndMembersMergeInEitherOrderWithoutChangingInputs() {
		Map<String, Object> whole = new LinkedHashMap<>();
		whole.put("part", new LinkedHashMap<>(Map.of("id", "unit-7")));
		Map<String, Object> values = Map.of("Whole", whole, "Id", "unit-7", "Mode", "RUN");
		Map<String, String> paths = Map.of("Id", "$.part.id", "Mode", "$.part.mode");
		Object expected = Map.of("part", Map.of("id", "unit-7", "mode", "RUN"));
		assertEquals(expected, PayloadAssembler.assemble(values, List.of("Whole", "Id", "Mode"), paths));
		assertEquals(expected, PayloadAssembler.assemble(values, List.of("Mode", "Id", "Whole"), paths));
		assertEquals(Map.of("part", Map.of("id", "unit-7")), whole);
	}

	@Test
	void conflictingWholeAndMemberOverlapFailsInEitherOrder() {
		Map<String, Object> values = Map.of("Whole", Map.of("part", Map.of("id", "a")), "Id", "b");
		for (List<String> ids : List.of(List.of("Whole", "Id"), List.of("Id", "Whole"))) {
			assertThrows(IllegalArgumentException.class,
					() -> PayloadAssembler.assemble(values, ids, Map.of("Id", "$.part.id")));
		}
	}

	@Test
	void disjointArrayIndexesAndNestedMembersDoNotConflictWithPadding() {
		Map<String, Object> values = Map.of("Last", "last", "First", "first", "Name", "unit");
		Map<String, String> paths = Map.of("Last", "$.items[3]", "First", "$.items[0]",
				"Name", "$.items[1].name");
		assertEquals(Map.of("items", Arrays.asList("first", Map.of("name", "unit"), null, "last")),
				PayloadAssembler.assemble(values, List.of("Last", "First", "Name"), paths));
	}

	@Test
	void wholeArrayMergesWithSparseMembersWithoutTreatingAuthoredNullAsPadding() {
		Map<String, Object> values = new LinkedHashMap<>();
		values.put("Whole", Map.of("items", Arrays.asList(null, "middle", "last")));
		values.put("Last", "last");
		values.put("First", "not-null");
		assertEquals(values.get("Whole"), PayloadAssembler.assemble(values,
				List.of("Last", "Whole"), Map.of("Last", "$.items[2]")));
		assertThrows(IllegalArgumentException.class, () -> PayloadAssembler.assemble(values,
				List.of("Whole", "First"), Map.of("First", "$.items[0]")));
	}

	@Test
	void intentionalNullMemberSurvivesAndConflictsWithAnAuthoredNonNull() {
		Map<String, Object> values = new LinkedHashMap<>();
		values.put("Null", null);
		values.put("Marker", JsonPointer.JSON_NULL);
		values.put("Last", "last");
		assertEquals(Map.of("items", Arrays.asList(null, null, "last")), PayloadAssembler.assemble(values,
				List.of("Null", "Last"), Map.of("Null", "$.items[0]", "Last", "$.items[2]")));
		assertNull(((Map<?, ?>) PayloadAssembler.assemble(values, List.of("Null", "Marker"),
				Map.of("Null", "$.value", "Marker", "$.value"))).get("value"));
		assertThrows(IllegalArgumentException.class, () -> PayloadAssembler.assemble(values,
				List.of("Null", "Last"), Map.of("Null", "$.items[0]", "Last", "$.items[0]")));
	}

	@Test
	void everyAuthoredIdIsRequiredEvenIfWholePayloadAlreadyContainsItsMember() {
		assertThrows(IllegalArgumentException.class, () -> PayloadAssembler.assemble(
				Map.of("Whole", Map.of("member", "a")), List.of("Whole", "Missing"),
				Map.of("Missing", "$.member")));
		assertThrows(IllegalArgumentException.class, () -> PayloadAssembler.assemble(
				Map.of("internal-id", "a"), List.of("AuthoredId"), Map.of()));
	}

	@Test
	void selectedWholePreservesNestedMemberNamedLikeItsSinkIdAndSharesItsValues() {
		Map<String, Object> payload = new LinkedHashMap<>();
		payload.put("sinkId", Map.of("inner", "103.55"));
		payload.put("sibling", new ArrayList<>(List.of(7L, true)));
		Object assembled = PayloadAssembler.assemble(Map.of("sinkId", payload), List.of("sinkId"), Map.of());
		assertEquals(payload, assembled);
		assertSame(payload.get("sibling"), ((Map<?, ?>) assembled).get("sibling"), "contributions are shared, not copied");
		assertEquals(List.of("sinkId", "sibling"), new ArrayList<>(((Map<?, ?>) assembled).keySet()));
		byte[] bytes = {0, 1, -1};
		Map<?, ?> binary = (Map<?, ?>) PayloadAssembler.assemble(
				Map.of("Whole", Map.of("bytes", bytes), "Member", bytes),
				List.of("Whole", "Member"), Map.of("Member", "$.bytes"));
		assertArrayEquals(bytes, (byte[]) binary.get("bytes"));
	}

	@Test
	void escapedPointerKeysAndNumericObjectKeysStayLiteral() {
		assertEquals(Map.of("a/b", Map.of("~", "value", "1", true)), PayloadAssembler.assemble(
				Map.of("Text", "value", "Flag", true), List.of("Text", "Flag"),
				Map.of("Text", "/a~1b/~0", "Flag", "$['a/b']['1']")));
		assertThrows(IllegalArgumentException.class, () -> PayloadAssembler.assemble(
				Map.of("Value", "a"), List.of("Value"), Map.of("Value", "$.items[-1]")));
	}
}
