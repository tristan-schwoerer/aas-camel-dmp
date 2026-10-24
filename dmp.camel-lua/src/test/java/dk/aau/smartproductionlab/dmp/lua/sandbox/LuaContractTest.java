/*******************************************************************************
 * Copyright (C) 2026 the Eclipse BaSyx Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons whom the Software is furnished to do so, subject to
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
package dk.aau.smartproductionlab.dmp.lua.sandbox;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;

/**
 * Contract test matrix for the Lua transformation blob (SPEC §8). These
 * tests exercise the same blob files that run in the DMP.
 */
class LuaContractTest {

	private static final double TEST_TIMEOUT_SECONDS = 1.0;

	private String blob(String name) throws IOException {
		return new String(
				getClass().getResourceAsStream("/blobs/" + name).readAllBytes(),
				StandardCharsets.UTF_8);
	}

	@Test
	void test01LoadBlobDefinesAimcMain() throws IOException {
		String script = blob("simple.lua");
		Globals globals = LuaSandbox.createGlobals();
		CompiledLuaScript.runChunk(CompiledLuaScript.compile(script, "simple.lua").prototype(), globals);

		LuaValue aimcMain = globals.get("aimc_main");
		assertTrue(aimcMain.isfunction(), "aimc_main must exist after loading");
	}

	@Test
	void test02CallAimcMainReturnsMappedResult() throws IOException {
		Object result = run(blob("simple.lua"), "@simple.lua",
				java.util.Map.of("a", 3.0, "b", 4.0), TEST_TIMEOUT_SECONDS);

		assertTrue(result instanceof java.util.Map, "result must be a map");
		@SuppressWarnings("unchecked")
		java.util.Map<String, Object> map = (java.util.Map<String, Object>) result;
		// integral results come back as Long (see codecKeepsIntegralNumbersIntegral)
		assertEquals(7L, map.get("sum"));
		assertEquals(12L, map.get("product"));
	}

	@Test
	void test02bMathLibraryWorks() throws IOException {
		Object result = run(blob("math.lua"), "@math.lua",
				java.util.Map.of("source1", 3.0, "source2", 4.0), TEST_TIMEOUT_SECONDS);

		@SuppressWarnings("unchecked")
		java.util.Map<String, Object> map = (java.util.Map<String, Object>) result;
		assertEquals(5L, map.get("hypot"));
		assertEquals(3L, map.get("abs"));
		assertEquals("(3.0, 4.0)", map.get("label"));
	}

	@Test
	void test02cJsonLibWorks() throws IOException {
		Object result = run(blob("usesJson.lua"), "@usesJson.lua",
				java.util.Map.of("body", "{\"a\": 2, \"b\": 3}"), TEST_TIMEOUT_SECONDS);

		@SuppressWarnings("unchecked")
		java.util.Map<String, Object> map = (java.util.Map<String, Object>) result;
		// json.encode renders integral numbers without a decimal tail
		assertEquals("{\"a\":4,\"b\":6}", map.get("encoded"));
		assertEquals("a,b", map.get("keys"));
		assertEquals("AIMC", map.get("upper"));
	}

	@Test
	void jsonCodecPreservesNullAndEmptyContainerShapes() throws IOException {
		Object result = run(
				"function aimc_main(sources)\n"
						+ " local v = json.decode('{\"n\":null,\"a\":[],\"o\":{}}')\n"
						+ " return { n = v.n, a = v.a, o = v.o }\n"
						+ "end\n",
				"@shape.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS);

		@SuppressWarnings("unchecked")
		java.util.Map<String, Object> map = (java.util.Map<String, Object>) result;
		assertTrue(map.containsKey("n"));
		assertEquals(null, map.get("n"));
		assertEquals(java.util.List.of(), map.get("a"));
		assertEquals(java.util.Map.of(), map.get("o"));
	}

	@Test
	void codecRejectsMixedKeyTables() {
		assertThrows(LuaError.class, () -> run(
				"function aimc_main(sources)\n"
						+ " return { out = { [1] = 'x', name = 'y' } }\n"
						+ "end\n",
				"@mixed.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS));
	}

	@Test
	void numericLookingStringKeysStayObjectsThroughLua() {
		for (String key : new String[] { "1", "01", "1.5", "-1" }) {
			java.util.Map<String, Object> input = java.util.Map.of(key, "value");
			Object result = run("""
					function aimc_main(sources)
					 return { native = sources, decoded = json.decode(json.encode(sources)) }
					end
					""", "@numeric-key.lua", input, TEST_TIMEOUT_SECONDS);
			assertEquals(java.util.Map.of("native", input, "decoded", input), result, key);
		}
		java.util.Map<String, Object> expected = java.util.Map.of(
				"1", "one", "01", "leading", "1.5", "fraction", "-1", "negative", "name", "ordinary");
		Object result = run("""
				function aimc_main(sources)
				 local authored = { ["1"] = "one", ["01"] = "leading", ["1.5"] = "fraction",
				                    ["-1"] = "negative", name = "ordinary" }
				 return { native = sources, authored = authored, decoded = json.decode(json.encode(authored)) }
				end
				""", "@mixed-string-keys.lua", expected, TEST_TIMEOUT_SECONDS);
		assertEquals(java.util.Map.of("native", expected, "authored", expected, "decoded", expected), result);
	}

	@Test
	void explicitObjectAndArrayShapesOverrideTableInference() {
		Object result = run("""
				function aimc_main(sources)
				 local object = json.object()
				 object[1] = "one"
				 object[3] = "three"
				 object.name = "ordinary"
				 local array = json.array()
				 array[1] = "01"
				 array[2] = json.null
				 return { object = object, encoded = json.encode(object), array = array,
				          emptyObject = json.encode(json.object()), emptyArray = json.encode(json.array()) }
				end
				""", "@explicit-shapes.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS);
		java.util.Map<?, ?> map = assertInstanceOf(java.util.Map.class, result);
		java.util.Map<String, Object> expected = java.util.Map.of("1", "one", "3", "three", "name", "ordinary");
		assertEquals(expected, map.get("object"));
		assertEquals(expected, dk.aau.smartproductionlab.dmp.core.util.JsonCodec.parseJson((String) map.get("encoded")));
		assertEquals(java.util.Arrays.asList("01", null), map.get("array"));
		assertEquals("{}", map.get("emptyObject"));
		assertEquals("[]", map.get("emptyArray"));
		assertEquals(java.util.Map.of("out", java.util.Map.of("1", "one")), run("""
				function aimc_main(sources)
				 local object = json.object()
				 object[1] = "one"
				 return { out = object }
				end
				""", "@numeric-object.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS));
	}

	@Test
	void arraysRejectStringKeysAndSparseIndices() {
		for (String assignment : new String[] { "a['1'] = 'x'", "a['01'] = 'x'", "a['1.5'] = 'x'",
				"a['-1'] = 'x'", "a.name = 'x'", "a[2] = 'x'", "a[-1] = 'x'", "a[1.5] = 'x'" }) {
			assertThrows(LuaError.class, () -> run(
					"function aimc_main(sources) local a = json.array(); " + assignment + "; return { out = a } end",
					"@invalid-array.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS), assignment);
		}
	}

	@Test
	void explicitDecodeRejectsMalformedBareAndTrailingJson() {
		for (String text : new String[] { "", " \n", "bare", "bare text", "01", "NaN", "Infinity",
				"/* comment */ {}", "{} // comment", "{unquoted:1}", "{'single':1}",
				"{\"a\":}", "{\"a\":1", "[1,]", "[1", "{} {}", "null false",
				"\"valid\" trailing", "{} trailing", "null trailing" }) {
			LuaError error = assertThrows(LuaError.class, () -> run(
					"function aimc_main(sources) return { out = json.decode(sources.text) } end",
					"@invalid-json.lua", java.util.Map.of("text", text), TEST_TIMEOUT_SECONDS), text);
			assertTrue(error.getMessage().contains("json.decode failed"), error.getMessage());
		}
		assertEquals(java.util.Map.of("out", "01"), run(
				"function aimc_main(sources) return { out = json.decode(sources.text) } end",
				"@valid-scalar-json.lua", java.util.Map.of("text", " \"01\" \n"), TEST_TIMEOUT_SECONDS));
	}

	@Test
	void actualLuaPreservesNativeTextBinaryNumbersAndNullMarkers() {
		java.util.Map<String, Object> input = new java.util.LinkedHashMap<>();
		input.put("text", "{\"looks\":\"json\"}");
		input.put("numericText", "01");
		input.put("quotedText", "\"quoted\"");
		input.put("bytes", new byte[] { 0, 1, -1 });
		input.put("large", new BigInteger("9007199254740993"));
		input.put("decimal", new java.math.BigDecimal("0.123456789012345678901"));
		input.put("integer", 1L);
		input.put("fraction", 0.5);
		input.put("null", null);
		java.util.Map<?, ?> result = assertInstanceOf(java.util.Map.class, run(
				"function aimc_main(sources) return sources end", "@native-values.lua", input, TEST_TIMEOUT_SECONDS));
		assertTrue(result.containsKey("null"));
		for (String key : input.keySet()) {
			if (!key.equals("bytes")) assertEquals(input.get(key), result.get(key), key);
		}
		assertArrayEquals((byte[]) input.get("bytes"), (byte[]) result.get("bytes"));
		assertNotSame(input.get("bytes"), result.get("bytes"));
	}

	@Test
	void codecPreservesLargeIntegersOutsideLuaNumberRange() {
		BigInteger value = new BigInteger("9007199254740993");
		assertEquals(value, LuaJsonCodec.fromLua(LuaJsonCodec.toLua(value)));
	}

	/**
	 * An integral value must stay an integer across the Lua boundary. LuaJ
	 * stores every number as a double, so a caller passing {@code 1} (e.g. a
	 * PackML occupation Uuid) used to leave the DMP as {@code 1.0} — which the
	 * device then stored verbatim and echoed back in its process queue.
	 */
	@Test
	void codecKeepsIntegralNumbersIntegral() {
		Object uuid = LuaJsonCodec.fromLua(LuaJsonCodec.toLua(1L));
		assertInstanceOf(Long.class, uuid, "1 must not become a float");
		assertEquals(1L, uuid);

		Object queue = LuaJsonCodec.fromLua(LuaJsonCodec.toLua(java.util.List.of(1L, 2L)));
		assertEquals(java.util.List.of(1L, 2L), queue);
	}

	@Test
	void codecStillEncodesGenuineFractionsAsFloatingPoint() {
		assertEquals(0.5, LuaJsonCodec.fromLua(LuaJsonCodec.toLua(0.5)));
	}

	@Test
	void test03InfiniteLoopTimesOut() throws IOException {
		long start = System.nanoTime();
		var error = assertThrows(LuaTimeoutHook.BudgetExceeded.class, () -> run(
				blob("infinite.lua"), "@infinite.lua", java.util.Map.of(), 0.5));
		long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

		assertTrue(error.getMessage().contains("timed out"), "unexpected error: " + error.getMessage());
		assertTrue(elapsedMs < 10_000, "timeout must fire promptly, took " + elapsedMs + " ms");
	}

	@Test
	void test04BlobCallingOsExecuteFails() throws IOException {
		assertThrows(LuaError.class, () -> run(
				blob("usesOsExecute.lua"), "@usesOsExecute.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS));
	}

	@Test
	void test04bBlobCallingRequireFails() throws IOException {
		assertThrows(LuaError.class, () -> run(
				blob("usesRequire.lua"), "@usesRequire.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS));
	}

	@Test
	void test04cBlobCallingIoFails() throws IOException {
		assertThrows(LuaError.class, () -> run(
				blob("usesIo.lua"), "@usesIo.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS));
	}

	@Test
	void test04dBlobCallingLuajavaFails() throws IOException {
		assertThrows(LuaError.class, () -> run(
				blob("usesLuajava.lua"), "@usesLuajava.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS));
	}

	@Test
	void test05NonTableReturnFailsWithClearError() throws IOException {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> run(blob("nonTable.lua"), "@nonTable.lua",
						java.util.Map.of(), TEST_TIMEOUT_SECONDS));
		assertTrue(ex.getMessage().contains("must return a table"), "unexpected message: " + ex.getMessage());
	}

	/** Evaluates a blob exactly as the lua: producer does, from in-memory source text. */
	private static Object run(String script, String chunkName, Object sources, double timeoutSeconds) {
		Globals globals = LuaSandbox.createGlobals();
		LuaTimeoutHook.install(globals, timeoutSeconds);
		try {
			return evaluate(globals, script, chunkName, sources);
		} finally {
			LuaTimeoutHook.release();
		}
	}

	private static Object evaluate(Globals globals, String script, String chunkName, Object sources) {
		CompiledLuaScript.runChunk(CompiledLuaScript.compile(script, chunkName.replaceFirst("^@", "")).prototype(), globals);
		LuaValue aimcMain = globals.get("aimc_main");
		if (!aimcMain.isfunction()) {
			throw new IllegalArgumentException("Blob must define the AIMC entry point: function aimc_main(sources)");
		}
		LuaValue result = aimcMain.invoke(LuaJsonCodec.toLua(sources)).arg1();
		if (!result.istable()) {
			throw new IllegalArgumentException("Blob must return a table (map keyed by SinkId), got: " + result.typename());
		}
		return LuaJsonCodec.fromLua(result);
	}
}
