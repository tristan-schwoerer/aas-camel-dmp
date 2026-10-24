/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.lua.sandbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;
import org.junit.jupiter.api.Test;

/** The review's reproducers (2026-10-23): what a blob cannot do to the DMP within its budget. */
class LuaBudgetTest {

	@Test
	void pcallCannotSwallowTheTimeout() {
		long started = System.nanoTime();
		assertThrows(LuaTimeoutHook.BudgetExceeded.class, () -> run("""
				function aimc_main(sources)
				    while true do pcall(function() while true do end end) end
				end
				""", 0.3));
		assertTrue(System.nanoTime() - started < 5_000_000_000L, "the budget ends the invocation promptly");
	}

	@Test
	void xpcallCannotSwallowTheTimeoutEither() {
		assertThrows(LuaTimeoutHook.BudgetExceeded.class, () -> run("""
				function aimc_main(sources)
				    while true do xpcall(function() while true do end end, function(e) return e end) end
				end
				""", 0.3));
	}

	@Test
	void aTopLevelLoopIsWithinTheBudget() {
		assertThrows(LuaTimeoutHook.BudgetExceeded.class, () -> run("while true do end", 0.3));
	}

	@Test
	void stringRepCannotAllocateAGigabyte() {
		LuaError error = assertThrows(LuaError.class, () -> run("""
				function aimc_main(sources) return { s = string.rep("x", 2^30) } end
				""", 2));
		assertTrue(error.getMessage().contains("longer than"), error.getMessage());
		assertThrows(LuaError.class, () -> run("""
				function aimc_main(sources) return { s = ("x"):rep(2^30) } end
				""", 2), "the method form is bounded too");
	}

	@Test
	void tableConcatIsBounded() {
		LuaError error = assertThrows(LuaError.class, () -> run("""
				function aimc_main(sources)
				    local chunk = string.rep("x", 1024 * 1024)
				    local parts = {}
				    for i = 1, 20 do parts[i] = chunk end
				    return { s = table.concat(parts) }
				end
				""", 2));
		assertTrue(error.getMessage().contains("longer than"), error.getMessage());
		assertEquals(Map.of("s", "a,b,c"), run("""
				function aimc_main(sources) return { s = table.concat({ "a", "b", "c" }, ",") } end
				""", 2));
	}

	@Test
	void aCyclicResultIsAnErrorNotAStackOverflow() {
		LuaError error = assertThrows(LuaError.class, () -> run("""
				function aimc_main(sources) local t = {} t.self = t return { sink = t } end
				""", 2));
		assertTrue(error.getMessage().contains("nested deeper"), error.getMessage());
	}

	@Test
	void printWritesToTheLogNotToStandardOutput() {
		var out = new java.io.ByteArrayOutputStream();
		var previous = System.out;
		System.setOut(new java.io.PrintStream(out));
		try {
			run("function aimc_main(sources) print('hello', 1) return {} end", 2);
		} finally {
			System.setOut(previous);
		}
		assertEquals("", out.toString());
	}

	@Test
	void identicalSourcesOfDifferentMappingsShareOnePrototype() {
		byte[] source = "function aimc_main(sources) return { shared = true } end".getBytes(java.nio.charset.StandardCharsets.UTF_8);
		var first = LuaCompiledSource.fromBytes(source, "mapping-a", 1 << 20);
		var second = LuaCompiledSource.fromBytes(source, "mapping-b", 1 << 20);
		org.junit.jupiter.api.Assertions.assertSame(first.compiled(), second.compiled());
		assertEquals("mapping-b", second.diagnosticIdentity(), "each mapping keeps its own name for errors");
	}

	private static Object run(String script, double timeoutSeconds) {
		Globals globals = LuaSandbox.createGlobals();
		LuaTimeoutHook.install(globals, timeoutSeconds);
		try {
			CompiledLuaScript.runChunk(CompiledLuaScript.compile(script, "budget").prototype(), globals);
			LuaValue main = globals.get("aimc_main");
			return LuaJsonCodec.fromLua(main.invoke(LuaJsonCodec.toLua(Map.of())).arg1());
		} finally {
			LuaTimeoutHook.release();
		}
	}
}
