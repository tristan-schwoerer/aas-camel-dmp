/*******************************************************************************
 * Copyright (C) 2026 the Eclipse BaSyx Authors
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
package dk.aau.smartproductionlab.dmp.lua.sandbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaValue;

/**
 * Verifies the whitelisted global environment (SPEC §8).
 */
class LuaSandboxTest {

	private final Globals globals = LuaSandbox.createGlobals();

	@Test
	void whitelistedBaseFunctionsAreAvailable() {
		for (String name : new String[] { "assert", "error", "ipairs", "next", "pairs", "pcall",
				"select", "tonumber", "tostring", "type" }) {
			assertTrue(globals.get(name).isfunction(), "expected base function: " + name);
		}
	}

	@Test
	void whitelistedLibrariesAreAvailable() {
		assertTrue(globals.get("math").istable(), "math library missing");
		assertTrue(globals.get("math").get("sqrt").isfunction(), "math.sqrt missing");
		assertTrue(globals.get("table").istable(), "table library missing");
		assertTrue(globals.get("string").istable(), "string library missing");
	}

	@Test
	void jsonLibIsAvailable() {
		assertTrue(globals.get("json").istable(), "json library missing");
		assertTrue(globals.get("json").get("decode").isfunction(), "json.decode missing");
		assertTrue(globals.get("json").get("encode").isfunction(), "json.encode missing");
	}

	@Test
	void osOnlyExposesClock() {
		assertTrue(globals.get("os").istable(), "os table missing");
		assertTrue(globals.get("os").get("clock").isfunction(), "os.clock missing");
		assertTrue(globals.get("os").get("execute").isnil(), "os.execute must not be exposed");
		assertTrue(globals.get("os").get("getenv").isnil(), "os.getenv must not be exposed");
		assertTrue(globals.get("os").get("date").isnil(), "os.date must not be exposed");
	}

	@Test
	void dangerousGlobalsAreExcluded() {
		for (String name : new String[] { "io", "require", "dofile", "loadfile", "load",
				"luajava", "package", "coroutine", "debug", "getmetatable", "setmetatable",
				"collectgarbage" }) {
			LuaValue v = globals.get(name);
			assertTrue(v.isnil() || v.isfunction() == false, "dangerous global not excluded: " + name);
		}
	}

	@Test
	void oneBlobCannotChangeStringMethodsForAnother() throws Exception {
		// LuaJ keeps the string metatable in a static field, so run in a fresh class
		// loader where the poisoning sandbox is the first one ever created.
		java.net.URL[] classpath = java.util.Arrays.stream(System.getProperty("java.class.path")
				.split(java.io.File.pathSeparator)).map(entry -> {
					try { return java.nio.file.Path.of(entry).toUri().toURL(); }
					catch (java.net.MalformedURLException e) { throw new IllegalStateException(e); }
				}).toArray(java.net.URL[]::new);
		try (var loader = new java.net.URLClassLoader(classpath, ClassLoader.getPlatformClassLoader())) {
			var sandbox = loader.loadClass(LuaSandbox.class.getName()).getMethod("createGlobals");
			Object poisoner = sandbox.invoke(null);
			run(loader, poisoner, "string.sub = function() return 'poisoned' end");
			Object other = sandbox.invoke(null);
			run(loader, other, "result = ('abc'):sub(1, 1)");
			Object result = other.getClass().getMethod("get", String.class).invoke(other, "result");
			assertEquals("a", result.getClass().getMethod("tojstring").invoke(result));
		}
	}

	/** Runs {@code chunk} in {@code globals}, both of {@code loader}, compiled as the DMP compiles blobs. */
	private static void run(ClassLoader loader, Object globals, String chunk) throws Exception {
		Class<?> compiled = loader.loadClass(CompiledLuaScript.class.getName());
		Object script = compiled.getMethod("compile", String.class, String.class).invoke(null, chunk, "test");
		Object prototype = script.getClass().getMethod("prototype").invoke(script);
		for (var method : compiled.getMethods())
			if (method.getName().equals("runChunk")) method.invoke(null, prototype, globals);
	}
}
