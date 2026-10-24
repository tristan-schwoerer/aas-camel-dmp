/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.lua.sandbox;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.StringLib;
import org.luaj.vm2.lib.VarArgFunction;
import org.junit.jupiter.api.Test;

/** The DMP's pattern functions answer as LuaJ's do, and stay within the time budget. */
class LuaPatternsTest {
	private static final LuaValue BOUNDED = LuaSandbox.createGlobals().get("string");
	private static final LuaValue LUAJ = luajStrings();

	private static final String[] SUBJECTS = { "", "a", "abc", "hello world from Lua", "  padded  ",
			"key=value; other = 42", "THE (quick) [brown] {fox}", "a.b.c", "x1y22z333", "aaa", "%d+",
			"line1\nline2\r\n", "caf\u00e9", "(nested (parens) here)", "THE END", "a,b,,c" };
	private static final String[] PATTERNS = { "", "a", "^a", "a$", ".", "%a+", "%d+", "%s*", "%w+", "%p",
			"[aeiou]", "[^%s]+", "[%a_][%w_]*", "(%w+)=(%w+)", "(%w+)%s*=%s*(%w+)", "%b()", "%b[]", "%f[%w]%w+",
			"()a()", "(a*)", "a-b", "a?b", "a+", ".-", "(.-)%s", "^(%s*)(.-)(%s*)$", "%u+", "%l%l", "%x+",
			"[%d.]+", "(a)%1", "(.)%1", "[a-c]+", "[%]]", "%.", "%%", "(h)(e)(l)(l)(o)", "$", "^$", "[^,]*",
			"%z", "%c", "%G", "%S+", "[]]", "[a-]" };

	/** LuaJ's {@code %f} fails at the subject's start and matches nowhere else: it follows Lua 5.2 instead. */
	private static boolean luajCannot(String pattern) {
		return pattern.contains("%f");
	}

	@Test
	void findMatchAndGmatchAnswerAsLuaJDoes() {
		for (String subject : SUBJECTS) for (String pattern : PATTERNS) {
			if (luajCannot(pattern)) continue;
			for (int init : new int[] { 1, 2, -2, 0, 50 }) {
				Varargs args = LuaValue.varargsOf(new LuaValue[] { LuaValue.valueOf(subject), LuaValue.valueOf(pattern),
						LuaValue.valueOf(init) });
				same("find", subject, pattern + " @" + init, args);
				same("match", subject, pattern + " @" + init, args);
			}
			same("find", subject, pattern + " plain", LuaValue.varargsOf(new LuaValue[] { LuaValue.valueOf(subject),
					LuaValue.valueOf(pattern), LuaValue.valueOf(1), LuaValue.TRUE }));
			List<String> expected = iterate(LUAJ, subject, pattern);
			// LuaJ's gmatch does not advance past an empty match: it never ends. Nothing to compare with.
			if (expected.contains("crash") || expected.size() == 100) LUAJ_CRASHES.incrementAndGet();
			else assertEquals(expected, iterate(BOUNDED, subject, pattern),
					"gmatch(" + quoted(subject) + ", " + quoted(pattern) + ")");
		}
	}

	@Test
	void gsubAnswersAsLuaJDoes() {
		LuaTable table = new LuaTable();
		table.set("hello", "HI");
		table.set("a", LuaValue.FALSE);
		LuaValue function = new VarArgFunction() {
			@Override public Varargs invoke(Varargs args) {
				return args.arg1().isstring() && args.arg1().checkjstring().length() == 1 ? NIL : valueOf("<" + args.arg1().tojstring() + ">");
			}
		};
		for (String subject : SUBJECTS) for (String pattern : PATTERNS) {
			if (luajCannot(pattern)) continue;
			for (LuaValue repl : new LuaValue[] { LuaValue.valueOf("[%0]"), LuaValue.valueOf("%1-%%"), LuaValue.valueOf(7),
					table, function }) {
				for (LuaValue max : new LuaValue[] { LuaValue.NIL, LuaValue.valueOf(1) }) {
					same("gsub", subject, pattern + " -> " + repl.tojstring() + " max " + max,
							LuaValue.varargsOf(new LuaValue[] { LuaValue.valueOf(subject), LuaValue.valueOf(pattern), repl, max }));
				}
			}
		}
	}

	@Test
	void malformedPatternsFailAsLuaJ() {
		for (String pattern : new String[] { "%", "[a", "(a", "a)", "%b", "%1", "(a)%2", "%f" }) {
			Varargs args = LuaValue.varargsOf(LuaValue.valueOf("abc"), LuaValue.valueOf(pattern));
			same("find", "abc", pattern, args);
		}
	}

	@Test
	void frontiersAreLua52s() {
		LuaValue find = BOUNDED.get("find");
		assertEquals("1|5", render2(find.invoke(LuaValue.valueOf("hello world"), LuaValue.valueOf("%f[%w]%w+"))));
		assertEquals("7|11", render2(find.invoke(LuaValue.valueOf("hello world"), LuaValue.valueOf("%f[%w]%w+"), LuaValue.valueOf(2))));
		assertEquals("2|2", render2(find.invoke(LuaValue.valueOf("x1y22"), LuaValue.valueOf("%f[%d]%d+"))));
		assertEquals("6|5", render2(find.invoke(LuaValue.valueOf("hello"), LuaValue.valueOf("%f[%z]"))), "an empty match at the end, a frontier to NUL");
	}

	private static String render2(Varargs found) {
		return found.arg1().tojstring() + "|" + found.arg(2).tojstring();
	}

	@Test
	void aBacktrackingPatternEndsWithTheBudget() {
		Globals globals = LuaSandbox.createGlobals();
		LuaTimeoutHook.install(globals, 0.2);
		long started = System.nanoTime();
		try {
			assertThrows(LuaTimeoutHook.BudgetExceeded.class, () -> globals.get("string").get("find").invoke(
					LuaValue.valueOf("a".repeat(2000)), LuaValue.valueOf(".-.-.-b")));
			// The method form uses the process-wide string metatable: bounded as well.
			assertThrows(LuaTimeoutHook.BudgetExceeded.class, () -> LuaValue.valueOf("a".repeat(2000))
					.method("match", LuaValue.valueOf(".-.-.-b")));
		} finally {
			LuaTimeoutHook.release();
		}
		assertTrue(System.nanoTime() - started < 5_000_000_000L, "the budget ends the match promptly");
	}

	/** Cases where LuaJ's own matcher fails with a Java exception or never ends (its bugs): nothing to compare with. */
	private static final java.util.concurrent.atomic.AtomicInteger LUAJ_CRASHES = new java.util.concurrent.atomic.AtomicInteger();

	@org.junit.jupiter.api.AfterAll
	static void report() {
		System.out.println("LuaJ's matcher crashed on " + LUAJ_CRASHES.get() + " compared cases");
	}

	private static void same(String function, String subject, String label, Varargs args) {
		String expected = outcome(LUAJ, function, args);
		String actual = outcome(BOUNDED, function, args);
		if (expected.equals("crash")) {
			LUAJ_CRASHES.incrementAndGet();
			assertTrue(!actual.equals("crash"), function + "(" + quoted(subject) + ", " + label + ") crashed");
			return;
		}
		assertEquals(expected.startsWith("error"), actual.startsWith("error"),
				function + "(" + quoted(subject) + ", " + label + "): LuaJ " + expected + ", DMP " + actual);
		if (!expected.startsWith("error"))
			assertEquals(expected, actual, function + "(" + quoted(subject) + ", " + label + ")");
	}

	private static String outcome(LuaValue strings, String function, Varargs args) {
		try {
			return render(strings.get(function).invoke(args));
		} catch (LuaError error) {
			return "error";
		} catch (RuntimeException crash) {
			return "crash";
		}
	}

	private static List<String> iterate(LuaValue strings, String subject, String pattern) {
		List<String> matches = new ArrayList<>();
		try {
			LuaValue next = strings.get("gmatch").call(LuaValue.valueOf(subject), LuaValue.valueOf(pattern));
			for (int i = 0; i < 100; i++) {
				Varargs found = next.invoke();
				if (found.arg1().isnil()) break;
				matches.add(render(found));
			}
		} catch (LuaError error) {
			matches.add("error");
		} catch (RuntimeException crash) {
			matches.add("crash");
		}
		return matches;
	}

	private static String render(Varargs values) {
		StringBuilder text = new StringBuilder();
		for (int i = 1; i <= values.narg(); i++) text.append(values.arg(i).typename()).append(':').append(values.arg(i).tojstring()).append('|');
		return text.toString();
	}

	private static String quoted(String text) {
		return "\"" + text.replace("\n", "\\n").replace("\r", "\\r") + "\"";
	}

	/** LuaJ's own string library, unbounded. */
	private static LuaValue luajStrings() {
		LuaSandbox.createGlobals(); // the sandbox's primer owns the string metatable
		Globals globals = new Globals();
		LuaTable packageTable = new LuaTable();
		packageTable.set("loaded", new LuaTable());
		globals.set("package", packageTable);
		globals.load(new StringLib());
		return globals.get("string");
	}
}
