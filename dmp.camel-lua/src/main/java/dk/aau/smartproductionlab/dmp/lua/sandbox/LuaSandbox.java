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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.BaseLib;
import org.luaj.vm2.lib.MathLib;
import org.luaj.vm2.lib.StringLib;
import org.luaj.vm2.lib.TableLib;
import org.luaj.vm2.lib.VarArgFunction;
import org.luaj.vm2.lib.ZeroArgFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Whitelisted global environment factory for Lua blobs (SPEC §8).
 *
 * <p>
 * Available to blobs: base functions ({@code assert, error, ipairs, next,
 * pairs, pcall, select, tonumber, tostring, type}, and {@code print}, which
 * logs at DEBUG), {@code math}, {@code table}, {@code string}, a custom
 * {@code json} lib, and {@code os.clock} only.
 *
 * <p>
 * Library functions that work in Java are held to the time budget
 * ({@link LuaTimeoutHook}) or to {@link #MAX_STRING_BYTES}: the pattern
 * functions ({@link LuaPatterns}), {@code string.rep}, {@code table.concat}
 * and {@code json.decode}. They replace LuaJ's in every sandbox and in the
 * process-wide string metatable, which method calls ({@code s:find(p)}) use.
 *
 * <p>
 * Explicitly excluded: {@code os} (except {@code clock}), {@code io},
 * {@code require}/{@code dofile}/{@code loadfile}/{@code load},
 * {@code debug}, {@code luajava}, {@code package}, {@code coroutine},
 * {@code getmetatable}/{@code setmetatable}.
 *
 * <p>
 * LuaJ {@code Globals} are not thread-safe (SPEC §8): the caller creates one
 * sandbox per exchange/invocation.
 *
 * @author aas-camel-dmp
 */
public final class LuaSandbox {

	/** The longest string a library function makes or decodes: 16 MiB. */
	public static final int MAX_STRING_BYTES = 16 * 1024 * 1024;

	private static final Logger PRINT = LoggerFactory.getLogger("dk.aau.smartproductionlab.dmp.lua.print");

	/** Base functions stripped from {@link BaseLib} that are not whitelisted. */
	private static final Set<String> STRIPPED_BASE_FUNCTIONS = new HashSet<>(Arrays.asList(
			"dofile", "loadfile", "load", "getmetatable", "setmetatable", "collectgarbage"));

	static {
		// LuaJ makes the first loaded string library the metatable of every string,
		// process-wide and permanently. Load a private one before any blob runs, so
		// no blob owns the table behind ("..."):method() calls in other sandboxes.
		Globals primer = new Globals();
		LuaValue packageTable = LuaValue.tableOf();
		packageTable.set("loaded", LuaValue.tableOf());
		primer.set("package", packageTable);
		primer.load(new StringLib());
		bound(primer.get("string"));
	}

	private LuaSandbox() {
		throw new IllegalStateException("Utility class");
	}

	/**
	 * Creates a fresh sandboxed {@link Globals} with the whitelisted
	 * environment.
	 */
	public static Globals createGlobals() {
		Globals globals = new Globals();

		// No compiler: blobs arrive compiled (LuaCompiledSource) and load* is stripped.
		// LuaJ library loaders register themselves into package.loaded; provide a
		// minimal package table first (it is removed again before blobs run).
		LuaValue packageTable = LuaValue.tableOf();
		packageTable.set("loaded", LuaValue.tableOf());
		packageTable.set("preload", LuaValue.tableOf());
		globals.set("package", packageTable);

		// whitelisted libraries
		globals.load(new BaseLib());
		globals.load(new TableLib());
		globals.load(new StringLib());
		globals.load(new MathLib());

		// strip non-whitelisted base functions
		STRIPPED_BASE_FUNCTIONS.forEach(name -> globals.set(name, LuaValue.NIL));
		globals.set("print", PRINT_TO_LOG);
		bound(globals.get("string"));
		LuaValue concat = globals.get("table").get("concat");
		globals.get("table").set("concat", new VarArgFunction() {
			@Override
			public Varargs invoke(Varargs args) {
				LuaValue list = args.checktable(1);
				int separator = args.isnoneornil(2) ? 0 : args.checkstring(2).length();
				int first = args.optint(3, 1);
				int last = args.isnoneornil(4) ? list.length() : args.checkint(4);
				long total = 0;
				for (int i = first; i <= last; i++) {
					LuaTimeoutHook.charge(1);
					LuaValue member = list.get(i);
					total += (member.isstring() ? member.checkstring().length() : 0) + (i < last ? separator : 0);
					if (total > MAX_STRING_BYTES) throw tooLong();
				}
				return concat.invoke(args);
			}
		});

		// os: only clock (SPEC §8)
		LuaValue os = LuaValue.tableOf();
		os.set("clock", new ZeroArgFunction() {
			@Override
			public LuaValue call() {
				return LuaValue.valueOf(System.nanoTime() / 1_000_000_000d);
			}
		});
		globals.set("os", os);

		// optional json lib for advanced use
		LuaJsonCodec.install(globals);

		// hide package/require machinery from blobs
		globals.set("package", LuaValue.NIL);

		return globals;
	}

	/** The time-bounded pattern functions and a bounded {@code rep}, in a {@code string} table. */
	private static void bound(LuaValue string) {
		LuaPatterns.install(string);
		LuaValue rep = string.get("rep");
		string.set("rep", new VarArgFunction() {
			@Override
			public Varargs invoke(Varargs args) {
				long count = Math.max(0, args.checkint(2));
				long separator = args.isnoneornil(3) ? 0 : args.checkstring(3).length();
				if (count * args.checkstring(1).length() + Math.max(0, count - 1) * separator > MAX_STRING_BYTES)
					throw tooLong();
				return rep.invoke(args);
			}
		});
	}

	private static LuaError tooLong() {
		return new LuaError("resulting string longer than " + MAX_STRING_BYTES + " bytes");
	}

	/** {@code print}: its arguments, tab-separated, at DEBUG. */
	private static final LuaValue PRINT_TO_LOG = new VarArgFunction() {
		@Override
		public Varargs invoke(Varargs args) {
			if (PRINT.isDebugEnabled()) {
				StringBuilder line = new StringBuilder();
				for (int i = 1; i <= args.narg(); i++) {
					if (i > 1) line.append('\t');
					line.append(args.arg(i).tojstring());
				}
				PRINT.debug("{}", line);
			}
			return NONE;
		}
	};
}
