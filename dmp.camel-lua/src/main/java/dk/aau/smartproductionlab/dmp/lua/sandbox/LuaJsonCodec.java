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

import java.util.ArrayList;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;


/**
 * Value bridge between decoded Java payload trees and native Lua values.
 *
 * <p>
 * The optional in-blob {@code json} library is registered by
 * {@link #install(org.luaj.vm2.Globals)} as {@code json.decode} /
 * {@code json.encode} for advanced use.
 *
 * @author aas-camel-dmp
 */
public final class LuaJsonCodec {

	/** Largest integer a Lua (double) number represents exactly: 2^53 - 1. */
	private static final double MAX_EXACT_LUA_INTEGER = 9007199254740991.0;
	private static final Object NULL_MARKER = new Object();
	/** The deepest value converted either way; deeper (or a table that contains itself) is an error. */
	static final int MAX_DEPTH = 200;

	private LuaJsonCodec() {
	}

	/**
	 * Registers the {@code json} table in the given globals.
	 */
	public static void install(LuaValue globals) {
		LuaValue json = LuaValue.tableOf();
		json.set("decode", new DecodeFunction());
		json.set("encode", new EncodeFunction());
		json.set("null", new LuaUserdata(NULL_MARKER));
		json.set("array", new ArrayFunction());
		json.set("object", new ObjectFunction());
		globals.set("json", json);
	}

	/**
	 * Converts a decoded Java payload value to a real Lua value
	 * (LuaTable for maps/lists, LuaNumber, LuaString, LuaBoolean). JSON null is
	 * exposed as {@code json.null}; use {@code json.array()} and
	 * {@code json.object()} when an empty table's shape matters. Does NOT use
	 * {@code CoerceJavaToLua} so blobs operate on genuine Lua values (and no
	 * JVM reflection / module access is involved).
	 */
	public static LuaValue toLua(Object jsonValue) {
		return toLua(jsonValue, 0);
	}

	private static LuaValue toLua(Object jsonValue, int depth) {
		if (jsonValue == null || jsonValue == JsonPointer.JSON_NULL) {
			return new LuaUserdata(NULL_MARKER);
		}
		if (jsonValue instanceof Map) {
			LuaTable table = new JsonObjectTable();
			for (Map.Entry<?, ?> entry : ((Map<?, ?>) jsonValue).entrySet()) {
				table.set(String.valueOf(entry.getKey()), toLua(entry.getValue(), deeper(depth)));
			}
			return table;
		}
		if (jsonValue instanceof List) {
			LuaTable table = new JsonArrayTable();
			int i = 1;
			for (Object item : (List<?>) jsonValue) {
				table.set(i++, toLua(item, deeper(depth)));
			}
			return table;
		}
		if (jsonValue instanceof Boolean) {
			return LuaValue.valueOf((Boolean) jsonValue);
		}
		if (jsonValue instanceof Number) {
			if (!isLuaExact((Number) jsonValue)) {
				return new LuaUserdata(new JsonNumber((Number) jsonValue));
			}
			return LuaValue.valueOf(((Number) jsonValue).doubleValue());
		}
		if (jsonValue instanceof byte[]) {
			return new LuaUserdata(new JsonBinary((byte[]) jsonValue));
		}
		return LuaValue.valueOf(jsonValue.toString());
	}

	/** Converts a Lua value to a JSON-compatible decoded Java value. */
	public static Object fromLua(LuaValue value) {
		return fromLua(value, 0);
	}

	private static int deeper(int depth) {
		if (depth >= MAX_DEPTH)
			throw new LuaError("value nested deeper than " + MAX_DEPTH + " levels (a table that contains itself?)");
		return depth + 1;
	}

	private static Object fromLua(LuaValue value, int depth) {
		if (value.isnil()) {
			return null;
		}
		if (value instanceof LuaUserdata) {
			Object userdata = value.touserdata();
			if (userdata == NULL_MARKER) {
				return null;
			}
			if (userdata instanceof JsonNumber number) {
				return number.value;
			}
			if (userdata instanceof JsonBinary binary) {
				return binary.value.clone();
			}
		}
		if (value.isboolean()) {
			return value.toboolean();
		}
		// LuaJ's isnumber() is coercive: a Lua string like "103.5" also
		// reports as numeric. Test the actual Lua type before numeric conversion.
		if (value.type() == LuaValue.TSTRING) {
			return value.tojstring();
		}
		if (value.type() == LuaValue.TNUMBER) {
			double number = value.todouble();
			// LuaJ stores EVERY number as a double, so without this an integer
			// that passed through a blob (e.g. a PackML occupation Uuid, or a
			// process queue of them) would leave the DMP as 1.0 and the device
			// would store/echo the float. Emit an integral double as a Long;
			// genuine fractions stay floating point.
			if (Double.isFinite(number)
					&& number == Math.rint(number)
					&& Math.abs(number) <= MAX_EXACT_LUA_INTEGER) {
				return (long) number;
			}
			return number;
		}
		if (value.istable()) {
			return toJavaMapOrList(value.checktable(), deeper(depth));
		}
		return value.tojstring();
	}

	private static Object toJavaMapOrList(LuaTable table, int depth) {
		boolean hasNumeric = false;
		boolean hasOther = false;
		int maxIndex = 0;
		int numericCount = 0;
		LuaValue key = LuaValue.NIL;
		while (true) {
			Varargs n = table.next(key);
			if (n.arg1().isnil()) {
				break;
			}
			key = n.arg1();
			if (!(table instanceof JsonObjectTable) && key.type() == LuaValue.TNUMBER
					&& key.toint() > 0 && key.todouble() == key.toint()) {
				hasNumeric = true;
				maxIndex = Math.max(maxIndex, key.toint());
				numericCount++;
			} else {
				hasOther = true;
			}
		}
		if ((hasNumeric && hasOther)
				|| (table instanceof JsonArrayTable && hasOther)) {
			throw new LuaError("cannot encode a mixed-key Lua table as JSON; use json.array() or json.object()");
		}
		if (hasNumeric) {
			if (maxIndex != numericCount) {
				throw new LuaError("cannot encode a sparse Lua array as JSON");
			}
			return readArray(table, depth);
		}
		if (table instanceof JsonArrayTable) {
			return List.of();
		}
		Map<String, Object> map = new LinkedHashMap<>();
		key = LuaValue.NIL;
		while (true) {
			Varargs n = table.next(key);
			if (n.arg1().isnil()) {
				break;
			}
			key = n.arg1();
			map.put(key.tojstring(), fromLua(n.arg(2), depth));
		}
		return map;
	}

	private static List<Object> readArray(LuaTable table, int depth) {
		List<Object> list = new ArrayList<>();
		for (int i = 1;; i++) {
			LuaValue item = table.get(i);
			if (item.isnil()) {
				break;
			}
			list.add(fromLua(item, depth));
		}
		for (int i = 1; i <= list.size(); i++) {
			if (table.get(i).isnil()) {
				throw new LuaError("cannot encode a sparse Lua array as JSON");
			}
		}
		return list;
	}

	private static boolean isLuaExact(Number number) {
		if (number instanceof BigInteger integer) {
			return integer.bitLength() <= 53;
		}
		if (number instanceof BigDecimal decimal) {
			return BigDecimal.valueOf(decimal.doubleValue()).compareTo(decimal) == 0;
		}
		if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte) {
			return Math.abs(number.longValue()) <= 9007199254740991L;
		}
		return true;
	}

	private static final class JsonArrayTable extends LuaTable { }
	private static final class JsonObjectTable extends LuaTable { }
	private record JsonNumber(Number value) { }
	private record JsonBinary(byte[] value) { }

	private static final class ArrayFunction extends org.luaj.vm2.lib.ZeroArgFunction {
		@Override
		public LuaValue call() {
			return new JsonArrayTable();
		}
	}

	private static final class ObjectFunction extends org.luaj.vm2.lib.ZeroArgFunction {
		@Override
		public LuaValue call() {
			return new JsonObjectTable();
		}
	}

	private static final class DecodeFunction extends org.luaj.vm2.lib.OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			if (arg.checkstring().length() > LuaSandbox.MAX_STRING_BYTES)
				throw new LuaError("json.decode failed: input longer than " + LuaSandbox.MAX_STRING_BYTES + " bytes");
			try {
				Object parsed = JsonCodec.parseJson(arg.checkjstring());
				return toLua(parsed);
			} catch (Exception e) {
				throw new LuaError("json.decode failed: " + e.getMessage());
			}
		}
	}

	private static final class EncodeFunction extends org.luaj.vm2.lib.OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			try {
				return LuaValue.valueOf(JsonCodec.toJson(fromLua(arg)));
			} catch (Exception e) {
				throw new LuaError("json.encode failed: " + e.getMessage());
			}
		}
	}
}
