/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT
 *
 * The matcher is a port of Lua 5.2's lstrlib.c (Copyright (C) 1994-2015 Lua.org, PUC-Rio; MIT). */
package dk.aau.smartproductionlab.dmp.lua.sandbox;

import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.VarArgFunction;

/**
 * {@code string.find}, {@code match}, {@code gmatch} and {@code gsub} with a matcher of the DMP's own, whose
 * steps count against the invocation's time budget ({@link LuaTimeoutHook}): LuaJ's matcher runs inside one VM
 * instruction, so a backtracking pattern could otherwise run for minutes. Patterns are Lua's, over bytes, with
 * the C locale's character classes. {@code gsub}'s result is held to {@link LuaSandbox#MAX_STRING_BYTES}.
 *
 * <p>Where LuaJ 3.0.1 differs from Lua 5.2 its behaviour is kept, so scripts answer as before: a start position
 * past the subject's end is clamped to it, {@code gmatch} does not try the subject's end, and the character
 * classes are LuaJ's (Latin-1 letters are letters, {@code %p} is {@code !} to {@code @} without the digits, no
 * {@code %g}), read from LuaJ's own matcher once. Where LuaJ's matcher fails with a Java exception (an empty
 * pattern, {@code %f} at the subject's end), this one answers as Lua 5.2.
 */
final class LuaPatterns {
	private static final int MAX_CAPTURES = 32;
	private static final int MAX_DEPTH = 200;
	private static final int CAP_UNFINISHED = -1;
	private static final int CAP_POSITION = -2;
	private static final String SPECIALS = "^$*+?.([%-";
	private static final String CLASSES = "acdlpsuwxz";
	/** {@code CLASS_MEMBERS[i][c]}: whether byte {@code c} is in class {@code CLASSES.charAt(i)}, as LuaJ has it. */
	private static final boolean[][] CLASS_MEMBERS = luajClasses();

	private LuaPatterns() { }

	/** Replaces the pattern functions of a {@code string} table. */
	static void install(LuaValue string) {
		string.set("find", new FindOrMatch(true));
		string.set("match", new FindOrMatch(false));
		string.set("gmatch", new GMatch());
		string.set("gsub", new GSub());
	}

	private static final class FindOrMatch extends VarArgFunction {
		private final boolean find;

		FindOrMatch(boolean find) { this.find = find; }

		@Override
		public Varargs invoke(Varargs args) {
			LuaString s = args.checkstring(1);
			LuaString p = args.checkstring(2);
			int ls = s.length();
			// LuaJ's (Lua 5.1's) start: 1-based, negative from the end, clamped to the subject.
			int init = args.optint(3, 1);
			init = init > 0 ? Math.min(init, ls + 1) : init < 0 ? Math.max(1, ls + init + 1) : 1;
			if (find && (args.arg(4).toboolean() || noSpecials(p))) {
				int found = s.indexOf(p, init - 1);
				return found < 0 ? NIL : varargsOf(valueOf(found + 1), valueOf(found + p.length()));
			}
			Matcher ms = new Matcher(s, p);
			boolean anchor = p.length() > 0 && p.luaByte(0) == '^';
			int start = init - 1;
			do {
				ms.reset();
				int end = ms.match(start, anchor ? 1 : 0);
				if (end != -1) {
					return find ? varargsOf(valueOf(start + 1), valueOf(end), ms.captures(-1, 0))
							: ms.captures(start, end);
				}
			} while (start++ < ls && !anchor);
			return NIL;
		}
	}

	private static final class GMatch extends VarArgFunction {
		@Override
		public Varargs invoke(Varargs args) {
			LuaString s = args.checkstring(1);
			LuaString p = args.checkstring(2);
			return new VarArgFunction() {
				private final Matcher ms = new Matcher(s, p);
				private int position;

				@Override
				public Varargs invoke(Varargs ignored) {
					for (; position < s.length(); position++) {
						ms.reset();
						int end = ms.match(position, 0);
						if (end != -1) {
							int start = position;
							position = end == start ? end + 1 : end;
							return ms.captures(start, end);
						}
					}
					return NIL;
				}
			};
		}
	}

	private static final class GSub extends VarArgFunction {
		@Override
		public Varargs invoke(Varargs args) {
			LuaString src = args.checkstring(1);
			LuaString p = args.checkstring(2);
			LuaValue repl = args.arg(3);
			if (!(repl.isstring() || repl.istable() || repl.isfunction()))
				argerror(3, "string/function/table expected");
			int length = src.length();
			int max = args.optint(4, length + 1);
			boolean anchor = p.length() > 0 && p.luaByte(0) == '^';
			Matcher ms = new Matcher(src, p);
			Buffer result = new Buffer();
			int position = 0;
			int count = 0;
			while (count < max) {
				ms.reset();
				int end = ms.match(position, anchor ? 1 : 0);
				if (end != -1) {
					count++;
					replace(ms, result, position, end, repl);
				}
				if (end != -1 && end > position) position = end;
				else if (position < length) result.add(src.luaByte(position++));
				else break;
				if (anchor) break;
			}
			result.add(src, position, length);
			return varargsOf(result.toLuaString(), valueOf(count));
		}

		private static void replace(Matcher ms, Buffer result, int start, int end, LuaValue repl) {
			LuaValue value;
			if (repl.isstring()) {
				LuaString template = repl.checkstring();
				for (int i = 0; i < template.length(); i++) {
					int b = template.luaByte(i);
					if (b != '%' || ++i >= template.length()) {
						result.add(b);
						continue;
					}
					b = template.luaByte(i);
					if (b < '0' || b > '9') result.add(b);
					else if (b == '0') result.add(ms.source, start, end);
					else result.add(ms.capture(b - '1', start, end).checkstring());
				}
				return;
			}
			value = repl.istable() ? repl.get(ms.capture(0, start, end)) : repl.invoke(ms.captures(start, end)).arg1();
			if (!value.toboolean()) result.add(ms.source, start, end);
			else if (value.isstring()) result.add(value.checkstring());
			else throw new LuaError("invalid replacement value (a " + value.typename() + ")");
		}
	}

	/** LuaJ's character classes, asked of LuaJ's own matcher with one-byte subjects (which cannot backtrack). */
	private static boolean[][] luajClasses() {
		org.luaj.vm2.Globals globals = new org.luaj.vm2.Globals();
		LuaTable packageTable = new LuaTable();
		packageTable.set("loaded", new LuaTable());
		globals.set("package", packageTable);
		globals.load(new org.luaj.vm2.lib.StringLib());
		LuaValue find = globals.get("string").get("find");
		boolean[][] members = new boolean[CLASSES.length()][256];
		for (int i = 0; i < CLASSES.length(); i++) {
			LuaValue pattern = LuaValue.valueOf("%" + CLASSES.charAt(i));
			for (int c = 0; c < 256; c++)
				members[i][c] = !find.invoke(LuaString.valueOf(new byte[] { (byte) c }), pattern).arg1().isnil();
		}
		return members;
	}

	private static boolean noSpecials(LuaString pattern) {
		for (int i = 0; i < pattern.length(); i++)
			if (SPECIALS.indexOf(pattern.luaByte(i)) >= 0) return false;
		return true;
	}

	/** The pattern matcher's state over one subject and one pattern ({@code MatchState}). */
	private static final class Matcher {
		final LuaString source;
		private final LuaString pattern;
		private final int sourceLength;
		private final int patternLength;
		private int level;
		private int depth;
		private final int[] captureStart = new int[MAX_CAPTURES];
		private final int[] captureLength = new int[MAX_CAPTURES];

		Matcher(LuaString source, LuaString pattern) {
			this.source = source;
			this.pattern = pattern;
			this.sourceLength = source.length();
			this.patternLength = pattern.length();
		}

		void reset() {
			level = 0;
			depth = MAX_DEPTH;
		}

		/** The subject's byte at {@code i}; 0 past its end, as C's terminating NUL. */
		private int src(int i) { return i < sourceLength ? source.luaByte(i) : 0; }

		/** The pattern's byte at {@code i}; 0 past its end. */
		private int pat(int i) { return i < patternLength ? pattern.luaByte(i) : 0; }

		/** Where a match of the pattern from {@code p} at subject position {@code s} ends; -1 for none. */
		int match(int s, int p) {
			if (depth-- == 0) throw new LuaError("pattern too complex");
			try {
				while (true) {
					LuaTimeoutHook.charge(1);
					if (p == patternLength) return s;
					boolean single = false;
					switch (pat(p)) {
					case '(':
						return pat(p + 1) == ')' ? startCapture(s, p + 2, CAP_POSITION) : startCapture(s, p + 1, CAP_UNFINISHED);
					case ')':
						return endCapture(s, p + 1);
					case '$':
						if (p + 1 == patternLength) return s == sourceLength ? s : -1;
						single = true;
						break;
					case '%':
						switch (pat(p + 1)) {
						case 'b':
							s = matchBalance(s, p + 2);
							if (s == -1) return -1;
							p += 4;
							continue;
						case 'f': {
							p += 2;
							if (pat(p) != '[') throw new LuaError("missing '[' after '%f' in pattern");
							int ep = classEnd(p);
							int previous = s == 0 ? 0 : src(s - 1);
							if (!matchBracketClass(previous, p, ep - 1) && matchBracketClass(src(s), p, ep - 1)) {
								p = ep;
								continue;
							}
							return -1;
						}
						default:
							if (pat(p + 1) >= '0' && pat(p + 1) <= '9') {
								s = matchCapture(s, pat(p + 1));
								if (s == -1) return -1;
								p += 2;
								continue;
							}
							single = true;
						}
						break;
					default:
						single = true;
					}
					if (!single) continue;
					int ep = classEnd(p);
					int suffix = pat(ep);
					if (!singleMatch(s, p, ep)) {
						if (suffix == '*' || suffix == '?' || suffix == '-') {
							p = ep + 1;
							continue;
						}
						return -1;
					}
					switch (suffix) {
					case '?': {
						int result = match(s + 1, ep + 1);
						if (result != -1) return result;
						p = ep + 1;
						continue;
					}
					case '+':
						return maxExpand(s + 1, p, ep);
					case '*':
						return maxExpand(s, p, ep);
					case '-':
						return minExpand(s, p, ep);
					default:
						s++;
						p = ep;
					}
				}
			} finally {
				depth++;
			}
		}

		private int classEnd(int p) {
			int c = pat(p++);
			if (c == '%') {
				if (p >= patternLength) throw new LuaError("malformed pattern (ends with '%')");
				return p + 1;
			}
			if (c == '[') {
				if (pat(p) == '^') p++;
				do {
					if (p >= patternLength) throw new LuaError("malformed pattern (missing ']')");
					if (pat(p++) == '%' && p < patternLength) p++;
				} while (pat(p) != ']');
				return p + 1;
			}
			return p;
		}

		private boolean singleMatch(int s, int p, int ep) {
			if (s >= sourceLength) return false;
			int c = src(s);
			return switch (pat(p)) {
			case '.' -> true;
			case '%' -> matchClass(c, pat(p + 1));
			case '[' -> matchBracketClass(c, p, ep - 1);
			default -> pat(p) == c;
			};
		}

		/** {@code %cl} for byte {@code c}; an upper-case class is the complement, any other byte itself. */
		private static boolean matchClass(int c, int cl) {
			int index = CLASSES.indexOf(cl | 0x20);
			if (index < 0 || !Character.isLetter(cl)) return cl == c;
			boolean matches = CLASS_MEMBERS[index][c];
			return cl >= 'A' && cl <= 'Z' ? !matches : matches;
		}

		/** {@code [set]} from {@code p} (its '[') to {@code end} (its ']'). */
		private boolean matchBracketClass(int c, int p, int end) {
			boolean matches = true;
			if (pat(p + 1) == '^') {
				matches = false;
				p++;
			}
			while (++p < end) {
				if (pat(p) == '%') {
					p++;
					if (matchClass(c, pat(p))) return matches;
				} else if (pat(p + 1) == '-' && p + 2 < end) {
					p += 2;
					if (pat(p - 2) <= c && c <= pat(p)) return matches;
				} else if (pat(p) == c) return matches;
			}
			return !matches;
		}

		private int matchBalance(int s, int p) {
			if (p >= patternLength - 1) throw new LuaError("malformed pattern (missing arguments to '%b')");
			if (s >= sourceLength || src(s) != pat(p)) return -1;
			int open = pat(p);
			int close = pat(p + 1);
			int nesting = 1;
			while (++s < sourceLength) {
				LuaTimeoutHook.charge(1);
				int c = src(s);
				if (c == close) {
					if (--nesting == 0) return s + 1;
				} else if (c == open) nesting++;
			}
			return -1;
		}

		private int maxExpand(int s, int p, int ep) {
			int count = 0;
			while (singleMatch(s + count, p, ep)) {
				LuaTimeoutHook.charge(1);
				count++;
			}
			for (; count >= 0; count--) {
				int result = match(s + count, ep + 1);
				if (result != -1) return result;
			}
			return -1;
		}

		private int minExpand(int s, int p, int ep) {
			while (true) {
				int result = match(s, ep + 1);
				if (result != -1) return result;
				if (!singleMatch(s, p, ep)) return -1;
				s++;
			}
		}

		private int startCapture(int s, int p, int what) {
			if (level >= MAX_CAPTURES) throw new LuaError("too many captures");
			captureStart[level] = s;
			captureLength[level] = what;
			level++;
			int result = match(s, p);
			if (result == -1) level--;
			return result;
		}

		private int endCapture(int s, int p) {
			int open = captureToClose();
			captureLength[open] = s - captureStart[open];
			int result = match(s, p);
			if (result == -1) captureLength[open] = CAP_UNFINISHED;
			return result;
		}

		private int captureToClose() {
			for (int open = level - 1; open >= 0; open--)
				if (captureLength[open] == CAP_UNFINISHED) return open;
			throw new LuaError("invalid pattern capture");
		}

		private int matchCapture(int s, int index) {
			int capture = checkCapture(index);
			int length = captureLength[capture];
			if (sourceLength - s < length) return -1;
			for (int i = 0; i < length; i++)
				if (src(captureStart[capture] + i) != src(s + i)) return -1;
			return s + length;
		}

		private int checkCapture(int index) {
			int capture = index - '1';
			if (capture < 0 || capture >= level || captureLength[capture] == CAP_UNFINISHED)
				throw new LuaError("invalid capture index");
			return capture;
		}

		/** Capture {@code i}; with no captures, capture 0 is the whole match. */
		LuaValue capture(int i, int start, int end) {
			if (i >= level) {
				if (i == 0) return source.substring(start, end);
				throw new LuaError("invalid capture index");
			}
			int length = captureLength[i];
			if (length == CAP_UNFINISHED) throw new LuaError("unfinished capture");
			if (length == CAP_POSITION) return LuaValue.valueOf(captureStart[i] + 1);
			return source.substring(captureStart[i], captureStart[i] + length);
		}

		/** The captures, or the whole match ({@code start} not -1) when the pattern has none. */
		Varargs captures(int start, int end) {
			int count = level == 0 && start != -1 ? 1 : level;
			LuaValue[] values = new LuaValue[count];
			for (int i = 0; i < count; i++) values[i] = capture(i, start, end);
			return LuaValue.varargsOf(values);
		}
	}

	/** A byte buffer held to {@link LuaSandbox#MAX_STRING_BYTES}. */
	private static final class Buffer {
		private byte[] bytes = new byte[64];
		private int length;

		void add(int b) {
			ensure(1);
			bytes[length++] = (byte) b;
		}

		void add(LuaString s) { add(s, 0, s.length()); }

		void add(LuaString s, int from, int to) {
			int count = to - from;
			ensure(count);
			s.copyInto(from, bytes, length, count);
			length += count;
		}

		private void ensure(int more) {
			if ((long) length + more > LuaSandbox.MAX_STRING_BYTES)
				throw new LuaError("string longer than " + LuaSandbox.MAX_STRING_BYTES + " bytes");
			if (length + more > bytes.length)
				bytes = java.util.Arrays.copyOf(bytes, Math.max(length + more, Math.min(LuaSandbox.MAX_STRING_BYTES, bytes.length * 2)));
		}

		LuaString toLuaString() { return LuaString.valueOf(java.util.Arrays.copyOf(bytes, length)); }
	}
}
