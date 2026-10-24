/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
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

import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaClosure;
import org.luaj.vm2.LuaFunction;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.DebugLib;

/**
 * The time budget of a Lua invocation (SPEC §8): wall time from {@link #install} on.
 *
 * <p>
 * LuaJ is not interruptible mid-evaluation, so the budget is checked from the VM: every
 * {@link #HOOK_INSTRUCTION_COUNT} instructions, and every {@link #HOOK_INSTRUCTION_COUNT} steps of
 * the pattern matcher ({@link LuaPatterns}), whose work runs inside a single instruction. The check
 * is a {@link DebugLib} that only counts: it is set as the globals' {@code debuglib} directly, so no
 * {@code debug} table is loaded and no call stack is kept.
 *
 * <p>
 * An exhausted budget throws {@link BudgetExceeded}, an {@link Error} rather than a {@code LuaError}:
 * LuaJ's {@code pcall} and {@code xpcall} catch {@code LuaError} and {@code Exception} only, so a
 * blob cannot swallow its own timeout.
 *
 * @author aas-camel-dmp
 */
public final class LuaTimeoutHook {

	/** Check the deadline every N VM instructions or matcher steps. */
	public static final int HOOK_INSTRUCTION_COUNT = 10000;

	/** The budget of the invocation running on this thread, for library functions that work in Java. */
	private static final ThreadLocal<Budget> CURRENT = new ThreadLocal<>();

	private LuaTimeoutHook() {
		throw new IllegalStateException("Utility class");
	}

	/**
	 * Starts the budget of an invocation on {@code globals}, on this thread; {@link #release} ends it.
	 *
	 * @param globals        the sandbox globals
	 * @param timeoutSeconds the budget per invocation; 0 or less for none
	 */
	public static void install(Globals globals, double timeoutSeconds) {
		if (timeoutSeconds <= 0) {
			CURRENT.remove();
			return;
		}
		Budget budget = new Budget(System.nanoTime() + (long) (timeoutSeconds * 1_000_000_000L), timeoutSeconds);
		globals.debuglib = budget;
		CURRENT.set(budget);
	}

	/** Ends this thread's invocation budget. */
	public static void release() {
		CURRENT.remove();
	}

	/** Charges {@code steps} units of Java-side work to this thread's budget. */
	static void charge(int steps) {
		Budget budget = CURRENT.get();
		if (budget != null) budget.charge(steps);
	}

	/** A Lua invocation ran out of its time budget; not catchable from Lua. */
	public static final class BudgetExceeded extends Error {
		private static final long serialVersionUID = 1L;

		BudgetExceeded(double timeoutSeconds) {
			super("Lua transformation timed out after " + timeoutSeconds + " s", null, false, false);
		}
	}

	private static final class Budget extends DebugLib {
		private final long deadlineNanos;
		private final double timeoutSeconds;
		private int pending;

		private Budget(long deadlineNanos, double timeoutSeconds) {
			this.deadlineNanos = deadlineNanos;
			this.timeoutSeconds = timeoutSeconds;
		}

		void charge(int steps) {
			pending += steps;
			if (pending < HOOK_INSTRUCTION_COUNT) return;
			pending = 0;
			if (System.nanoTime() - deadlineNanos > 0) throw new BudgetExceeded(timeoutSeconds);
		}

		@Override public void onInstruction(int pc, Varargs v, int top) { charge(1); }
		@Override public void onCall(LuaFunction f) { }
		@Override public void onCall(LuaClosure c, Varargs varargs, LuaValue[] stack) { }
		@Override public void onReturn() { }
		/** No call stack is kept: an error message carries its position, not a traceback. */
		@Override public String traceback(int level) { return ""; }
	}
}
