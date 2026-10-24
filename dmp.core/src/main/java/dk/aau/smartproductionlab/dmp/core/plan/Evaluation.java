/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.plan;

/**
 * What a source's value does for its mapping (SPEC §6), authored as {@code Sources[i]/Evaluation/Mode}: whether
 * it triggers an evaluation and how its value is obtained.
 */
public enum Evaluation {
	/** Every new value evaluates the mapping; the value is observed or polled. */
	ON_UPDATE("OnUpdate"),
	/** A new value evaluates the mapping only when it differs from the source's previous one. */
	ON_CHANGE("OnChange"),
	/** Never evaluates the mapping; the latest observed or polled value is used. */
	CACHED("Cached"),
	/** Never evaluates the mapping; the source is read once per evaluation. */
	READ_AT_EVALUATION("ReadAtEvaluation");

	private final String authored;

	Evaluation(String authored) { this.authored = authored; }

	/** The {@code Mode} value that selects it. */
	public String authored() { return authored; }

	/** Whether a new value of the source evaluates the mapping (when it changed, for {@link #ON_CHANGE}). */
	public boolean triggers() { return this == ON_UPDATE || this == ON_CHANGE; }

	/** The mode an authored {@code Mode} value selects, or null for any other value. */
	public static Evaluation ofAuthored(String value) {
		for (Evaluation evaluation : values()) if (evaluation.authored.equals(value)) return evaluation;
		return null;
	}
}
