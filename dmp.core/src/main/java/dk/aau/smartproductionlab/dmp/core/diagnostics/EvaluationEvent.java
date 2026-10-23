/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.diagnostics;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
import jdk.jfr.Timespan;

/**
 * One evaluation of an ordinary mapping, from the triggering source's arrival to the end of its sinks (and its
 * response half): the DMP's own share of a value's way to its sinks (EVALUATION.md, observation point O3). The
 * event's duration is the evaluation itself; {@link #waitedNanos} is the time between the source's arrival and
 * the evaluation's start, waiting for the one before it. Record with {@code -XX:StartFlightRecording} or
 * {@code jcmd <pid> JFR.start}.
 */
@Name("dk.aau.smartproductionlab.dmp.Evaluation")
@Label("DMP Evaluation")
@Category({ "AAS-Camel-DMP" })
@Description("An ordinary mapping's evaluation: Lua and every sink")
@StackTrace(false)
public final class EvaluationEvent extends jdk.jfr.Event {
	@Label("Mapping")
	@Description("The mapping's route id")
	public String mapping;

	@Label("Triggers")
	@Description("The SourceIds whose new value caused the evaluation, comma-separated")
	public String triggers;

	@Label("Waited")
	@Description("From the triggering source's arrival to the evaluation's start")
	@Timespan(Timespan.NANOSECONDS)
	public long waitedNanos;

	@Label("Succeeded")
	public boolean succeeded;
}
