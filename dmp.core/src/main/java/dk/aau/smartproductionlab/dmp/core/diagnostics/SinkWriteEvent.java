/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.diagnostics;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/**
 * One send of an ordinary mapping's sink payload, until its connector acknowledged it (SPEC §6): within an
 * {@link EvaluationEvent}, the part spent in the sink's connector and the system behind it, e.g. the AAS
 * repository.
 */
@Name("dk.aau.smartproductionlab.dmp.SinkWrite")
@Label("DMP Sink Write")
@Category({ "AAS-Camel-DMP" })
@Description("One sink payload sent and acknowledged")
@StackTrace(false)
public final class SinkWriteEvent extends jdk.jfr.Event {
	@Label("Mapping")
	public String mapping;

	@Label("Sink")
	@Description("The sink's id in the mapping")
	public String sink;

	@Label("Kind")
	@Description("The connector, the scheme of the sink's endpoint")
	public String kind;

	@Label("Succeeded")
	public boolean succeeded;
}
