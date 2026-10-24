/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.plan;

import java.util.List;

import org.apache.camel.builder.RouteBuilder;

/**
 * The routes of one mapping as the compiler planned them: an immutable value, built into Camel routes once per
 * bundle that installs it. Building creates what the bundle owns (bound credentials, Lua bindings, caches), so the
 * same plan can be staged again, e.g. to restore a bundle whose replacement failed.
 */
public interface RoutePlan {
	/** The mapping's main route; the IDs of its other routes derive from it. */
	String routeId();

	/** Adds the mapping's routes to {@code builder}; a {@code DmpRouteBuilder} also records what they own. */
	void build(RouteBuilder builder);

	/** Every endpoint the routes use: sources, transformations, sinks and the response half's. */
	List<Endpoint> endpoints();
}
