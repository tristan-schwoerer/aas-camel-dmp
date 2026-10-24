/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.plan;

import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;

/** Where a bundle being built keeps what its endpoints bind in the Camel registry; released with the bundle. */
public interface BundleResources {
	/** Binds {@code resource} in the bundle's context and has the bundle close it when it retires. */
	<T extends ManagedRouteResource> T own(T resource);
}
