/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.routebuilder;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;

/**
 * One build of a plan's routes: each endpoint's URI is resolved once, binding what it names. On a
 * {@link DmpRouteBuilder} the bundle owns the bindings and records the URIs; on any other builder the bindings
 * stay in its context.
 */
public final class PlanBuild implements BundleResources {
	private final RouteBuilder builder;
	private final Map<Endpoint, String> uris = new IdentityHashMap<>();

	PlanBuild(RouteBuilder builder) {
		this.builder = builder;
	}

	/** The build of {@code builder}: the bundle's own for a {@link DmpRouteBuilder}, else a new one. */
	public static PlanBuild of(RouteBuilder builder) {
		return builder instanceof DmpRouteBuilder managed ? managed.planBuild() : new PlanBuild(builder);
	}

	/** {@code endpoint}'s URI, resolved on first use. */
	public synchronized String uri(Endpoint endpoint) {
		return uris.computeIfAbsent(endpoint, resolved -> {
			String uri = resolved.uri(this);
			if (uri == null || uri.isBlank()) throw new IllegalArgumentException("an endpoint resolved to no URI");
			return uri;
		});
	}

	/** The URIs of {@code endpoints}, in order. */
	public String[] uris(List<? extends Endpoint> endpoints) {
		return endpoints.stream().map(this::uri).toArray(String[]::new);
	}

	@Override
	public <T extends ManagedRouteResource> T own(T resource) {
		if (builder instanceof DmpRouteBuilder managed) {
			managed.registerManagedResource(resource);
		} else {
			try {
				resource.bind(builder.getContext());
			} catch (Exception e) {
				throw new IllegalStateException("failed to bind route resource", e);
			}
		}
		return resource;
	}

	synchronized Collection<String> resolvedUris() {
		return List.copyOf(uris.values());
	}
}
