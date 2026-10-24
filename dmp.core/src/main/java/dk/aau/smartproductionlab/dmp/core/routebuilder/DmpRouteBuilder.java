/*******************************************************************************
 * Copyright (C) 2021 the Eclipse BaSyx Authors
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
package dk.aau.smartproductionlab.dmp.core.routebuilder;

import java.util.Collections;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.rest.RestDefinition;
import org.apache.camel.tooling.model.Strings;
import dk.aau.smartproductionlab.dmp.core.plan.RoutePlan;

/**
 * Builds the Camel routes of one managed bundle (one mapping) and records everything they own:
 * route IDs, ingress routes, endpoints, REST reservations and registry resources.
 *
 * @author fischer, mateusmolina
 *
 */
public class DmpRouteBuilder extends RouteBuilder {
	private final RoutePlan plan;
	private final PlanBuild build = new PlanBuild(this);
	private final Set<String> routeInventory = new LinkedHashSet<>();
	private final Set<String> ingressInventory = new LinkedHashSet<>();
	private final Set<String> endpointInventory = new LinkedHashSet<>();
	private volatile boolean ingressEnabled;
	private final List<RestReservation> restInventory = new ArrayList<>();
	private final Set<ManagedRouteResource> managedResources = Collections.newSetFromMap(new IdentityHashMap<>());
	private List<RouteDefinition> preparedRouteDefinitions;
	private boolean preparationStarted;
	private boolean configurationStarted;
	private boolean configurationComplete;

	private DmpRouteBuilder(RoutePlan plan) {
		if (plan == null || Strings.isNullOrEmpty(plan.routeId()) || plan.routeId().isBlank()) {
			throw new IllegalArgumentException("managed bundle requires a route plan with an explicit route ID");
		}
		this.plan = plan;
	}

	/** Creates a builder for one managed bundle: the routes of {@code plan}. */
	public static DmpRouteBuilder forManagedBundle(RoutePlan plan) {
		return new DmpRouteBuilder(plan);
	}

	/** The bundle's build: its endpoints' URIs, resolved once, and what they bind. */
	PlanBuild planBuild() {
		return build;
	}

	/** Returns the exact route IDs registered while constructing this bundle. */
	public Set<String> getRouteInventory() {
		return Collections.unmodifiableSet(new LinkedHashSet<>(routeInventory));
	}

	/** Configures this managed bundle once and snapshots its exact route definitions. */
	public void prepareBundle(CamelContext camelContext) throws Exception {
		if (preparationStarted) {
			throw new IllegalStateException("managed bundle preparation has already been attempted");
		}
		preparationStarted = true;
		setCamelContext(camelContext);
		configure();
		List<RouteDefinition> definitions = new ArrayList<>(getRouteCollection().getRoutes());
		for (RestDefinition rest : getRestCollection().getRests()) {
			definitions.addAll(rest.asRouteDefinition(camelContext));
		}
		Set<String> definitionIds = new LinkedHashSet<>();
		int restIngressDefinitions = 0;
		for (RouteDefinition definition : definitions) {
			definition.autoStartup(false);
			registerEndpointUri(definition.getInput().getEndpointUri());
			collectEndpoints(definition);
			if (ingressInventory.contains(definition.getRouteId())) {
				definition.getOutputs().add(0, new org.apache.camel.model.ProcessDefinition(exchange -> {
					if (!ingressEnabled) {
						exchange.getMessage().setHeader(org.apache.camel.Exchange.HTTP_RESPONSE_CODE, 503);
						exchange.getMessage().setBody("DMP mapping temporarily unavailable");
						exchange.setRouteStop(true);
					}
				}));
			}
			if (definition.getRouteId() == null) {
				restIngressDefinitions++;
			} else if (!definitionIds.add(definition.getRouteId())) {
				throw new IllegalArgumentException("managed bundle has duplicate constructed route IDs: "
						+ definitions.stream().map(RouteDefinition::getRouteId).toList());
			}
		}
		long restReservations = restInventory.size();
		if (restIngressDefinitions > restReservations) {
			throw new IllegalArgumentException("managed REST reservations do not match anonymous REST ingress definitions: "
					+ restIngressDefinitions + " > " + restReservations);
		}
		Set<String> expectedRouteIds = new LinkedHashSet<>(definitionIds);
		for (RestReservation rest : restInventory) {
			if (rest.restRouteId() != null) expectedRouteIds.add(rest.restRouteId());
		}
		if (!expectedRouteIds.equals(routeInventory)) {
			throw new IllegalStateException("managed route inventory does not match constructed route definitions: inventory="
					+ routeInventory + " definitions=" + definitions.stream().map(RouteDefinition::getRouteId).toList());
		}
		if (!routeInventory.containsAll(ingressInventory)) {
			throw new IllegalStateException("ingress inventory contains an unowned route");
		}
		preparedRouteDefinitions = List.copyOf(definitions);
	}

	/** Returns prepared route definitions for exact preflight/installation handling. */
	public List<RouteDefinition> getPreparedRouteDefinitions() {
		if (preparedRouteDefinitions == null) throw new IllegalStateException("managed bundle is not prepared");
		return preparedRouteDefinitions;
	}

	/** Returns the distinct resources bound by this managed mapping. */
	public List<ManagedRouteResource> getManagedResources() {
		return List.copyOf(managedResources);
	}

	public List<RestReservation> getRestInventory() { return List.copyOf(restInventory); }
	public Set<String> getIngressInventory() { return Set.copyOf(ingressInventory); }
	public Set<String> getEndpointInventory() { return Set.copyOf(endpointInventory); }
	public void setIngressEnabled(boolean enabled) { ingressEnabled = enabled; }
	public void registerIngressRouteId(String routeId) { ingressInventory.add(routeId); }
	public void registerEndpointUri(String uri) {
		if (uri != null && !uri.isBlank()) endpointInventory.add(uri);
	}

	private void collectEndpoints(org.apache.camel.model.ProcessorDefinition<?> node) {
		if (node instanceof org.apache.camel.model.ToDefinition to) registerEndpointUri(to.getEndpointUri());
		if (node instanceof org.apache.camel.model.WireTapDefinition tap) registerEndpointUri(tap.getUri());
		for (var output : node.getOutputs()) collectEndpoints(output);
	}

	public void registerRestReservation(String method, String path) {
		registerRestReservation(method, path, null, null);
	}

	public void registerRestReservation(String method, String path, RestDefinition definition) {
		registerRestReservation(method, path, definition, null);
	}

	public void registerRestReservation(String method, String path, RestDefinition definition, String restRouteId) {
		if (configurationComplete) throw new IllegalStateException("cannot register REST reservations after preparation");
		if (restRouteId != null) {
			registerRouteId(restRouteId);
			registerIngressRouteId(restRouteId);
		}
		RestReservation reservation = new RestReservation(method.toUpperCase(java.util.Locale.ROOT), path,
				restRouteId, definition);
		if (restInventory.contains(reservation)) throw new IllegalArgumentException("duplicate REST reservation " + reservation);
		restInventory.add(reservation);
	}

	public void registerManagedResource(ManagedRouteResource resource) {
		if (configurationComplete) throw new IllegalStateException("cannot register resources after preparation");
		if (managedResources.contains(resource)) return;
		try {
			resource.bind(getCamelContext());
			managedResources.add(resource);
		} catch (Exception e) {
			throw new IllegalStateException("failed to bind managed route resource", e);
		}
	}

	/** Records a route created by this builder; duplicate/invalid IDs are rejected. */
	public void registerRouteId(String routeId) {
		if (configurationComplete) {
			throw new IllegalStateException("cannot register routes after managed bundle preparation has started");
		}
		if (Strings.isNullOrEmpty(routeId) || routeId.isBlank()) {
			throw new IllegalArgumentException("managed bundle route ID must not be blank");
		}
		if (!routeInventory.add(routeId)) {
			throw new IllegalArgumentException("duplicate route ID in managed bundle: " + routeId);
		}
	}

	@Override
	public void configure() throws Exception {
		if (configurationStarted) {
			if (configurationComplete) return;
			throw new IllegalStateException("route builder configuration failed after it had started");
		}
		configurationStarted = true;
		plan.build(this);
		configurationComplete = true;
		build.resolvedUris().forEach(this::registerEndpointUri);
		managedResources.forEach(resource -> resource.endpointUris().forEach(this::registerEndpointUri));
	}

	public record RestReservation(String method, String path, String restRouteId, RestDefinition definition) {
		public RestReservation(String method, String path) { this(method, path, null, null); }
		@Override public boolean equals(Object other) {
			return other instanceof RestReservation that && method.equals(that.method) && path.equals(that.path);
		}
		@Override public int hashCode() { return java.util.Objects.hash(method, path); }
	}
}
