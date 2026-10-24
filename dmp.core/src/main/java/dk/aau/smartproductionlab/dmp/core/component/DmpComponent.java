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
package dk.aau.smartproductionlab.dmp.core.component;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.time.Duration;
import java.util.function.Supplier;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.Model;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.rest.RestDefinition;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreatorFactory;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.core.configuration.route.execution.policy.ExecutionPolicyRouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.routebuilder.DmpRouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The DMP's Camel context and the managed bundles (one per mapping) installed in it.
 *
 * @author haque, fischer, danish
 *
 */
public class DmpComponent {
	private static Logger logger = LoggerFactory.getLogger(DmpComponent.class);
	private final Map<String, RouteCreatorFactory> routeCreatorFactories;
	private final Map<String, ManagedBundle> managedBundles = new HashMap<>();
	private final Map<String, AtomicLong> generations = new ConcurrentHashMap<>();
	private final AtomicLong generationSequence = new AtomicLong();
	private final Set<StagedBundle> stagedBundles = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
	private final Map<Endpoint, Integer> endpointOwners = new java.util.IdentityHashMap<>();
	private final Set<Endpoint> removableEndpoints = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
	private final Duration drainTimeout;
	private record OwnerSnapshot(List<ManagedRouteResource> resources, List<Endpoint> endpoints, Set<String> endpointUris) { }
	private record ResourceSnapshot(Map<String, Long> counts, Map<Object, OwnerSnapshot> owners, Set<String> ingress) { }
	private record AccountingSnapshot(Map<String, Long> aggregate, Map<Object, Map<String, Long>> owners) { }
	private volatile ResourceSnapshot resourceSnapshot = new ResourceSnapshot(Map.of("managedBundles", 0L, "stagedBundles", 0L,
			"managedRoutes", 0L, "ownedEndpoints", 0L, "restReservations", 0L, "managedResources", 0L), Map.of(), Set.of());
	private static final Set<String> RESOURCE_COUNTERS = Set.of("luaBindings", "pendingExtraWrites", "rejectedExtraWrites", "failedExtraWrites", "extraWriteExecutors",
			"luaSourceRetainedBytes", "mqttClients", "mqttConnectedClients", "mqttObservedOpenTcpSockets", "mqttSocketUnobservableClients",
			"unaccountedEndpointLedgerEntries");

	/** Reads ownership snapshots and live counters without the lifecycle lock held during drain. */
	public Map<String, Long> resourceMetrics() {
		ResourceSnapshot snapshot = resourceSnapshot;
		Map<String, Long> result = new LinkedHashMap<>(snapshot.counts());
		result.putAll(accountingMetrics(snapshot, false).aggregate());
		result.put("generationFences", (long) generations.size());
		CamelContext context = camelContext;
		result.put("inflightIngressExchanges", context == null ? 0L : snapshot.ingress().stream()
				.mapToLong(id -> context.getInflightRepository().size(id)).sum());
		return Map.copyOf(result);
	}

	/**
	 * Attribution to an exact installed-generation handle, including while draining.
	 * Shared byte lengths are reported in full, not divided into fictional heap ownership.
	 * Sharing is relative to currently tracked managed/staged owners; compiler caches,
	 * standalone routes and other JVM references are outside this accounting scope.
	 */
	public Map<String, Long> ownerResourceMetrics(ManagedBundle bundle) {
		return accountingMetrics(resourceSnapshot, true).owners().getOrDefault(java.util.Objects.requireNonNull(bundle, "bundle"), emptyOwnerMetrics());
	}

	/** One sample for all tracked installed/draining handles, without quadratic per-owner sampling. */
	public Map<ManagedBundle, Map<String, Long>> bundleResourceMetrics() {
		Map<ManagedBundle, Map<String, Long>> result = new java.util.IdentityHashMap<>();
		accountingMetrics(resourceSnapshot, true).owners().forEach((owner, metrics) -> {
			if (owner instanceof ManagedBundle bundle) result.put(bundle, metrics);
		});
		return Map.copyOf(result);
	}

	private static Map<String, Long> emptyOwnerMetrics() {
		Map<String, Long> result = new LinkedHashMap<>();
		RESOURCE_COUNTERS.forEach(counter -> result.put(counter, 0L));
		result.put("sharedLuaSourceRetainedBytes", 0L);
		result.put("exclusiveLuaSourceRetainedBytes", 0L);
		result.put("accountingReadFailures", 0L);
		return Map.copyOf(result);
	}

	private AccountingSnapshot accountingMetrics(ResourceSnapshot snapshot, boolean attributeOwners) {
		Map<Object, Map<String, Long>> entries = new java.util.IdentityHashMap<>();
		Map<Object, Integer> owners = new java.util.IdentityHashMap<>();
		Map<Object, Set<Object>> references = new java.util.IdentityHashMap<>();
		Map<Object, Long> readFailures = new java.util.IdentityHashMap<>();
		long failures = 0;
		CamelContext context = camelContext;
		for (var owned : snapshot.owners().entrySet()) {
			long priorFailures = failures;
			Map<Object, Map<String, Long>> local = new java.util.IdentityHashMap<>();
			for (ManagedRouteResource resource : owned.getValue().resources()) {
				try { for (var entry : resource.accounting()) local.put(entry.identity(), entry.counters()); }
				catch (RuntimeException unavailable) { failures++; }
			}
			Set<Endpoint> endpoints = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
			endpoints.addAll(owned.getValue().endpoints());
			for (String uri : owned.getValue().endpointUris()) {
				try {
					// hasEndpoint is a registry lookup only: never resolve/create a lazy endpoint.
					Endpoint endpoint = context == null ? null : context.hasEndpoint(uri);
					if (endpoint != null) endpoints.add(endpoint);
				} catch (RuntimeException unavailable) { failures++; }
			}
			for (Endpoint endpoint : endpoints) {
				try {
					if (endpoint instanceof ManagedRouteResource.AccountedEndpoint accounted) {
						for (var entry : accounted.accounting()) local.put(entry.identity(), entry.counters());
					} else local.put(endpoint, Map.of("unaccountedEndpointLedgerEntries", 1L));
				} catch (RuntimeException unavailable) { failures++; }
			}
			local.forEach((identity, counters) -> {
				entries.put(identity, counters);
				owners.merge(identity, 1, Integer::sum);
			});
			if (attributeOwners) {
				references.put(owned.getKey(), local.keySet());
				readFailures.put(owned.getKey(), failures - priorFailures);
			}
		}
		Map<Object, Map<String, Long>> attributed = new java.util.IdentityHashMap<>();
		for (var reference : references.entrySet()) {
			attributed.put(reference.getKey(), sumAccounting(entries, owners, reference.getValue(), readFailures.get(reference.getKey()), true));
		}
		return new AccountingSnapshot(sumAccounting(entries, owners, entries.keySet(), failures, false), Map.copyOf(attributed));
	}

	private static Map<String, Long> sumAccounting(Map<Object, Map<String, Long>> entries,
			Map<Object, Integer> owners, Set<Object> selected, long failures, boolean attributeOwner) {
		Map<String, Long> result = new LinkedHashMap<>();
		RESOURCE_COUNTERS.forEach(counter -> result.put(counter, 0L));
		long sharedBytes = 0, exclusiveBytes = 0;
		for (Object identity : selected) {
			Map<String, Long> counters = entries.get(identity);
			counters.forEach((key, value) -> {
				if (RESOURCE_COUNTERS.contains(key) && value != null) result.merge(key, Math.max(0, value), Long::sum);
			});
			long bytes = Math.max(0, counters.getOrDefault("luaSourceRetainedBytes", 0L));
			if (owners.get(identity) > 1) sharedBytes += bytes; else exclusiveBytes += bytes;
		}
		if (attributeOwner) {
			result.put("sharedLuaSourceRetainedBytes", sharedBytes);
			result.put("exclusiveLuaSourceRetainedBytes", exclusiveBytes);
		}
		result.put("accountingReadFailures", failures);
		return Map.copyOf(result);
	}

	/** Process totals only, not per-asset heap isolation or a count of every protocol socket. */
	public Map<String, Long> processMetrics() {
		var memory = java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
		var threads = java.lang.management.ManagementFactory.getThreadMXBean();
		return Map.of("heapUsedBytes", memory.getUsed(), "heapCommittedBytes", memory.getCommitted(),
				"heapMaxBytes", memory.getMax(), "threadCount", (long) threads.getThreadCount(),
				"daemonThreadCount", (long) threads.getDaemonThreadCount());
	}

	private void publishResourceSnapshot() {
		Set<ManagedRouteResource> resources = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
		Map<Object, OwnerSnapshot> owners = new java.util.IdentityHashMap<>();
		Set<String> routes = new LinkedHashSet<>(), ingress = new LinkedHashSet<>();
		Set<DmpRouteBuilder.RestReservation> rests = new LinkedHashSet<>();
		for (ManagedBundle bundle : managedBundles.values()) {
			resources.addAll(bundle.remainingResources); routes.addAll(bundle.remainingRouteIds); rests.addAll(bundle.remainingRestReservations);
			owners.put(bundle, new OwnerSnapshot(List.copyOf(bundle.remainingResources), List.copyOf(bundle.remainingEndpoints),
					bundle.releasingEndpoints ? Set.of() : Set.copyOf(bundle.endpointBaseline.keySet())));
			if (bundle.builder != null) ingress.addAll(bundle.builder.getIngressInventory());
		}
		for (StagedBundle staged : stagedBundles) {
			var stagedResources = staged.remainingResources == null ? staged.builder.getManagedResources() : staged.remainingResources;
			resources.addAll(stagedResources);
			owners.put(staged, new OwnerSnapshot(List.copyOf(stagedResources), List.of(), Set.of()));
		}
		Map<String, Long> counts = Map.of("managedBundles", (long) managedBundles.size(), "stagedBundles", (long) stagedBundles.size(),
				"managedRoutes", (long) routes.size(), "ownedEndpoints", (long) endpointOwners.size(),
				"restReservations", (long) rests.size(), "managedResources", (long) resources.size());
		resourceSnapshot = new ResourceSnapshot(counts, Map.copyOf(owners), Set.copyOf(ingress));
	}

	protected CamelContext camelContext;

	public DmpComponent() {
		this(Duration.ofSeconds(10));
	}

	public DmpComponent(Duration drainTimeout) {
		if (drainTimeout == null || drainTimeout.isNegative() || drainTimeout.isZero()) {
			throw new IllegalArgumentException("positive bundle drain timeout is required");
		}
		this.drainTimeout = drainTimeout;
		camelContext = new DefaultCamelContext();
		routeCreatorFactories = getRouteCreatorFactoryMapDefault();
	}

	/** One-shot installation of a definition that is never replaced; replacement needs a fresh-definition factory. */
	public synchronized ManagedBundle installBundle(String bundleId, RoutesConfiguration configuration) {
		if (managedBundles.containsKey(bundleId)) throw new IllegalStateException("bundle already installed: " + bundleId);
		return installBundle(stageBundle(bundleId, nextBundleGeneration(bundleId), () -> configuration, false));
	}

	/** Fences already compiled/staged work, including after removal. Call before asynchronous compilation. */
	public long nextBundleGeneration(String bundleId) {
		if (bundleId == null || bundleId.isBlank()) throw new IllegalArgumentException("bundleId is required");
		synchronized (generations) {
			if (!generations.containsKey(bundleId) && generations.size() >= 8192) {
				throw new IllegalStateException("bundle generation tracking capacity exceeded");
			}
			AtomicLong fence = generations.computeIfAbsent(bundleId, ignored -> new AtomicLong());
			synchronized (fence) {
				long epoch = generationSequence.incrementAndGet();
				if (epoch <= 0) throw new IllegalStateException("bundle generation sequence exhausted");
				fence.set(epoch);
				return epoch;
			}
		}
	}

	public int generationTrackingCount() { return generations.size(); }

	/** Prunes an abandoned generation only when no staged or managed bundle remains for the ID. */
	public synchronized void abandonUninstalledBundle(String bundleId) {
		if (managedBundles.containsKey(bundleId) || stagedBundles.stream().anyMatch(staged -> staged.bundleId.equals(bundleId))) return;
		synchronized (generations) {
			AtomicLong fence = generations.get(bundleId);
			if (fence != null) generations.remove(bundleId, fence);
		}
	}

	/** Advances an existing bundle fence without creating tracking state for a removed bundle. */
	public long fenceBundleGeneration(String bundleId) {
		synchronized (generations) {
			if (!generations.containsKey(bundleId)) return 0;
			return nextBundleGeneration(bundleId);
		}
	}

	/**
	 * Builds an inactive, owned candidate. Close abandoned candidates. The factory
	 * must create fresh mutable configuration/entities on every call, including rollback.
	 * It must reconstruct the same validated definition, not fetch changing desired state.
	 */
	public synchronized StagedBundle stageBundle(String bundleId, long generation,
			Supplier<RoutesConfiguration> definition) {
		return stageBundle(bundleId, generation, definition, true);
	}

	private StagedBundle stageBundle(String bundleId, long generation,
			Supplier<RoutesConfiguration> definition, boolean restorable) {
		requireGeneration(bundleId, generation);
		if (camelContext == null || !camelContext.getStatus().isStarted()) {
			throw new IllegalStateException("managed bundles require a running DMP context");
		}
		if (stagedBundles.size() >= 128) throw new IllegalStateException("staged bundle capacity exceeded");
		RoutesConfiguration configuration = definition.get();
		DmpRouteBuilder candidate = DmpRouteBuilder.forManagedBundle(configuration, routeCreatorFactories);
		StagedBundle staged = new StagedBundle(bundleId, generation, candidate,
				configuration, restorable ? definition : null);
		stagedBundles.add(staged);
		try {
			candidate.prepareBundle(camelContext);
			requireGeneration(bundleId, generation);
			publishResourceSnapshot();
			return staged;
		} catch (Exception failure) {
			try { staged.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
			throw new IllegalStateException("unable to stage managed bundle '" + bundleId + "'", failure);
		}
	}

	public synchronized ManagedBundle installBundle(StagedBundle staged) {
		validateStaged(staged);
		if (managedBundles.containsKey(staged.bundleId)) {
			throw new IllegalStateException("bundle already installed: " + staged.bundleId);
		}
		return activate(staged);
	}

	public synchronized ManagedBundle replaceBundle(ManagedBundle previous, StagedBundle staged) {
		validateStaged(staged);
		if (previous == null || managedBundles.get(staged.bundleId) != previous || !previous.bundleId.equals(staged.bundleId)) {
			throw new IllegalArgumentException("replacement does not own the installed bundle");
		}
		if (previous.definitionFactory == null || !previous.active) {
			throw new IllegalStateException("replacement requires an active bundle installed with a fresh-definition factory");
		}
		if (staged.generation <= previous.generation) {
			throw new IllegalStateException("replacement requires a newer bundle generation");
		}
		try {
			validateFreshDefinition(previous.configuration, staged.configuration);
			preflight(staged.builder, previous);
			requireGeneration(staged.bundleId, staged.generation);
			retireBundle(previous);
			return activate(staged);
		} catch (Exception failure) {
			if (!staged.consumed) {
				try { staged.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
			}
			// The previous bundle comes back rather than leave the mapping without one, also when an invalidation
			// fenced the replacement meanwhile: the pass it queued replaces (or removes) the restored bundle in turn.
			// A removal cannot interleave (this component is synchronized); a dropped generation means none is wanted.
			AtomicLong fence = generations.get(previous.bundleId);
			if (!managedBundles.containsKey(previous.bundleId) && fence != null) {
				try {
					StagedBundle restore = stageBundle(previous.bundleId, fence.get(), previous.definitionFactory);
					try {
						validateFreshDefinition(previous.configuration, restore.configuration);
						activate(restore);
					} finally { if (!restore.consumed) restore.close(); }
				} catch (Exception restoreFailure) { failure.addSuppressed(restoreFailure); }
			}
			throw new IllegalStateException("unable to replace managed bundle '" + previous.bundleId + "'", failure);
		}
	}

	private static void validateFreshDefinition(RoutesConfiguration old, RoutesConfiguration candidate) {
		Set<Object> oldObjects = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
		oldObjects.add(old);
		oldObjects.addAll(old.getRoutes());
		oldObjects.addAll(old.getDatasources().values());
		oldObjects.addAll(old.getDatasinks().values());
		oldObjects.addAll(old.getTransformers().values());
		List<Object> newObjects = new ArrayList<>();
		newObjects.add(candidate);
		newObjects.addAll(candidate.getRoutes());
		newObjects.addAll(candidate.getDatasources().values());
		newObjects.addAll(candidate.getDatasinks().values());
		newObjects.addAll(candidate.getTransformers().values());
		if (newObjects.stream().anyMatch(oldObjects::contains)) {
			throw new IllegalArgumentException("bundle generations must not reuse mutable configuration/entities");
		}
	}

	private void validateStaged(StagedBundle staged) {
		if (staged == null || !stagedBundles.contains(staged) || staged.consumed) {
			throw new IllegalArgumentException("staged bundle is not owned by this component or has already been consumed");
		}
	}

	private void requireGeneration(String bundleId, long generation) {
		AtomicLong fence = generations.get(bundleId);
		if (fence == null || fence.get() != generation) throw new IllegalStateException("stale bundle generation: " + bundleId);
	}

	private void preflight(DmpRouteBuilder candidate, ManagedBundle previous) {
		for (String routeId : candidate.getRouteInventory()) {
			if ((camelContext.getRoute(routeId) != null || model().getRouteDefinition(routeId) != null)
					&& (previous == null || !previous.remainingRouteIds.contains(routeId))) {
				throw new IllegalStateException("managed route ID is already in use: " + routeId);
			}
		}
		// Routes of different bundles may consume the same endpoint: each route has a consumer of its own
		// (its own subscription, listener or poll), and endpointOwners keeps a shared endpoint until its last owner goes.
		for (var reservation : candidate.getRestInventory()) {
			if (model().getRestDefinitions().stream().anyMatch(rest -> hasRestReservation(rest, reservation)
					&& (previous == null || previous.remainingRestReservations.stream()
							.noneMatch(owned -> owned.definition() == rest)))) {
				throw new IllegalStateException("managed REST path is already reserved");
			}
		}
	}

	private ManagedBundle activate(StagedBundle staged) {
		String bundleId = staged.bundleId;
		DmpRouteBuilder candidate = staged.builder;
		boolean addAttempted = false;
		Map<DmpRouteBuilder.RestReservation, String> restEndpoints = new java.util.LinkedHashMap<>();
		Map<String, Endpoint> existingEndpoints = new HashMap<>();
		try {
			requireGeneration(bundleId, staged.generation);
			Set<String> routeIds = candidate.getRouteInventory();
			List<RouteDefinition> definitions = candidate.getPreparedRouteDefinitions();
			List<DmpRouteBuilder.RestReservation> reservations = candidate.getRestInventory();
			if (routeIds.isEmpty()) throw new IllegalArgumentException("managed bundle must define at least one route");
			preflight(candidate, null);
			for (String uri : candidate.getEndpointInventory()) existingEndpoints.put(uri, camelContext.hasEndpoint(uri));
			addAttempted = true;
			model().addRestDefinitions(reservations.stream().map(DmpRouteBuilder.RestReservation::definition).distinct().toList(), false);
			model().addRouteDefinitions(definitions);
			restEndpoints.putAll(captureRestEndpointUris(reservations));
			// REST listeners first: gated until the bundle is active, they answer "unavailable" while the rest
			// starts (devices connect), so a replaced Operation's path is never unserved for longer than the swap.
			Set<String> restRoutes = new LinkedHashSet<>();
			for (var reservation : reservations) if (reservation.restRouteId() != null) restRoutes.add(reservation.restRouteId());
			for (String routeId : routeIds) {
				if (restRoutes.contains(routeId)) camelContext.getRouteController().startRoute(routeId);
			}
			for (String routeId : routeIds) {
				if (!candidate.getIngressInventory().contains(routeId)) camelContext.getRouteController().startRoute(routeId);
			}
			requireGeneration(bundleId, staged.generation);
			for (String routeId : routeIds) {
				if (candidate.getIngressInventory().contains(routeId) && !restRoutes.contains(routeId))
					camelContext.getRouteController().startRoute(routeId);
			}
			for (String routeId : routeIds) {
				var route = camelContext.getRoute(routeId);
				if (route == null || !camelContext.getRouteController().getRouteStatus(routeId).isStarted()) {
					throw new IllegalStateException("managed route did not start: " + routeId);
				}
			}
			ManagedBundle installed = new ManagedBundle(bundleId, definitions, reservations, restEndpoints,
					candidate.getManagedResources());
			installed.builder = candidate;
			installed.configuration = staged.configuration;
			installed.definitionFactory = staged.definitionFactory;
			installed.generation = staged.generation;
			AtomicLong fence = generations.get(bundleId);
			synchronized (fence) {
				requireGeneration(bundleId, staged.generation);
				claimEndpoints(installed, existingEndpoints);
				candidate.setIngressEnabled(true);
				installed.active = true;
				managedBundles.put(bundleId, installed);
			}
			staged.consumed = true;
			stagedBundles.remove(staged);
			return installed;
		} catch (Exception failure) {
			candidate.setIngressEnabled(false);
			List<RouteDefinition> definitions = candidate.getPreparedRouteDefinitions();
			Set<DmpRouteBuilder.RestReservation> reservations = new LinkedHashSet<>(candidate.getRestInventory());
			if (addAttempted) {
				try { restEndpoints.putAll(captureRestEndpointUris(reservations)); }
				catch (Exception endpointInspectionFailure) { failure.addSuppressed(endpointInspectionFailure); }
			}
			Set<String> remaining = addAttempted ? cleanupCandidate(definitions, failure) : new LinkedHashSet<>();
			if (remaining.isEmpty()) cleanupRestReservations(reservations, failure);
			if (remaining.isEmpty()) cleanupRestEndpointUris(restEndpoints, failure);
			Set<ManagedRouteResource> resources = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
			resources.addAll(candidate.getManagedResources());
			if (remaining.isEmpty() && reservations.isEmpty() && restEndpoints.isEmpty()) closeResources(resources, failure);
			ManagedBundle cleanup = new ManagedBundle(bundleId, definitions.stream()
							.filter(definition -> remaining.contains(definition.getId())).toList(), reservations,
								restEndpoints, resources);
			cleanup.builder = candidate;
			cleanup.generation = staged.generation;
			claimEndpoints(cleanup, existingEndpoints);
			if (remaining.isEmpty() && resources.isEmpty()) {
				for (ManagedBundle owned : managedBundles.values()) claimLazyEndpoints(owned);
				releaseEndpoints(cleanup, failure);
			}
			if (!remaining.isEmpty() || !reservations.isEmpty() || !restEndpoints.isEmpty() || !resources.isEmpty()
					|| !cleanup.remainingEndpoints.isEmpty()) managedBundles.put(bundleId, cleanup);
			staged.consumed = true;
			stagedBundles.remove(staged);
			throw new IllegalStateException("unable to install managed bundle '" + bundleId + "'", failure);
		} finally {
			publishResourceSnapshot();
		}
	}

	/** Returns an owned handle when partial cleanup left routes to retire. */
	public synchronized ManagedBundle managedBundle(String bundleId) {
		return managedBundles.get(bundleId);
	}

	/** Removes only the exact route IDs recorded by the returned bundle handle. */
	public synchronized void removeBundle(ManagedBundle bundle) {
		if (bundle == null || managedBundles.get(bundle.bundleId) != bundle) {
			throw new IllegalArgumentException("bundle handle is not owned by this DMP component");
		}
		removeBundle(bundle, nextBundleGeneration(bundle.bundleId));
	}

	/** Applies a pre-reserved deletion epoch, rejecting stale asynchronous deletion work. */
	public synchronized void removeBundle(ManagedBundle bundle, long removalGeneration) {
		if (bundle == null || managedBundles.get(bundle.bundleId) != bundle) {
			throw new IllegalArgumentException("bundle handle is not owned by this DMP component");
		}
		requireGeneration(bundle.bundleId, removalGeneration);
		if (removalGeneration <= bundle.generation) throw new IllegalStateException("removal requires a newer bundle generation");
		retireBundle(bundle);
		// Global epochs make pruning safe: a recreated ID can never reuse a stale token.
		synchronized (generations) {
			AtomicLong fence = generations.get(bundle.bundleId);
			if (fence != null) {
				synchronized (fence) {
					if (fence.get() == removalGeneration) generations.remove(bundle.bundleId, fence);
				}
			}
		}
	}

	private void retireBundle(ManagedBundle bundle) {
		if (bundle.builder != null) bundle.builder.setIngressEnabled(false);
		bundle.active = false;
		publishResourceSnapshot();
		long deadline = System.nanoTime() + drainTimeout.toNanos();
		List<String> routeIds = new ArrayList<>(bundle.remainingRouteIds);
		Collections.reverse(routeIds);
		List<Exception> failures = new ArrayList<>();
		// Stop/drain ingress first, retaining all downstream and detached-write targets.
		List<String> ingress = routeIds.stream().filter(id -> bundle.builder == null
				|| bundle.builder.getIngressInventory().contains(id)).toList();
		try {
			// Keep REST listeners accepting unavailable responses while already admitted calls finish.
			while (ingress.stream().anyMatch(id -> camelContext.getInflightRepository().size(id) > 0)) {
				if (System.nanoTime() >= deadline) throw new IllegalStateException("ingress did not drain before bundle retirement deadline");
				java.util.concurrent.TimeUnit.NANOSECONDS.sleep(Math.min(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(10),
						Math.max(1, deadline - System.nanoTime())));
			}
		} catch (Exception failure) {
			if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
			failures.add(failure);
		}
		if (!failures.isEmpty()) throw bundleFailure(bundle, "drain ingress for", failures);
		for (String routeId : ingress) {
			try {
				stopRouteBefore(routeId, deadline);
			} catch (Exception failure) {
				failures.add(failure);
				break;
			}
		}
		if (!failures.isEmpty()) throw bundleFailure(bundle, "stop", failures);
		for (ManagedRouteResource resource : bundle.remainingResources) {
			try { resource.drain(Duration.ofNanos(Math.max(0, deadline - System.nanoTime()))); }
			catch (Exception failure) { failures.add(failure); }
		}
		if (!failures.isEmpty()) throw bundleFailure(bundle, "drain detached work for", failures);
		for (String routeId : routeIds) {
			if (ingress.contains(routeId)) continue;
			try { stopRouteBefore(routeId, deadline); }
			catch (Exception failure) { failures.add(failure); break; }
		}
		if (!failures.isEmpty()) throw bundleFailure(bundle, "stop downstream routes for", failures);
		for (String routeId : routeIds) {
			try {
				if (camelContext.getRoute(routeId) != null) {
					camelContext.removeRoute(routeId);
				}
				if (camelContext.getRoute(routeId) != null) {
					throw new IllegalStateException("Camel retained runtime route " + routeId);
				}
				removeOrphanDefinition(bundle.definitions.get(routeId));
				bundle.remainingRouteIds.remove(routeId);
			} catch (Exception failure) {
				failures.add(failure);
			}
		}
		if (!failures.isEmpty()) throw bundleFailure(bundle, "remove", failures);
		cleanupRestReservations(bundle.remainingRestReservations, failures);
		if (!failures.isEmpty()) throw bundleFailure(bundle, "release REST reservations for", failures);
		cleanupRestEndpointUris(bundle.remainingRestEndpointUris, failures);
		if (!failures.isEmpty()) throw bundleFailure(bundle, "release REST consumers for", failures);
		closeResources(bundle.remainingResources, failures);
		if (!failures.isEmpty()) throw bundleFailure(bundle, "release resources for", failures);
		for (ManagedBundle owned : managedBundles.values()) claimLazyEndpoints(owned);
		releaseEndpoints(bundle, failures);
		if (!failures.isEmpty()) throw bundleFailure(bundle, "release endpoints for", failures);
		managedBundles.remove(bundle.bundleId);
		bundle.definitions.clear();
		publishResourceSnapshot();
	}

	private void stopRouteBefore(String routeId, long deadline) throws Exception {
		var status = camelContext.getRouteController().getRouteStatus(routeId);
		if (status == null || status.isStopped()) return;
		long remaining = Math.max(1, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
		if (!camelContext.getRouteController().stopRoute(routeId, remaining, java.util.concurrent.TimeUnit.MILLISECONDS, true)) {
			throw new IllegalStateException("route did not drain before bundle retirement deadline: " + routeId);
		}
	}

	private void claimEndpoints(ManagedBundle bundle, Map<String, Endpoint> existing) {
		bundle.endpointBaseline.putAll(existing);
		claimLazyEndpoints(bundle);
	}

	private void claimLazyEndpoints(ManagedBundle bundle) {
		for (var entry : bundle.endpointBaseline.entrySet()) {
			Endpoint endpoint = camelContext.hasEndpoint(entry.getKey());
			if (endpoint != null && bundle.remainingEndpoints.add(endpoint)) {
				endpointOwners.merge(endpoint, 1, Integer::sum);
				if (entry.getValue() == null) removableEndpoints.add(endpoint);
			}
		}
	}

	private void releaseEndpoints(ManagedBundle bundle, Exception failure) {
		List<Exception> failures = new ArrayList<>();
		releaseEndpoints(bundle, failures);
		failures.forEach(failure::addSuppressed);
	}

	private void releaseEndpoints(ManagedBundle bundle, List<Exception> failures) {
		bundle.releasingEndpoints = true;
		for (Endpoint endpoint : new ArrayList<>(bundle.remainingEndpoints)) {
			try {
				int owners = endpointOwners.getOrDefault(endpoint, 1);
				if (owners == 1) {
					if (removableEndpoints.contains(endpoint)) camelContext.removeEndpoint(endpoint);
					endpointOwners.remove(endpoint);
					removableEndpoints.remove(endpoint);
				} else endpointOwners.put(endpoint, owners - 1);
				bundle.remainingEndpoints.remove(endpoint);
			} catch (Exception failure) { failures.add(failure); }
		}
	}

	private static void closeResources(Set<ManagedRouteResource> resources, Exception failure) {
		List<Exception> failures = new ArrayList<>();
		closeResources(resources, failures);
		failures.forEach(failure::addSuppressed);
	}

	private static void closeResources(Set<ManagedRouteResource> resources, List<Exception> failures) {
		for (ManagedRouteResource resource : new ArrayList<>(resources)) {
			try {
				resource.close();
				resources.remove(resource);
			} catch (Exception failure) {
				failures.add(failure);
			}
		}
	}

	private Set<String> cleanupCandidate(List<RouteDefinition> definitions, Exception failure) {
		long deadline = System.nanoTime() + drainTimeout.toNanos();
		List<RouteDefinition> reverse = new ArrayList<>(definitions);
		Collections.reverse(reverse);
		Set<String> remaining = new LinkedHashSet<>();
		for (RouteDefinition definition : reverse) {
			String routeId = definition.getId();
			if (routeId == null) continue;
			try {
				if (camelContext.getRoute(routeId) != null) {
					var status = camelContext.getRouteController().getRouteStatus(routeId);
					if (status != null && !status.isStopped()) stopRouteBefore(routeId, deadline);
					camelContext.removeRoute(routeId);
				}
				if (camelContext.getRoute(routeId) != null) throw new IllegalStateException("Camel retained runtime route " + routeId);
				removeOrphanDefinition(definition);
			} catch (Exception cleanupFailure) {
				failure.addSuppressed(cleanupFailure);
				remaining.add(routeId);
			}
		}
		return remaining;
	}

	private void removeOrphanDefinition(RouteDefinition definition) throws Exception {
		if (definition == null || camelContext.getRoute(definition.getId()) != null) return;
		RouteDefinition current = model().getRouteDefinition(definition.getId());
		if (current == definition) {
			model().removeRouteDefinition(definition);
		} else if (current != null) {
			throw new IllegalStateException("refusing to remove another route definition with ID " + definition.getId());
		}
	}

	private Model model() {
		return camelContext.getCamelContextExtension().getContextPlugin(Model.class);
	}

	private void cleanupRestReservations(Set<DmpRouteBuilder.RestReservation> reservations, Exception aggregate) {
		List<Exception> failures = new ArrayList<>();
		cleanupRestReservations(reservations, failures);
		failures.forEach(aggregate::addSuppressed);
	}

	private void cleanupRestReservations(Set<DmpRouteBuilder.RestReservation> reservations,
			List<Exception> failures) {
		for (DmpRouteBuilder.RestReservation reservation : new ArrayList<>(reservations)) {
			try {
				List<RestDefinition> rests = model().getRestDefinitions();
				if (reservation.definition() != null) {
					if (rests.contains(reservation.definition()) && !rests.remove(reservation.definition())) {
						throw new IllegalStateException("Camel REST model refused to remove " + reservation);
					}
				} else {
					List<RestDefinition> matches = rests.stream()
							.filter(rest -> hasRestReservation(rest, reservation)).toList();
					if (matches.size() > 1) throw new IllegalStateException("ambiguous REST reservation: " + reservation);
					if (!matches.isEmpty() && !rests.remove(matches.get(0))) {
						throw new IllegalStateException("Camel REST model refused to remove " + reservation);
					}
				}
				reservations.remove(reservation);
			} catch (Exception failure) {
				failures.add(failure);
			}
		}
	}

	private Map<DmpRouteBuilder.RestReservation, String> captureRestEndpointUris(
			Collection<DmpRouteBuilder.RestReservation> reservations) {
		Map<DmpRouteBuilder.RestReservation, String> result = new java.util.LinkedHashMap<>();
		for (DmpRouteBuilder.RestReservation reservation : reservations) {
			if (reservation.restRouteId() == null) {
				throw new IllegalStateException("managed REST routes require an explicit consumer route ID: " + reservation);
			}
			var route = camelContext.getRoute(reservation.restRouteId());
			if (route == null) throw new IllegalStateException("managed REST consumer route is missing: " + reservation);
			String endpointUri = route.getEndpoint().getEndpointUri();
			if (endpointUri == null || !endpointUri.startsWith("rest:")) {
				throw new IllegalStateException("managed REST consumer has no owned REST endpoint: " + endpointUri);
			}
			result.put(reservation, endpointUri);
		}
		return result;
	}

	private void cleanupRestEndpointUris(Map<DmpRouteBuilder.RestReservation, String> endpoints,
			Exception aggregate) {
		List<Exception> failures = new ArrayList<>();
		cleanupRestEndpointUris(endpoints, failures);
		failures.forEach(aggregate::addSuppressed);
	}

	private void cleanupRestEndpointUris(Map<DmpRouteBuilder.RestReservation, String> endpoints,
			List<Exception> failures) {
		for (var entry : new ArrayList<>(endpoints.entrySet())) {
			try {
				Endpoint endpoint = camelContext.hasEndpoint(entry.getValue());
				if (endpoint != null) camelContext.removeEndpoint(endpoint);
				endpoints.remove(entry.getKey());
			} catch (Exception failure) {
				failures.add(failure);
			}
		}
	}

	private static boolean hasRestReservation(RestDefinition definition,
			DmpRouteBuilder.RestReservation reservation) {
		return definition.getVerbs().stream().anyMatch(verb -> reservation.method().equalsIgnoreCase(verb.asVerb())
				&& java.util.Objects.equals(reservation.path(), verb.getPath()));
	}

	private IllegalStateException bundleFailure(ManagedBundle bundle, String action,
			List<Exception> failures) {
		publishResourceSnapshot();
		IllegalStateException error = new IllegalStateException("unable to " + action + " managed bundle '"
				+ bundle.bundleId + "'");
		failures.forEach(error::addSuppressed);
		return error;
	}

	public static final class ManagedBundle {
		private final String bundleId;
		private final Set<String> remainingRouteIds;
		private final Map<String, RouteDefinition> definitions;
		private final Set<DmpRouteBuilder.RestReservation> remainingRestReservations;
		private final Map<DmpRouteBuilder.RestReservation, String> remainingRestEndpointUris;
		private final Set<ManagedRouteResource> remainingResources;
		private final Set<Endpoint> remainingEndpoints = Collections.synchronizedSet(Collections.newSetFromMap(new java.util.IdentityHashMap<>()));
		private final Map<String, Endpoint> endpointBaseline = new HashMap<>();
		private DmpRouteBuilder builder;
		private RoutesConfiguration configuration;
		private Supplier<RoutesConfiguration> definitionFactory;
		private long generation;
		private volatile boolean active;
		private boolean releasingEndpoints;

		private ManagedBundle(String bundleId, List<RouteDefinition> routeDefinitions,
				Collection<DmpRouteBuilder.RestReservation> restReservations,
				Map<DmpRouteBuilder.RestReservation, String> restEndpointUris,
				Collection<ManagedRouteResource> resources) {
			this.bundleId = bundleId;
			this.definitions = new java.util.LinkedHashMap<>();
			for (RouteDefinition definition : routeDefinitions) {
				if (definition.getId() != null) definitions.put(definition.getId(), definition);
			}
			this.remainingRouteIds = Collections.synchronizedSet(new LinkedHashSet<>(definitions.keySet()));
			this.remainingRestReservations = Collections.synchronizedSet(new LinkedHashSet<>(restReservations));
			this.remainingRestEndpointUris = new java.util.LinkedHashMap<>(restEndpointUris);
			this.remainingResources = Collections.synchronizedSet(Collections.newSetFromMap(new java.util.IdentityHashMap<>()));
			this.remainingResources.addAll(resources);
		}

		public String bundleId() { return bundleId; }
		public long generation() { return generation; }
		public boolean active() { return active; }
		public int resourceCount() { return remainingResources.size(); }
		public int endpointCount() { return remainingEndpoints.size(); }
		public Set<String> routeIds() { return Set.copyOf(remainingRouteIds); }
		public Set<DmpRouteBuilder.RestReservation> restReservations() {
			return Set.copyOf(remainingRestReservations);
		}
	}

	public final class StagedBundle implements AutoCloseable {
		private final String bundleId;
		private final long generation;
		private final DmpRouteBuilder builder;
		private final RoutesConfiguration configuration;
		private final Supplier<RoutesConfiguration> definitionFactory;
		private Set<ManagedRouteResource> remainingResources;
		private boolean consumed;

		private StagedBundle(String bundleId, long generation, DmpRouteBuilder builder,
				RoutesConfiguration configuration, Supplier<RoutesConfiguration> definitionFactory) {
			this.bundleId = bundleId;
			this.generation = generation;
			this.builder = builder;
			this.configuration = configuration;
			this.definitionFactory = definitionFactory;
		}

		public long generation() { return generation; }
		public Set<String> routeIds() { return builder.getRouteInventory(); }

		@Override public void close() {
			synchronized (DmpComponent.this) {
				if (consumed) return;
				if (remainingResources == null) {
					remainingResources = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
					remainingResources.addAll(builder.getManagedResources());
				}
				List<Exception> failures = new ArrayList<>();
				closeResources(remainingResources, failures);
				if (!failures.isEmpty()) {
					publishResourceSnapshot();
					IllegalStateException error = new IllegalStateException("unable to close staged bundle resources");
					failures.forEach(error::addSuppressed);
					throw error;
				}
				consumed = true;
				stagedBundles.remove(this);
				publishResourceSnapshot();
			}
		}
	}

	private static Map<String, RouteCreatorFactory> getRouteCreatorFactoryMapDefault() {
		Map<String, RouteCreatorFactory> defaultRouteCreatorFactoryMap = new HashMap<>();
		for (RouteCreatorFactory factory : java.util.ServiceLoader
				.load(RouteCreatorFactory.class, DmpComponent.class.getClassLoader())) {
			RouteCreatorFactory existing = defaultRouteCreatorFactoryMap.putIfAbsent(
					factory.routeTrigger(), factory);
			if (existing != null) {
				throw new IllegalStateException("multiple route creator factories register trigger '"
						+ factory.routeTrigger() + "'");
			}
		}
		if (!defaultRouteCreatorFactoryMap.containsKey(ExecutionPolicyRouteConfiguration.ROUTE_TRIGGER)) {
			throw new IllegalStateException("no connector registers the standard execution-policy route");
		}
		return defaultRouteCreatorFactoryMap;
	}

	/**
	 * Starts the Camel component
	 */
	public void startComponent() {
		startRoutes();
	}

	/** The started context, for runtime services that work beside the bundles. */
	public synchronized CamelContext runtimeContext() {
		if (camelContext == null || !camelContext.getStatus().isStarted())
			throw new IllegalStateException("the DMP Camel context is not started");
		return camelContext;
	}

	/** Adds a runtime-owned control route after core health and REST endpoints have started. */
	public synchronized void addRuntimeRoutes(RouteBuilder routes) throws Exception {
		if (camelContext == null || !camelContext.getStatus().isStarted())
			throw new IllegalStateException("runtime routes require a started DMP Camel context");
		camelContext.addRoutes(routes);
	}

	public synchronized void startRoutes() {
		try {
			for (DmpRuntimeExtension extension : java.util.ServiceLoader
					.load(DmpRuntimeExtension.class, Thread.currentThread().getContextClassLoader())) {
				extension.configure(camelContext);
			}
			camelContext.start();
			logger.info("DMP Camel context started");
		} catch (Exception e) {
			try {
				if (camelContext != null && !camelContext.isStopped()) camelContext.stop();
			} catch (Exception cleanupFailure) {
				e.addSuppressed(cleanupFailure);
			}
			throw new IllegalStateException("DMP Camel context failed to start", e);
		}
	}

	/**
	 * Stops the Camel component
	 */
	public synchronized void stopComponent() {
		if (camelContext == null || camelContext.isStopped()) return;
		List<Exception> failures = new ArrayList<>();
		for (StagedBundle staged : new ArrayList<>(stagedBundles)) {
			try { staged.close(); } catch (Exception failure) { failures.add(failure); }
		}
		for (ManagedBundle bundle : new ArrayList<>(managedBundles.values())) {
			try {
				removeBundle(bundle);
			} catch (Exception failure) {
				failures.add(failure);
			}
		}
		if (!failures.isEmpty()) {
			IllegalStateException error = new IllegalStateException("unable to retire managed bundles before component stop");
			failures.forEach(error::addSuppressed);
			throw error;
		}
		camelContext.stop();
		logger.info("DMP Camel context stopped");
	}
}
