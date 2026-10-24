/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent.ManagedBundle;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.influxdb2.InfluxValues;
import dk.aau.smartproductionlab.dmp.influxdb2.RecordColumn;
import dk.aau.smartproductionlab.dmp.influxdb2.configuration.InfluxDb2DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.influxdb2.configuration.InfluxDb2DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory.DesiredBundle;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory.RuntimeSettings;
import dk.aau.smartproductionlab.dmp.management.compiler.EffectiveFingerprint;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.MappingPlan;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.Plan;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery.MappingCandidate;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery.MappingKey;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery.Scan;

/**
 * Mapping-level bundle convergence. One full scan at startup; afterwards only relevant repository events
 * trigger targeted refreshes. A pass with transient failures is retried with backoff.
 */
public final class MappingReconciliationService implements AutoCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger(MappingReconciliationService.class);
	public enum State { INSTALLED, BLOCKED, STALE, FAILED, REMOVED }
	/** Supplies complete authoritative desired state; repository scans also revalidate installed membership. */
	@FunctionalInterface public interface ScanProvider { Scan scan(); }
	@FunctionalInterface public interface DefinitionFactory { Supplier<RoutesConfiguration> freshDefinition(MappingCandidate candidate); }
	/**
	 * One mapping's reconciliation state. {@code generation} is the installed bundle's fence, drawn from a sequence
	 * shared by all bundles; {@code revision} counts this mapping's installations since the DMP started (1 for the
	 * first, +1 per replacement) and is for display only.
	 */
	/** One reason a mapping does not run as authored: where in the mapping ({@code Sources/value/0}, or empty) and what. */
	public record Diagnostic(String path, String message) { }
	/**
	 * {@code cause} is set when installation failed: one of {@link InstallationCause}'s fixed causes, never the
	 * exception text. {@code diagnostics} explain a blocked or stale mapping.
	 */
	public record MappingStatus(MappingKey key, State state, String desiredFingerprint,
			String installedFingerprint, long generation, String category, long revision,
			String cause, List<Diagnostic> diagnostics) {
		public MappingStatus(MappingKey key, State state, String desiredFingerprint,
				String installedFingerprint, long generation, String category, long revision) {
			this(key, state, desiredFingerprint, installedFingerprint, generation, category, revision, "", List.of());
		}
		public MappingStatus { cause = cause == null ? "" : cause; diagnostics = List.copyOf(diagnostics); }
	}
	public record Report(boolean complete, boolean superseded, int installedOrUpdated,
			int unchanged, int removed, List<MappingStatus> mappings, List<String> categories) {
		public Report { mappings = List.copyOf(mappings); categories = List.copyOf(categories); }
	}

	private record Active(MappingKey key, String bundleId, ManagedBundle handle,
			EffectiveFingerprint fingerprint, Set<String> dependencies) { }
	private record MutableStatus(State state, String desired, String installed, long generation, String category,
			String cause, List<Diagnostic> diagnostics) { }

	private final DmpComponent component;
	private final ScanProvider scanProvider;
	private final java.util.function.Function<Set<String>, Scan> trackedScanProvider;
	private final java.util.function.Predicate<AasControlEvent> relevance;
	private RepositoryDiscovery discovery;
	private final DefinitionFactory definitions;
	private final int maxMappings;
	private final int maxQueuedInvalidations;
	private final long retryMillis;
	private final Map<MappingKey, Active> active = new LinkedHashMap<>();
	private final Map<MappingKey, MutableStatus> statuses = new LinkedHashMap<>();
	/** Installations per mapping since start; dropped with the mapping's status. Not a fence. */
	private final Map<MappingKey, Long> revisions = new LinkedHashMap<>();
	/** Per dependency, the bundles it concerns (installed or being installed); fenced on its invalidation. */
	private final Map<String, Set<String>> activeBundleDependencies = new java.util.concurrent.ConcurrentHashMap<>();
	private final Map<String, Long> dependencyEpochs = new LinkedHashMap<>();
	private long fullRescanEpoch;
	private final AtomicLong invalidationEpoch = new AtomicLong();
	private final AtomicLong invalidationOverflows = new AtomicLong();
	private final AtomicLong ignoredEvents = new AtomicLong();
	private final AtomicBoolean scanQueued = new AtomicBoolean();
	private final AtomicInteger consecutiveFullScanFailures = new AtomicInteger();
	private final Object lock = new Object();
	private final Object scheduleLock = new Object();
	private volatile ScheduledExecutorService executor;
	private volatile ScheduledFuture<?> scheduledRetry;
	private volatile Consumer<Report> reportListener = ignored -> { };
	private volatile boolean closed;
	private volatile long appliedEpoch;
	private volatile long retryNotBeforeNanos;
	private volatile int overflowMappings;
	private volatile boolean scanned;
	/** Events accepted while a pass reads the repository, decided again once its index is in place. */
	private final List<AasControlEvent> heldEvents = new ArrayList<>();
	private boolean discovering;
	private boolean heldOverflow;
	private static final int MAX_HELD_EVENTS = 10_000;

	public MappingReconciliationService(DmpComponent component, RepositoryDiscovery discovery, RuntimeSettings runtime,
			int maxMappings, int maxQueuedInvalidations, long retryMillis) {
		this(component, discovery::scan, defaultDefinitions(runtime), maxMappings, maxQueuedInvalidations, retryMillis,
				changed -> changed == null ? discovery.scan() : discovery.refresh(changed), discovery::relevant);
		this.discovery = discovery;
	}

	/** Every scan is a full scan and every event is relevant; for tests and simple providers. */
	public MappingReconciliationService(DmpComponent component, ScanProvider scanProvider,
			DefinitionFactory definitions, int maxMappings, int maxQueuedInvalidations, long retryMillis) {
		this(component, scanProvider, definitions, maxMappings, maxQueuedInvalidations, retryMillis, null, event -> true);
	}

	private MappingReconciliationService(DmpComponent component, ScanProvider scanProvider,
			DefinitionFactory definitions, int maxMappings, int maxQueuedInvalidations, long retryMillis,
			java.util.function.Function<Set<String>, Scan> trackedScanProvider,
			java.util.function.Predicate<AasControlEvent> relevance) {
		this.component = Objects.requireNonNull(component);
		this.scanProvider = Objects.requireNonNull(scanProvider);
		this.trackedScanProvider = trackedScanProvider;
		this.relevance = Objects.requireNonNull(relevance);
		this.definitions = Objects.requireNonNull(definitions);
		if (maxMappings < 1 || maxMappings > 8192) throw new IllegalArgumentException("maxMappings must be 1..8192");
		if (maxQueuedInvalidations < 1 || maxQueuedInvalidations > 65_536)
			throw new IllegalArgumentException("maxQueuedInvalidations must be 1..65536");
		if (retryMillis < 100 || retryMillis > TimeUnit.DAYS.toMillis(1))
			throw new IllegalArgumentException("retryMillis must be 100..86400000");
		this.maxMappings = maxMappings;
		this.maxQueuedInvalidations = maxQueuedInvalidations;
		this.retryMillis = retryMillis;
	}

	private static DefinitionFactory defaultDefinitions(RuntimeSettings runtime) {
		ConnectorConfigurationFactory factory = new ConnectorConfigurationFactory();
		return candidate -> {
			MappingPlan mapping = candidate.desired().desired().plan();
			Plan plan = new Plan(candidate.key().aimcId(), mapping == null ? List.of() : List.of(mapping));
			return () -> {
				var result = factory.createBundles(plan, runtime);
				if (!result.issues().isEmpty() || result.bundles().size() != 1)
					throw new IllegalStateException("validated mapping definition could not be reconstructed");
				return result.bundles().get(0).configuration();
			};
		};
	}

	/** Starts processing invalidations; a scan runs now only when one is pending or the last pass must be retried. */
	public synchronized void start() {
		if (closed) throw new IllegalStateException("reconciliation service is closed");
		if (executor != null) return;
		start(Executors.newSingleThreadScheduledExecutor(r -> {
			Thread thread = new Thread(r, "dmp-reconciliation");
			thread.setDaemon(true);
			return thread;
		}));
	}

	synchronized void start(ScheduledExecutorService worker) {
		if (closed) throw new IllegalStateException("reconciliation service is closed");
		if (executor != null) return;
		executor = Objects.requireNonNull(worker);
		if (!scanned || invalidationEpoch.get() != appliedEpoch) queueScan();
		else if (consecutiveFullScanFailures.get() > 0) scheduleRetry(nextRetryDelay());
	}

	public void setReportListener(Consumer<Report> listener) {
		reportListener = listener == null ? ignored -> { } : listener;
	}

	/**
	 * Invalidates what a relevant repository event concerns; irrelevant events are dropped. Relevance is judged
	 * against the desired state the last pass found. While a pass reads the repository, that state is about to
	 * change (at startup it is empty), so the event is also held and judged again against what the pass found.
	 */
	public void acceptEvent(AasControlEvent event) {
		if (closed) throw new IllegalStateException("reconciliation service is closed");
		if (event.ignored()) { ignoredEvents.incrementAndGet(); return; }
		boolean held = hold(event);
		if (!relevance.test(event)) { if (!held) ignoredEvents.incrementAndGet(); return; }
		invalidate(event.fullRescan() ? null : event.resourceId());
	}

	private boolean hold(AasControlEvent event) {
		synchronized (heldEvents) {
			if (!discovering) return false;
			if (heldEvents.size() < MAX_HELD_EVENTS) heldEvents.add(event);
			else heldOverflow = true;
			return true;
		}
	}

	/** Around a pass's repository reads: hold the events that arrive meanwhile, then judge them against its result. */
	private void discovering(boolean started) {
		List<AasControlEvent> held;
		boolean overflow;
		synchronized (heldEvents) {
			discovering = started;
			if (started) return;
			held = List.copyOf(heldEvents);
			overflow = heldOverflow;
			heldEvents.clear();
			heldOverflow = false;
		}
		if (overflow) { invalidate(null); return; }
		for (AasControlEvent event : held)
			if (relevance.test(event)) invalidate(event.fullRescan() ? null : event.resourceId());
	}

	/** Accepts an event invalidation into bounded tracking; overflow schedules a full repair. */
	public void invalidate(String dependencyId) {
		if (closed) throw new IllegalStateException("reconciliation service is closed");
		synchronized (dependencyEpochs) {
			long epoch = invalidationEpoch.incrementAndGet();
			boolean unidentified = dependencyId == null || dependencyId.isBlank();
			boolean overflow = !unidentified && !dependencyEpochs.containsKey(dependencyId)
					&& dependencyEpochs.size() >= maxQueuedInvalidations;
			boolean full = unidentified || overflow;
			if (overflow) invalidationOverflows.incrementAndGet();
			if (full) {
				fullRescanEpoch = epoch;
				dependencyEpochs.clear();
			} else dependencyEpochs.put(dependencyId, epoch);
			Set<String> affected = full ? activeBundleDependencies.values().stream().flatMap(Set::stream)
					.collect(java.util.stream.Collectors.toSet()) : Set.copyOf(activeBundleDependencies.getOrDefault(dependencyId, Set.of()));
			for (String bundleId : affected) component.fenceBundleGeneration(bundleId);
		}
		queueScan();
	}

	/** A full scan now, on the caller's thread. */
	public Report reconcileNow() {
		return reconcile(true);
	}

	private Report reconcile(boolean fullRepair) {
		synchronized (lock) {
			if (closed) return report(false, false, 0, 0, 0, List.of("service-closed"));
			long epoch;
			Set<String> changedIds;
			synchronized (dependencyEpochs) {
				epoch = invalidationEpoch.get();
				boolean full = fullRepair || fullRescanEpoch > appliedEpoch || !scanned;
				changedIds = full ? null : Set.copyOf(dependencyEpochs.keySet());
			}
			Scan scan;
			discovering(true);
			try {
				scan = trackedScanProvider == null ? scanProvider.scan() : trackedScanProvider.apply(changedIds);
			}
			catch (RuntimeException failure) { return failWholeScan("repository-scan-failed"); }
			finally { discovering(false); }
			if (!scan.complete()) return failWholeScan(scan.failureCategory());
			scanned = true;
			int changed = 0, unchanged = 0, removed = 0;
			boolean superseded = false;
			List<String> categories = new ArrayList<>();
			scan.failures().forEach(failure -> categories.add(failure.category()));
			Map<MappingKey, List<Diagnostic>> restConflicts = restConflicts(scan.mappings().values());
			Map<MappingKey, List<Diagnostic>> timeSeriesConflicts = timeSeriesConflicts(scan.mappings().values());
			for (var entry : new ArrayList<>(active.entrySet())) {
				MappingKey key = entry.getKey();
				if (superseded(epoch, key, entry.getValue().dependencies())) { superseded = true; continue; }
				if (scan.failedAimcIds().contains(key.aimcId())) {
					String category = scan.failures().stream().filter(failure -> failure.aimcId().equals(key.aimcId()))
							.map(RepositoryDiscovery.AimcFailure::category).findFirst().orElse("aimc-fetch-failed");
					mark(key, State.STALE, null, entry.getValue().fingerprint().exported(),
							entry.getValue().handle().generation(), category);
					continue;
				}
				if (!scan.mappings().containsKey(key)) {
					if (retire(entry.getValue())) { active.remove(key); statuses.remove(key); revisions.remove(key); removed++; }
					else {
						mark(key, State.STALE, null, entry.getValue().fingerprint().exported(),
								entry.getValue().handle().generation(), "bundle-removal-failed");
						categories.add("bundle-removal-failed");
					}
				}
			}
			for (MappingKey key : new ArrayList<>(statuses.keySet())) {
				if (!active.containsKey(key) && !superseded(epoch, key, Set.of())
						&& !scan.failedAimcIds().contains(key.aimcId()) && !scan.mappings().containsKey(key)) {
					statuses.remove(key);
					revisions.remove(key);
				}
			}
			overflowMappings = 0;
			for (MappingCandidate candidate : scan.mappings().values()) {
				Set<String> dependencies = new LinkedHashSet<>(dependencyIds(candidate));
				Active prior = active.get(candidate.key());
				if (prior != null) dependencies.addAll(prior.dependencies());
				if (superseded(epoch, candidate.key(), dependencies)) { superseded = true; continue; }
				if (statuses.size() >= maxMappings && !statuses.containsKey(candidate.key())) {
					if (overflowMappings++ == 0) categories.add("mapping-capacity-exceeded");
					continue;
				}
				Active previous = active.get(candidate.key());
				if (restConflicts.containsKey(candidate.key())) {
					mark(candidate.key(), previous == null ? State.BLOCKED : State.STALE,
							candidate.desired().fingerprint().exported(),
							previous == null ? null : previous.fingerprint().exported(),
							previous == null ? 0 : previous.handle().generation(), "rest-path-conflict",
							"", restConflicts.get(candidate.key()));
					categories.add("rest-path-conflict");
					continue;
				}
				if (timeSeriesConflicts.containsKey(candidate.key())) {
					mark(candidate.key(), previous == null ? State.BLOCKED : State.STALE,
							candidate.desired().fingerprint().exported(),
							previous == null ? null : previous.fingerprint().exported(),
							previous == null ? 0 : previous.handle().generation(), "timeseries-type-conflict",
							"", timeSeriesConflicts.get(candidate.key()));
					categories.add("timeseries-type-conflict");
					continue;
				}
				if (!candidate.desired().valid()) {
					if (previous != null && confirmedMissingDependency(candidate.desired())) {
						if (retire(previous)) { active.remove(candidate.key()); removed++; previous = null; }
						else {
						mark(candidate.key(), State.FAILED, candidate.desired().fingerprint().exported(),
								previous.fingerprint().exported(), previous.handle().generation(), "bundle-removal-failed");
						categories.add("bundle-removal-failed");
						continue;
						}
					}
					mark(candidate.key(), previous == null ? State.BLOCKED : State.STALE,
							candidate.desired().fingerprint().exported(),
							previous == null ? null : previous.fingerprint().exported(),
							previous == null ? 0 : previous.handle().generation(), failureCategory(candidate.desired()),
							"", diagnostics(candidate.desired()));
					categories.add(failureCategory(candidate.desired()));
					continue;
				}
				EffectiveFingerprint desiredFingerprint = candidate.desired().fingerprint();
				if (previous != null && !previous.handle().active()) {
					if (!retire(previous)) {
						mark(candidate.key(), State.FAILED, desiredFingerprint.exported(), previous.fingerprint().exported(),
								previous.handle().generation(), "bundle-removal-failed");
						categories.add("bundle-removal-failed");
						continue;
					}
					active.remove(candidate.key());
					previous = null;
				}
				if (previous != null && previous.handle().active()
						&& previous.fingerprint().sameEffectiveConfiguration(desiredFingerprint)) {
					active.put(candidate.key(), new Active(candidate.key(), previous.bundleId(), previous.handle(),
							desiredFingerprint, dependencyIds(candidate)));
					mark(candidate.key(), State.INSTALLED, desiredFingerprint.exported(), desiredFingerprint.exported(),
							previous.handle().generation(), "");
					unchanged++;
					continue;
				}
				try {
					String bundleId = candidate.desired().bundle().bundleId();
					Supplier<RoutesConfiguration> definition = definitions.freshDefinition(candidate);
					if (superseded(epoch, candidate.key(), dependencies)) { superseded = true; continue; }
					publishPendingDependencies(bundleId, dependencyIds(candidate));
					long generation = component.nextBundleGeneration(bundleId);
					DmpComponent.StagedBundle staged = component.stageBundle(bundleId, generation, definition);
					if (superseded(epoch, candidate.key(), dependencies)) {
						staged.close();
						if (previous == null) component.abandonUninstalledBundle(bundleId);
						superseded = true;
						continue;
					}
					ManagedBundle installed = previous == null
							? component.installBundle(staged) : component.replaceBundle(previous.handle(), staged);
					active.put(candidate.key(), new Active(candidate.key(), bundleId, installed, desiredFingerprint,
							dependencyIds(candidate)));
					revisions.merge(candidate.key(), 1L, Long::sum);
					mark(candidate.key(), State.INSTALLED, desiredFingerprint.exported(), desiredFingerprint.exported(), generation, "");
					changed++;
				} catch (RuntimeException failure) {
					LOGGER.warn("Bundle {} failed to install: {}", candidate.desired().bundle().bundleId(), failure.toString(), failure);
					ManagedBundle surviving = component.managedBundle(previous == null
							? candidate.desired().bundle().bundleId() : previous.bundleId());
					if (surviving != null) {
						active.put(candidate.key(), new Active(candidate.key(), surviving.bundleId(), surviving,
								previous == null ? desiredFingerprint : previous.fingerprint(),
								previous == null ? dependencyIds(candidate) : previous.dependencies()));
					} else {
						active.remove(candidate.key());
						component.abandonUninstalledBundle(candidate.desired().bundle().bundleId());
					}
					mark(candidate.key(), State.FAILED, desiredFingerprint.exported(),
							surviving == null || previous == null ? null : previous.fingerprint().exported(),
							surviving == null ? 0 : surviving.generation(), "bundle-installation-failed",
							InstallationCause.of(failure), List.of());
					if (superseded(epoch, candidate.key(), dependencies)) { superseded = true; continue; }
					categories.add("bundle-installation-failed");
				}
			}
			refreshReverseDependencies();
			appliedEpoch = epoch;
			synchronized (dependencyEpochs) {
				superseded |= epoch != invalidationEpoch.get();
				dependencyEpochs.entrySet().removeIf(entry -> entry.getValue() <= epoch);
			}
			if (superseded) categories.add("scan-superseded");
			return report(true, superseded, changed, unchanged, removed, categories);
		}
	}

	public List<MappingStatus> status() {
		synchronized (lock) {
			return statuses.entrySet().stream().map(entry -> new MappingStatus(entry.getKey(), entry.getValue().state(),
					entry.getValue().desired(), entry.getValue().installed(), entry.getValue().generation(),
					entry.getValue().category(), revisions.getOrDefault(entry.getKey(), 0L),
					entry.getValue().cause(), entry.getValue().diagnostics())).toList();
		}
	}

	public long acceptedInvalidationCount() { return invalidationEpoch.get(); }
	public int consecutiveScanFailures() { return consecutiveFullScanFailures.get(); }
	public int overflowMappingCount() { return overflowMappings; }
	public long invalidationOverflowCount() { return invalidationOverflows.get(); }
	public long ignoredEventCount() { return ignoredEvents.get(); }
	public boolean repairPending() { return invalidationEpoch.get() > appliedEpoch; }
	public long fullScanCount() { return discovery == null ? 0L : discovery.fullScanCount(); }
	public long targetedScanCount() { return discovery == null ? 0L : discovery.targetedScanCount(); }

	private boolean superseded(long epoch, MappingKey key, Set<String> dependencies) {
		synchronized (dependencyEpochs) {
			if (fullRescanEpoch > epoch || dependencyEpochs.getOrDefault(key.aimcId(), 0L) > epoch) return true;
			return dependencies.stream().anyMatch(id -> dependencyEpochs.getOrDefault(id, 0L) > epoch);
		}
	}

	private Report failWholeScan(String category) {
		for (Active value : active.values()) mark(value.key(), State.STALE, null, value.fingerprint().exported(),
				value.handle().generation(), category);
		return report(false, false, 0, 0, 0, List.of(category));
	}

	private boolean retire(Active activeBundle) {
		try {
			ManagedBundle owned = component.managedBundle(activeBundle.bundleId());
			if (owned == null) {
				component.abandonUninstalledBundle(activeBundle.bundleId());
				return true;
			}
			long generation = component.nextBundleGeneration(activeBundle.bundleId());
			component.removeBundle(owned, generation);
			return true;
		} catch (RuntimeException failure) { return false; }
	}

	private static Set<String> dependencyIds(MappingCandidate candidate) {
		Set<String> ids = candidate.desired().desired().dependencies().stream().map(
				MappingPlanCompiler.Dependency::id).collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
		ids.add(candidate.key().aimcId());
		return Set.copyOf(ids);
	}

	/**
	 * Rebuilds the reverse dependencies from the active bundles, dropping what a pass staged but did not install.
	 * Once per pass; within it, {@link #publishPendingDependencies} adds to them bundle by bundle.
	 */
	private void refreshReverseDependencies() {
		Map<String, Set<String>> reverse = new LinkedHashMap<>();
		for (Active installed : active.values()) for (String dependency : installed.dependencies())
			reverse.computeIfAbsent(dependency, ignored -> java.util.concurrent.ConcurrentHashMap.newKeySet()).add(installed.bundleId());
		activeBundleDependencies.keySet().retainAll(reverse.keySet());
		activeBundleDependencies.putAll(reverse);
	}

	/** Before a bundle installs: it concerns its dependencies from now on, so their invalidation fences it. */
	private void publishPendingDependencies(String bundleId, Set<String> dependencies) {
		for (String dependency : dependencies)
			activeBundleDependencies.computeIfAbsent(dependency, ignored -> java.util.concurrent.ConcurrentHashMap.newKeySet()).add(bundleId);
	}

	private static boolean confirmedMissingDependency(DesiredBundle desired) {
		return desired.desired().failures().stream()
				.anyMatch(failure -> failure.status() == AasRepositoryClient.Status.NOT_FOUND);
	}

	private static Map<MappingKey, List<Diagnostic>> restConflicts(Iterable<MappingCandidate> candidates) {
		Map<String, List<MappingKey>> reservations = new LinkedHashMap<>();
		for (MappingCandidate candidate : candidates) {
			if (!candidate.desired().valid() || candidate.desired().bundle() == null) continue;
			for (RouteConfiguration route : candidate.desired().bundle().configuration().getRoutes()) {
				if (!"bridge".equals(route.getRouteTrigger()) || route.getTriggerData() == null) continue;
				Object path = route.getTriggerData().get("restPath");
				if (path instanceof String restPath && !restPath.isBlank()) {
					String reservation = "POST " + restPath;
					reservations.computeIfAbsent(reservation, ignored -> new ArrayList<>()).add(candidate.key());
				}
			}
		}
		Map<MappingKey, List<Diagnostic>> conflicts = new LinkedHashMap<>();
		reservations.forEach((reservation, keys) -> {
			if (keys.size() < 2) return;
			for (MappingKey key : keys) {
				String others = keys.stream().filter(other -> !other.equals(key)).map(MappingReconciliationService::describe)
						.distinct().collect(java.util.stream.Collectors.joining(", "));
				conflicts.computeIfAbsent(key, ignored -> new ArrayList<>())
						.add(new Diagnostic("", reservation + " is also served by " + others));
			}
		});
		return Map.copyOf(conflicts);
	}

	private static String describe(MappingKey key) {
		return "mapping " + key.mappingIdentity() + " of AIMC " + key.aimcId();
	}

	/**
	 * Mappings that write or read one InfluxDB field (bucket, measurement, field) as different types. InfluxDB fixes
	 * a field's type per measurement, so every mapping of such a disagreement is held back, and the declarations are
	 * logged together.
	 */
	private static Map<MappingKey, List<Diagnostic>> timeSeriesConflicts(Iterable<MappingCandidate> candidates) {
		Map<String, Map<String, Set<MappingKey>>> types = new LinkedHashMap<>();
		for (MappingCandidate candidate : candidates) {
			if (!candidate.desired().valid() || candidate.desired().bundle() == null) continue;
			var configuration = candidate.desired().bundle().configuration();
			List<Object> endpoints = new ArrayList<>(configuration.getDatasinks().values());
			endpoints.addAll(configuration.getDatasources().values());
			for (Object endpoint : endpoints) {
				String bucket, measurement;
				List<RecordColumn> columns;
				if (endpoint instanceof InfluxDb2DataSinkConfiguration sink) {
					bucket = sink.getBucket(); measurement = sink.getMeasurement(); columns = sink.getColumns();
				} else if (endpoint instanceof InfluxDb2DataSourceConfiguration source) {
					bucket = source.getBucket(); measurement = source.getMeasurement(); columns = source.getColumns();
				} else continue;
				for (var column : columns)
					types.computeIfAbsent(bucket + "/" + measurement + "/" + column.path(), ignored -> new LinkedHashMap<>())
							.computeIfAbsent(InfluxValues.kind(column.valueType()).name()
									+ " (" + column.valueType() + ")", ignored -> new LinkedHashSet<>())
							.add(candidate.key());
			}
		}
		Map<MappingKey, List<Diagnostic>> conflicts = new LinkedHashMap<>();
		types.forEach((field, byType) -> {
			if (byType.keySet().stream().map(MappingReconciliationService::kind).distinct().count() < 2) return;
			byType.forEach((type, keys) -> {
				String others = byType.entrySet().stream().filter(other -> !kind(other.getKey()).equals(kind(type)))
						.map(other -> other.getKey() + " in " + other.getValue().stream().map(MappingReconciliationService::describe)
								.collect(java.util.stream.Collectors.joining(", ")))
						.collect(java.util.stream.Collectors.joining("; "));
				for (MappingKey key : keys) conflicts.computeIfAbsent(key, ignored -> new ArrayList<>())
						.add(new Diagnostic("", "InfluxDB field " + field + " is " + type + " here, but " + others));
			});
			LOGGER.warn("TimeSeries field {} is declared as {}; the mappings involved are held back", field, byType.keySet());
		});
		return Map.copyOf(conflicts);
	}

	/** The InfluxDB kind of a {@code "KIND (xs:type)"} declaration. */
	private static String kind(String type) { return type.substring(0, type.indexOf(' ')); }

	private static final java.util.regex.Pattern MAPPING_PREFIX =
			java.util.regex.Pattern.compile("^/?(?:MappingConfigurations|submodelElements/\\d+)/value/\\d+/?");

	/** Why a mapping did not compile, each at its path relative to the mapping. */
	static List<Diagnostic> diagnostics(DesiredBundle desired) {
		var mapping = desired.desired();
		List<Diagnostic> result = new ArrayList<>();
		if (mapping.projectionFailure() != null) {
			result.add(new Diagnostic(relativePath(mapping.projectionFailure().pointer()), mapping.projectionFailure().message()));
			return List.copyOf(result);
		}
		for (int i = 0; i < mapping.failures().size(); i++) {
			var failure = mapping.failures().get(i);
			String status = failure.status().name().toLowerCase(java.util.Locale.ROOT).replace('_', ' ');
			result.add(new Diagnostic(mapping.failurePath(i), failure.detail().isBlank() ? status : status + ": " + failure.detail()));
		}
		for (var issue : desired.issues()) result.add(new Diagnostic(relativePath(issue.path()), issue.message()));
		return List.copyOf(result);
	}

	/** A compiler path ({@code MappingConfigurations/value/3/Sinks/value/1}) relative to its mapping ({@code Sinks/value/1}). */
	static String relativePath(String path) {
		return path == null ? "" : MAPPING_PREFIX.matcher(path).replaceFirst("");
	}

	private static String failureCategory(DesiredBundle desired) {
		if (!desired.desired().failures().isEmpty())
			return "mapping-resolution-" + desired.desired().failures().get(0).status().name().toLowerCase(java.util.Locale.ROOT);
		return desired.issues().isEmpty() ? "invalid-desired-mapping" : "unsupported-connector-capability";
	}

	private void mark(MappingKey key, State state, String desired, String installed, long generation, String category) {
		mark(key, state, desired, installed, generation, category, "", List.of());
	}

	private void mark(MappingKey key, State state, String desired, String installed, long generation, String category,
			String cause, List<Diagnostic> diagnostics) {
		statuses.remove(key);
		statuses.put(key, new MutableStatus(state, desired, installed, generation, category, cause, List.copyOf(diagnostics)));
	}

	private Report report(boolean complete, boolean superseded, int changed, int unchanged, int removed, List<String> categories) {
		Report result = new Report(complete, superseded, changed, unchanged, removed, status(), categories);
		try { reportListener.accept(result); } catch (RuntimeException ignored) { }
		return result;
	}

	private void queueScan() {
		ScheduledExecutorService worker = executor;
		long retryWait = retryNotBeforeNanos - System.nanoTime();
		if (retryWait > 0) {
			scheduleRetry(Math.max(1, TimeUnit.NANOSECONDS.toMillis(retryWait)));
			return;
		}
		if (closed || worker == null || !scanQueued.compareAndSet(false, true)) return;
		synchronized (scheduleLock) {
			if (scheduledRetry != null) scheduledRetry.cancel(false);
			scheduledRetry = null;
		}
		try { worker.execute(() -> {
			boolean retry = false;
			long delay = retryMillis;
			try {
				Report report;
				try { report = reconcile(false); }
				catch (RuntimeException failure) { report = failWholeScan("repository-scan-failed"); }
				retry = !report.complete() || hasTransientFailure(report.categories());
				if (!retry) consecutiveFullScanFailures.set(0);
				else consecutiveFullScanFailures.updateAndGet(count -> Math.min(10, count + 1));
				delay = nextRetryDelay();
				retryNotBeforeNanos = retry ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay) : 0;
			} finally {
				scanQueued.set(false);
				if (!closed && !retry && invalidationEpoch.get() != appliedEpoch) queueScan();
				else if (retry) scheduleRetry(delay);
			}
		}); } catch (java.util.concurrent.RejectedExecutionException closedDuringSubmit) {
			scanQueued.set(false);
		}
	}

	private long nextRetryDelay() {
		int failures = consecutiveFullScanFailures.get();
		if (failures == 0) return retryMillis;
		long multiplier = 1L << Math.min(4, failures - 1);
		return Math.min(TimeUnit.HOURS.toMillis(1), retryMillis * multiplier);
	}

	private static boolean hasTransientFailure(List<String> categories) {
		return categories.stream().anyMatch(category -> category.equals("aimc-fetch-failed")
				|| category.equals("aimc-membership-unconfirmed") || category.startsWith("bundle-")
				|| category.equals("aimc-scan-timeout") || category.equals("aimc-scan-interrupted")
				|| category.contains("transient_failure") || category.contains("unauthorized")
				|| category.startsWith("repository-"));
	}

	private void scheduleRetry(long delayMillis) {
		ScheduledExecutorService worker = executor;
		if (closed || worker == null || scanQueued.get()) return;
		synchronized (scheduleLock) {
			if (closed || worker != executor || scanQueued.get()) return;
			if (scheduledRetry != null) scheduledRetry.cancel(false);
			scheduledRetry = worker.schedule(() -> { retryNotBeforeNanos = 0; queueScan(); }, Math.max(0, delayMillis), TimeUnit.MILLISECONDS);
		}
	}

	@Override public synchronized void close() {
		closed = true;
		ScheduledExecutorService worker = executor;
		if (worker != null) {
			if (scheduledRetry != null) scheduledRetry.cancel(false);
			worker.shutdownNow();
			try { worker.awaitTermination(2, TimeUnit.SECONDS); }
			catch (InterruptedException e) { Thread.currentThread().interrupt(); }
			executor = null;
		}
	}
}
