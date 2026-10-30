/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;

import dk.aau.smartproductionlab.dmp.influxdb2.component.DmpInfluxDb2Component;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService;

/** Process-wide, credential-free readiness and reconciliation summary. */
public final class ManagedRuntimeStatus {
	public enum State { STARTING, READY, DEGRADED, STOPPING, STOPPED, FAILED }
	public record Snapshot(State state, boolean ready, boolean initialScanComplete,
			int installedMappings, int blockedMappings, int staleMappings, int failedMappings,
			String category, String updatedAt) { }

	private static final AtomicReference<Snapshot> CURRENT = new AtomicReference<>(
			initialSnapshot());
	private static final AtomicReference<MappingReconciliationService.Report> LAST_REPORT = new AtomicReference<>();
	private static final AtomicReference<MappingReconciliationService> SERVICE = new AtomicReference<>();
	private static final AtomicReference<dk.aau.smartproductionlab.dmp.core.component.DmpComponent> COMPONENT = new AtomicReference<>();
	private static final AtomicReference<DmpInfluxDb2Component> INFLUX = new AtomicReference<>();
	private static final AtomicBoolean CONTROL_ROUTE_STARTED = new AtomicBoolean();
	private static final AtomicReference<ManagedControlReadiness> CONTROL_READINESS = new AtomicReference<>();
	private ManagedRuntimeStatus() { }

	public static Snapshot snapshot() { refreshReadiness(); return CURRENT.get(); }

	public static void beginManaged() {
		SERVICE.set(null);
		COMPONENT.set(null);
		INFLUX.set(null);
		LAST_REPORT.set(null);
		CONTROL_ROUTE_STARTED.set(false);
		CONTROL_READINESS.set(null);
		CURRENT.set(new Snapshot(State.STARTING, false, false, 0, 0, 0, 0, "initial-reconciliation", Instant.now().toString()));
	}

	/** The InfluxDB component of the runtime, for its asynchronous write counters; read without any lock. */
	static void trackInflux(DmpInfluxDb2Component influx) { INFLUX.set(influx); }

	static void trackReconciliation(MappingReconciliationService service, dk.aau.smartproductionlab.dmp.core.component.DmpComponent component) {
		SERVICE.set(service); COMPONENT.set(component);
	}

	static void trackControlReadiness(ManagedControlReadiness readiness) { CONTROL_READINESS.set(readiness); }

	public static void publish(MappingReconciliationService.Report report) {
		State current = CURRENT.get().state();
		if (current == State.STOPPING || current == State.STOPPED || current == State.FAILED) return;
		LAST_REPORT.set(report);
		refreshReadiness();
	}

	public static void controlRouteStarted() {
		CONTROL_ROUTE_STARTED.set(true);
		refreshReadiness();
	}

	private static void refreshReadiness() {
		Snapshot old = CURRENT.get();
		if (old.state() == State.STOPPING || old.state() == State.STOPPED || old.state() == State.FAILED) return;
		MappingReconciliationService.Report report = LAST_REPORT.get();
		if (report == null) return;
		int installed = 0, blocked = 0, stale = 0, failed = 0;
		for (MappingReconciliationService.MappingStatus status : report.mappings()) {
			switch (status.state()) {
			case INSTALLED -> installed++;
			case BLOCKED -> blocked++;
			case STALE -> stale++;
			case FAILED -> failed++;
			case REMOVED -> { }
			}
		}
		boolean transientFailure = report.categories().stream().anyMatch(ManagedRuntimeStatus::transientCategory);
		var observer = CONTROL_READINESS.get();
		// The package-private non-Kafka route factory is only a test fixture.
		var control = observer == null ? new ManagedControlReadiness.Snapshot(CONTROL_ROUTE_STARTED.get(), "control-assignment-pending")
				: observer.snapshot();
		// Ready (SPEC §4.4): the initial scan completed once and the event transport is connected. A later pass that
		// fails, or a mapping that cannot install, degrades the status without taking the process out of service.
		boolean initialScanComplete = old.initialScanComplete() || report.complete();
		boolean ready = CONTROL_ROUTE_STARTED.get() && control.ready() && initialScanComplete;
		boolean degraded = !report.complete() || report.superseded() || transientFailure;
		State state = !CONTROL_ROUTE_STARTED.get() ? State.STARTING : ready && !degraded ? State.READY : State.DEGRADED;
		String category = !control.ready() ? control.category()
				: firstCategory(report.categories(), report.complete() ? "" : "repository-scan-incomplete");
		if (old.state() == state && old.ready() == ready && old.initialScanComplete() == initialScanComplete
				&& old.installedMappings() == installed && old.blockedMappings() == blocked && old.staleMappings() == stale
				&& old.failedMappings() == failed && old.category().equals(category)) return;
		CURRENT.compareAndSet(old, new Snapshot(state, ready, initialScanComplete, installed, blocked, stale, failed,
				category, Instant.now().toString()));
	}

	public static void stopping() { updateState(State.STOPPING, false, "shutdown"); }
	public static void stopped() { SERVICE.set(null); COMPONENT.set(null); INFLUX.set(null); updateState(State.STOPPED, false, ""); }
	public static void failed(String category) { updateState(State.FAILED, false, sanitize(category)); }
	/** Returns to the pre-start state; used between runtime instances, e.g. in tests. */
	public static void reset() {
		SERVICE.set(null);
		COMPONENT.set(null);
		INFLUX.set(null);
		CONTROL_ROUTE_STARTED.set(false);
		CONTROL_READINESS.set(null);
		LAST_REPORT.set(null);
		CURRENT.set(initialSnapshot());
	}

	private static Snapshot initialSnapshot() {
		return new Snapshot(State.STARTING, false, false, 0, 0, 0, 0, "not-started", Instant.now().toString());
	}

	public static Map<String, Object> asMap() {
		Snapshot value = snapshot();
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("state", value.state().name().toLowerCase(java.util.Locale.ROOT));
		result.put("ready", value.ready());
		result.put("initialScanComplete", value.initialScanComplete());
		result.put("installedMappings", value.installedMappings());
		result.put("blockedMappings", value.blockedMappings());
		result.put("staleMappings", value.staleMappings());
		result.put("failedMappings", value.failedMappings());
		result.put("category", value.category());
		result.put("updatedAt", value.updatedAt());
		var observer = CONTROL_READINESS.get();
		var control = observer == null ? null : observer.snapshot();
		result.put("controlReady", CONTROL_ROUTE_STARTED.get() && (control == null || control.ready()));
		result.put("controlCategory", control == null ? "" : control.category());
		var service = SERVICE.get();
		result.put("acceptedInvalidations", service == null ? 0L : service.acceptedInvalidationCount());
		result.put("consecutiveScanFailures", service == null ? 0 : service.consecutiveScanFailures());
		result.put("overflowMappings", service == null ? 0 : service.overflowMappingCount());
		result.put("invalidationOverflows", service == null ? 0L : service.invalidationOverflowCount());
		result.put("ignoredEvents", service == null ? 0L : service.ignoredEventCount());
		result.put("fullDiscoveryScans", service == null ? 0L : service.fullScanCount());
		result.put("targetedDiscoveryScans", service == null ? 0L : service.targetedScanCount());
		boolean pending = service != null && service.repairPending();
		result.put("repairPending", pending);
		var component = COMPONENT.get();
		result.put("resources", component == null ? Map.of() : component.resourceMetrics());
		result.put("resourceOwners", component == null ? java.util.List.of() : component.bundleResourceMetrics().entrySet().stream()
				.map(entry -> Map.<String, Object>of("bundleToken", token(entry.getKey().bundleId()),
						"generation", entry.getKey().generation(), "active", entry.getKey().active(), "resources", entry.getValue()))
				.sorted(java.util.Comparator.comparing(owner -> (String) owner.get("bundleToken"))).toList());
		result.put("process", component == null ? Map.of() : component.processMetrics());
		var influx = INFLUX.get();
		result.put("influxAsynchronousWrites", influx == null ? Map.of() : influx.asynchronousWrites());
		var report = LAST_REPORT.get();
		result.put("converged", value.ready() && !pending && value.blockedMappings() == 0 && value.staleMappings() == 0
				&& value.failedMappings() == 0 && report != null && report.categories().isEmpty());
		result.put("mappings", report == null ? java.util.List.of() : report.mappings().stream().map(mapping -> Map.of(
				"aimcToken", token(mapping.key().aimcId()),
				"mappingToken", token(mapping.key().mappingIdentity()),
				"generation", mapping.generation(), "revision", mapping.revision(),
				"state", mapping.state().name().toLowerCase(java.util.Locale.ROOT),
				"desiredFingerprint", mapping.desiredFingerprint() == null ? "" : mapping.desiredFingerprint(),
				"installedFingerprint", mapping.installedFingerprint() == null ? "" : mapping.installedFingerprint(),
				"category", sanitize(mapping.category()))).toList());
		return Map.copyOf(result);
	}

	/**
	 * Why each mapping of one AIMC is or is not running: state, category, the installation cause and the
	 * diagnostics, with their messages. Unlike {@link #asMap()} it names the mappings, so it is asked for by the
	 * AIMC's id; the messages come from the model the AIMC's readers can read already.
	 */
	public static Map<String, Object> mappings(String aimcId) {
		var report = LAST_REPORT.get();
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("aimc", aimcId);
		result.put("reported", report != null);
		result.put("mappings", report == null ? java.util.List.of() : report.mappings().stream()
				.filter(mapping -> mapping.key().aimcId().equals(aimcId))
				.map(mapping -> {
					Map<String, Object> entry = new LinkedHashMap<>();
					entry.put("identity", mapping.key().mappingIdentity());
					entry.put("listIndex", mapping.key().listIndex());
					entry.put("state", mapping.state().name().toLowerCase(java.util.Locale.ROOT));
					entry.put("category", mapping.category() == null ? "" : mapping.category());
					entry.put("cause", mapping.cause());
					entry.put("diagnostics", mapping.diagnostics().stream()
							.map(diagnostic -> Map.of("path", diagnostic.path(), "message", diagnostic.message())).toList());
					return entry;
				}).toList());
		return result;
	}

	private static void updateState(State state, boolean ready, String category) {
		Snapshot old = CURRENT.get();
		CURRENT.set(new Snapshot(state, ready, old.initialScanComplete(), old.installedMappings(),
				old.blockedMappings(), old.staleMappings(), old.failedMappings(), sanitize(category), Instant.now().toString()));
	}

	private static boolean transientCategory(String category) {
		return category.startsWith("repository-") || category.equals("aimc-fetch-failed")
				|| category.equals("aimc-membership-unconfirmed")
				|| category.equals("mapping-resolution-transient_failure") || category.equals("mapping-resolution-unauthorized")
				|| category.startsWith("aimc-scan-") || category.equals("bundle-installation-failed")
				|| category.equals("bundle-removal-failed") || category.equals("scan-superseded");
	}

	private static String firstCategory(java.util.List<String> categories, String fallback) {
		return categories.isEmpty() ? fallback : sanitize(categories.get(0));
	}

	/** Stable SHA-256 token that identifies repository IDs in status output without disclosing them. */
	static String token(String value) {
		if (value == null) return "";
		try {
			return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
					.digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
		} catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
	}

	private static String sanitize(String category) {
		if (category == null) return "";
		String safe = category.replaceAll("[^A-Za-z0-9_.:-]", "_");
		return safe.length() > 96 ? safe.substring(0, 96) : safe;
	}
}
