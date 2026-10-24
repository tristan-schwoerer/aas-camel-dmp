/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory.DesiredBundle;
import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory.RuntimeSettings;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.Dependency;
import dk.aau.smartproductionlab.dmp.management.model.WireModel;
import dk.aau.smartproductionlab.dmp.management.model.WireProjection;

/**
 * Desired mapping state from the repository, read on a need-to-know basis.
 *
 * <p>{@link #scan()} is the one full discovery: it lists the AIMCs by semanticId, reads every submodel their
 * endpoints reference (grouped per submodel, see {@link AasReferenceResolver}) and compiles them. Afterwards
 * {@link #refresh(Set)} re-reads only what changed: a changed AIMC, the submodels a changed dependency
 * concerns, or a new submodel that may be an AIMC. {@link #relevant(AasControlEvent)} tells repository events
 * that can change a mapping from the rest.
 */
public final class RepositoryDiscovery {
	public record MappingKey(String aimcId, String mappingIdentity, int listIndex) {
		public MappingKey {
			if (aimcId == null || aimcId.isBlank() || mappingIdentity == null || mappingIdentity.isBlank() || listIndex < 0)
				throw new IllegalArgumentException("mapping identity is incomplete");
		}
		@Override public boolean equals(Object other) {
			return other instanceof MappingKey key && aimcId.equals(key.aimcId) && mappingIdentity.equals(key.mappingIdentity);
		}
		@Override public int hashCode() { return Objects.hash(aimcId, mappingIdentity); }
	}
	public record MappingCandidate(MappingKey key, DesiredBundle desired) { }
	public record AimcFailure(String aimcId, AasRepositoryClient.Status status, String category) { }
	public record Scan(boolean complete, String failureCategory, Set<String> aimcIds,
			Set<String> failedAimcIds, Map<MappingKey, MappingCandidate> mappings,
			Map<String, Set<MappingKey>> reverseDependencies, List<AimcFailure> failures) {
		public Scan {
			aimcIds = Set.copyOf(aimcIds);
			failedAimcIds = Set.copyOf(failedAimcIds);
			mappings = Collections.unmodifiableMap(new LinkedHashMap<>(mappings));
			Map<String, Set<MappingKey>> reverse = new LinkedHashMap<>();
			reverseDependencies.forEach((id, keys) -> reverse.put(id, Set.copyOf(keys)));
			reverseDependencies = Collections.unmodifiableMap(reverse);
			failures = List.copyOf(failures);
		}
	}

	/** One AIMC's outcome: its mappings, or why it could not be read. */
	private record AimcState(String id, WireModel.Aimc aimc, AasRepositoryClient.Status failureStatus,
			String failureCategory, List<DesiredBundle> mappings) {
		boolean failed() { return failureStatus != null; }
		static AimcState failure(String id, AasRepositoryClient.Status status, String category) {
			return new AimcState(id, null, status, category, List.of());
		}
	}

	/**
	 * What events can concern: the AIMCs, and per dependency submodel the referenced element paths. Of most paths
	 * only the structure counts; of {@code valuePaths} (TimeSeries containers, whose segments carry their period)
	 * the values, too.
	 */
	private record Index(Set<String> aimcIds, Map<String, Set<String>> paths, Set<String> wholeSubmodels,
			Map<String, Set<String>> valuePaths) {
		static final Index EMPTY = new Index(Set.of(), Map.of(), Set.of(), Map.of());
	}

	private static final int MAX_TRACKED_AIMCS = 10_000;

	private final AasRepositoryClient repository;
	private final AasReferenceResolver resolver;
	private final MappingPlanCompiler compiler;
	private final ConnectorConfigurationFactory factory;
	private final RuntimeSettings runtime;
	private final int parallelReads;
	private final Set<String> aimcScope;
	private final Map<String, AimcState> aimcs = new LinkedHashMap<>();
	private volatile Index index = Index.EMPTY;
	private volatile boolean bootstrapped;
	private volatile long fullScans, targetedScans;

	public RepositoryDiscovery(AasRepositoryClient repository, RuntimeSettings runtime) {
		this(repository, runtime, 4, Set.of());
	}

	/** {@code aimcIds} limits discovery to exact AIMC IDs; empty means every AIMC in the repository. */
	public RepositoryDiscovery(AasRepositoryClient repository, RuntimeSettings runtime, int parallelReads, Set<String> aimcIds) {
		this.repository = Objects.requireNonNull(repository);
		this.resolver = new AasReferenceResolver(repository);
		this.compiler = new MappingPlanCompiler(resolver);
		this.factory = new ConnectorConfigurationFactory();
		this.runtime = Objects.requireNonNull(runtime);
		if (parallelReads < 1 || parallelReads > 64) throw new IllegalArgumentException("parallelReads must be 1..64");
		this.parallelReads = parallelReads;
		this.aimcScope = Set.copyOf(aimcIds);
		if (aimcScope.size() > MAX_TRACKED_AIMCS || aimcScope.stream().anyMatch(String::isBlank))
			throw new IllegalArgumentException("AIMC scope exceeds capacity or contains a blank ID");
	}

	public long fullScanCount() { return fullScans; }
	public long targetedScanCount() { return targetedScans; }
	public boolean bootstrapped() { return bootstrapped; }

	/** Full discovery: every AIMC and everything its mappings reference, read afresh. */
	public synchronized Scan scan() {
		fullScans++;
		Map<String, AimcState> listed = new LinkedHashMap<>();
		if (aimcScope.isEmpty()) {
			var page = repository.listSubmodelsBySemanticId(WireProjection.AIMC);
			if (!page.found()) return incomplete(page.status());
			for (var item : page.value()) {
				// A repository that ignores the semanticId filter lists other submodels too.
				if (item.submodel() == null) {
					if (aimcs.containsKey(item.id())) listed.put(item.id(), AimcState.failure(item.id(),
							AasRepositoryClient.Status.INVALID_RESPONSE, "aimc-invalid"));
				} else if (WireProjection.isAimc(item.submodel())) listed.put(item.id(), project(item.submodel()));
			}
		} else {
			for (String id : aimcScope) {
				var state = read(id);
				if (state != null) listed.put(id, state);
			}
		}
		if (listed.size() > MAX_TRACKED_AIMCS) return incomplete(AasRepositoryClient.Status.INVALID_RESPONSE);
		resolver.retain(Set.of());
		aimcs.clear();
		compile(listed);
		bootstrapped = true;
		return result();
	}

	/**
	 * Re-reads what the changed submodels concern. Unknown IDs are read once to see whether they are new AIMCs;
	 * AIMCs that failed transiently are retried. Without a completed {@link #scan()} this is a full scan.
	 */
	public synchronized Scan refresh(Set<String> changedIds) {
		if (!bootstrapped) return scan();
		targetedScans++;
		resolver.evictFailures();
		Set<String> changed = changedIds == null ? Set.of() : Set.copyOf(changedIds);
		Index current = index;
		Set<String> affected = new LinkedHashSet<>();
		Map<String, AimcState> reread = new LinkedHashMap<>();
		Map<String, AimcState> probeFailures = new LinkedHashMap<>();
		for (String id : changed) {
			if (aimcs.containsKey(id)) continue;
			if (current.paths().containsKey(id)) { resolver.evict(Set.of(id)); continue; }
			if (!aimcScope.isEmpty() && !aimcScope.contains(id)) continue;
			var probe = repository.getSubmodel(id);
			if (probe.found() && WireProjection.isAimc(probe.value())) reread.put(id, project(probe.value()));
			// A new submodel that could not be read may be an AIMC: kept as a failed one, read again on retry.
			else if (!probe.found() && transientFailure(probe.status()))
				probeFailures.put(id, AimcState.failure(id, probe.status(), "aimc-fetch-failed"));
		}
		for (AimcState state : aimcs.values()) {
			if (changed.contains(state.id()) || state.failed() && transientFailure(state.failureStatus())) {
				var fresh = read(state.id());
				if (fresh == null) affected.add(state.id());
				else reread.put(state.id(), fresh);
			}
		}
		for (AimcState state : aimcs.values()) {
			if (reread.containsKey(state.id()) || affected.contains(state.id()) || state.aimc() == null) continue;
			boolean depends = state.mappings().stream().anyMatch(bundle -> bundle.desired().dependencies().stream()
					.map(Dependency::id).anyMatch(changed::contains));
			// A mapping whose dependency could not be read for a while is compiled again (its failed reads were evicted).
			boolean retry = state.mappings().stream().anyMatch(bundle -> bundle.desired().failures().stream()
					.anyMatch(failure -> transientFailure(failure.status())));
			if (depends || retry) reread.put(state.id(), state);
		}
		// An AIMC that is gone or no longer an AIMC drops its mappings.
		affected.forEach(aimcs::remove);
		probeFailures.forEach(aimcs::putIfAbsent);
		compile(reread);
		return result();
	}

	/**
	 * Whether a repository event can change desired state. Element events are compared with the referenced
	 * paths, and an update that changes only values of what they reference, of any element type, is not a
	 * change (SPEC §5.3: values are not configuration), nor is a submodel update that leaves the referenced
	 * elements' structure as it was. Only a TimeSeries container's values count (its segments' period).
	 */
	public boolean relevant(AasControlEvent event) {
		if (event.ignored()) return false;
		if (event.fullRescan()) return true;
		String type = event.type() == null ? "" : event.type();
		if (type.startsWith("AAS_")) return false;
		String id = event.resourceId();
		Index current = index;
		if (current.aimcIds().contains(id)) return true;
		Set<String> referenced = current.paths().get(id);
		if (referenced != null) {
			if (current.wholeSubmodels().contains(id)) return true;
			Set<String> values = current.valuePaths().getOrDefault(id, Set.of());
			if ("SM_UPDATED".equals(type)) return !values.isEmpty() || !sameStructure(id, referenced, event.submodel());
			if (!type.startsWith("SME_") || event.elementPath() == null) return true;
			String path = event.elementPath().replace('/', '.');
			boolean overlaps = referenced.stream().anyMatch(read -> AasReferenceResolver.within(read, path)
					|| AasReferenceResolver.within(path, read));
			if (!overlaps) return false;
			if (!"SME_UPDATED".equals(type)) return true;
			if (values.stream().anyMatch(series -> AasReferenceResolver.within(series, path) || AasReferenceResolver.within(path, series)))
				return true;
			// Only now is the event's element parsed.
			return !sameStructure(id, path, referenced, event.element());
		}
		if (!aimcScope.isEmpty() && !aimcScope.contains(id)) return false;
		if (!"SM_CREATED".equals(type) && !"SM_UPDATED".equals(type)) return false;
		return event.semanticIds() == null || event.semanticIds().contains(WireProjection.AIMC);
	}

	/** Whether the element updated at {@code path} leaves every referenced element it concerns as structured as read. */
	private boolean sameStructure(String id, String path, Set<String> referenced, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement updated) {
		if (updated == null) return false;
		for (String read : referenced) {
			org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement before, after;
			if (AasReferenceResolver.within(read, path)) {
				// The update carries the referenced element inside it.
				before = resolver.cachedElement(id, read).orElse(null);
				after = AasReferenceResolver.descend(updated, read.substring(path.length()));
			} else if (AasReferenceResolver.within(path, read)) {
				before = resolver.cachedElement(id, path).orElse(null);
				after = updated;
			} else continue;
			if (before == null || after == null || !Aas.withoutValues(before).equals(Aas.withoutValues(after))) return false;
		}
		return true;
	}

	/** Whether an updated submodel leaves its identity and every referenced element's structure as read. */
	private boolean sameStructure(String id, Set<String> referenced, Submodel updated) {
		if (updated == null) return false;
		Submodel header = resolver.cachedHeader(id).orElse(null);
		if (header == null || !Objects.equals(header.getIdShort(), updated.getIdShort())
				|| !Objects.equals(header.getSemanticId(), updated.getSemanticId())
				|| !Objects.equals(header.getSupplementalSemanticIds(), updated.getSupplementalSemanticIds())) return false;
		for (String read : referenced) {
			var before = resolver.cachedElement(id, read).orElse(null);
			var after = AasReferenceResolver.elementAt(updated, read);
			if (before == null || after == null || !Aas.withoutValues(before).equals(Aas.withoutValues(after))) return false;
		}
		return true;
	}

	/** Every submodel the current desired state depends on, AIMCs included. */
	public Set<String> watchedSubmodels() {
		Index current = index;
		Set<String> ids = new LinkedHashSet<>(current.aimcIds());
		ids.addAll(current.paths().keySet());
		return Set.copyOf(ids);
	}

	private AimcState read(String id) {
		var fetched = repository.getSubmodel(id);
		if (fetched.status() == AasRepositoryClient.Status.NOT_FOUND) return null;
		if (!fetched.found()) return AimcState.failure(id, fetched.status(), switch (fetched.status()) {
			case INVALID_RESPONSE -> "aimc-invalid";
			default -> "aimc-fetch-failed";
		});
		if (!id.equals(fetched.value().getId()))
			return AimcState.failure(id, AasRepositoryClient.Status.INVALID_RESPONSE, "aimc-invalid");
		return WireProjection.isAimc(fetched.value()) ? project(fetched.value()) : null;
	}

	private static AimcState project(Submodel submodel) {
		try { return new AimcState(submodel.getId(), WireProjection.aimcIsolated(submodel), null, "", List.of()); }
		catch (IllegalArgumentException invalid) {
			return AimcState.failure(submodel.getId(), AasRepositoryClient.Status.INVALID_RESPONSE, "aimc-invalid");
		}
	}

	/** Reads, grouped and in parallel, what the AIMCs reference, then compiles each AIMC. */
	private void compile(Map<String, AimcState> states) {
		List<Reference> references = new ArrayList<>();
		for (AimcState state : states.values()) if (state.aimc() != null) {
			for (Reference reference : MappingPlanCompiler.endpointReferences(state.aimc())) {
				references.add(reference);
			}
		}
		ExecutorService readers = Executors.newFixedThreadPool(parallelReads, task -> {
			Thread thread = new Thread(task, "dmp-repository-read");
			thread.setDaemon(true);
			return thread;
		});
		try { resolver.prefetch(references, readers); }
		finally { readers.shutdownNow(); }
		for (AimcState state : states.values()) {
			if (state.aimc() == null) { aimcs.put(state.id(), state); continue; }
			try {
				var desired = compiler.compileDesired(state.aimc());
				aimcs.put(state.id(), new AimcState(state.id(), state.aimc(), null, "",
						factory.createDesiredBundles(desired, runtime)));
			} catch (RuntimeException failure) {
				aimcs.put(state.id(), new AimcState(state.id(), state.aimc(), AasRepositoryClient.Status.INVALID_RESPONSE,
						"aimc-compilation-failed", List.of()));
			}
		}
		reindex();
		resolver.retain(watchedSubmodels());
	}

	private void reindex() {
		Map<String, Set<String>> paths = new LinkedHashMap<>();
		Map<String, Set<String>> valuePaths = new LinkedHashMap<>();
		Set<String> whole = new LinkedHashSet<>();
		for (AimcState state : aimcs.values()) {
			if (state.aimc() == null) continue;
			for (Reference reference : MappingPlanCompiler.endpointReferences(state.aimc())) {
				var keys = Aas.keys(reference);
				if (keys.size() < 2 || !"Submodel".equals(Aas.keyType(keys.get(0)))) continue;
				paths.computeIfAbsent(keys.get(0).getValue(), ignored -> new LinkedHashSet<>())
						.add(AasReferenceResolver.idShortPath(keys));
			}
			for (DesiredBundle bundle : state.mappings())
				for (Dependency dependency : bundle.desired().dependencies()) {
					if (dependency.id().equals(state.id())) continue;
					Set<String> referenced = paths.computeIfAbsent(dependency.id(), ignored -> new LinkedHashSet<>());
					if (dependency.path() != null) referenced.add(dependency.path());
					if ("series".equals(dependency.kind()) && dependency.path() != null)
						valuePaths.computeIfAbsent(dependency.id(), ignored -> new LinkedHashSet<>()).add(dependency.path());
				}
		}
		// An AID is consumed whole; a dependency without referenced paths (a ReferenceElement target) concerns all of it.
		for (var entry : paths.entrySet()) if (entry.getValue().isEmpty() || resolver.cachedAid(entry.getKey())) whole.add(entry.getKey());
		Map<String, Set<String>> frozen = new LinkedHashMap<>(), frozenValues = new LinkedHashMap<>();
		paths.forEach((id, set) -> frozen.put(id, Set.copyOf(set)));
		valuePaths.forEach((id, set) -> frozenValues.put(id, Set.copyOf(set)));
		index = new Index(Set.copyOf(aimcs.keySet()), Map.copyOf(frozen), Set.copyOf(whole), Map.copyOf(frozenValues));
	}

	private Scan result() {
		Set<String> ids = new LinkedHashSet<>(), failed = new LinkedHashSet<>();
		Map<MappingKey, MappingCandidate> mappings = new LinkedHashMap<>();
		List<AimcFailure> failures = new ArrayList<>();
		for (AimcState state : aimcs.values()) {
			ids.add(state.id());
			if (state.failed()) {
				failed.add(state.id());
				failures.add(new AimcFailure(state.id(), state.failureStatus(), state.failureCategory()));
				continue;
			}
			for (DesiredBundle bundle : state.mappings()) {
				MappingKey key = new MappingKey(state.id(), bundle.desired().identity(), bundle.desired().listIndex());
				mappings.put(key, new MappingCandidate(key, bundle));
			}
		}
		Map<String, Set<MappingKey>> reverse = new LinkedHashMap<>();
		for (MappingCandidate candidate : mappings.values())
			for (Dependency dependency : candidate.desired().desired().dependencies())
				reverse.computeIfAbsent(dependency.id(), ignored -> new LinkedHashSet<>()).add(candidate.key());
		return new Scan(true, "", ids, failed, mappings, reverse, failures);
	}

	private static boolean transientFailure(AasRepositoryClient.Status status) {
		return status == AasRepositoryClient.Status.TRANSIENT_FAILURE || status == AasRepositoryClient.Status.UNAUTHORIZED;
	}

	private static Scan incomplete(AasRepositoryClient.Status status) {
		return new Scan(false, "repository-" + status.name().toLowerCase(java.util.Locale.ROOT),
				Set.of(), Set.of(), Map.of(), Map.of(), List.of());
	}
}
