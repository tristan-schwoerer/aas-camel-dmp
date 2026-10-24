/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.management.repository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.Operation;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.management.model.WireModel;
import dk.aau.smartproductionlab.dmp.management.model.WireModel.Presence;
import dk.aau.smartproductionlab.dmp.management.model.WireProjection;

/**
 * Resolves mapping references on a need-to-know basis and caches what it fetched.
 *
 * <p>A reference names its submodel in the first key and the idShort path to the element in the rest. AID
 * submodels are read whole, because a target needs the interface's endpoint metadata, forms and schemas as well
 * as the referenced affordance; they are recognised by the fixed AID idShort at the end of the submodel ID, or
 * else by the semanticId in {@code $metadata}. Any other submodel is read as {@code $metadata} plus the referenced
 * element subtrees, or whole when more than {@value #MAX_ELEMENT_READS} subtrees are referenced. A missing path
 * is confirmed against the whole submodel, so a repository without element-path reads still resolves correctly.
 */
public final class AasReferenceResolver {
	static final String AID_ID_SHORT = "AssetInterfacesDescription";
	static final int MAX_ELEMENT_READS = 8;
	private static final List<String> AID_TRANSPARENT_SCHEMA_SEMANTICS = List.of(
			"https://www.w3.org/2019/wot/json-schema#properties",
			"https://www.w3.org/2019/wot/td#hasInputSchema",
			"https://www.w3.org/2019/wot/td#hasOutputSchema");

	private final AasRepositoryClient repository;
	private final Map<String, Entry> entries = new ConcurrentHashMap<>();
	private final Map<String, Object> locks = new ConcurrentHashMap<>();

	public AasReferenceResolver(AasRepositoryClient repository) {
		this.repository = Objects.requireNonNull(repository);
	}

	/** A resolved mapping endpoint: an element of an ordinary submodel, or an AID affordance. */
	public sealed interface Resolution permits ResolvedElement, AidTarget { }
	/** {@code submodel} carries the submodel's identity and semantics; its elements may be absent. */
	public record ResolvedElement(Submodel submodel, SubmodelElement element,
			Operation operation, String operationPath) implements Resolution {
		public ResolvedElement(Submodel submodel, SubmodelElement element) { this(submodel, element, null, null); }
	}
	/** Which data schema of the affordance a reference names: an action's {@code input} or {@code output}, or none. */
	public enum SchemaPart { NONE, INPUT, OUTPUT }

	public record AidTarget(WireModel.Interface owner, WireModel.Affordance affordance,
			Presence<WireModel.DataSchema> schema, List<String> schemaPath, String submodelId, SchemaPart part) implements Resolution {
		public AidTarget(WireModel.Interface owner,WireModel.Affordance affordance,Presence<WireModel.DataSchema> schema,List<String> schemaPath) {
			this(owner,affordance,schema,schemaPath,null,SchemaPart.NONE);
		}
		public AidTarget(WireModel.Interface owner,WireModel.Affordance affordance,Presence<WireModel.DataSchema> schema,
				List<String> schemaPath,String submodelId) {
			this(owner,affordance,schema,schemaPath,submodelId,SchemaPart.NONE);
		}
		public AidTarget { schemaPath = List.copyOf(schemaPath); part = part == null ? SchemaPart.NONE : part; }
	}

	/** What was read for one submodel. {@code header} carries the failure when the submodel is unusable. */
	private record Entry(AasRepositoryClient.Result<Submodel> header, Submodel full,
			AasRepositoryClient.Result<WireModel.Aid> aid, Map<String, AasRepositoryClient.Result<SubmodelElement>> elements) {
		boolean failed() { return !header.found(); }
		boolean covers(String path) { return full != null || elements.keySet().stream().anyMatch(read -> within(path, read)); }
	}

	/** Reads, grouped per submodel and in parallel across submodels, everything the references need. */
	public void prefetch(Collection<Reference> references, Executor executor) {
		Map<String, List<String>> paths = new LinkedHashMap<>();
		for (Reference reference : references) {
			List<Key> keys = Aas.keys(reference);
			if (!Aas.isModelReference(reference) || keys.size() < 2 || !"Submodel".equals(Aas.keyType(keys.get(0)))) continue;
			List<String> wanted = paths.computeIfAbsent(keys.get(0).getValue(), ignored -> new ArrayList<>());
			wanted.add(idShortPath(keys));
			int operation = operationKeyIndex(keys);
			if (operation >= 1) wanted.add(idShortPath(keys.subList(0, operation + 1)));
		}
		List<CompletableFuture<?>> loads = new ArrayList<>();
		paths.forEach((id, wanted) -> loads.add(CompletableFuture.runAsync(() -> load(id, wanted), executor)));
		loads.forEach(CompletableFuture::join);
	}

	/** Resolves one endpoint reference, reading what the cache does not hold yet. */
	public AasRepositoryClient.Result<Resolution> resolve(Reference reference) {
		if (!Aas.isModelReference(reference)) return invalid("mapping endpoint reference must be a ModelReference");
		List<Key> keys = Aas.keys(reference);
		if (keys.isEmpty() || !"Submodel".equals(Aas.keyType(keys.get(0)))) return invalid("mapping endpoint must reference an exact Submodel");
		if (keys.size() < 2) return invalid("reference identifies the Submodel, not an element");
		if (keys.stream().anyMatch(key -> key.getValue().contains("{aas_id"))) return invalid("reference contains an unresolved {aas_id} template");
		String id = keys.get(0).getValue();
		Entry entry = load(id, List.of(idShortPath(keys)));
		if (entry.failed()) return propagate(entry.header());
		if (entry.aid() != null) {
			if (!entry.aid().found()) return propagate(entry.aid());
			var target = resolveAidTarget(keys, entry.aid().value());
			return target.found() ? found(target.value()) : propagate(target);
		}
		var element = entry.full() != null ? resolveElement(keys, entry.full()) : resolvePartial(keys, entry);
		if (!element.found()) return propagate(element);
		try { WireProjection.validate(element.value().element(), "/" + idShortPath(keys)); }
		catch (IllegalArgumentException retired) { return invalid("referenced element is not consumable: " + retired.getMessage()); }
		int operationIndex = operationKeyIndex(keys);
		if (operationIndex < 1) return found(element.value());
		List<Key> operationKeys = keys.subList(0, operationIndex + 1);
		var operationElement = entry.full() != null ? resolveElement(operationKeys, entry.full()) : resolvePartial(operationKeys, entry);
		if (!operationElement.found()) return propagate(operationElement);
		if (!(operationElement.value().element() instanceof Operation operation))
			return invalid("Operation ancestor does not resolve to an AAS Operation");
		return found(new ResolvedElement(element.value().submodel(), element.value().element(), operation,
				idShortPath(operationKeys)));
	}

	/** The cached AID behind a submodel ID, read if needed. */
	public AasRepositoryClient.Result<WireModel.Aid> aid(String submodelId) {
		Entry entry = load(submodelId, List.of());
		if (entry.failed()) return propagate(entry.header());
		return entry.aid() == null ? invalid("referenced submodel is not an AID") : entry.aid();
	}

	/** Drops what was read for these submodels; the next resolution reads them again. */
	public void evict(Collection<String> submodelIds) { submodelIds.forEach(entries::remove); }

	/** Drops failed reads, so a later pass retries them. */
	public void evictFailures() { entries.values().removeIf(Entry::failed); }

	/** Keeps only what the given submodels need. */
	public void retain(Set<String> submodelIds) {
		entries.keySet().retainAll(submodelIds);
		locks.keySet().retainAll(submodelIds);
	}

	public Set<String> cachedSubmodelIds() { return Set.copyOf(entries.keySet()); }

	/** True when this submodel was read and is an AID; never reads. */
	public boolean cachedAid(String submodelId) {
		Entry entry = entries.get(submodelId);
		return entry != null && entry.aid() != null;
	}

	/** The cached element at an exact idShort path, when a read covers it. */
	public Optional<SubmodelElement> cachedElement(String submodelId, String idShortPath) {
		Entry entry = entries.get(submodelId);
		if (entry == null || entry.failed()) return Optional.empty();
		SubmodelElement current = null;
		String covered = null;
		if (entry.full() != null) {
			for (String segment : segments(idShortPath)) {
				List<SubmodelElement> children = current == null ? Aas.nonNull(entry.full().getSubmodelElements()) : Aas.children(current);
				current = child(children, segment);
				if (current == null) return Optional.empty();
			}
			return Optional.of(current);
		}
		for (var read : entry.elements().entrySet()) if (within(idShortPath, read.getKey()) && read.getValue().found()) {
			covered = read.getKey(); current = read.getValue().value();
		}
		if (covered == null) return Optional.empty();
		List<String> rest = segments(idShortPath).subList(segments(covered).size(), segments(idShortPath).size());
		for (String segment : rest) {
			current = child(Aas.children(current), segment);
			if (current == null) return Optional.empty();
		}
		return Optional.of(current);
	}

	/** The repository idShort path of a reference: {@code a.b[0].c}, list members addressed by index. */
	public static String idShortPath(List<Key> keys) {
		StringBuilder path = new StringBuilder();
		for (int i = 1; i < keys.size(); i++) {
			if (i > 1 && "SubmodelElementList".equals(Aas.keyType(keys.get(i - 1)))) path.append('[').append(keys.get(i).getValue()).append(']');
			else { if (i > 1) path.append('.'); path.append(keys.get(i).getValue()); }
		}
		return path.toString();
	}

	/** The index of the first Operation key below the submodel, or -1 when the reference is outside an Operation. */
	public static int operationKeyIndex(List<Key> keys) {
		for (int i = 1; i < keys.size(); i++) if ("Operation".equals(Aas.keyType(keys.get(i)))) return i;
		return -1;
	}

	/** The element at {@code relativePath} below {@code element} (an empty path: the element itself), or null. */
	public static SubmodelElement descend(SubmodelElement element, String relativePath) {
		String rest = relativePath.startsWith(".") ? relativePath.substring(1) : relativePath;
		if (element == null || rest.isEmpty()) return element;
		SubmodelElement current = element;
		for (String segment : segments(rest)) {
			current = child(Aas.children(current), segment);
			if (current == null) return null;
		}
		return current;
	}

	/** The element at {@code idShortPath} in {@code submodel}, or null. */
	public static SubmodelElement elementAt(Submodel submodel, String idShortPath) {
		SubmodelElement current = null;
		for (String segment : segments(idShortPath)) {
			current = child(current == null ? Aas.nonNull(submodel.getSubmodelElements()) : Aas.children(current), segment);
			if (current == null) return null;
		}
		return current;
	}

	/** The submodel's identity and semantics as last read, without its elements. */
	public Optional<Submodel> cachedHeader(String submodelId) {
		Entry entry = entries.get(submodelId);
		return entry == null || entry.failed() ? Optional.empty() : Optional.ofNullable(entry.header().value());
	}

	/** True when {@code path} is {@code ancestor} or lies below it. */
	public static boolean within(String path, String ancestor) {
		return path.equals(ancestor) || path.startsWith(ancestor + ".") || path.startsWith(ancestor + "[");
	}

	private Entry load(String id, Collection<String> wanted) {
		synchronized (locks.computeIfAbsent(id, ignored -> new Object())) {
			Entry current = entries.get(id);
			if (current != null && (current.failed() || wanted.stream().allMatch(current::covers))) return current;
			Entry next = current == null ? header(id) : current;
			if (!next.failed() && next.full() == null) next = elements(id, next, wanted);
			entries.put(id, next);
			return next;
		}
	}

	private Entry header(String id) {
		if (id.endsWith("/" + AID_ID_SHORT) || id.equals(AID_ID_SHORT)) return whole(id);
		var header = repository.getSubmodelMetadata(id);
		if (unsupported(header)) return whole(id);
		if (!header.found()) return failure(header);
		if (!id.equals(header.value().getId())) return failure(invalid("repository returned a different Submodel identity"));
		if (WireProjection.isAid(header.value())) return whole(id);
		return new Entry(header, null, null, Map.of());
	}

	private Entry whole(String id) {
		var fetched = repository.getSubmodel(id);
		if (!fetched.found()) return failure(fetched);
		Submodel submodel = fetched.value();
		if (!id.equals(submodel.getId())) return failure(invalid("repository returned a different Submodel identity"));
		AasRepositoryClient.Result<WireModel.Aid> aid = null;
		if (WireProjection.isAid(submodel)) {
			try { aid = found(WireProjection.aid(submodel)); }
			catch (IllegalArgumentException invalid) { aid = invalid("invalid anchored AID projection"); }
		}
		return new Entry(fetched, submodel, aid, Map.of());
	}

	private Entry elements(String id, Entry entry, Collection<String> wanted) {
		Map<String, AasRepositoryClient.Result<SubmodelElement>> read = new LinkedHashMap<>(entry.elements());
		List<String> missing = new ArrayList<>();
		for (String path : cover(wanted)) if (!entry.covers(path)) missing.add(path);
		if (read.size() + missing.size() > MAX_ELEMENT_READS) return whole(id);
		for (String path : missing) {
			var element = repository.getSubmodelElement(id, path);
			// The whole submodel tells a missing path apart from a repository without element-path reads.
			if (element.status() == AasRepositoryClient.Status.NOT_FOUND || unsupported(element)) return whole(id);
			if (!element.found()) return failure(element);
			read.put(path, element);
		}
		return new Entry(entry.header(), null, null, Map.copyOf(read));
	}

	/** The fewest paths whose subtrees contain every wanted path. */
	private static List<String> cover(Collection<String> wanted) {
		List<String> sorted = wanted.stream().distinct().sorted(java.util.Comparator.comparingInt(String::length)).toList();
		List<String> cover = new ArrayList<>();
		for (String path : sorted) if (cover.stream().noneMatch(read -> within(path, read))) cover.add(path);
		return cover;
	}

	private static boolean unsupported(AasRepositoryClient.Result<?> result) {
		return result.status() == AasRepositoryClient.Status.INVALID_RESPONSE
				&& (result.httpStatus() == 400 || result.httpStatus() == 405 || result.httpStatus() == 501);
	}

	private static Entry failure(AasRepositoryClient.Result<?> result) {
		return new Entry(propagate(result), null, null, Map.of());
	}

	private AasRepositoryClient.Result<ResolvedElement> resolvePartial(List<Key> keys, Entry entry) {
		String target = idShortPath(keys);
		int depth = -1;
		AasRepositoryClient.Result<SubmodelElement> read = null;
		for (int n = 2; n <= keys.size(); n++) {
			read = entry.elements().get(idShortPath(keys.subList(0, n)));
			if (read != null) { depth = n - 1; break; }
		}
		if (read == null) return invalid("reference element path was not read: " + target);
		if (!read.found()) return propagate(read);
		SubmodelElement current = read.value();
		Key anchor = keys.get(depth);
		if (!Aas.keyType(anchor).equals(Aas.modelType(current))) return invalid("reference key type " + Aas.keyType(anchor)
				+ " does not match element type " + Aas.modelType(current) + " at " + anchor.getValue());
		for (int i = depth + 1; i < keys.size(); i++) {
			Step step = descend(Aas.children(current), keys.get(i), false);
			if (step.failure != null) return invalid(step.failure);
			current = step.element;
		}
		return found(new ResolvedElement(entry.header().value(), current));
	}

	private static AasRepositoryClient.Result<ResolvedElement> resolveElement(List<Key> keys, Submodel submodel) {
		SubmodelElement current = null;
		for (int i = 1; i < keys.size(); i++) {
			Step step = descend(current == null ? Aas.nonNull(submodel.getSubmodelElements()) : Aas.children(current), keys.get(i),
					WireProjection.isAid(submodel));
			if (step.failure != null) return invalid(step.failure);
			current = step.element;
		}
		return found(new ResolvedElement(submodel, current));
	}

	/** Resolves AID protocol/affordance/schema paths without guessing by idShort. */
	private static AasRepositoryClient.Result<AidTarget> resolveAidTarget(List<Key> keys, WireModel.Aid aid) {
		if (keys.size() < 4) return invalid("AID affordance reference has no exact Submodel/interface/InteractionMetadata path");
		int index = 1;
		if (!"SubmodelElementCollection".equals(Aas.keyType(keys.get(index)))) return invalid("AID interface key type is not SubmodelElementCollection");
		WireModel.Interface iface = findById(aid.interfaces(), keys.get(index++).getValue(), WireModel.Interface::idShort)
				.orElse(null);
		if (iface == null) return invalid("AID interface is missing at exact reference path");
		if (index < keys.size() && "InteractionMetadata".equals(keys.get(index).getValue())) {
			if (!"SubmodelElementCollection".equals(Aas.keyType(keys.get(index)))) return invalid("AID InteractionMetadata key type is invalid");
			index++;
		}
		if (index >= keys.size()) return invalid("AID affordance collection is missing");
		if (!"SubmodelElementCollection".equals(Aas.keyType(keys.get(index)))) return invalid("AID affordance collection key type is invalid");
		String collection = keys.get(index++).getValue();
		List<WireModel.Affordance> affordances = switch (collection) {
			case "properties" -> iface.properties();
			case "actions" -> iface.actions();
			case "events" -> iface.events();
			default -> List.of();
		};
		if (affordances.isEmpty() || index >= keys.size()) return invalid("AID affordance collection or member is missing: "
				+ collection + " at interface " + iface.idShort());
		if (!"SubmodelElementCollection".equals(Aas.keyType(keys.get(index)))) return invalid("AID affordance key type is invalid");
		String affordanceId = keys.get(index++).getValue();
		WireModel.Affordance affordance = findById(affordances, affordanceId, value -> value.idShort().value())
				.orElse(null);
		if (affordance == null) return invalid("AID affordance is missing at exact reference path: "
				+ collection + "/" + affordanceId + " (available="
				+ affordances.stream().map(value -> value.idShort().value()).toList() + ")");

		Presence<WireModel.DataSchema> schema = affordance.schema();
		SchemaPart part = SchemaPart.NONE;
		if (index < keys.size() && "input".equals(keys.get(index).getValue())) {
			if (!"SubmodelElementCollection".equals(Aas.keyType(keys.get(index)))) return invalid("AID input key type is invalid");
			schema = affordance.input(); index++; part = SchemaPart.INPUT;
		} else if (index < keys.size() && "output".equals(keys.get(index).getValue())) {
			if (!"SubmodelElementCollection".equals(Aas.keyType(keys.get(index)))) return invalid("AID output key type is invalid");
			schema = affordance.output(); index++; part = SchemaPart.OUTPUT;
		}
		List<String> schemaPath = new ArrayList<>();
		while (index < keys.size()) {
			String token = keys.get(index++).getValue();
			if ("properties".equals(token) && schema.present() && schema.value() != null) {
				if (!"SubmodelElementCollection".equals(Aas.keyType(keys.get(index - 1)))) {
					return invalid("AID properties key type is invalid");
				}
				continue;
			}
			if ("items".equals(token)) {
				if (!schema.present() || schema.value() == null || !schema.value().items().present()
						|| schema.value().items().value() == null) {
					return invalid("AID array schema has no exact items target");
				}
				schema = schema.value().items();
				if (!Aas.keyType(keys.get(index - 1)).equals(Aas.modelType(schema.value().element()))) return invalid("AID items key type does not match its schema element");
				schemaPath.add(token);
				continue;
			}
			if (!schema.present() || schema.value() == null) return invalid("AID data schema is absent at referenced path");
			WireModel.DataSchema child = schema.value().properties().get(token);
			if (child == null) return invalid("AID schema property is missing at exact path: " + token);
			if (!Aas.keyType(keys.get(index - 1)).equals(Aas.modelType(child.element()))) return invalid("AID property key type does not match its schema element: " + token);
			schema = Presence.of(child);
			schemaPath.add(token);
		}
		return found(new AidTarget(iface, affordance, schema, schemaPath, aid.submodel().getId(), part));
	}

	private static <T> Optional<T> findById(List<T> values, String id, java.util.function.Function<T,String> getId) {
		List<T> matches = values.stream().filter(value -> id.equals(getId.apply(value))).toList();
		return matches.size() == 1 ? Optional.of(matches.get(0)) : Optional.empty();
	}

	private static Step descend(List<SubmodelElement> children, Key key, boolean aid) {
		String keyType = Aas.keyType(key);
		SubmodelElement exact = children.stream().filter(element ->
				key.getValue().equals(element.getIdShort()) && keyType.equals(Aas.modelType(element)))
				.findFirst().orElse(null);
		if (exact != null) return new Step(exact, null);
		SubmodelElement sameName = children.stream().filter(element -> key.getValue().equals(element.getIdShort()))
				.findFirst().orElse(null);
		if (aid) {
			for (SubmodelElement wrapper : children) {
				if (AID_TRANSPARENT_SCHEMA_SEMANTICS.stream().noneMatch(semantic -> Aas.hasSemantic(wrapper, semantic))) continue;
				Step nested = descend(Aas.children(wrapper), key, true);
				if (nested.element != null) return nested;
			}
		}
		if (sameName != null) return new Step(null, "reference key type " + keyType
				+ " does not match element type " + Aas.modelType(sameName) + " at " + key.getValue());
		return new Step(null, "reference element path is missing: " + key.getValue() + " among "
				+ children.stream().map(element -> element.getIdShort() + ":" + Aas.modelType(element)).toList());
	}

	private static List<String> segments(String idShortPath) {
		List<String> segments = new ArrayList<>();
		for (String part : idShortPath.split("\\.")) {
			int bracket = part.indexOf('[');
			if (bracket < 0) { segments.add(part); continue; }
			segments.add(part.substring(0, bracket));
			for (String index : part.substring(bracket + 1).split("\\]\\[?")) if (!index.isEmpty()) segments.add("[" + index + "]");
		}
		return segments;
	}

	private static SubmodelElement child(List<SubmodelElement> children, String segment) {
		if (segment.startsWith("[")) {
			int index = Integer.parseInt(segment.substring(1, segment.length() - 1));
			return index < children.size() ? children.get(index) : null;
		}
		return children.stream().filter(element -> segment.equals(element.getIdShort())).findFirst().orElse(null);
	}

	private record Step(SubmodelElement element, String failure) { }

	private static <T> AasRepositoryClient.Result<T> found(T value) {
		return new AasRepositoryClient.Result<>(AasRepositoryClient.Status.FOUND, value, 200, "");
	}
	private static <T> AasRepositoryClient.Result<T> invalid(String detail) {
		return new AasRepositoryClient.Result<>(AasRepositoryClient.Status.INVALID_RESPONSE, null, 0, detail);
	}
	private static <T> AasRepositoryClient.Result<T> propagate(AasRepositoryClient.Result<?> result) {
		return new AasRepositoryClient.Result<>(result.status(), null, result.httpStatus(), result.detail());
	}
}
