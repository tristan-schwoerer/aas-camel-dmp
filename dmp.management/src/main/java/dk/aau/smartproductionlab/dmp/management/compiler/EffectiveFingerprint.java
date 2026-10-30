package dk.aau.smartproductionlab.dmp.management.compiler;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.Qualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.management.model.TimeSeriesProjection;
import dk.aau.smartproductionlab.dmp.management.model.WireModel;

/**
 * The effective configuration of one mapping as an immutable Java value. Two
 * fingerprints describe the same routes exactly when their values are equal:
 * the compiled routes are a deterministic function of the mapping plan, the
 * repository root and the validation mode.
 *
 * <p>{@link #exported()} is a SHA-256 digest of a redacted description of the
 * value for status output; transport credentials and URI queries never
 * contribute to it.
 */
public final class EffectiveFingerprint {
	/** An AAS element target, reduced to its submodel identity and the element's shape. */
	/** An AAS element target: its submodel's identity, the element's structure, and the Operation it belongs to. */
	private record DirectValue(String submodelId, String submodelIdShort, Reference submodelSemanticId, SubmodelElement element,
			SubmodelElement operation) {
		DirectValue(String submodelId, String submodelIdShort, Reference submodelSemanticId, SubmodelElement element) {
			this(submodelId, submodelIdShort, submodelSemanticId, element, null);
		}
	}
	private record SeriesValue(DirectValue container, TimeSeriesProjection.Series projection) { }
	private record EndpointValue(String id, Reference reference, WireModel.Presence<String> pollingInterval,
			WireModel.Presence<String> evaluation, Object target) { }
	private record PlanValue(String identity, int listIndex, List<EndpointValue> sources, List<EndpointValue> sinks,
			WireModel.Presence<String> defaultPollingInterval, WireModel.Presence<WireModel.Blob> transformation,
			WireModel.Presence<List<EndpointValue>> responseSources, WireModel.Presence<List<EndpointValue>> responseSinks,
			WireModel.Presence<WireModel.Blob> responseTransformation, List<Qualifier> qualifiers,
			WireModel.Presence<String> invocationTimeout) { }
	/** The mapping's list position names no route; reordering the AIMC list keeps its routes. */
	private record PositionFreePlan(String identity, List<EndpointValue> sources, List<EndpointValue> sinks,
			WireModel.Presence<String> defaultPollingInterval, WireModel.Presence<WireModel.Blob> transformation,
			WireModel.Presence<List<EndpointValue>> responseSources, WireModel.Presence<List<EndpointValue>> responseSinks,
			WireModel.Presence<WireModel.Blob> responseTransformation, List<Qualifier> qualifiers,
			WireModel.Presence<String> invocationTimeout) { }
	private record DesiredValue(String identity, int listIndex, SubmodelElement input, Optional<String> authoredId,
			WireModel.MappingFailure projectionFailure, PlanValue plan, List<MappingPlanCompiler.Dependency> dependencies,
			List<String> failures, List<String> issues, String repository, String validation) { }

	/** Transient: the value may hold transport credentials and must not leak through serialization. */
	private final transient Object value;
	private final String exported;

	private EffectiveFingerprint(Object value) {
		this.value = Objects.requireNonNull(value);
		try {
			this.exported = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
					.digest(describe(value, false).getBytes(StandardCharsets.UTF_8)));
		} catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
	}

	public String exported() { return exported; }

	public boolean sameEffectiveConfiguration(EffectiveFingerprint other) {
		return other != null && value.equals(other.value);
	}

	@Override public boolean equals(Object other) { return other instanceof EffectiveFingerprint fingerprint && sameEffectiveConfiguration(fingerprint); }
	@Override public int hashCode() { return value.hashCode(); }
	@Override public String toString() { return exported; }

	/** The fingerprint of a compiled mapping plan. */
	public static EffectiveFingerprint of(MappingPlanCompiler.MappingPlan plan, ConnectorConfigurationFactory.RuntimeSettings runtime) {
		PlanValue value = plan(plan);
		return new EffectiveFingerprint(List.of(new PositionFreePlan(value.identity(), value.sources(), value.sinks(),
				value.defaultPollingInterval(), value.transformation(), value.responseSources(), value.responseSinks(),
				value.responseTransformation(), value.qualifiers(), value.invocationTimeout()),
				String.valueOf(repository(runtime)), validation()));
	}

	/** A fingerprint over an arbitrary immutable value, e.g. for externally defined bundles. */
	public static EffectiveFingerprint ofValue(Object value) {
		return new EffectiveFingerprint(value);
	}

	static EffectiveFingerprint ofDesired(MappingPlanCompiler.DesiredMapping desired,
			ConnectorConfigurationFactory.RuntimeSettings runtime, List<ConnectorConfigurationFactory.Issue> issues) {
		return new EffectiveFingerprint(new DesiredValue(desired.identity(), desired.listIndex(),
				desired.input() == null ? null : desired.input().element(),
				desired.input() == null ? Optional.empty() : desired.input().authoredId(),
				desired.projectionFailure(), plan(desired.plan()),
				desired.dependencies().stream().sorted(Comparator.comparing(MappingPlanCompiler.Dependency::kind)
						.thenComparing(MappingPlanCompiler.Dependency::id)
						.thenComparing(MappingPlanCompiler.Dependency::path, Comparator.nullsFirst(Comparator.naturalOrder()))).toList(),
				desired.failures().stream().map(failure -> failure.status().name()).sorted().toList(),
				issues.stream().map(ConnectorConfigurationFactory.Issue::path).sorted().toList(),
				repository(runtime), validation()));
	}

	private static String repository(ConnectorConfigurationFactory.RuntimeSettings runtime) {
		if (runtime == null) return null;
		// The InfluxDB a TimeSeries segment must name; its token is no compilation input.
		String influx = runtime.influx() == null ? "" : " influx=" + runtime.influx().url() + "?org=" + runtime.influx().org();
		return runtime.aasRepositoryBaseUri() + influx;
	}

	private static String validation() {
		return System.getenv().getOrDefault("DMP_AFFORDANCE_VALIDATION", "OFF").toUpperCase(java.util.Locale.ROOT);
	}

	private static PlanValue plan(MappingPlanCompiler.MappingPlan plan) {
		if (plan == null) return null;
		return new PlanValue(plan.identity(), plan.listIndex(), endpoints(plan.sources()), endpoints(plan.sinks()),
				plan.defaultPollingInterval(), plan.transformation(), endpoints(plan.responseSources()),
				endpoints(plan.responseSinks()), plan.responseTransformation(), plan.qualifiers(), plan.invocationTimeout());
	}

	private static WireModel.Presence<List<EndpointValue>> endpoints(WireModel.Presence<List<MappingPlanCompiler.EndpointPlan>> endpoints) {
		if (!endpoints.present()) return WireModel.Presence.absent();
		if (endpoints.value() == null) return WireModel.Presence.of(null);
		return WireModel.Presence.of(endpoints(endpoints.value()));
	}

	private static List<EndpointValue> endpoints(List<MappingPlanCompiler.EndpointPlan> endpoints) {
		return endpoints.stream().map(endpoint -> new EndpointValue(endpoint.id(), endpoint.reference(),
				endpoint.pollingInterval(), endpoint.evaluation(), target(endpoint.target()))).toList();
	}

	private static Object target(MappingPlanCompiler.Target target) {
		if (target instanceof MappingPlanCompiler.DirectTarget direct) {
			var resolved = direct.resolved();
			// Values are not configuration (SPEC §5.3): only the structure counts, at every depth. A Source on an
			// Operation's variable counts the whole Operation, whose delegation URL and variables shape the bridge.
			return new DirectValue(resolved.submodel().getId(), resolved.submodel().getIdShort(),
					resolved.submodel().getSemanticId(), Aas.withoutValues(resolved.element()),
					resolved.operation() == null || resolved.operation() == resolved.element() ? null
							: Aas.withoutValues(resolved.operation()));
		}
		if (target instanceof MappingPlanCompiler.SeriesTarget series) {
			var segment = series.segment();
			// The projection carries the resolved columns, so a change of a referenced element changes it, too.
			return new SeriesValue(new DirectValue(segment.submodel().getId(), segment.submodel().getIdShort(),
					segment.submodel().getSemanticId(), series.series()), series.projection());
		}
		return ((MappingPlanCompiler.AidTarget) target).resolved();
	}

	/** Deterministic, redacted description used only for the exported digest. */
	private static String describe(Object value, boolean secret) {
		if (value == null) return "null";
		if (secret) return "<redacted>";
		if (value instanceof String text) return text.contains("://") ? redactUri(text) : '"' + text + '"';
		if (value instanceof Number || value instanceof Boolean) return value.toString();
		if (value instanceof Enum<?> constant) return constant.name();
		if (value instanceof byte[] bytes) return Base64.getEncoder().encodeToString(bytes);
		if (value instanceof Optional<?> optional) return describe(optional.orElse(null), false);
		if (value instanceof Key key) return Aas.keyType(key) + ":" + describe(key.getValue(), false);
		if (value instanceof Reference reference)
			return "Reference(" + reference.getType() + describe(Aas.keys(reference), false) + ")";
		if (value instanceof Qualifier qualifier)
			return "Qualifier(" + qualifier.getKind() + "," + qualifier.getType() + "," + qualifier.getValueType() + ","
					+ describe(qualifier.getValue(), secretName(qualifier.getType())) + ")";
		if (value instanceof OperationVariable variable) return describe(variable.getValue(), false);
		if (value instanceof SubmodelElement element) {
			StringBuilder result = new StringBuilder(Aas.modelType(element)).append('(').append(element.getIdShort())
					.append(',').append(describe(element.getSemanticId(), false))
					.append(',').append(describe(element.getSupplementalSemanticIds(), false))
					.append(',').append(describe(element.getQualifiers(), false));
			if (element instanceof Property property) result.append(',').append(property.getValueType())
					.append(',').append(describe(property.getValue(), secretName(element.getIdShort())));
			else if (element instanceof org.eclipse.digitaltwin.aas4j.v3.model.Blob blob)
				result.append(',').append(blob.getContentType()).append(',').append(describe(blob.getValue(), false));
			else if (element instanceof org.eclipse.digitaltwin.aas4j.v3.model.ReferenceElement reference)
				result.append(',').append(describe(reference.getValue(), false));
			else if (!(element instanceof org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection
					|| element instanceof org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementList
					|| element instanceof org.eclipse.digitaltwin.aas4j.v3.model.Operation))
				result.append(',').append(element);
			return result.append(',').append(describe(Aas.children(element), false)).append(')').toString();
		}
		if (value instanceof Record record) {
			StringBuilder result = new StringBuilder(record.getClass().getSimpleName()).append('(');
			for (var component : record.getClass().getRecordComponents()) {
				try {
					result.append(component.getName()).append('=')
							.append(describe(component.getAccessor().invoke(record), secretName(component.getName()))).append(',');
				} catch (ReflectiveOperationException inaccessible) { throw new IllegalStateException(inaccessible); }
			}
			return result.append(')').toString();
		}
		if (value instanceof Map<?, ?> map) {
			Map<String, String> sorted = new TreeMap<>();
			map.forEach((key, member) -> sorted.put(String.valueOf(key), describe(member, secretName(String.valueOf(key)))));
			return sorted.toString();
		}
		if (value instanceof Collection<?> collection) {
			StringBuilder result = new StringBuilder("[");
			for (Object member : collection) result.append(describe(member, false)).append(',');
			return result.append(']').toString();
		}
		return value.toString();
	}

	private static String redactUri(String text) {
		try {
			URI uri = URI.create(text);
			return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(),
					uri.getQuery() == null ? null : "redacted", null).toString();
		} catch (IllegalArgumentException | java.net.URISyntaxException invalid) { return "<redacted-uri>"; }
	}

	private static boolean secretName(String name) {
		if (name == null) return false;
		String key = name.toLowerCase(java.util.Locale.ROOT);
		return key.contains("password") || key.contains("credential") || key.contains("username")
				|| key.contains("secret") || key.contains("token") || key.contains("authorization") || key.equals("query")
				|| key.equals("htv_fieldvalue"); // a static HTTP header may carry an API key
	}
}
