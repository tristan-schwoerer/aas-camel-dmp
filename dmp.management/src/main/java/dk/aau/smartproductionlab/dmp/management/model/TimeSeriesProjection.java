/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.model;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.digitaltwin.aas4j.v3.model.HasSemantics;
import org.eclipse.digitaltwin.aas4j.v3.model.File;
import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.LangStringTextType;
import org.eclipse.digitaltwin.aas4j.v3.model.MultiLanguageProperty;
import org.eclipse.digitaltwin.aas4j.v3.model.Operation;
import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.Range;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.ReferenceElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementList;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.influxdb2.InfluxValues;
import dk.aau.smartproductionlab.dmp.influxdb2.RecordColumn;

/**
 * Projects a TimeSeries LinkedSegment (IDTA 02008, this project's DMP profile) and its series container into what
 * the DMP writes and reads. Elements are recognised by semanticId only: the series' Record is the row shape, a field's
 * InfluxDB field is its idShortPath below the Record ({@code Position.x}, list members by index), and the segment's
 * {@code InfluxDb2Target} names bucket, measurement and tags. A ReferenceElement in the Record counts as if the element
 * it references were declared at its place. Nothing missing is filled in.
 */
public final class TimeSeriesProjection {
	public static final String IDTA_TIME_SERIES = "https://admin-shell.io/idta/TimeSeries/1/1";
	public static final String IDTA_METADATA = "https://admin-shell.io/idta/TimeSeries/Metadata/1/1";
	public static final String IDTA_RECORD = "https://admin-shell.io/idta/TimeSeries/Record/1/1";
	public static final String IDTA_SEGMENTS = "https://admin-shell.io/idta/TimeSeries/Segments/1/1";
	public static final String IDTA_LINKED_SEGMENT = "https://admin-shell.io/idta/TimeSeries/Segments/LinkedSegment/1/1";
	public static final String IDTA_ENDPOINT = "https://admin-shell.io/idta/TimeSeries/Endpoint/1/1";
	public static final String IDTA_UTC_TIME = "https://admin-shell.io/idta/TimeSeries/UtcTime/1/1";
	public static final String IDTA_START_TIME = "https://admin-shell.io/idta/TimeSeries/Segment/StartTime/1/1";
	public static final String IDTA_END_TIME = "https://admin-shell.io/idta/TimeSeries/Segment/EndTime/1/1";
	public static final String IDTA_STATE = "https://admin-shell.io/idta/TimeSeries/Segment/State/1/1";
	/** The IDTA segment states: a running segment, and one whose period is over. */
	public static final String IN_PROGRESS = "InProgress";
	public static final String COMPLETED = "Completed";
	/** Time semantics a row can carry; only UtcTime places a point on InfluxDB's absolute time axis. */
	static final Set<String> IDTA_TIMES = Set.of(IDTA_UTC_TIME, "https://admin-shell.io/idta/TimeSeries/TaiTime/1/1",
			"https://admin-shell.io/idta/TimeSeries/RelativePointInTime/1/1",
			"https://admin-shell.io/idta/TimeSeries/RelativeTimeDuration/1/1");

	private static final String BASE = "https://smartproductionlab.aau.dk";
	public static final String SERIES = BASE + "/variables/TimeSeries/1/0";
	public static final String INFLUX_TARGET = BASE + "/timeseries/InfluxDb2Target/1/0";
	public static final String BUCKET = BASE + "/timeseries/InfluxDb2Target/Bucket/1/0";
	public static final String MEASUREMENT = BASE + "/timeseries/InfluxDb2Target/Measurement/1/0";
	public static final String TAGS = BASE + "/timeseries/InfluxDb2Target/Tags/1/0";
	public static final String TAG = BASE + "/timeseries/InfluxDb2Target/Tag/1/0";
	public static final String WRITE_MODE = BASE + "/timeseries/InfluxDb2Target/WriteMode/1/0";
	private static final String SYNCHRONOUS = "Synchronous";
	private static final String ASYNCHRONOUS = "Asynchronous";

	/**
	 * What one LinkedSegment writes and reads: one point per row of its series' Record. {@code start} and {@code end}
	 * are the segment's authored period ({@code null} when not authored); a segment is open for writes unless it is
	 * {@code Completed} or has an end. {@code asynchronous}: its target's WriteMode is {@code Asynchronous}, so its
	 * points are batched and not waited for (absent or {@code Synchronous}: each point is acknowledged).
	 */
	public record Series(URI endpoint, String bucket, String measurement, Map<String, String> tags, String timePath,
			List<RecordColumn> columns, Instant start, Instant end, boolean open, List<Reference> references,
			boolean asynchronous) {
		public Series { tags = Map.copyOf(tags); columns = List.copyOf(columns); references = List.copyOf(references); }
	}

	/** Resolves a Record's ReferenceElement to the element it references; fails with {@link IllegalArgumentException}. */
	@FunctionalInterface
	public interface Resolver {
		SubmodelElement resolve(Reference reference);
	}

	private TimeSeriesProjection() { }

	/** Whether an element is a TimeSeries LinkedSegment. */
	public static boolean isLinkedSegment(SubmodelElement element) {
		return element instanceof SubmodelElementCollection && Aas.hasSemantic(element, IDTA_LINKED_SEGMENT);
	}

	/** Whether an element is a TimeSeries container: this project's series SMC, or one carrying the IDTA id. */
	public static boolean isSeries(SubmodelElement element) {
		return element instanceof SubmodelElementCollection
				&& (Aas.hasSemantic(element, SERIES) || semanticOrSupplemental(element, IDTA_TIME_SERIES));
	}

	/**
	 * Projects a segment of {@code series}; {@code segments} is the Segments collection between them. The Record's
	 * references are resolved through {@code resolver} and listed in {@link Series#references()}.
	 */
	public static Series project(SubmodelElement series, SubmodelElement segments, SubmodelElement segment, Resolver resolver) {
		if (!isSeries(series)) throw error("the segment's container is not a TimeSeries (" + SERIES + ")");
		if (!Aas.hasSemantic(segments, IDTA_SEGMENTS) || !Aas.children(series).contains(segments)
				|| !Aas.children(segments).contains(segment))
			throw error("a LinkedSegment must lie in the Segments of its TimeSeries");
		if (!isLinkedSegment(segment)) throw error("the target is not a TimeSeries LinkedSegment");
		SubmodelElement record = only(Aas.children(only(Aas.children(series), IDTA_METADATA, "Metadata")), IDTA_RECORD, "Record");

		String timePath = null;
		List<RecordColumn> columns = new ArrayList<>();
		List<Reference> references = new ArrayList<>();
		for (SubmodelElement child : Aas.children(record)) {
			if (IDTA_TIMES.stream().anyMatch(time -> Aas.hasSemantic(child, time))) {
				if (!Aas.hasSemantic(child, IDTA_UTC_TIME))
					throw error("only UtcTime records can be written or read; " + child.getIdShort() + " has another time semantic");
				if (timePath != null) throw error("the Record has more than one time");
				if (!(child instanceof Property time) || !"xs:dateTime".equals(Aas.xsd(time.getValueType())))
					throw error("the Record's UtcTime must be an xs:dateTime Property");
				timePath = child.getIdShort();
				continue;
			}
			if (child.getIdShort() == null || child.getIdShort().isBlank()) throw error("a Record field has no idShort");
			entry(child, child.getIdShort(), columns, references, resolver);
		}
		if (timePath == null) throw error("the Record has no UtcTime");
		if (columns.isEmpty()) throw error("the Record has no field");

		SubmodelElement target = only(Aas.children(segment), INFLUX_TARGET, "InfluxDb2Target");
		Map<String, String> tags = new LinkedHashMap<>();
		for (SubmodelElement tag : Aas.children(only(Aas.children(target), TAGS, "Tags"))) {
			if (!Aas.hasSemantic(tag, TAG)) throw error("InfluxDb2Target Tags holds an element that is not a Tag: " + tag.getIdShort());
			tags.put(tag.getIdShort(), value(tag, "Tag " + tag.getIdShort()));
		}
		URI endpoint;
		try { endpoint = URI.create(value(only(Aas.children(segment), IDTA_ENDPOINT, "Endpoint"), "Endpoint")); }
		catch (IllegalArgumentException invalid) { throw error("the segment's Endpoint is not a URI"); }

		Instant start = time(optional(Aas.children(segment), IDTA_START_TIME, "StartTime"), "StartTime");
		Instant end = time(optional(Aas.children(segment), IDTA_END_TIME, "EndTime"), "EndTime");
		if (start != null && end != null && !end.isAfter(start)) throw error("the segment's EndTime must lie after its StartTime");
		String state = optional(Aas.children(segment), IDTA_STATE, "State");
		if (state != null && !IN_PROGRESS.equals(state) && !COMPLETED.equals(state))
			throw error("the segment's State must be " + IN_PROGRESS + " or " + COMPLETED + ", not " + state);
		if (IN_PROGRESS.equals(state) && end != null) throw error("an InProgress segment has no EndTime");
		String writeMode = optional(Aas.children(target), WRITE_MODE, "WriteMode");
		if (writeMode != null && !SYNCHRONOUS.equals(writeMode) && !ASYNCHRONOUS.equals(writeMode))
			throw error("the InfluxDb2Target's WriteMode must be " + SYNCHRONOUS + " or " + ASYNCHRONOUS + ", not " + writeMode);
		return new Series(endpoint, value(only(Aas.children(target), BUCKET, "Bucket"), "Bucket"),
				value(only(Aas.children(target), MEASUREMENT, "Measurement"), "Measurement"), tags, timePath, columns,
				start, end, end == null && !COMPLETED.equals(state), references, ASYNCHRONOUS.equals(writeMode));
	}

	/** The value of an optional segment property; an empty value counts as not authored. */
	private static String optional(List<SubmodelElement> children, String semantic, String what) {
		List<SubmodelElement> matches = children.stream().filter(child -> Aas.hasSemantic(child, semantic)).toList();
		if (matches.size() > 1) throw error(what + " is declared twice");
		if (matches.isEmpty()) return null;
		if (!(matches.get(0) instanceof Property property)) throw error(what + " must be a Property");
		return property.getValue() == null || property.getValue().isBlank() ? null : property.getValue().strip();
	}

	/** A segment time in the format of a UtcTime series: an xs:dateTime with an offset. */
	private static Instant time(String value, String what) {
		if (value == null) return null;
		try { return OffsetDateTime.parse(value).toInstant(); }
		catch (DateTimeParseException invalid) { throw error("the segment's " + what + " is not an xs:dateTime with an offset: " + value); }
	}

	/** A Record entry at {@code path}: a reference is resolved and then counts as the element it references. */
	private static void entry(SubmodelElement element, String path, List<RecordColumn> columns, List<Reference> references,
			Resolver resolver) {
		if (element instanceof ReferenceElement reference) {
			if (reference.getValue() == null) throw error("Record reference " + path + " references nothing");
			references.add(reference.getValue());
			SubmodelElement referenced;
			try { referenced = resolver.resolve(reference.getValue()); }
			catch (IllegalArgumentException unresolved) { throw error("Record reference " + path + ": " + unresolved.getMessage()); }
			shape(referenced, path, columns, references, null);
			return;
		}
		shape(element, path, columns, references, resolver);
	}

	/**
	 * The columns of an element at {@code path}: a Property is one, a Range two ({@code .min}, {@code .max}), a File
	 * one string (its path or URL), a MultiLanguageProperty one string per language its value declares
	 * ({@code .en}); a collection's members follow by idShort, a list's by index, an Operation's input, InOut and
	 * output variables by idShort. Inside a referenced element ({@code resolver == null}) references do not chain.
	 */
	private static void shape(SubmodelElement element, String path, List<RecordColumn> columns, List<Reference> references,
			Resolver resolver) {
		switch (element) {
			case Property property -> {
				String valueType = Aas.xsd(property.getValueType());
				if (InfluxValues.kind(valueType) == null) throw error("Record field " + path + " has unsupported valueType " + valueType);
				columns.add(new RecordColumn(path, valueType));
			}
			case Range range -> {
				String valueType = Aas.xsd(range.getValueType());
				if (InfluxValues.kind(valueType) == null) throw error("Record range " + path + " has unsupported valueType " + valueType);
				columns.add(new RecordColumn(path + ".min", valueType));
				columns.add(new RecordColumn(path + ".max", valueType));
			}
			case File file -> columns.add(new RecordColumn(path, "xs:string"));
			case MultiLanguageProperty text -> {
				Set<String> languages = new LinkedHashSet<>();
				for (LangStringTextType entry : Aas.nonNull(text.getValue())) {
					String language = entry.getLanguage();
					if (language == null || language.isBlank() || language.contains(".") || language.contains("["))
						throw error("Record text " + path + " declares an invalid language " + language);
					if (!languages.add(language)) throw error("Record text " + path + " declares " + language + " twice");
					columns.add(new RecordColumn(path + "." + language, "xs:string"));
				}
				if (languages.isEmpty()) throw error("Record text " + path + " declares no language; its value names them");
			}
			case SubmodelElementCollection collection -> {
				for (SubmodelElement child : Aas.children(collection))
					member(member(child, path), path + "." + child.getIdShort(), columns, references, resolver);
			}
			case SubmodelElementList list -> {
				List<SubmodelElement> members = Aas.children(list);
				for (int i = 0; i < members.size(); i++) member(members.get(i), path + "[" + i + "]", columns, references, resolver);
			}
			case Operation operation -> {
				Set<String> names = new LinkedHashSet<>();
				for (var category : List.of(Aas.nonNull(operation.getInputVariables()), Aas.nonNull(operation.getInoutputVariables()),
						Aas.nonNull(operation.getOutputVariables())))
					for (OperationVariable variable : category) {
						SubmodelElement value = member(variable.getValue(), path);
						if (!names.add(value.getIdShort())) throw error("Operation " + path + " declares " + value.getIdShort() + " twice");
						member(value, path + "." + value.getIdShort(), columns, references, resolver);
					}
			}
			case ReferenceElement nested -> throw error("a referenced element holds a reference at " + path + "; references do not chain");
			default -> throw error("Record field " + path + " is a " + Aas.modelType(element) + ", which has no InfluxDB form");
		}
	}

	/** A member of a structure: resolved like a Record entry inline, as plain shape inside a referenced element. */
	private static void member(SubmodelElement element, String path, List<RecordColumn> columns, List<Reference> references,
			Resolver resolver) {
		if (resolver == null) shape(element, path, columns, references, null);
		else entry(element, path, columns, references, resolver);
	}

	private static SubmodelElement member(SubmodelElement element, String parent) {
		if (element == null || element.getIdShort() == null || element.getIdShort().isBlank())
			throw error("an element of " + parent + " has no idShort");
		return element;
	}

	private static SubmodelElement only(List<SubmodelElement> children, String semantic, String what) {
		List<SubmodelElement> matches = children.stream().filter(child -> Aas.hasSemantic(child, semantic)).toList();
		if (matches.size() != 1) throw error(matches.isEmpty() ? what + " (" + semantic + ") is missing" : what + " is declared twice");
		return matches.get(0);
	}

	private static String value(SubmodelElement element, String what) {
		if (!(element instanceof Property property) || property.getValue() == null || property.getValue().isBlank())
			throw error(what + " must be a Property with a value");
		return property.getValue();
	}

	private static boolean semanticOrSupplemental(HasSemantics element, String semantic) {
		if (Aas.hasSemantic(element, semantic)) return true;
		for (Reference supplemental : Aas.nonNull(element.getSupplementalSemanticIds()))
			for (Key key : Aas.keys(supplemental)) if (semantic.equals(key.getValue())) return true;
		return false;
	}

	private static IllegalArgumentException error(String message) { return new IllegalArgumentException(message); }
}
