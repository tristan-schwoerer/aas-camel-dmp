/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.influxdb2.component;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Consumer;
import org.apache.camel.PollingConsumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.component.influxdb2.InfluxDb2Endpoint;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.influxdb2.InfluxSettings;
import dk.aau.smartproductionlab.dmp.influxdb2.InfluxValues;
import dk.aau.smartproductionlab.dmp.influxdb2.RecordColumn;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.influxdb.client.write.Point;
import com.influxdb.exceptions.InfluxException;

/**
 * The points of one LinkedSegment: one measurement in one bucket, the segment's tags, and the Record's fields by
 * idShort path. A row is the transformation's value for the segment's SinkId: a table keyed by Record idShort,
 * groups nested ({@code Position.X}). A field the row omits is not written; an unknown field or a value of another
 * type rejects the row.
 */
public final class DmpInfluxDb2Endpoint extends InfluxDb2Endpoint {
	private final String measurement;
	private final String timePath;
	private final Map<String, String> tags;
	/** The Record's fields by idShortPath, which is also their InfluxDB field. */
	private final Map<String, RecordColumn> columns = new LinkedHashMap<>();
	/** One segment of an idShortPath: a name, or a list index. */
	private static final java.util.regex.Pattern SEGMENT = java.util.regex.Pattern.compile("([^.\\[\\]]+)|\\[(\\d+)\\]");
	/** The segment's authored period; {@code null} where it has none. */
	private final Instant start;
	private final Instant stop;
	/** Points go to the component's batching writer instead of waiting for InfluxDB (WriteMode Asynchronous). */
	private final boolean asynchronous;

	DmpInfluxDb2Endpoint(String uri, DmpInfluxDb2Component component, String measurement, String bucket,
			String timePath, Map<String, String> tags, List<RecordColumn> columns, Instant start, Instant stop,
			boolean asynchronous) {
		super(uri, component);
		this.start = start;
		this.stop = stop;
		this.asynchronous = asynchronous;
		this.measurement = measurement;
		this.timePath = timePath;
		this.tags = Map.copyOf(tags);
		for (RecordColumn column : columns) this.columns.put(column.path(), column);
		setBucket(bucket);
		setWritePrecision(WritePrecision.NS);
		setAutoCreateOrg(false);
		setAutoCreateBucket(false);
	}

	@Override public DmpInfluxDb2Component getComponent() { return (DmpInfluxDb2Component) super.getComponent(); }

	/** The DMP never provisions InfluxDB: unlike camel-influxdb2 it neither creates the org nor the bucket. */
	@Override
	protected void doInit() { }

	@Override public Producer createProducer() { return new DmpInfluxDb2Producer(this); }

	@Override public Consumer createConsumer(Processor processor) {
		throw new UnsupportedOperationException("dmp-influxdb2 is read by polling; InfluxDB has no subscriptions");
	}

	@Override public PollingConsumer createPollingConsumer() { return new DmpInfluxDb2PollingConsumer(this); }

	/**
	 * The Flux of the latest row: each Record field's last value in this segment's measurement and tags, within the
	 * segment's period, or within the lookback where it has no StartTime (exposed for tests).
	 */
	String latestQuery(Duration lookback) {
		String range = (start == null ? "start: -" + lookback.toSeconds() + "s" : "start: " + start)
				+ (stop == null ? "" : ", stop: " + stop.plusNanos(1));
		StringBuilder flux = new StringBuilder("from(bucket: ").append(InfluxValues.flux(getBucket())).append(")\n")
				.append("  |> range(").append(range).append(")\n")
				.append("  |> filter(fn: (r) => r._measurement == ").append(InfluxValues.flux(measurement)).append(")\n");
		tags.forEach((key, value) -> flux.append("  |> filter(fn: (r) => r[").append(InfluxValues.flux(key)).append("] == ")
				.append(InfluxValues.flux(value)).append(")\n"));
		flux.append("  |> filter(fn: (r) => ");
		boolean first = true;
		for (RecordColumn column : columns.values()) {
			if (!first) flux.append(" or ");
			flux.append("r._field == ").append(InfluxValues.flux(column.path()));
			first = false;
		}
		return flux.append(")\n  |> last()").toString();
	}

	/**
	 * The latest row as the Lua value of the segment: a table keyed by Record idShort, groups nested, each field
	 * its last value and {@code Time} the newest of their times. A series without a point yet yields {@code null},
	 * so the source has no value, as before an event-sourced Property's first change.
	 */
	Map<String, Object> read() {
		DmpInfluxDb2Component component = getComponent();
		InfluxSettings settings = component.settings();
		InfluxDBClient client = component.getInfluxDBClient();
		if (settings == null || client == null) throw notConfigured();
		List<FluxTable> tables;
		try {
			tables = client.getQueryApi().query(latestQuery(settings.lookback()), settings.org());
		} catch (InfluxException failure) {
			int status = failure.status();
			if (status >= 400 && status < 500 && status != 408 && status != 429)
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE, "INFLUX_QUERY_REJECTED",
						"InfluxDB rejected the query", Map.of("status", status, "cause", String.valueOf(failure.getMessage())));
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.TRANSPORT, "INFLUX_QUERY_FAILED",
					"InfluxDB could not be reached", Map.of("status", status, "cause", String.valueOf(failure.getMessage())));
		}
		Map<String, Object> row = new LinkedHashMap<>();
		Instant newest = null;
		for (FluxTable table : tables) for (FluxRecord record : table.getRecords()) {
			RecordColumn column = columns.get(record.getField());
			if (column == null) continue;
			Object value;
			try { value = InfluxValues.read(column, record.getValue()); }
			catch (IllegalArgumentException mismatch) {
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE, "INFLUX_VALUE_MISMATCH",
						mismatch.getMessage(), Map.of("field", column.path()));
			}
			place(row, column.path(), value);
			if (record.getTime() != null && (newest == null || record.getTime().isAfter(newest))) newest = record.getTime();
		}
		if (row.isEmpty()) return null;
		row.put(timePath, newest == null ? null : newest.toString());
		return row;
	}

	/** Puts {@code value} at {@code path} in the row: {@code .} into a table, {@code [i]} into a list. */
	private static void place(Map<String, Object> row, String path, Object value) {
		List<Object> segments = new java.util.ArrayList<>();
		java.util.regex.Matcher matcher = SEGMENT.matcher(path);
		while (matcher.find()) segments.add(matcher.group(1) != null ? matcher.group(1) : Integer.valueOf(matcher.group(2)));
		Object container = row;
		for (int i = 0; i < segments.size() - 1; i++) {
			Object next = get(container, segments.get(i));
			if (next == null) {
				next = segments.get(i + 1) instanceof Integer ? new java.util.ArrayList<Object>() : new LinkedHashMap<String, Object>();
				put(container, segments.get(i), next);
			}
			container = next;
		}
		put(container, segments.get(segments.size() - 1), value);
	}

	@SuppressWarnings("unchecked")
	private static Object get(Object container, Object segment) {
		if (segment instanceof Integer index) {
			List<Object> list = (List<Object>) container;
			return index < list.size() ? list.get(index) : null;
		}
		return ((Map<String, Object>) container).get(segment);
	}

	@SuppressWarnings("unchecked")
	private static void put(Object container, Object segment, Object value) {
		if (segment instanceof Integer index) {
			List<Object> list = (List<Object>) container;
			while (list.size() <= index) list.add(null);
			list.set(index, value);
		} else {
			((Map<String, Object>) container).put((String) segment, value);
		}
	}

	/** The point a row becomes (exposed for tests). */
	Point point(Object row, Instant now) {
		if (!(row instanceof Map<?, ?> values))
			throw rejected("a LinkedSegment row must be a table keyed by Record idShort, not "
					+ (row == null ? "null" : row.getClass().getSimpleName()));
		Point point = Point.measurement(measurement);
		tags.forEach(point::addTag);
		Instant[] time = { now };
		int fields = 0;
		for (Map.Entry<?, ?> entry : values.entrySet()) fields += addValue(point, String.valueOf(entry.getKey()), entry.getValue(), time);
		if (fields == 0) throw rejected("the row sets no Record field");
		if (start != null && time[0].isBefore(start)) throw rejected("the row's time " + time[0] + " lies before the segment's StartTime " + start);
		if (stop != null && time[0].isAfter(stop)) throw rejected("the row's time " + time[0] + " lies after the segment's EndTime " + stop);
		return point.time(time[0], WritePrecision.NS);
	}

	/**
	 * Writes the row as one point and waits for InfluxDB to acknowledge it; a segment written asynchronously hands
	 * the point to the batching writer instead. Either way a row that does not fit the Record raises before any write.
	 */
	void write(Object row) {
		Point point = point(row, Instant.now());
		DmpInfluxDb2Component component = getComponent();
		InfluxSettings settings = component.settings();
		InfluxDBClient client = component.getInfluxDBClient();
		if (settings == null || client == null) throw notConfigured();
		if (asynchronous) {
			com.influxdb.client.WriteApi batches = component.batches();
			if (batches == null) throw notConfigured();
			batches.writePoint(getBucket(), settings.org(), point);
			component.submitted();
			return;
		}
		try {
			client.getWriteApiBlocking().writePoint(getBucket(), settings.org(), point);
		} catch (InfluxException failure) {
			int status = failure.status();
			if (status >= 400 && status < 500 && status != 408 && status != 429)
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE, "INFLUX_WRITE_REJECTED",
						"InfluxDB rejected the point", Map.of("status", status, "cause", String.valueOf(failure.getMessage())));
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.TRANSPORT, "INFLUX_WRITE_FAILED",
					"InfluxDB could not be reached", Map.of("status", status, "cause", String.valueOf(failure.getMessage())));
		}
	}

	/** Adds the field(s) of the row's value at {@code path}: tables follow {@code .}, lists {@code [i]}. */
	private int addValue(Point point, String path, Object value, Instant[] time) {
		if (path.equals(timePath)) {
			try { time[0] = InfluxValues.time(value); }
			catch (IllegalArgumentException invalid) { throw rejected(invalid.getMessage()); }
			return 0;
		}
		RecordColumn column = columns.get(path);
		if (column != null) {
			Object field;
			try { field = InfluxValues.field(column, value); }
			catch (IllegalArgumentException | ArithmeticException mismatch) { throw rejected(mismatch.getMessage()); }
			switch (field) {
				case String text -> point.addField(column.path(), text);
				case Boolean flag -> point.addField(column.path(), flag);
				case Long number -> point.addField(column.path(), number);
				case Double number -> point.addField(column.path(), number);
				default -> throw new IllegalStateException("unexpected field value " + field.getClass());
			}
			return 1;
		}
		int fields = 0;
		if (value instanceof Map<?, ?> group && hasFieldsBelow(path + ".")) {
			for (Map.Entry<?, ?> entry : group.entrySet()) fields += addValue(point, path + "." + entry.getKey(), entry.getValue(), time);
			return fields;
		}
		if (value instanceof List<?> list && hasFieldsBelow(path + "[")) {
			for (int i = 0; i < list.size(); i++) if (list.get(i) != null) fields += addValue(point, path + "[" + i + "]", list.get(i), time);
			return fields;
		}
		throw rejected("the Record has no field " + path);
	}

	private boolean hasFieldsBelow(String prefix) {
		return columns.keySet().stream().anyMatch(path -> path.startsWith(prefix));
	}

	private static ConnectorInvocationException notConfigured() {
		return new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
				"INFLUX_NOT_CONFIGURED", "no InfluxDB is configured (" + InfluxSettings.URL + ")", Map.of());
	}

	private static ConnectorInvocationException rejected(String message) {
		return new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
				"INFLUX_ROW_INVALID", message, Map.of());
	}
}
