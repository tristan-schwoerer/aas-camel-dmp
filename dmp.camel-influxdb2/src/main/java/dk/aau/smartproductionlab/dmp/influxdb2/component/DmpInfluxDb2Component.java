/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.influxdb2.component;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.Endpoint;
import org.apache.camel.component.influxdb2.InfluxDb2Component;
import org.apache.camel.util.PropertiesHelper;
import dk.aau.smartproductionlab.dmp.influxdb2.InfluxSettings;
import dk.aau.smartproductionlab.dmp.influxdb2.RecordColumn;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.write.events.BackpressureEvent;
import com.influxdb.client.write.events.WriteErrorEvent;
import com.influxdb.client.write.events.WriteRetriableErrorEvent;
import com.influxdb.client.write.events.WriteSuccessEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code dmp-influxdb2:<measurement>?bucket=…&time=Time&tag.<key>=…&field.<path>=<valueType>[&start=…][&stop=…][&writeMode=Asynchronous]}:
 * the points of one TimeSeries LinkedSegment, within its authored period when it has one. A write is one point per
 * row. By default InfluxDB acknowledges it and a rejected or failed write raises; camel-influxdb2 writes only through
 * the asynchronous batching API, whose failures never reach the exchange, and creates missing orgs and buckets, so
 * the DMP ships this component (SPEC §9). A segment authored {@code Asynchronous} takes that batching API on
 * purpose: its points go to one shared buffer and the exchange does not wait; what InfluxDB rejects, what fails
 * after the retries and what overflows the buffer is counted ({@link #asynchronousWrites}) and logged. All endpoints
 * share the one configured InfluxDB ({@link #configure}); its token never appears in a URI.
 */
public final class DmpInfluxDb2Component extends InfluxDb2Component {
	public static final String SCHEME = "dmp-influxdb2";
	/** The URI option of a segment written asynchronously. */
	public static final String ASYNCHRONOUS = "Asynchronous";
	/** InfluxDB's preferred batch: a batch leaves at this many points or after the flush interval. */
	private static final int BATCH_SIZE = 1000;
	private static final Logger LOG = LoggerFactory.getLogger(DmpInfluxDb2Component.class);

	private InfluxSettings settings;
	/** The batching writer of the asynchronous segments, made at the first such write. */
	private WriteApi batches;
	private final AtomicLong submitted = new AtomicLong();
	private final AtomicLong written = new AtomicLong();
	private final AtomicLong failedBatches = new AtomicLong();
	private final AtomicLong retries = new AtomicLong();
	private final AtomicLong overflows = new AtomicLong();
	private final AtomicReference<String> lastError = new AtomicReference<>("");

	/** The endpoint URI of one segment written synchronously. */
	public static String uri(String measurement, String bucket, String timePath, Map<String, String> tags,
			List<RecordColumn> columns, Instant start, Instant stop) {
		return uri(measurement, bucket, timePath, tags, columns, start, stop, false);
	}

	/** The endpoint URI of one segment. Values are passed RAW so Camel leaves them unchanged. */
	public static String uri(String measurement, String bucket, String timePath, Map<String, String> tags,
			List<RecordColumn> columns, Instant start, Instant stop, boolean asynchronous) {
		StringBuilder uri = new StringBuilder(SCHEME).append(':').append(encode(measurement))
				.append("?bucket=RAW(").append(bucket).append(')')
				.append("&time=RAW(").append(timePath).append(')');
		tags.forEach((key, value) -> uri.append("&tag.").append(encode(key)).append("=RAW(").append(value).append(')'));
		for (RecordColumn column : columns)
			uri.append("&field.").append(encode(column.path())).append('=').append(column.valueType());
		if (start != null) uri.append("&start=RAW(").append(start).append(')');
		if (stop != null) uri.append("&stop=RAW(").append(stop).append(')');
		if (asynchronous) uri.append("&writeMode=").append(ASYNCHRONOUS);
		return uri.toString();
	}

	/** Sets the InfluxDB every endpoint writes to; replaces the client of an earlier configuration. */
	public synchronized void configure(InfluxSettings settings) {
		closeBatches();
		InfluxDBClient previous = getInfluxDBClient();
		this.settings = settings;
		setInfluxDBClient(settings == null ? null
				: InfluxDBClientFactory.create(settings.url().toString(), settings.token().toCharArray(), settings.org()));
		if (previous != null) previous.close();
	}

	synchronized InfluxSettings settings() { return settings; }

	/**
	 * The shared batching writer, made on first use; {@code null} without a configured InfluxDB. Its batches leave
	 * after {@link InfluxSettings#flushInterval()} or at {@value #BATCH_SIZE} points; retriable failures (unreachable,
	 * 5xx, 408, 429) are retried by the client, and beyond {@link InfluxSettings#bufferLimit()} the oldest points drop.
	 */
	synchronized WriteApi batches() {
		InfluxDBClient client = getInfluxDBClient();
		if (settings == null || client == null) return null;
		if (batches == null) {
			batches = client.makeWriteApi(WriteOptions.builder()
					.batchSize(Math.min(BATCH_SIZE, settings.bufferLimit()))
					.flushInterval((int) settings.flushInterval().toMillis())
					.bufferLimit(settings.bufferLimit())
					.build());
			batches.listenEvents(WriteSuccessEvent.class, event -> written.addAndGet(points(event.getLineProtocol())));
			batches.listenEvents(WriteRetriableErrorEvent.class, event -> retries.incrementAndGet());
			batches.listenEvents(WriteErrorEvent.class, event -> {
				failedBatches.incrementAndGet();
				String cause = String.valueOf(event.getThrowable().getMessage());
				lastError.set(cause);
				LOG.warn("InfluxDB dropped a batch of asynchronously written points: {}", cause);
			});
			batches.listenEvents(BackpressureEvent.class, event -> {
				overflows.incrementAndGet();
				lastError.set("buffer full (" + event.getReason() + ")");
				LOG.warn("the asynchronous InfluxDB buffer is full ({}); the oldest points are dropped", event.getReason());
			});
		}
		return batches;
	}

	/** Counts a point handed to the batching writer. */
	void submitted() { submitted.incrementAndGet(); }

	/**
	 * The asynchronous writes so far, for {@code /management/status}: points handed to the buffer, points InfluxDB
	 * acknowledged, batches that failed for good, retries, buffer overflows, and the last failure's message.
	 */
	public Map<String, Object> asynchronousWrites() {
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("submittedPoints", submitted.get());
		result.put("writtenPoints", written.get());
		result.put("failedBatches", failedBatches.get());
		result.put("retries", retries.get());
		result.put("overflows", overflows.get());
		result.put("lastError", lastError.get());
		return result;
	}

	private static long points(String lineProtocol) {
		if (lineProtocol == null || lineProtocol.isEmpty()) return 0;
		long lines = 1;
		for (int i = 0; i < lineProtocol.length(); i++) if (lineProtocol.charAt(i) == '\n') lines++;
		return lines;
	}

	/** Sends what waits in the buffer and closes the batching writer. */
	private synchronized void closeBatches() {
		if (batches == null) return;
		try { batches.close(); }
		finally { batches = null; }
	}

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		if (remaining == null || remaining.isBlank()) throw new IllegalArgumentException("dmp-influxdb2 endpoint must name a measurement");
		String bucket = getAndRemoveParameter(parameters, "bucket", String.class);
		if (bucket == null || bucket.isBlank()) throw new IllegalArgumentException("dmp-influxdb2 endpoint requires a bucket");
		String timePath = getAndRemoveParameter(parameters, "time", String.class);
		if (timePath == null || timePath.isBlank()) throw new IllegalArgumentException("dmp-influxdb2 endpoint requires the Record's time path");
		Map<String, String> tags = new LinkedHashMap<>();
		PropertiesHelper.extractProperties(parameters, "tag.").forEach((key, value) -> tags.put(key, String.valueOf(value)));
		List<RecordColumn> columns = PropertiesHelper.extractProperties(parameters, "field.").entrySet().stream()
				.map(entry -> new RecordColumn(entry.getKey(), String.valueOf(entry.getValue())))
				.toList();
		if (columns.isEmpty()) throw new IllegalArgumentException("dmp-influxdb2 endpoint needs at least one Record field");
		Instant start = instant(getAndRemoveParameter(parameters, "start", String.class), "start");
		Instant stop = instant(getAndRemoveParameter(parameters, "stop", String.class), "stop");
		String writeMode = getAndRemoveParameter(parameters, "writeMode", String.class);
		if (writeMode != null && !ASYNCHRONOUS.equals(writeMode) && !"Synchronous".equals(writeMode))
			throw new IllegalArgumentException("dmp-influxdb2 writeMode must be Synchronous or Asynchronous, not " + writeMode);
		DmpInfluxDb2Endpoint endpoint = new DmpInfluxDb2Endpoint(uri, this, remaining, bucket, timePath, tags, columns,
				start, stop, ASYNCHRONOUS.equals(writeMode));
		setProperties(endpoint, parameters);
		return endpoint;
	}

	@Override
	protected synchronized void doStop() throws Exception {
		closeBatches();
		InfluxDBClient client = getInfluxDBClient();
		setInfluxDBClient(null);
		if (client != null) client.close();
		super.doStop();
	}

	private static Instant instant(String value, String name) {
		if (value == null || value.isBlank()) return null;
		try { return Instant.parse(value); }
		catch (java.time.format.DateTimeParseException invalid) {
			throw new IllegalArgumentException("dmp-influxdb2 " + name + " must be an instant such as 2026-10-21T00:00:00Z");
		}
	}

	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8);
	}
}
