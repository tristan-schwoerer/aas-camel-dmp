/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.influxdb2;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The one InfluxDB 2 the DMP writes TimeSeries points to and reads them from. The token comes from the
 * environment only: it is never part of a model, a Camel URI or a diagnostic. {@code lookback} bounds how far
 * back a segment Source looks for the latest row: InfluxDB's {@code last()} needs a range, and a field
 * unchanged for longer than it is not found. {@code flushInterval} and {@code bufferLimit} shape the batching of
 * segments written asynchronously (WriteMode {@code Asynchronous}): a batch leaves at the latest after the interval,
 * and at most {@code bufferLimit} points wait; beyond it the oldest are dropped.
 */
public record InfluxSettings(URI url, String org, String token, Duration lookback, Duration flushInterval, int bufferLimit) {
	public static final String URL = "DMP_INFLUX_URL";
	public static final String ORG = "DMP_INFLUX_ORG";
	public static final String TOKEN = "DMP_INFLUX_TOKEN";
	public static final String LOOKBACK = "DMP_INFLUX_LOOKBACK";
	public static final String FLUSH_INTERVAL = "DMP_INFLUX_FLUSH_INTERVAL";
	public static final String BUFFER_LIMIT = "DMP_INFLUX_BUFFER_LIMIT";
	public static final Duration DEFAULT_LOOKBACK = Duration.ofDays(30);
	public static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofSeconds(1);
	public static final int DEFAULT_BUFFER_LIMIT = 10_000;

	public InfluxSettings(URI url, String org, String token) { this(url, org, token, DEFAULT_LOOKBACK); }

	public InfluxSettings(URI url, String org, String token, Duration lookback) {
		this(url, org, token, lookback, DEFAULT_FLUSH_INTERVAL, DEFAULT_BUFFER_LIMIT);
	}

	public InfluxSettings {
		Objects.requireNonNull(url, "url");
		if (url.getHost() == null || url.getUserInfo() != null || url.getQuery() != null || url.getFragment() != null
				|| !("http".equalsIgnoreCase(url.getScheme()) || "https".equalsIgnoreCase(url.getScheme())))
			throw new IllegalArgumentException(URL + " must be a credential-free HTTP(S) URI without a query");
		if (org == null || org.isBlank()) throw new IllegalArgumentException(ORG + " is required with " + URL);
		if (token == null || token.isBlank()) throw new IllegalArgumentException(TOKEN + " is required with " + URL);
		if (lookback == null || lookback.isNegative() || lookback.isZero() || lookback.toSeconds() < 1)
			throw new IllegalArgumentException(LOOKBACK + " must be a positive ISO-8601 duration of at least one second");
		if (flushInterval == null || flushInterval.toMillis() < 1 || flushInterval.toMillis() > Integer.MAX_VALUE)
			throw new IllegalArgumentException(FLUSH_INTERVAL + " must be a positive ISO-8601 duration of at least one millisecond");
		if (bufferLimit < 1) throw new IllegalArgumentException(BUFFER_LIMIT + " must be a positive number of points");
	}

	/** The configured InfluxDB, or empty when {@value #URL} is unset. */
	public static Optional<InfluxSettings> fromEnvironment(Map<String, String> environment) {
		String url = environment.get(URL);
		if (url == null || url.isBlank()) return Optional.empty();
		Duration window = duration(environment, LOOKBACK, DEFAULT_LOOKBACK, "P30D");
		Duration flush = duration(environment, FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL, "PT1S");
		String limit = environment.get(BUFFER_LIMIT);
		int points;
		try { points = limit == null || limit.isBlank() ? DEFAULT_BUFFER_LIMIT : Integer.parseInt(limit.strip()); }
		catch (NumberFormatException invalid) {
			throw new IllegalArgumentException(BUFFER_LIMIT + " must be a number of points, not " + limit);
		}
		return Optional.of(new InfluxSettings(URI.create(url.strip()), environment.get(ORG), environment.get(TOKEN), window,
				flush, points));
	}

	private static Duration duration(Map<String, String> environment, String name, Duration fallback, String example) {
		String value = environment.get(name);
		try { return value == null || value.isBlank() ? fallback : Duration.parse(value.strip()); }
		catch (DateTimeParseException invalid) {
			throw new IllegalArgumentException(name + " must be an ISO-8601 duration such as " + example + ", not " + value);
		}
	}

	/**
	 * Whether a LinkedSegment's {@code Endpoint} (the InfluxDB 2 query URL, {@code …/api/v2/query?org=…}) names this
	 * InfluxDB: same scheme, host and port, and the same org.
	 */
	public boolean serves(URI endpoint) {
		if (endpoint == null || endpoint.getHost() == null || endpoint.getScheme() == null) return false;
		return endpoint.getScheme().equalsIgnoreCase(url.getScheme()) && endpoint.getHost().equalsIgnoreCase(url.getHost())
				&& port(endpoint) == port(url) && org.equals(queryParameter(endpoint, "org"));
	}

	private static int port(URI uri) {
		if (uri.getPort() >= 0) return uri.getPort();
		return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
	}

	private static String queryParameter(URI uri, String name) {
		String query = uri.getRawQuery();
		if (query == null) return null;
		for (String pair : query.split("&")) {
			int equals = pair.indexOf('=');
			String key = URLDecoder.decode(equals < 0 ? pair : pair.substring(0, equals), StandardCharsets.UTF_8);
			if (key.equals(name)) return equals < 0 ? "" : URLDecoder.decode(pair.substring(equals + 1), StandardCharsets.UTF_8);
		}
		return null;
	}

	@Override public String toString() {
		return "InfluxSettings[url=" + url + ", org=" + org + ", token=<redacted>, lookback=" + lookback
				+ ", flushInterval=" + flushInterval + ", bufferLimit=" + bufferLimit + "]";
	}
}
