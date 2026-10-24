/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.influxdb2.configuration;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.influxdb2.RecordColumn;
import dk.aau.smartproductionlab.dmp.influxdb2.component.DmpInfluxDb2Component;

/**
 * A TimeSeries LinkedSegment on the configured InfluxDB: written one point per row as a sink, its latest row read
 * as a source.
 *
 * @param start the segment's authored period's start, {@code null} where it has none
 * @param end   its end, {@code null} where it has none
 */
public record InfluxDb2Endpoint(String measurement, String bucket, String timePath, Map<String, String> tags,
		List<RecordColumn> columns, Instant start, Instant end) implements Endpoint {
	public InfluxDb2Endpoint {
		tags = Map.copyOf(tags);
		columns = List.copyOf(columns);
	}

	@Override
	public String uri(BundleResources resources) {
		return DmpInfluxDb2Component.uri(measurement, bucket, timePath, tags, columns, start, end);
	}
}
