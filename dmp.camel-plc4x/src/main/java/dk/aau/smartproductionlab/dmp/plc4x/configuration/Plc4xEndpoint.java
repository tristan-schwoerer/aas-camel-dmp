/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.plc4x.configuration;

import java.net.URISyntaxException;
import java.util.List;

import org.apache.hc.core5.net.URIBuilder;
import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;

/**
 * Tags of a PLC4X device ({@code dmp-plc4x:<driver>://host:port/servicePath}): read as a source, written as a sink
 * (e.g. Modbus coils and registers through the modbus-tcp driver).
 *
 * @author danish
 */
public record Plc4xEndpoint(String host, int port, String driver, String servicePath, List<Option> options,
		List<Tag> tags) implements Endpoint {
	private static final String PLC4X_PROTOCOL_NAME = "dmp-plc4x:";
	private static final String TAG_PREFIX = "tag.";

	public Plc4xEndpoint {
		servicePath = servicePath == null ? "" : servicePath;
		options = options == null ? List.of() : List.copyOf(options);
		tags = tags == null ? List.of() : List.copyOf(tags);
	}

	@Override
	public String uri(BundleResources resources) {
		URIBuilder uriBuilder = new URIBuilder();
		uriBuilder.setScheme(PLC4X_PROTOCOL_NAME + driver);
		uriBuilder.setHost(host);
		uriBuilder.setPort(port);
		uriBuilder.setPath(servicePath);
		options.forEach(option -> uriBuilder.addParameter(option.getName(), option.getValue()));
		tags.forEach(tag -> uriBuilder.addParameter(TAG_PREFIX + tag.getName(), tag.getValue()));
		try {
			return uriBuilder.build().toString();
		} catch (URISyntaxException e) {
			throw new IllegalStateException("Exception occurred while creating Plc4XEndpoint", e);
		}
	}
}
