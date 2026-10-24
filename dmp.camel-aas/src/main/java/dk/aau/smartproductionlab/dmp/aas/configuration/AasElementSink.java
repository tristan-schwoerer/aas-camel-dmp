/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.aas.configuration;

import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;

/**
 * An AAS element written in the repository. The endpoint takes the transformation's whole result and writes
 * the value at {@code valuePath}, the sink's SinkId.
 *
 * @param elementType the element's model type, Property or SubmodelElementCollection: how the value is encoded
 */
public record AasElementSink(String submodelEndpoint, String idShortPath, String elementType, String valuePath)
		implements Endpoint {
	@Override
	public String uri(BundleResources resources) {
		String uri = "aas:" + submodelEndpoint + "?propertyPath=" + idShortPath;
		if (!"Property".equals(elementType)) uri += "&elementType=" + elementType;
		if (valuePath != null && !valuePath.isBlank()) uri += "&valuePath=" + valuePath;
		return uri;
	}

	@Override public boolean appliesValuePath() { return true; }
}
