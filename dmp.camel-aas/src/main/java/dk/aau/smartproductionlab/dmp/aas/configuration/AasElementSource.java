/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.aas.configuration;

import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;

/**
 * An AAS element read from the repository: polled, or read once per evaluation.
 *
 * @param submodelEndpoint the submodel's repository URL
 * @param idShortPath      the element's idShortPath ({@code Location.x}, {@code Items[0]})
 * @param elementType      its model type, which decides how its value is decoded
 */
public record AasElementSource(String submodelEndpoint, String idShortPath, String elementType) implements Endpoint {
	@Override
	public String uri(BundleResources resources) {
		StringBuilder uri = new StringBuilder("aas:").append(submodelEndpoint).append("?propertyPath=").append(idShortPath);
		if (elementType != null && !elementType.isBlank()) uri.append("&elementType=").append(elementType);
		return uri.toString();
	}
}
