/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.aas.configuration;

import org.apache.camel.Processor;
import dk.aau.smartproductionlab.dmp.aas.event.AasEventComponent;
import dk.aau.smartproductionlab.dmp.aas.event.AasElementEventProcessor;
import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.Source;

/**
 * An AAS element followed through the repository's change events instead of polling. Each matching element
 * event contributes the element's new value; an event without one is enriched by reading {@code readUrl}.
 *
 * @param modelType the element's model type: Property, SubmodelElementCollection or SubmodelElementList
 */
public record AasEventSource(String submodelId, String idShortPath, String modelType, String readUrl) implements Endpoint {

	@Override
	public String uri(BundleResources resources) {
		return AasEventComponent.uri(submodelId, idShortPath);
	}

	@Override
	public Processor normalizer(Source source) {
		return new AasElementEventProcessor(this);
	}
}
