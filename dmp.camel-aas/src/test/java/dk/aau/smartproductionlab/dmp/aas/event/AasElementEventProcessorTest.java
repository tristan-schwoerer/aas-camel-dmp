/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas.event;


import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import dk.aau.smartproductionlab.dmp.aas.configuration.AasEventSource;
import org.junit.jupiter.api.Test;

class AasElementEventProcessorTest extends CamelTestSupport {

	private static AasEventSource source(String modelType, String elementPath) {
		return new AasEventSource("urn:test:sm", elementPath, modelType, null);
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		AasEventSource source = source("Property", "Runtime.State");
		return new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:event-source")
						.process(new AasElementEventProcessor(source))
						.filter(exchange -> Boolean.TRUE.equals(
							exchange.getProperty(AasElementEventProcessor.MATCH_PROPERTY)))
						.to("mock:matched");
				from("direct:smc-event-source")
						.process(new AasElementEventProcessor(
								source("SubmodelElementCollection", "Runtime.State")))
						.filter(exchange -> Boolean.TRUE.equals(
							exchange.getProperty(AasElementEventProcessor.MATCH_PROPERTY)))
						.to("mock:smc-matched");
			}
		};
	}

	@Test
	void matchingPropertyEventContributesItsValue() throws Exception {
		MockEndpoint matched = getMockEndpoint("mock:matched");
		matched.expectedMessageCount(1);
		matched.expectedBodiesReceived("RUNNING");

		template.sendBody("direct:event-source", """
				{"type":"SME_UPDATED","id":"urn:test:sm",
				 "smElementPath":"Runtime.State",
				 "smElement":{"modelType":"Property","value":"RUNNING"}}
				""");

		matched.assertIsSatisfied();
	}

	@Test
	void unrelatedPropertyEventDoesNotReachMapping() throws Exception {
		MockEndpoint matched = getMockEndpoint("mock:matched");
		matched.expectedMessageCount(0);

		template.sendBody("direct:event-source", """
				{"type":"SME_UPDATED","id":"urn:test:sm",
				 "smElementPath":"Runtime.Other",
				 "smElement":{"modelType":"Property","value":"STOPPED"}}
				""");

		matched.assertIsSatisfied();
	}

	@Test
	void fullSubmodelUpdateIsFilteredToTheTargetProperty() throws Exception {
		MockEndpoint matched = getMockEndpoint("mock:matched");
		matched.expectedMessageCount(1);
		matched.expectedBodiesReceived("READY");

		template.sendBody("direct:event-source", """
				{"type":"SM_UPDATED","id":"urn:test:sm",
				 "submodel":{"submodelElements":[
				   {"idShort":"Runtime","value":[
				     {"idShort":"State","modelType":"Property","value":"READY"}
				   ]}
				 ]}}
				""");

		matched.assertIsSatisfied();
	}

	@Test
	void collectionEventDeliversItsValueKeyedByIdShort() throws Exception {
		MockEndpoint matched = getMockEndpoint("mock:smc-matched");
		matched.expectedMessageCount(1);

		template.sendBody("direct:smc-event-source", """
				{"type":"SME_UPDATED","id":"urn:test:sm",
				 "smElementPath":"Runtime.State",
				 "smElement":{"modelType":"SubmodelElementCollection",
				              "idShort":"State","value":[
				                {"idShort":"Mode","modelType":"Property","valueType":"xs:string","value":"READY"},
				                {"idShort":"Speed","modelType":"Property","valueType":"xs:double","value":"1.5"}]}}
				""");

		matched.assertIsSatisfied();
		assertEquals(Map.of("Mode", "READY", "Speed", 1.5), matched.getExchanges().get(0).getMessage().getBody(Map.class));
	}
}
