/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.aas.event;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import dk.aau.smartproductionlab.dmp.aas.configuration.AasEventSource;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.sun.net.httpserver.HttpServer;

/**
 * Event-sourced collection and list sources (SPEC §4.3): an event on the element itself, an ancestor or the
 * submodel carries the value (an empty one too); an event on a member, created, updated or removed, carries at most
 * the member, so the element is read again.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AasCollectionEventTest extends CamelTestSupport {
	private static final String SUBMODEL = "https://example.org/aas/a/submodels/Variables";
	/** The collection's current state in the repository, as its read answers it. */
	private static final String STATUS = "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Status\",\"value\":["
			+ "{\"modelType\":\"Property\",\"idShort\":\"Mode\",\"valueType\":\"xs:string\",\"value\":\"AUTO\"},"
			+ "{\"modelType\":\"Property\",\"idShort\":\"Speed\",\"valueType\":\"xs:double\",\"value\":\"2.5\"}]}";
	private final AtomicInteger reads = new AtomicInteger();
	private final HttpServer repository = start();

	private HttpServer start() {
		try {
			HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
			server.createContext("/", exchange -> {
				reads.incrementAndGet();
				byte[] body = STATUS.getBytes(StandardCharsets.UTF_8);
				exchange.getResponseHeaders().set("Content-Type", "application/json");
				exchange.sendResponseHeaders(200, body.length);
				exchange.getResponseBody().write(body);
				exchange.close();
			});
			server.start();
			return server;
		} catch (java.io.IOException failure) {
			throw new IllegalStateException(failure);
		}
	}

	@AfterAll
	void stopRepository() { repository.stop(0); }

	@Override
	protected RouteBuilder createRouteBuilder() {
		String read = "http://127.0.0.1:" + repository.getAddress().getPort() + "/submodel-elements/Status";
		Source status = new Source("status", "Status",
				new AasEventSource(SUBMODEL, "Status", "SubmodelElementCollection", read), Source.Delivery.OBSERVED, 0);
		Source items = new Source("items", "Items",
				new AasEventSource(SUBMODEL, "Items", "SubmodelElementList", read), Source.Delivery.OBSERVED, 0);
		return new RouteBuilder() {
			@Override public void configure() {
				for (var source : List.of(status, items))
					from(source.endpoint().uri(null)).process(source.endpoint().normalizer(source))
							.filter(exchange -> Boolean.TRUE.equals(exchange.getProperty(Source.CONTRIBUTION_PROPERTY)))
							.to("mock:" + source.id());
			}
		};
	}

	private AasEventHub hub() { return AasEventComponent.hub(context); }

	private static String elementEvent(String path, String element) {
		return "{\"type\":\"SME_UPDATED\",\"id\":\"" + SUBMODEL + "\",\"smElementPath\":\"" + path + "\",\"smElement\":" + element + "}";
	}

	@Test
	void aCollectionTakesItsValueFromItsOwnEventAndReadsItAgainAfterAMemberChanged() throws Exception {
		MockEndpoint values = getMockEndpoint("mock:status");
		values.reset();
		values.expectedMessageCount(2);
		int before = reads.get();
		hub().publish(elementEvent("Status", STATUS.replace("AUTO", "MANUAL")), Map.of());
		hub().publish(elementEvent("Status.Mode",
				"{\"modelType\":\"Property\",\"idShort\":\"Mode\",\"valueType\":\"xs:string\",\"value\":\"AUTO\"}"), Map.of());
		values.assertIsSatisfied(5000);
		assertEquals(Map.of("Mode", "MANUAL", "Speed", 2.5), values.getExchanges().get(0).getMessage().getBody(Map.class),
				"the collection's own event carries its value");
		assertEquals(Map.of("Mode", "AUTO", "Speed", 2.5), values.getExchanges().get(1).getMessage().getBody(Map.class),
				"a member's event has the collection read again");
		assertEquals(before + 1, reads.get(), "only the member's event reads");
	}

	@Test
	void aListFollowsAnEventOnOneOfItsItems() throws Exception {
		MockEndpoint values = getMockEndpoint("mock:items");
		values.reset();
		values.expectedMessageCount(1);
		hub().publish(elementEvent("Items[0]",
				"{\"modelType\":\"Property\",\"valueType\":\"xs:int\",\"value\":\"3\"}"), Map.of());
		values.assertIsSatisfied(5000);
	}

	@Test
	void aRemovedMemberHasTheCollectionReadAgainAndTheCollectionsOwnRemovalDoesNot() throws Exception {
		MockEndpoint values = getMockEndpoint("mock:status");
		values.reset();
		values.expectedMessageCount(1);
		int before = reads.get();
		hub().publish(deleted("Status.Speed"), Map.of());
		hub().publish(deleted("Status"), Map.of());
		values.assertIsSatisfied(5000);
		assertEquals(Map.of("Mode", "AUTO", "Speed", 2.5), values.getExchanges().get(0).getMessage().getBody(Map.class));
		assertEquals(before + 1, reads.get(), "only the member's removal reads");
	}

	@Test
	void anEmptyCollectionOrListInASubmodelEventIsAnEmptyValue() throws Exception {
		MockEndpoint status = getMockEndpoint("mock:status");
		MockEndpoint items = getMockEndpoint("mock:items");
		status.reset();
		items.reset();
		status.expectedMessageCount(1);
		items.expectedMessageCount(1);
		int before = reads.get();
		// aas4j leaves out an empty value.
		hub().publish("{\"type\":\"SM_UPDATED\",\"id\":\"" + SUBMODEL + "\",\"submodel\":{\"id\":\"" + SUBMODEL
				+ "\",\"submodelElements\":[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Status\"},"
				+ "{\"modelType\":\"SubmodelElementList\",\"idShort\":\"Items\"}]}}", Map.of());
		status.assertIsSatisfied(5000);
		items.assertIsSatisfied(5000);
		assertEquals(Map.of(), status.getExchanges().get(0).getMessage().getBody(Map.class));
		assertEquals(List.of(), items.getExchanges().get(0).getMessage().getBody(List.class));
		assertEquals(before, reads.get(), "the submodel carries the empty value; nothing is read");
	}

	@Test
	void aBulkPatchCarriesTheValueOfTheElementsItPatchesAndConcernsNoOther() throws Exception {
		MockEndpoint status = getMockEndpoint("mock:status");
		MockEndpoint items = getMockEndpoint("mock:items");
		status.reset();
		items.reset();
		status.expectedMessageCount(1);
		items.expectedMessageCount(0);
		int before = reads.get();
		// PATCH /submodels/{id}/$value, as BaSyx's MQTT feature reports it: the patched top-level elements.
		hub().publish("{\"type\":\"SME_PATCHED\",\"id\":\"" + SUBMODEL + "\",\"smElements\":["
				+ STATUS.replace("AUTO", "MANUAL") + "]}", Map.of());
		status.assertIsSatisfied(5000);
		items.assertIsSatisfied(500);
		assertEquals(Map.of("Mode", "MANUAL", "Speed", 2.5), status.getExchanges().get(0).getMessage().getBody(Map.class));
		assertEquals(before, reads.get(), "the patch carries the value; nothing is read");
	}

	@Test
	void aCollectionWhoseValueBaSyxWithheldIsReadNotEmpty() throws Exception {
		MockEndpoint values = getMockEndpoint("mock:status");
		values.reset();
		values.expectedMessageCount(1);
		int before = reads.get();
		hub().publish(elementEvent("Status", "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Status\","
				+ "\"qualifiers\":[{\"type\":\"emptyValueUpdateEvent\",\"valueType\":\"xs:boolean\",\"value\":\"true\"}]}"), Map.of());
		values.assertIsSatisfied(5000);
		assertEquals(Map.of("Mode", "AUTO", "Speed", 2.5), values.getExchanges().get(0).getMessage().getBody(Map.class));
		assertEquals(before + 1, reads.get());
	}

	private static String deleted(String path) {
		return "{\"type\":\"SME_DELETED\",\"id\":\"" + SUBMODEL + "\",\"smElementPath\":\"" + path + "\"}";
	}

	@Test
	void anEventBesideTheCollectionIsNotItsChange() throws Exception {
		MockEndpoint values = getMockEndpoint("mock:status");
		values.reset();
		values.expectedMessageCount(0);
		hub().publish(elementEvent("StatusHistory",
				"{\"modelType\":\"Property\",\"idShort\":\"StatusHistory\",\"valueType\":\"xs:string\",\"value\":\"x\"}"), Map.of());
		hub().publish(elementEvent("Other.Mode",
				"{\"modelType\":\"Property\",\"idShort\":\"Mode\",\"valueType\":\"xs:string\",\"value\":\"x\"}"), Map.of());
		values.assertIsSatisfied(500);
	}
}
