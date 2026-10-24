/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.builder.AdviceWith;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import org.junit.jupiter.api.Test;

class AasControlEventTest {
	@Test
	void extractsWrappedFullAndElementInvalidationsAndFailsClosedOnUnknownEvents() {
		var full = AasControlEvent.parse("{\"event\":{\"type\":\"SM_UPDATED\",\"id\":\"urn:sm\"}}");
		assertEquals("SM_UPDATED", full.type());
		assertEquals("urn:sm", full.resourceId());
		assertFalse(full.fullRescan());

		var element = AasControlEvent.parse("{\"type\":\"SME_UPDATED\",\"submodelId\":\"urn:sm\","
				+ "\"smElementPath\":\"Variables.State\",\"smElement\":{\"modelType\":\"Property\",\"value\":\"READY\"}}");
		assertEquals("urn:sm", element.resourceId());
		assertEquals("Variables.State", element.elementPath());
		assertFalse(element.fullRescan());

		assertTrue(AasControlEvent.parse("not-json").ignored(), "an unreadable event is ignored, not a rescan");
		assertTrue(AasControlEvent.parse("{\"type\":\"SM_REF_ADDED\",\"id\":\"urn:aas\"}").ignored(),
				"an event type the DMP does not know cannot concern a mapping");
		assertTrue(AasControlEvent.parse("{\"type\":\"SM_DELETED\"}").fullRescan());
		assertEquals("urn:sm", AasControlEvent.parse("{\"payload\":{\"notification\":{\"type\":\"SME_DELETED\","
				+ "\"submodelId\":\"urn:sm\",\"smElementPath\":\"Variables.State\"}}}").resourceId());
	}

	@Test
	void carriesTheSemanticIdsOfAnEventSubmodelAndTheEventInKafkaShape() {
		var created = AasControlEvent.parse("{\"type\":\"SM_CREATED\",\"id\":\"urn:sm\",\"submodel\":{\"modelType\":\"Submodel\","
				+ "\"id\":\"urn:sm\",\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"urn:aimc\"}]}}}");
		assertEquals(Set.of("urn:aimc"), created.semanticIds());
		assertEquals("SM_CREATED", created.json().get("type").getAsString());
		assertEquals("urn:sm", created.submodel().getId());
		assertEquals(null, AasControlEvent.parse("{\"type\":\"SM_CREATED\",\"id\":\"urn:sm\"}").semanticIds(),
				"no submodel in the event: semantics unknown");
	}

	@Test
	void readsBasyxMqttTopicsAndPayloads() {
		String id = "https://example.org/aas/a/submodels/Variables";
		String encoded = AasRepositoryClient.base64UrlIdentifier(id);
		byte[] property = "{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:int\",\"value\":\"3\"}"
				.getBytes(java.nio.charset.StandardCharsets.UTF_8);
		var updated = AasControlEvent.parseMqtt("sm-repository/sm-repo/submodels/" + encoded + "/submodelElements/Status.State/updated", property);
		assertEquals("SME_UPDATED", updated.type());
		assertEquals(id, updated.resourceId());
		assertEquals("Status.State", updated.elementPath());
		assertEquals("State", updated.element().getIdShort());
		assertTrue(updated.json().has("smElement"));

		var valueWrite = AasControlEvent.parseMqtt("sm-repository/sm-repo/submodels/" + encoded + "/submodelElements/Status.State/value/updated", property);
		assertEquals("SME_UPDATED", valueWrite.type());
		assertEquals("Status.State", valueWrite.elementPath(), "the value topic names the element, not a child called value");
		assertEquals("3", ((org.eclipse.digitaltwin.aas4j.v3.model.Property) valueWrite.element()).getValue(),
				"the payload carries the new value");

		var deleted = AasControlEvent.parseMqtt("sm-repository/sm-repo/submodels/" + encoded + "/submodelElements/Status/deleted", property);
		assertEquals("SME_DELETED", deleted.type());
		assertEquals("Status", deleted.elementPath());

		byte[] submodel = ("{\"modelType\":\"Submodel\",\"id\":\"" + id + "\",\"semanticId\":{\"type\":\"ExternalReference\","
				+ "\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"urn:aimc\"}]}}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
		var created = AasControlEvent.parseMqtt("sm-repository/sm-repo/submodels/created", submodel);
		assertEquals("SM_CREATED", created.type());
		assertEquals(id, created.resourceId());
		assertEquals(Set.of("urn:aimc"), created.semanticIds());

		var patched = AasControlEvent.parseMqtt("sm-repository/sm-repo/submodels/" + encoded + "/submodelElements/patched", "[]".getBytes());
		assertEquals("SME_PATCHED", patched.type());
		assertEquals(null, patched.elementPath());
		var file = AasControlEvent.parseMqtt("sm-repository/sm-repo/submodels/" + encoded + "/submodelElements/Docs.Manual/attachment/updated", property);
		assertEquals("SME_UPDATED", file.type());
		assertEquals("Docs.Manual", file.elementPath());
		assertEquals(null, file.element(), "an attachment event carries no element");

		var unparsed = AasControlEvent.parseMqtt("sm-repository/sm-repo/submodels/" + encoded + "/submodelElements/Busy/updated",
				"not json".getBytes());
		assertFalse(unparsed.fullRescan(), "an element event is identified by its topic; the payload is read only on demand");
		assertEquals(id, unparsed.resourceId());
		assertEquals("Busy", unparsed.elementPath());
		assertEquals(null, unparsed.element(), "a payload that cannot be read has no element");
		assertEquals(null, unparsed.json());
		assertTrue(AasControlEvent.parseMqtt("sm-repository/sm-repo/submodels/created", "not json".getBytes()).fullRescan());
		assertTrue(AasControlEvent.parseMqtt("aas-repository/aas-repo/shells/created", "{}".getBytes()).ignored());
	}

	@Test
	void listenerUsesManualOrderedCommitAndASeparateSingleConsumerGroup() {
		var settings = new KafkaControlEventRouteBuilder.Settings("127.0.0.1:9092", "aas-control",
				"dmp-control");
		var route = new KafkaControlEventRouteBuilder(settings, null);
		String uri = route.endpointUri();
		assertTrue(uri.contains("groupId=dmp-control"));
		assertTrue(uri.contains("consumersCount=1"));
		assertTrue(uri.contains("batching=true"), "one commit per poll, not per record");
		assertTrue(uri.contains("autoCommitEnable=false"));
		assertTrue(uri.contains("allowManualCommit=true"));
		String values = route.valueEndpointUri();
		assertTrue(values.contains("autoOffsetReset=latest"), "the value feed never replays a backlog");
		assertTrue(values.contains("autoCommitEnable=false"));
		assertTrue(values.contains("groupId=dmp-control-values-"), "its own consumer group, never the control group");
		assertFalse(values.contains("groupId=dmp-control&"));
		var both = new KafkaControlEventRouteBuilder.Settings("127.0.0.1:9092", "aas-events, submodel-events",
				"dmp-control");
		assertEquals(List.of("aas-events", "submodel-events"), both.topics());
		assertTrue(new KafkaControlEventRouteBuilder(both, null).endpointUri().startsWith("kafka:aas-events,submodel-events?"));
		assertEquals(List.of("aas-control"), settings.topics(), "the existing single-topic API remains unchanged");
		for (String invalidTopics : List.of("", ",aas-events", "aas-events,", "aas-events,,submodel-events",
				"aas-events,aas-events", "bad/topic", ".", "..", "x".repeat(250), "a,b,c,d,e,f,g,h,i")) {
			assertThrows(IllegalArgumentException.class, () -> new KafkaControlEventRouteBuilder.Settings(
					"127.0.0.1:9092", invalidTopics, "dmp-control"));
		}
		assertThrows(IllegalArgumentException.class, () -> new KafkaControlEventRouteBuilder.Settings(
				"127.0.0.1:9092&groupId=other", "aas-control", "dmp-control"));
	}

	@Test
	void controlRouteInvalidatesBeforeManualOffsetCommit() throws Exception {
		var component = new DmpComponent();
		var service = new MappingReconciliationService(component,
				() -> new RepositoryDiscovery.Scan(true, "", Set.of(), Set.of(), Map.of(), Map.of(), List.of()), 8, 8, 60_000);
		var settings = new KafkaControlEventRouteBuilder.Settings("127.0.0.1:9092", "aas-control",
				"dmp-control");
		var context = new DefaultCamelContext();
		context.addRoutes(new KafkaControlEventRouteBuilder(settings, service));
		AdviceWith.adviceWith(context, "dmp-management-control-events", advice -> advice.replaceFromWith("direct:control"));
		AtomicInteger commits = new AtomicInteger();
		try {
			context.start();
			var template = context.createProducerTemplate();
			KafkaManualCommit commit = commits::incrementAndGet;
			// One poll is one batch: every record is accepted, then each partition is committed once.
			List<org.apache.camel.Exchange> batch = new java.util.ArrayList<>();
			for (String id : List.of("urn:sm", "urn:other")) {
				var record = new org.apache.camel.support.DefaultExchange(context);
				record.getMessage().setBody("{\"type\":\"SM_UPDATED\",\"id\":\"" + id + "\"}");
				record.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);
				record.getMessage().setHeader(KafkaConstants.TOPIC, "aas-control");
				record.getMessage().setHeader(KafkaConstants.PARTITION, 0);
				batch.add(record);
			}
			template.sendBody("direct:control", batch);
			assertEquals(2, service.acceptedInvalidationCount());
			assertEquals(1, commits.get(), "one commit for the partition, after both records");
		} finally {
			service.close();
			context.stop();
		}
	}
}
