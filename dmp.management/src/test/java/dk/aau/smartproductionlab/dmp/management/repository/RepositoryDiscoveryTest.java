/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import dk.aau.smartproductionlab.dmp.management.compiler.ConnectorConfigurationFactory;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

class RepositoryDiscoveryTest {
	private static final String AIMC_SEMANTIC = "https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel";
	private static final String OWNER = "urn:fixture:asset";
	private static final String AIMC_ID = OWNER + "/submodels/AIMC";
	private static final String AID_ID = OWNER + "/submodels/AssetInterfacesDescription";
	private static final String DATA_ID = OWNER + "/submodels/OperationalData";

	@Test
	void bootstrapListsAimcsBySemanticIdAndReadsOnlyWhatTheirEndpointsReference() throws Exception {
		JsonObject fixture = caseFixture("stoppering");
		try (var repository = new FakeAasRepository()) {
			repository.put(resource(fixture.get("aimc").getAsString())).put(resource(fixture.get("aid").getAsString()));
			for (var dependency : fixture.getAsJsonArray("dependencies"))
				repository.put(resource("stoppering/" + dependency.getAsJsonObject().get("path").getAsString()));
			repository.put(submodel("urn:unrelated", "urn:semantic:nameplate", "[]"));
			String aimcId = JsonParser.parseString(resource(fixture.get("aimc").getAsString())).getAsJsonObject().get("id").getAsString();
			JsonObject expected = JsonParser.parseString(resource("stoppering/expected.json")).getAsJsonObject();
			// The station's history and skill logs name the workspace's InfluxDB.
			JsonObject context = expected.getAsJsonObject("compilerContext");
			var discovery = new RepositoryDiscovery(repository.client(), new ConnectorConfigurationFactory.RuntimeSettings(repository.uri(),
					new dk.aau.smartproductionlab.dmp.influxdb2.InfluxSettings(java.net.URI.create(context.get("influx_url").getAsString()),
							context.get("influx_org").getAsString(), "fixture-token")));
			var scan = discovery.scan();
			assertTrue(scan.complete());
			assertEquals(Set.of(aimcId), scan.aimcIds());
			assertTrue(scan.failures().isEmpty(), scan.failures().toString());
			assertEquals(expected.getAsJsonArray("authoredMappings").size(), scan.mappings().size());
			assertTrue(scan.mappings().values().stream().allMatch(candidate -> candidate.desired().valid()),
					"the station's mappings compile");
			String aidId = JsonParser.parseString(resource(fixture.get("aid").getAsString())).getAsJsonObject().get("id").getAsString();
			assertFalse(scan.reverseDependencies().get(aidId).isEmpty());
			assertFalse(scan.reverseDependencies().get(aimcId).isEmpty());

			var requests = repository.requests();
			assertEquals("/repo/submodels?semanticId=" + AasRepositoryClient.base64UrlIdentifier(AIMC_SEMANTIC) + "&limit=50",
					requests.get(0), "one filtered listing returns the AIMCs whole");
			assertTrue(requests.stream().noneMatch(request -> request.startsWith("/repo/shells")), "owners are never read");
			assertTrue(requests.stream().noneMatch(request -> request.contains(AasRepositoryClient.base64UrlIdentifier("urn:unrelated"))));
			assertTrue(requests.stream().noneMatch(request -> request.equals(path(aimcId))), "the listed AIMC is not read again");
			assertEquals(1, requests.stream().filter(request -> request.equals(path(aidId))).count(), "the AID is read whole once");
			assertEquals(1, discovery.fullScanCount());
		}
	}

	@Test
	void incompleteListingIsNotReportedAsAnEmptySuccessfulScan() throws Exception {
		try (var repository = new FakeAasRepository()) {
			repository.override("down", request -> new FakeAasRepository.Response(503, "{}"));
			var scan = discovery(repository).scan();
			assertFalse(scan.complete());
			assertTrue(scan.aimcIds().isEmpty());
			assertEquals("repository-transient_failure", scan.failureCategory());
		}
	}

	@Test
	void aRepositoryIgnoringTheSemanticFilterStillYieldsOnlyAimcs() throws Exception {
		try (var repository = fixture()) {
			repository.ignoreSemanticFilter();
			repository.put("{\"modelType\":\"Submodel\",\"id\":\"urn:broken\",\"submodelElements\":[{\"modelType\":\"NoSuchElement\"}]}");
			var scan = discovery(repository).scan();
			assertEquals(Set.of(AIMC_ID), scan.aimcIds());
			assertTrue(scan.failures().isEmpty(), "an unreadable non-AIMC is not an AIMC failure");
			assertEquals(1, scan.mappings().size());
		}
	}

	@Test
	void oneUnreachableDependencyBlocksOnlyItsMappingAndIsRetriedOnTheNextRefresh() throws Exception {
		try (var repository = fixture()) {
			String second = aimc("urn:second/submodels/AIMC", refProperty("urn:busy", "Value"), refProperty(DATA_ID, "Speed"));
			repository.put(second).put(submodel("urn:busy", "urn:semantic:data",
					"[{\"modelType\":\"Property\",\"idShort\":\"Value\",\"valueType\":\"xs:string\",\"value\":\"x\"}]"));
			String busy = path("urn:busy");
			repository.override("busy", request -> request.startsWith(busy) ? new FakeAasRepository.Response(503, "{}") : null);
			var discovery = discovery(repository);
			var scan = discovery.scan();
			var blocked = scan.mappings().values().stream().filter(candidate -> candidate.key().aimcId().equals("urn:second/submodels/AIMC")).findFirst().orElseThrow();
			assertEquals(AasRepositoryClient.Status.TRANSIENT_FAILURE, blocked.desired().desired().failures().get(0).status());
			var healthy = scan.mappings().values().stream().filter(candidate -> candidate.key().aimcId().equals(AIMC_ID)).findFirst().orElseThrow();
			assertTrue(healthy.desired().desired().valid(), "the other AIMC resolves; its connector issues are its own");
			repository.clearOverride("busy");
			var retried = discovery.refresh(Set.of("urn:busy"));
			assertTrue(retried.mappings().values().stream().filter(candidate -> candidate.key().aimcId().equals("urn:second/submodels/AIMC"))
					.allMatch(candidate -> candidate.desired().desired().failures().isEmpty()));
		}
	}

	@Test
	void aDependencyThatFailedTransientlyIsReadAgainByARetryThatNamesNothing() throws Exception {
		// The retry pass after a transient failure refreshes no changed ids: the failed reads must be retried anyway.
		try (var repository = fixture()) {
			String data = path(DATA_ID);
			repository.override("busy", request -> request.startsWith(data) ? new FakeAasRepository.Response(503, "{}") : null);
			var discovery = discovery(repository);
			var blocked = discovery.scan().mappings().values().iterator().next();
			assertEquals(AasRepositoryClient.Status.TRANSIENT_FAILURE, blocked.desired().desired().failures().get(0).status());
			repository.clearOverride("busy");
			var retried = discovery.refresh(Set.of()).mappings().values().iterator().next();
			assertTrue(retried.desired().desired().failures().isEmpty(), retried.desired().desired().failures().toString());
		}
	}

	@Test
	void aNewSubmodelThatCouldNotBeReadIsKeptAsAFailedAimcAndReadAgain() throws Exception {
		try (var repository = fixture()) {
			var discovery = discovery(repository);
			discovery.scan();
			String newId = "urn:new/submodels/AIMC";
			repository.put(aimc(newId, ref(AID_ID, "interface_http", "InteractionMetadata", "properties", "Temperature"), refProperty(DATA_ID, "Speed")));
			repository.override("new", request -> request.startsWith(path(newId)) ? new FakeAasRepository.Response(503, "{}") : null);
			var unread = discovery.refresh(Set.of(newId));
			assertTrue(unread.failures().stream().anyMatch(failure -> failure.aimcId().equals(newId)
					&& failure.category().equals("aimc-fetch-failed")), "the pass reports it, so it is retried");
			repository.clearOverride("new");
			var retried = discovery.refresh(Set.of());
			assertTrue(retried.mappings().keySet().stream().anyMatch(key -> key.aimcId().equals(newId)), "the retry adopts the new AIMC");
		}
	}

	@Test
	void valueUpdatesOfAReferencedCollectionAreNotChangesButItsStructureIs() throws Exception {
		try (var repository = fixture()) {
			// A second mapping writes the whole Status collection.
			repository.put(aimc("urn:collection/submodels/AIMC", ref(AID_ID, "interface_http", "InteractionMetadata", "properties", "Temperature"),
					"{\"type\":\"ModelReference\",\"keys\":[" + key("Submodel", DATA_ID) + "," + key("SubmodelElementCollection", "Status") + "]}"));
			var discovery = discovery(repository);
			discovery.scan();
			String element = "{\"type\":\"SME_UPDATED\",\"id\":\"" + DATA_ID + "\",\"smElementPath\":\"Status\",\"smElement\":%s}";
			String status = "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Status\",\"value\":["
					+ "{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:int\",\"value\":\"%s\"}%s]}";
			assertFalse(relevant(discovery, element.formatted(status.formatted("9", ""))),
					"a write of the collection's values is data: its mapping is not compiled again");
			assertTrue(relevant(discovery, element.formatted(status.formatted("1",
					",{\"modelType\":\"Property\",\"idShort\":\"Mode\",\"valueType\":\"xs:string\"}"))), "a new member is structure");
			String others = ",{\"modelType\":\"Property\",\"idShort\":\"Speed\",\"valueType\":\"xs:double\",\"value\":\"%s\"},"
					+ "{\"modelType\":\"Property\",\"idShort\":\"Unrelated\",\"valueType\":\"xs:int\",\"value\":\"3\"}";
			String updated = "{\"type\":\"SM_UPDATED\",\"id\":\"" + DATA_ID + "\",\"submodel\":%s}";
			assertFalse(relevant(discovery, updated.formatted(submodel(DATA_ID, "urn:semantic:data",
					"[" + status.formatted("4", "") + others.formatted("8") + "]"))), "a batch update of values changes nothing");
			assertTrue(relevant(discovery, updated.formatted(submodel(DATA_ID, "urn:semantic:data",
					"[" + status.formatted("4", "").replace("xs:int", "xs:long") + others.formatted("8") + "]"))),
					"a referenced valueType changed with the batch");
		}
	}

	@Test
	void anEventDuringTheInitialScanIsJudgedAgainstWhatTheScanFound() throws Exception {
		// The AID is created while the scan reads it: the scan finds none, and its creation event arrives while the
		// desired state is still empty. Judged then, it concerns nothing; judged once the scan is done, it does.
		var component = new dk.aau.smartproductionlab.dmp.core.component.DmpComponent();
		component.startComponent();
		try (var repository = fixture()) {
			var holder = new java.util.concurrent.atomic.AtomicReference<MappingReconciliationService>();
			var late = new java.util.concurrent.atomic.AtomicBoolean();
			repository.override("late-aid", request -> {
				if (!request.startsWith(path(AID_ID)) || !late.compareAndSet(false, true)) return null;
				holder.get().acceptEvent(AasControlEvent.parse("{\"type\":\"SM_CREATED\",\"id\":\"" + AID_ID + "\",\"submodel\":" + aid() + "}"));
				return new FakeAasRepository.Response(404, "{}");
			});
			var discovery = discovery(repository);
			try (var service = new MappingReconciliationService(component, discovery,
					new ConnectorConfigurationFactory.RuntimeSettings(repository.uri()), 16, 8, 60_000)) {
				holder.set(service);
				service.reconcileNow();
				assertTrue(late.get(), "the event arrived during the scan");
				assertEquals(1, service.acceptedInvalidationCount(), "the AID's creation concerns the blocked mapping");
				assertTrue(service.repairPending(), "so a pass reads it");
			}
		} finally {
			component.stopComponent();
		}
	}

	@Test
	void refreshReReadsOnlyTheChangedSubmodelAndTheAimcsDependingOnIt() throws Exception {
		try (var repository = fixture()) {
			var discovery = discovery(repository);
			var first = discovery.scan();
			assertEquals(1, first.mappings().size());
			repository.clearRequests();
			var refreshed = discovery.refresh(Set.of(DATA_ID));
			assertEquals(first.mappings().keySet(), refreshed.mappings().keySet());
			assertTrue(repository.requests().stream().allMatch(request -> request.startsWith(path(DATA_ID))), repository.requests().toString());
			assertFalse(repository.requests().isEmpty(), "the changed dependency is read again");

			repository.clearRequests();
			discovery.refresh(Set.of(AIMC_ID));
			assertEquals(List.of(path(AIMC_ID)), repository.requests(), "a changed AIMC is read again; unchanged dependencies stay cached");
			assertEquals(1, discovery.fullScanCount());
			assertEquals(2, discovery.targetedScanCount());

			repository.remove(AIMC_ID);
			var removed = discovery.refresh(Set.of(AIMC_ID));
			assertTrue(removed.mappings().isEmpty(), "a deleted AIMC drops its mappings");
			assertFalse(removed.aimcIds().contains(AIMC_ID));
		}
	}

	@Test
	void refreshAdoptsANewAimcAndIgnoresNewSubmodelsThatAreNot() throws Exception {
		try (var repository = fixture()) {
			var discovery = discovery(repository);
			discovery.scan();
			String newId = "urn:new/submodels/AIMC";
			repository.put(aimc(newId, ref(AID_ID, "interface_http", "InteractionMetadata", "properties", "Temperature"), refProperty(DATA_ID, "Speed")));
			repository.put(submodel("urn:other", "urn:semantic:nameplate", "[]"));
			var refreshed = discovery.refresh(Set.of(newId, "urn:other"));
			assertEquals(Set.of(AIMC_ID, newId), refreshed.aimcIds());
			assertEquals(2, refreshed.mappings().size());
			assertTrue(discovery.watchedSubmodels().contains(newId));
			assertFalse(discovery.watchedSubmodels().contains("urn:other"));
		}
	}

	@Test
	void onlyEventsThatCanChangeAMappingAreRelevant() throws Exception {
		try (var repository = fixture()) {
			var discovery = discovery(repository);
			discovery.scan();
			String state = "{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:int\",\"value\":\"1\"}";
			assertFalse(relevant(discovery, "{\"type\":\"AAS_UPDATED\",\"id\":\"" + OWNER + "\"}"), "shells no longer matter");
			assertTrue(relevant(discovery, "{\"type\":\"SME_UPDATED\",\"id\":\"" + AIMC_ID + "\",\"smElementPath\":\"x\"}"));
			assertFalse(relevant(discovery, "not json"), "an unreadable event is ignored");

			String element = "{\"type\":\"SME_UPDATED\",\"id\":\"" + DATA_ID + "\",\"smElementPath\":\"%s\",\"smElement\":%s}";
			assertFalse(relevant(discovery, element.formatted("Status.State", state.replace("\"1\"", "\"2\""))),
					"a new value of a referenced Property is data, not configuration");
			assertTrue(relevant(discovery, element.formatted("Status.State", state.replace("xs:int", "xs:string"))), "a valueType change is structural");
			assertTrue(relevant(discovery, element.formatted("Status", "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Status\",\"value\":[]}")),
					"replacing an ancestor can change the referenced element");
			assertFalse(relevant(discovery, element.formatted("Unrelated", state)), "elements nothing references are irrelevant");
			assertTrue(relevant(discovery, "{\"type\":\"SME_DELETED\",\"id\":\"" + DATA_ID + "\",\"smElementPath\":\"Status.State\"}"));
			assertTrue(relevant(discovery, "{\"type\":\"SM_UPDATED\",\"id\":\"" + DATA_ID + "\"}"));
			assertTrue(relevant(discovery, "{\"type\":\"SME_UPDATED\",\"id\":\"" + AID_ID + "\",\"smElementPath\":\"interface_http.EndpointMetadata.base\"}"),
					"an AID is consumed whole");

			assertFalse(relevant(discovery, "{\"type\":\"SME_UPDATED\",\"id\":\"urn:unrelated\",\"smElementPath\":\"x\"}"));
			assertFalse(relevant(discovery, "{\"type\":\"SM_DELETED\",\"id\":\"urn:unrelated\"}"));
			assertFalse(relevant(discovery, "{\"type\":\"SM_CREATED\",\"id\":\"urn:unrelated\",\"submodel\":"
					+ submodel("urn:unrelated", "urn:semantic:nameplate", "[]") + "}"), "a new submodel that is not an AIMC");
			assertTrue(relevant(discovery, "{\"type\":\"SM_CREATED\",\"id\":\"urn:new\",\"submodel\":"
					+ submodel("urn:new", AIMC_SEMANTIC, "[]") + "}"), "a new AIMC");
			assertTrue(relevant(discovery, "{\"type\":\"SM_CREATED\",\"id\":\"urn:new\"}"), "without a payload it must be checked");
		}
	}

	@Test
	void anAimcScopeReadsExactIdsWithoutListing() throws Exception {
		try (var repository = fixture()) {
			repository.put(aimc("urn:other/submodels/AIMC", refProperty(DATA_ID, "Speed"), refProperty(DATA_ID, "Speed")));
			var discovery = new RepositoryDiscovery(repository.client(),
					new ConnectorConfigurationFactory.RuntimeSettings(repository.uri()), 2, Set.of(AIMC_ID));
			var scan = discovery.scan();
			assertEquals(Set.of(AIMC_ID), scan.aimcIds());
			assertTrue(repository.requests().stream().noneMatch(request -> request.startsWith("/repo/submodels?")));
			assertFalse(relevant(discovery, "{\"type\":\"SM_CREATED\",\"id\":\"urn:outside\",\"submodel\":"
					+ submodel("urn:outside", AIMC_SEMANTIC, "[]") + "}"), "AIMCs outside the scope are ignored");
		}
	}

	private static boolean relevant(RepositoryDiscovery discovery, String event) {
		return discovery.relevant(AasControlEvent.parse(event));
	}

	private static RepositoryDiscovery discovery(FakeAasRepository repository) {
		return new RepositoryDiscovery(repository.client(), new ConnectorConfigurationFactory.RuntimeSettings(repository.uri()));
	}

	/** One AIMC mapping an AID property to {@code OperationalData/Status.State}. */
	private static FakeAasRepository fixture() throws Exception {
		return new FakeAasRepository()
				.put(aimc(AIMC_ID, ref(AID_ID, "interface_http", "InteractionMetadata", "properties", "Temperature"),
						"{\"type\":\"ModelReference\",\"keys\":[" + key("Submodel", DATA_ID) + "," + key("SubmodelElementCollection", "Status")
						+ "," + key("Property", "State") + "]}"))
				.put(aid())
				.put(submodel(DATA_ID, "urn:semantic:data", "[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Status\",\"value\":["
						+ "{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:int\",\"value\":\"1\"}]},"
						+ "{\"modelType\":\"Property\",\"idShort\":\"Speed\",\"valueType\":\"xs:double\",\"value\":\"2\"},"
						+ "{\"modelType\":\"Property\",\"idShort\":\"Unrelated\",\"valueType\":\"xs:int\",\"value\":\"1\"}]"));
	}

	private static String path(String id) {
		return "/repo/submodels/" + AasRepositoryClient.base64UrlIdentifier(id);
	}

	private static String submodel(String id, String semantic, String elements) {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + id + "\",\"idShort\":\"x\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\""
				+ semantic + "\"}]},\"submodelElements\":" + elements + "}";
	}

	private static String aimc(String id, String source, String sink) {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + id + "\",\"idShort\":\"AIMC\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"" + AIMC_SEMANTIC + "\"}]},"
				+ "\"submodelElements\":[{\"modelType\":\"SubmodelElementList\",\"idShort\":\"MappingConfigurations\",\"typeValueListElement\":\"SubmodelElementCollection\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/1/0/MappingConfigurations\"}]},"
				+ "\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"value\":["
				+ endpoint("Sources", "Source", "SourceId", "temperature", source) + ","
				+ endpoint("Sinks", "Sink", "SinkId", "Value", sink)
				+ "]}]}]}";
	}

	private static String endpoint(String listName, String referenceName, String idName, String id, String reference) {
		return "{\"modelType\":\"SubmodelElementList\",\"idShort\":\"" + listName
				+ "\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"value\":["
				+ "{\"modelType\":\"ReferenceElement\",\"idShort\":\"" + referenceName + "\",\"value\":" + reference + "},"
				+ "{\"modelType\":\"Property\",\"idShort\":\"" + idName + "\",\"valueType\":\"xs:string\",\"value\":\"" + id + "\"}]}]}";
	}

	private static String ref(String submodel, String... path) {
		List<String> keys = new ArrayList<>();
		keys.add(key("Submodel", submodel));
		for (String key : path) keys.add(key("SubmodelElementCollection", key));
		return "{\"type\":\"ModelReference\",\"keys\":[" + String.join(",", keys) + "]}";
	}

	private static String refProperty(String submodel, String property) {
		return "{\"type\":\"ModelReference\",\"keys\":[" + key("Submodel", submodel) + "," + key("Property", property) + "]}";
	}

	private static String key(String type, String value) {
		return "{\"type\":\"" + type + "\",\"value\":\"" + value + "\"}";
	}

	private static String aid() {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + AID_ID + "\",\"idShort\":\"AssetInterfacesDescription\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/AssetInterfacesDescription/1/1/Submodel\"}]},"
				+ "\"submodelElements\":[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"interface_http\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/AssetInterfacesDescription/1/0/Interface\"}]},"
				+ "\"supplementalSemanticIds\":[{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"http://www.w3.org/2011/http\"}]}],"
				+ "\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"InteractionMetadata\",\"value\":["
				+ "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"properties\",\"value\":["
				+ "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Temperature\",\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://www.w3.org/2019/wot/td#PropertyAffordance\"}]},\"value\":[]}"
				+ "]}]}]}]}";
	}

	private static JsonObject caseFixture(String name) throws Exception {
		for (var raw : JsonParser.parseString(resource("manifest.json")).getAsJsonObject().getAsJsonArray("cases")) {
			JsonObject fixture = raw.getAsJsonObject();
			if (name.equals(fixture.get("name").getAsString())) return fixture;
		}
		throw new IllegalArgumentException("fixture not found: " + name);
	}

	private static String resource(String path) throws Exception {
		try (var input = RepositoryDiscoveryTest.class.getResourceAsStream("/runtime-model-contracts/" + path)) {
			assertNotNull(input, "required fixture " + path);
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
