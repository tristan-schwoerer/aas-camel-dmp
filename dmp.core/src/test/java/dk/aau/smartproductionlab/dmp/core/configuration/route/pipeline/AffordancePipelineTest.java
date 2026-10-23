package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.test.junit5.CamelTestSupport;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.Evaluation;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataTransformerConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreatorHelper;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AffordancePipelineTest extends CamelTestSupport {
	private static final AtomicReference<String> OBSERVED_BRANCH_STATE = new AtomicReference<>();
	private FanIn triggerFanIn;
	private static final Map<String, Object> NESTED = Map.of("type", "object",
			"required", List.of("Part", "Count"), "properties", Map.of(
				"Part", Map.of("type", "object", "required", List.of("Uuid", "Mode"),
						"properties", Map.of("Uuid", Map.of("type", "string"),
								"Mode", Map.of("type", "string"))),
				"Count", Map.of("type", "integer")));

	static final class Sink extends DataSinkConfiguration {
		private final String uri;
		Sink(String id, String key, String path, String group, Object memberSchema, Object payloadSchema,
				String uri) {
			super(id);
			setSinkId(key);
			setValuePath(path);
			setPayloadGroup(group);
			setSchema(memberSchema);
			setPayloadSchema(payloadSchema);
			this.uri = uri;
		}
		@Override public String getConnectionURI() { return uri; }
	}

	static final class Source extends DataSourceConfiguration {
		Source() { super("source", "", 0); setSourceId("Reading"); setValuePath("$.Reading");
			setSchema(Map.of("type", "integer")); }
		@Override public String getConnectionURI() { return "direct:source"; }
	}

	static final class TriggerSource extends DataSourceConfiguration {
		TriggerSource(String endpointId, String sourceId, Evaluation evaluation) {
			super(endpointId, "", 0);
			setSourceId(sourceId);
			setEvaluation(evaluation);
		}
		@Override public String getConnectionURI() { return "direct:" + getUniqueId(); }
	}

	private static Sink sink(String id, String key, String path, String group, Object schema) {
		return new Sink(id, key, path, group, schema, NESTED, "mock:device");
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override public void configure() {
				RoutesConfiguration configs = new RoutesConfiguration(List.of(), List.of(), List.of(
						sink("uuid", "Uuid", "$.Part.Uuid", "device/action",
								Map.of("type", "string")),
						sink("mode", "Mode", "$.Part.Mode", "device/action",
								Map.of("type", "string")),
						sink("count", "Count", "$.Count", "device/action",
								Map.of("type", "integer")),
						new Sink("other", "Other", null, null, null, null, "mock:other"),
						sink("solo", "Uuid", "$.Part.Uuid", "device/solo",
								Map.of("type", "string")),
						new Sink("whole", "Whole", null, "device/action", NESTED, NESTED,
								"mock:whole"),
						new Sink("wholeGroup", "Whole", null, "device/action", NESTED, NESTED,
								"mock:device"),
						new Sink("arrayWhole", "WholeArray", null, "device/array", Map.of(), Map.of(),
								"mock:array"),
						new Sink("arrayMember", "First", "$.values[0]", "device/array",
								Map.of(), Map.of(), "mock:array"),
						new Sink("arraySecond", "Last", "$.values[2]", "device/array",
								Map.of(), Map.of(), "mock:array"),
						new Sink("unrelatedGroup", "Other", "$.value", "device/unrelated",
								Map.of(), Map.of(), "mock:unrelatedGroup"),
						new Sink("numericString", "Text", null, null, Map.of("type", "string"),
								null, "mock:numeric"),
						new Sink("mutator", "First", null, null, null, null, "direct:mutatingSink"),
						new Sink("observer", "Second", null, null, null, null, "direct:observingSink"),
						new Sink("order1", "One", null, null, null, null, "mock:ordered"),
						new Sink("order2", "Two", null, null, null, null, "mock:ordered"),
						new Sink("order3", "Three", null, null, null, null, "mock:ordered")), List.of());
				RouteConfiguration complete = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("uuid", "other", "mode", "count"));
				FanOut.apply(from("direct:complete"), configs, complete, new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, complete.getDatasinks(), "complete"),
						"complete", AffordanceValidation.Mode.ON);

				RouteConfiguration incomplete = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("solo"));
				FanOut.apply(from("direct:incomplete"), configs, incomplete, new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, incomplete.getDatasinks(), "incomplete"),
						"incomplete", AffordanceValidation.Mode.ON);
				FanOut.apply(from("direct:report"), configs, incomplete, new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, incomplete.getDatasinks(), "report"),
						"report", AffordanceValidation.Mode.REPORT);
				RouteConfiguration whole = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("whole"));
				FanOut.apply(from("direct:whole"), configs, whole, new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, whole.getDatasinks(), "whole"),
						"whole", AffordanceValidation.Mode.ON);
				RouteConfiguration numeric = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("numericString"));
				FanOut.apply(from("direct:numeric"), configs, numeric, new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, numeric.getDatasinks(), "numeric"),
						"numeric", AffordanceValidation.Mode.ON);
				RouteConfiguration wholeAndMember = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("wholeGroup", "uuid"));
				FanOut.apply(from("direct:wholeAndMember"), configs, wholeAndMember, new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, wholeAndMember.getDatasinks(), "wholeAndMember"),
						"wholeAndMember", AffordanceValidation.Mode.ON);
					RouteConfiguration nullArrayOverlap = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("arrayWhole", "arrayMember"));
				FanOut.apply(from("direct:nullArrayOverlap"), configs, nullArrayOverlap, new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, nullArrayOverlap.getDatasinks(), "nullArrayOverlap"),
						"nullArrayOverlap", AffordanceValidation.Mode.ON);
				RouteConfiguration arrayMembers = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("arraySecond", "unrelatedGroup", "arrayMember"));
				for (AffordanceValidation.Mode mode : AffordanceValidation.Mode.values()) {
					String routeId = "arrayMembers" + mode;
					FanOut.apply(from("direct:" + routeId), configs, arrayMembers, new String[0],
							RouteCreatorHelper.getDataSinkEndpoints(configs, arrayMembers.getDatasinks(), routeId),
							routeId, mode);
				}
				RouteConfiguration memberAndWhole = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("uuid", "wholeGroup", "other"));
				FanOut.apply(from("direct:memberAndWhole"), configs, memberAndWhole, new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, memberAndWhole.getDatasinks(), "memberAndWhole"),
						"memberAndWhole", AffordanceValidation.Mode.ON);
				RouteConfiguration isolated = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("mutator", "observer"));
				var isolatedRoute = from("direct:isolated")
						.setHeader("branch", constant("source"))
						.setProperty("branch", constant("source"))
						.setProperty("branchMeta").constant(new java.util.LinkedHashMap<>(Map.of("value", "source")))
						.setBody().constant(Map.of("First", 1, "Second", 2));
				FanOut.apply(isolatedRoute, configs, isolated, new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, isolated.getDatasinks(), "isolated"),
						"isolated", AffordanceValidation.Mode.ON);
				from("direct:mutatingSink").process(exchange -> {
					exchange.getMessage().setHeader("branch", "poison");
					exchange.setProperty("branch", "poison");
					@SuppressWarnings("unchecked")
					Map<String, Object> branchMeta = exchange.getProperty("branchMeta", Map.class);
					branchMeta.put("value", "poison");
					exchange.getMessage().setBody("poison");
					throw new IllegalStateException("mutating sink failure");
				});
				from("direct:observingSink").process(exchange -> OBSERVED_BRANCH_STATE.set(
						exchange.getMessage().getHeader("branch", String.class) + ":"
						+ exchange.getProperty("branch", String.class) + ":"
						+ exchange.getMessage().getBody(String.class) + ":"
						+ exchange.getProperty("branchMeta", Map.class).get("value")));
				RouteConfiguration ordered = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("order1", "order2", "order3"));
				FanOut.apply(from("direct:ordered"), configs, ordered, new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, ordered.getDatasinks(), "ordered"),
						"ordered", AffordanceValidation.Mode.ON);

				RoutesConfiguration triggerConfigs = new RoutesConfiguration(
						List.of(new TriggerSource("position", "Position", Evaluation.CACHED),
								new TriggerSource("invoke", "Invoke", Evaluation.ON_UPDATE),
								new TriggerSource("state", "State", Evaluation.ON_CHANGE)), List.of(), List.of(), List.of());
				RouteConfiguration triggerRoute = new RouteConfiguration("execution-policy",
						List.of("position", "invoke", "state"), List.of(), List.of());
				triggerFanIn = new FanIn(triggerRoute, triggerConfigs, AffordanceValidation.Mode.OFF);
				for (String endpointId : triggerRoute.getDatasources()) {
					from("direct:" + endpointId)
						.process(triggerFanIn.disassemble(List.of(endpointId)))
						.filter(triggerFanIn::contributed)
						.filter(triggerFanIn::selectedTrigger)
						.to("direct:selected-trigger");
				}
				from("direct:selected-trigger")
					.filter(triggerFanIn.gatePredicate())
					.process(triggerFanIn.merge())
					.to("mock:selected-trigger");
			}
		};
	}

	@AfterEach
	void closeTriggerFanIn() {
		if (triggerFanIn != null) {
			triggerFanIn.close();
			triggerFanIn = null;
		}
	}

	@Test
	void wholeAffordanceStillSendsUnchangedSinglePayload() throws Exception {
		MockEndpoint whole = getMockEndpoint("mock:whole");
		whole.expectedMessageCount(1);
		Map<String, Object> payload = Map.of("Part", Map.of("Uuid", "a", "Mode", "RUN"), "Count", 2);
		template.sendBody("direct:whole", Map.of("Whole", payload));
		whole.assertIsSatisfied();
		assertEquals(payload, whole.getExchanges().get(0).getMessage().getBody());
	}

	@Test
	void wholeStringRetainsDeclaredTypeEvenIfItLooksNumeric() throws Exception {
		MockEndpoint numeric = getMockEndpoint("mock:numeric");
		numeric.expectedMessageCount(1);
		template.sendBody("direct:numeric", Map.of("Text", "103.55"));
		numeric.assertIsSatisfied();
		assertEquals("103.55", numeric.getExchanges().get(0).getMessage().getBody(String.class));
	}

	@Test
	void compatibleWholeAndMemberContributionsProduceOneMergedPayload() throws Exception {
		MockEndpoint device = getMockEndpoint("mock:device");
		device.expectedMessageCount(1);
		template.sendBody("direct:wholeAndMember", Map.of("Whole", Map.of(
				"Part", Map.of("Uuid", "a", "Mode", "RUN"), "Count", 2), "Uuid", "a"));
		device.assertIsSatisfied();
		assertEquals(Map.of("Part", Map.of("Uuid", "a", "Mode", "RUN"), "Count", 2),
				device.getExchanges().get(0).getMessage().getBody());
		device.reset();
		device.expectedMessageCount(0);
		assertThrows(CamelExecutionException.class, () -> template.requestBody("direct:wholeAndMember",
				Map.of("Whole", Map.of("Part", Map.of("Uuid", "a", "Mode", "RUN"), "Count", 2),
						"Uuid", "b")));
		device.assertIsSatisfied();
	}

	@Test
	void explicitNullArrayElementConflictsWithOverlappingValue() throws Exception {
		MockEndpoint array = getMockEndpoint("mock:array");
		array.expectedMessageCount(0);
		assertThrows(CamelExecutionException.class, () -> template.requestBody("direct:nullArrayOverlap",
			Map.of("WholeArray", Map.of("values", java.util.Arrays.asList((Object) null)), "First", "value")));
		array.assertIsSatisfied();
	}

	@Test
	void disjointArrayMembersSendOnceAndRetainIntentionalNullAlongsidePadding() throws Exception {
		MockEndpoint array = getMockEndpoint("mock:array");
		array.expectedMessageCount(2);
		template.sendBody("direct:arrayMembersON", Map.of("Last", "last", "First", "first", "Other", "ok"));
		Map<String, Object> values = new java.util.LinkedHashMap<>();
		values.put("Last", "last");
		values.put("First", null);
		values.put("Other", "ok");
		template.sendBody("direct:arrayMembersON", values);
		array.assertIsSatisfied();
		assertEquals(Map.of("values", java.util.Arrays.asList("first", null, "last")),
				array.getExchanges().get(0).getMessage().getBody());
		assertEquals(Map.of("values", java.util.Arrays.asList(null, null, "last")),
				dk.aau.smartproductionlab.dmp.core.util.JsonCodec.toJsonCompatibleValue(
						array.getExchanges().get(1).getMessage().getBody()));
	}

	@Test
	void missingOrdinaryGroupNeverStopsUnrelatedDestinationRegardlessOfValidationMode() throws Exception {
		MockEndpoint array = getMockEndpoint("mock:array");
		MockEndpoint other = getMockEndpoint("mock:unrelatedGroup");
		array.expectedMessageCount(0);
		other.expectedMessageCount(AffordanceValidation.Mode.values().length * 2);
		for (AffordanceValidation.Mode mode : AffordanceValidation.Mode.values()) {
			assertThrows(CamelExecutionException.class, () -> template.requestBody("direct:arrayMembers" + mode,
					Map.of("First", "first", "Other", "ok")));
			assertThrows(CamelExecutionException.class, () -> template.requestBody("direct:arrayMembers" + mode,
					Map.of("Last", "last", "Other", "ok")));
		}
		array.assertIsSatisfied();
		other.assertIsSatisfied();
		for (var exchange : other.getExchanges()) assertEquals(Map.of("value", "ok"), exchange.getMessage().getBody());
	}

	@Test
	void invalidOrConflictingOrdinaryGroupDoesNotStopUnrelatedDestination() throws Exception {
		MockEndpoint device = getMockEndpoint("mock:device");
		MockEndpoint other = getMockEndpoint("mock:other");
		device.expectedMessageCount(0);
		other.expectedMessageCount(2);
		assertThrows(CamelExecutionException.class, () -> template.requestBody("direct:complete",
				Map.of("Uuid", 7, "Mode", "RUN", "Count", 2, "Other", "ok")));
		assertThrows(CamelExecutionException.class, () -> template.requestBody("direct:memberAndWhole",
				Map.of("Whole", Map.of("Part", Map.of("Uuid", "a", "Mode", "RUN"), "Count", 2),
						"Uuid", "b", "Other", "ok")));
		device.assertIsSatisfied();
		other.assertIsSatisfied();
	}

	@Test
	void wholeContributionAfterCompatibleMemberStillProducesExactlyOneGroupSend() throws Exception {
		MockEndpoint device = getMockEndpoint("mock:device");
		device.expectedMessageCount(1);
		Map<String, Object> whole = Map.of("Part", Map.of("Uuid", "a", "Mode", "RUN"), "Count", 2);
		template.sendBody("direct:memberAndWhole", Map.of("Whole", whole, "Uuid", "a", "Other", "ok"));
		device.assertIsSatisfied();
		assertEquals(whole, device.getExchanges().get(0).getMessage().getBody());
	}

	@Test
	void mutatingSinkCannotLeakHeadersPropertiesOrBodyIntoSiblingOrCallerExchange() {
		OBSERVED_BRANCH_STATE.set(null);
		var exchange = new org.apache.camel.support.DefaultExchange(context);
		exchange.getMessage().setBody("ignored");
		var result = template.send("direct:isolated", exchange);
		assertTrue(result.isFailed());
		assertEquals("source:source:2:source", OBSERVED_BRANCH_STATE.get());
		assertEquals("source", result.getMessage().getHeader("branch", String.class));
		assertEquals("source", result.getProperty("branch", String.class));
		assertEquals("source", result.getProperty("branchMeta", Map.class).get("value"));
		assertEquals(Map.of("First", 1, "Second", 2), result.getMessage().getBody());
	}

	@Test
	void splitDispatchRetainsConfiguredSequentialOrder() throws Exception {
		MockEndpoint ordered = getMockEndpoint("mock:ordered");
		ordered.expectedBodiesReceived("1", "2", "3");
		template.sendBody("direct:ordered", Map.of("One", 1, "Two", 2, "Three", 3));
		ordered.assertIsSatisfied();
	}

	@Test
	void groupedNestedFieldsSendOnePayloadWhileOtherAffordanceStaysSeparate() throws Exception {
		MockEndpoint device = getMockEndpoint("mock:device");
		MockEndpoint other = getMockEndpoint("mock:other");
		device.expectedMessageCount(1);
		other.expectedMessageCount(1);
		template.sendBody("direct:complete", Map.of("Uuid", "a", "Mode", "RUN", "Count", 2, "Other", "unrelated"));
		MockEndpoint.assertIsSatisfied(context);
		assertEquals(Map.of("Part", Map.of("Uuid", "a", "Mode", "RUN"), "Count", 2),
				device.getExchanges().get(0).getMessage().getBody());
		assertEquals("unrelated", other.getExchanges().get(0).getMessage().getBody(String.class));
	}

	@Test
	void missingRequiredMemberRejectsWholeGroupAndReportSendsOne() throws Exception {
		MockEndpoint device = getMockEndpoint("mock:device");
		device.expectedMessageCount(0);
		assertThrows(CamelExecutionException.class,
				() -> template.requestBody("direct:incomplete", Map.of("Uuid", "a")));
		device.assertIsSatisfied();
		device.reset();
		device.expectedMessageCount(1);
		template.sendBody("direct:report", Map.of("Uuid", "a"));
		device.assertIsSatisfied();
		assertEquals(Map.of("Part", Map.of("Uuid", "a")),
				device.getExchanges().get(0).getMessage().getBody());
	}

	@Test
	void invalidIncomingIsNotCachedButReportAllowsIt() throws Exception {
		RoutesConfiguration configs = new RoutesConfiguration(List.of(new Source()), List.of(), List.of(), List.of());
		RouteConfiguration route = new RouteConfiguration("execution-policy", List.of("source"), List.of(), List.of());
		try (var context = new DefaultCamelContext();
				FanIn on = new FanIn(route, configs, AffordanceValidation.Mode.ON);
				FanIn report = new FanIn(route, configs, AffordanceValidation.Mode.REPORT)) {
			var invalid = new DefaultExchange(context);
			invalid.getMessage().setHeader(org.apache.camel.Exchange.CONTENT_TYPE, "application/json");
			invalid.getMessage().setBody("{\"Reading\":\"wrong\"}");
			on.disassemble("source").process(invalid);
			assertFalse(on.contributed(invalid));
			assertFalse(on.gatePredicate().matches(invalid));
			var valid = new DefaultExchange(context);
			valid.getMessage().setHeader(org.apache.camel.Exchange.CONTENT_TYPE, "application/json");
			valid.getMessage().setBody("{\"Reading\":2}");
			on.disassemble("source").process(valid);
			assertTrue(on.gatePredicate().matches(valid));
			report.disassemble("source").process(invalid);
			assertTrue(report.contributed(invalid));
		}
	}

	@Test
	void acceptedTriggerUsesItsAtomicSnapshotAfterLaterContributions() throws Exception {
		RoutesConfiguration configs = new RoutesConfiguration(List.of(new Source()), List.of(), List.of(), List.of());
		RouteConfiguration route = new RouteConfiguration("execution-policy", List.of("source"), List.of(), List.of());
		try (var context = new DefaultCamelContext();
				FanIn fanIn = new FanIn(route, configs, AffordanceValidation.Mode.OFF)) {
			var trigger = new DefaultExchange(context);
			trigger.getMessage().setHeader(org.apache.camel.Exchange.CONTENT_TYPE, "application/json");
			trigger.getMessage().setBody("{\"Reading\":2}");
			fanIn.disassemble("source").process(trigger);

			var later = new DefaultExchange(context);
			later.getMessage().setHeader(org.apache.camel.Exchange.CONTENT_TYPE, "application/json");
			later.getMessage().setBody("{\"Reading\":3}");
			fanIn.disassemble("source").process(later);

			assertTrue(fanIn.gatePredicate().matches(trigger));
			fanIn.merge().process(trigger);
			assertEquals(Map.of("Reading", 2L), trigger.getMessage().getBody());
			fanIn.merge().process(later);
			assertEquals(Map.of("Reading", 3L), later.getMessage().getBody());
		}
	}

	@Test
	void invalidSchemaFailsAtRouteBuildEvenWhenOff() {
		assertThrows(IllegalArgumentException.class, () -> new AffordanceValidation(
				"not a schema", "bad", AffordanceValidation.Mode.OFF));
	}

	@Test
	void cachedUpdatesRefreshEnrichmentWithoutExecutingUntilATriggerArrives() throws Exception {
		MockEndpoint executed = getMockEndpoint("mock:selected-trigger");
		executed.expectedMessageCount(2);
		template.sendBody("direct:state", "IDLE");
		template.sendBody("direct:position", "10");
		template.sendBody("direct:invoke", "1");
		template.sendBody("direct:position", "11");
		template.sendBody("direct:invoke", "2");
		executed.assertIsSatisfied();
		var gson = new com.google.gson.Gson();
		Map<?, ?> first = gson.fromJson(executed.getExchanges().get(0).getMessage().getBody(String.class), Map.class);
		Map<?, ?> second = gson.fromJson(executed.getExchanges().get(1).getMessage().getBody(String.class), Map.class);
		assertEquals(10.0, first.get("Position"));
		assertEquals(11.0, second.get("Position"));
		assertEquals(1.0, first.get("Invoke"));
		assertEquals(2.0, second.get("Invoke"));
		Map<?, ?> meta = executed.getExchanges().get(1).getProperty(DataTransformerConfiguration.META, Map.class);
		assertEquals(Boolean.TRUE, ((Map<?, ?>) ((Map<?, ?>) meta.get("sources")).get("Invoke")).get("trigger"));
		assertFalse(((Map<?, ?>) ((Map<?, ?>) meta.get("sources")).get("Position")).containsKey("trigger"),
				"only the triggering source is marked");
	}

	@Test
	void anOnChangeSourceTriggersOnlyWhenItsValueChanges() throws Exception {
		MockEndpoint executed = getMockEndpoint("mock:selected-trigger");
		template.sendBody("direct:position", "10");
		template.sendBody("direct:invoke", "1");
		executed.reset();
		executed.expectedMessageCount(2);
		template.sendBody("direct:state", "IDLE");
		template.sendBody("direct:state", "IDLE");
		template.sendBody("direct:state", "EXECUTE");
		template.sendBody("direct:state", "EXECUTE");
		executed.assertIsSatisfied();
		var gson = new com.google.gson.Gson();
		assertEquals("IDLE", gson.fromJson(executed.getExchanges().get(0).getMessage().getBody(String.class), Map.class).get("State"));
		assertEquals("EXECUTE", gson.fromJson(executed.getExchanges().get(1).getMessage().getBody(String.class), Map.class).get("State"));
	}

	@Test
	void offSkipsEnforcementAndOnRejectsInvalidScalar() {
		var schema = Map.of("type", "integer");
		new AffordanceValidation(schema, "off", AffordanceValidation.Mode.OFF).outgoing("bad");
		PayloadValidationException failure = assertThrows(PayloadValidationException.class, () -> new AffordanceValidation(
				schema, "on", AffordanceValidation.Mode.ON).outgoing("bad"));
		assertEquals(PayloadValidationException.Stage.COMMAND, failure.getStage());
	}

	@Test
	void coreOnlyTreatsIdenticalEndpointUrisAsTheSameDestination() {
		assertTrue(FanOut.sameDestination("direct:a", "direct:a"));
		assertFalse(FanOut.sameDestination("direct:a", "direct:b"));
	}
}
