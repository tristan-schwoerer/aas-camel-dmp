/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.core.configuration.route.execution.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.routebuilder.DmpRouteBuilder;
import org.junit.jupiter.api.Test;

/**
 * Coverage of the ONE aggregation core under the FIXED execution semantic
 * (decision 2026-09-15): every source feeds a latest-value cache — async on
 * arrival, sync polled at its declared interval — and an update of any
 * source evaluates the mapping only once ALL sources have delivered. There
 * is no authorable policy; a config carrying an {@code executionPolicy}
 * block is a load-time error.
 */
class ExecutionPolicyRouteCreatorTest extends CamelTestSupport {

	private static final java.util.concurrent.atomic.AtomicInteger RUNNING = new java.util.concurrent.atomic.AtomicInteger();
	private static final java.util.concurrent.atomic.AtomicInteger MOST_CONCURRENT = new java.util.concurrent.atomic.AtomicInteger();

	static final class FixedSource extends DataSourceConfiguration {
		private final String uri;

		FixedSource(String uniqueId, String sourceId, String delivery, String uri) {
			super(uniqueId, null, 0);
			setSourceId(sourceId);
			setDelivery(delivery);
			if ("sync".equals(delivery)) {
				setPollingIntervalMs(100);
			}
			this.uri = uri;
		}

		@Override
		public String getConnectionURI() {
			return uri;
		}
	}

	static final class FixedSink extends DataSinkConfiguration {
		private final String uri;

		FixedSink(String uniqueId, String uri) {
			super(uniqueId);
			this.uri = uri;
		}

		@Override
		public String getConnectionURI() {
			return uri;
		}
	}

	private static RouteConfiguration route(String routeId,
			List<String> datasourceIds, List<String> sinkIds) {
		RouteConfiguration route = new RouteConfiguration(
				ExecutionPolicyRouteConfiguration.ROUTE_TRIGGER, datasourceIds, List.of(), sinkIds);
		route.setRouteId(routeId);
		return route;
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() {
				FixedSource nullable = new FixedSource("nullSource", "Nullable", "async", "direct:inNull");
				nullable.setValuePath("$.value");
				FixedSource sharedLeft = new FixedSource("sharedLeft", "Left", "sync", "seda:inShared");
				sharedLeft.setValuePath("$.left");
				FixedSource sharedRight = new FixedSource("sharedRight", "Right", "sync", "seda:inShared");
				sharedRight.setValuePath("$.right");
				FixedSource fastCadence = new FixedSource("fastCadence", "Fast", "sync", "seda:inDifferentCadence");
				FixedSource slowCadence = new FixedSource("slowCadence", "Slow", "sync", "seda:inDifferentCadence");
				slowCadence.setPollingIntervalMs(200);
				FixedSource eventLeft = new FixedSource("eventLeft", "EventLeft", "async", "seda:inSharedEvents");
				eventLeft.setValuePath("$.left");
				FixedSource eventRight = new FixedSource("eventRight", "EventRight", "async", "seda:inSharedEvents");
				eventRight.setValuePath("$.right");
				eventRight.setPollingIntervalMs(300);
				FixedSource changing = new FixedSource("changing", "Changing", "async", "direct:inChanging");
				changing.setEvaluation(dk.aau.smartproductionlab.dmp.core.configuration.entity.Evaluation.ON_CHANGE);
				FixedSource cached = new FixedSource("cached", "Cached", "async", "direct:inCached");
				cached.setEvaluation(dk.aau.smartproductionlab.dmp.core.configuration.entity.Evaluation.CACHED);
				RoutesConfiguration routes = new RoutesConfiguration(
						Arrays.asList(
								// single-async route
								new FixedSource("stateA", "StationState", "async", "direct:inA"),
								nullable,
								// two-async route (gate on the full set)
								new FixedSource("stateB", "State", "async", "direct:inB"),
								new FixedSource("modeB", "Mode", "async", "direct:inModeB"),
								// periodic-style route: async cache + sync poller
								new FixedSource("pAsync", "PAsync", "async", "seda:inPAsync"),
								new FixedSource("pSync", "PSync", "sync", "seda:inPSync"),
								sharedLeft,
								sharedRight,
								fastCadence,
								slowCadence,
								eventLeft,
								eventRight,
								// sync-only route: the poller drives everything
								new FixedSource("sOnly", "S", "sync", "seda:inSOnly"),
								// two async sources arriving on threads of their own
								new FixedSource("serialA", "A", "async", "direct:inSerialA"),
								new FixedSource("serialB", "B", "async", "direct:inSerialB"),
								// headers of the source's message
								new FixedSource("headed", "Headed", "async", "direct:inHeaded"),
								new FixedSource("polledHeaded", "PolledHeaded", "sync", "seda:inPolledHeaded"),
								// an OnChange trigger the gate holds back
								changing,
								cached),
						List.of(),
						List.of(new FixedSink("outAny", "mock:anyOut"),
								new FixedSink("outNull", "mock:nullOut"),
								new FixedSink("outAll", "mock:allOut"),
								new FixedSink("outPeriodic", "mock:periodicOut"),
								new FixedSink("outShared", "mock:sharedOut"),
								new FixedSink("outSharedEvents", "mock:sharedEventsOut"),
								new FixedSink("outSyncOnly", "mock:syncOnlyOut"),
								new FixedSink("outSerial", "direct:serialSink"),
								new FixedSink("outHeaded", "mock:headedOut"),
								new FixedSink("outPolledHeaded", "mock:polledHeadedOut"),
								new FixedSink("outChanging", "mock:changingOut")),
						List.of());

				add(route("onAny", List.of("stateA"), List.of("outAny")), routes);
				add(route("onNull", List.of("nullSource"), List.of("outNull")), routes);
				add(route("onAll", List.of("stateB", "modeB"), List.of("outAll")), routes);
				add(route("periodic", List.of("pAsync", "pSync"), List.of("outPeriodic")), routes);
				add(route("shared", List.of("sharedLeft", "sharedRight"), List.of("outShared")), routes);
				add(route("differentCadence", List.of("fastCadence", "slowCadence"), List.of()), routes);
				add(route("sharedEvents", List.of("eventLeft", "eventRight"), List.of("outSharedEvents")), routes);
				add(route("syncOnly", List.of("sOnly"), List.of("outSyncOnly")), routes);
				add(route("serial", List.of("serialA", "serialB"), List.of("outSerial")), routes);
				add(route("headed", List.of("headed"), List.of("outHeaded")), routes);
				add(route("polledHeaded", List.of("polledHeaded"), List.of("outPolledHeaded")), routes);
				add(route("changingBehindGate", List.of("changing", "cached"), List.of("outChanging")), routes);
				// a slow sink that counts the evaluations writing at the same time
				from("direct:serialSink").process(exchange -> {
					MOST_CONCURRENT.accumulateAndGet(RUNNING.incrementAndGet(), Math::max);
					try { Thread.sleep(20); } finally { RUNNING.decrementAndGet(); }
				}).to("mock:serialOut");
			}

			private void add(RouteConfiguration route, RoutesConfiguration routes) {
				new ExecutionPolicyRouteCreator(this, routes).addRouteToRouteBuilder(route);
			}
		};
	}


	private static Map<?, ?> sourcesOf(Object body) { return (Map<?, ?>) body; }

	@Test
	void evaluationsOfOneMappingRunOneAtATimeOnTheLatestValues() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:serialOut");
		int updates = 10;
		out.expectedMessageCount(1 + 2 * updates);
		template.sendBody("direct:inSerialA", "a0");
		template.sendBody("direct:inSerialB", "b0");
		var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
		try {
			var a = pool.submit(() -> { for (int i = 1; i <= updates; i++) template.sendBody("direct:inSerialA", "a" + i); });
			var b = pool.submit(() -> { for (int i = 1; i <= updates; i++) template.sendBody("direct:inSerialB", "b" + i); });
			a.get(); b.get();
		} finally {
			pool.shutdownNow();
		}
		out.assertIsSatisfied(10000);
		assertEquals(1, MOST_CONCURRENT.get(), "a mapping evaluates once at a time");
		Map<?, ?> last = sourcesOf(out.getExchanges().get(out.getExchanges().size() - 1).getMessage().getBody());
		assertEquals(Map.of("A", "a" + updates, "B", "b" + updates), last, "the last evaluation writes the latest values");
	}

	@Test
	void noHeaderOfTheSourcesMessageReachesASink() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:headedOut");
		out.expectedMessageCount(1);
		template.sendBodyAndHeaders("direct:inHeaded", "value",
				Map.of("CamelMiloNodeIds", List.of("ns=2;s=Read"), "Content-Encoding", "gzip"));
		out.assertIsSatisfied(2000);
		var headers = out.getExchanges().get(0).getMessage().getHeaders();
		assertNull(headers.get("CamelMiloNodeIds"), "a Read's node list would turn an OPC UA write into a read");
		assertNull(headers.get("Content-Encoding"), "a source's transport headers are not a sink's");
	}

	@Test
	void noHeaderOfAPolledReadReachesASink() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:polledHeadedOut");
		out.expectedMinimumMessageCount(1);
		template.sendBodyAndHeader("seda:inPolledHeaded", "value", "CamelMiloNodeIds", List.of("ns=2;s=Read"));
		out.assertIsSatisfied(4000);
		assertNull(out.getExchanges().get(0).getMessage().getHeader("CamelMiloNodeIds"));
	}

	@Test
	void anOnChangeTriggerTheGateHoldsBackEvaluatesOnceTheGateOpens() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:changingOut");
		out.expectedMessageCount(1);
		template.sendBody("direct:inChanging", "5"); // the first value counts, but Cached has not delivered
		template.sendBody("direct:inCached", "c"); // not a trigger itself: releases the held one
		out.assertIsSatisfied(2000);
		assertEquals(Map.of("Changing", "5", "Cached", "c"), sourcesOf(out.getExchanges().get(0).getMessage().getBody()));
		template.sendBody("direct:inChanging", "5"); // unchanged
		template.sendBody("direct:inCached", "d"); // never a trigger
		Thread.sleep(300);
		assertEquals(1, out.getReceivedCounter(), "an unchanged value and a Cached update do not evaluate");
	}

	@Test
	void anEvaluationCopiesItsSourceValueOnce() throws Exception {
		var threads = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
		List<Object> payload = new java.util.ArrayList<>();
		for (int i = 0; i < 20_000; i++) payload.add(new java.util.LinkedHashMap<>(Map.of("index", (long) i, "name", "item")));
		MockEndpoint out = getMockEndpoint("mock:anyOut");
		for (int i = 0; i < 5; i++) template.sendBody("direct:inA", payload); // warm up
		long copy = Long.MAX_VALUE;
		for (int i = 0; i < 5; i++) {
			long before = threads.getCurrentThreadAllocatedBytes();
			dk.aau.smartproductionlab.dmp.core.util.JsonCodec.freeze(payload);
			copy = Math.min(copy, threads.getCurrentThreadAllocatedBytes() - before);
		}
		long evaluation = Long.MAX_VALUE;
		for (int i = 0; i < 5; i++) {
			out.reset();
			long before = threads.getCurrentThreadAllocatedBytes();
			template.sendBody("direct:inA", payload);
			evaluation = Math.min(evaluation, threads.getCurrentThreadAllocatedBytes() - before);
		}
		assertEquals(payload, sourcesOf(out.getExchanges().get(0).getMessage().getBody()).get("StationState"));
		assertTrue(evaluation < 2 * copy, "one evaluation allocated " + evaluation + " bytes, one copy of its source "
				+ copy + ": the value is copied once, when the cache accepts it");
	}

	@Test
	void evaluationsAndSinkWritesAreRecordedAsFlightRecorderEvents() throws Exception {
		java.nio.file.Path file = java.nio.file.Files.createTempFile("dmp-evaluation", ".jfr");
		try (var recording = new jdk.jfr.Recording()) {
			recording.enable("dk.aau.smartproductionlab.dmp.Evaluation");
			recording.enable("dk.aau.smartproductionlab.dmp.SinkWrite");
			recording.start();
			MockEndpoint out = getMockEndpoint("mock:anyOut");
			out.expectedMessageCount(1);
			template.sendBody("direct:inA", "RECORDED");
			out.assertIsSatisfied(2000);
			recording.stop();
			recording.dump(file);
			var events = jdk.jfr.consumer.RecordingFile.readAllEvents(file);
			var evaluation = events.stream().filter(event -> event.getEventType().getName().endsWith(".Evaluation")).findFirst().orElseThrow();
			assertEquals("onAny", evaluation.getString("mapping"));
			assertEquals("StationState", evaluation.getString("triggers"));
			assertTrue(evaluation.getBoolean("succeeded"));
			var write = events.stream().filter(event -> event.getEventType().getName().endsWith(".SinkWrite")).findFirst().orElseThrow();
			assertEquals("outAny", write.getString("sink"));
			assertEquals("mock", write.getString("kind"));
			assertTrue(!write.getDuration().isNegative() && evaluation.getDuration().compareTo(write.getDuration()) >= 0,
					"the sink write is part of its evaluation");
		} finally {
			java.nio.file.Files.deleteIfExists(file);
		}
	}

	@Test
	void asyncOnlyRouteFiresOnEveryArrivalOnceComplete() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:anyOut");
		out.expectedMessageCount(2);
		template.sendBody("direct:inA", "ONE");
		template.sendBody("direct:inA", "TWO");
		out.assertIsSatisfied(2000);

		Object body = out.getExchanges().get(1).getMessage().getBody();
		assertInstanceOf(Map.class, body);
		assertEquals("TWO", sourcesOf(body).get("StationState"));
	}

	@Test
	void deliveredJsonNullRemainsInSourcesMap() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:nullOut");
		out.expectedMessageCount(1);
		template.sendBodyAndHeader("direct:inNull", "{\"value\":null}",
				org.apache.camel.Exchange.CONTENT_TYPE, "application/json");
		out.assertIsSatisfied(2000);
		Map<?, ?> sources = sourcesOf(out.getExchanges().get(0).getMessage().getBody());
		assertTrue(sources.containsKey("Nullable"), "a delivered null must not disappear from the blob input");
		assertEquals(null, sources.get("Nullable"));
	}

	@Test
	void gateHoldsUntilAllSourcesDeliveredThenStreamMerges() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:allOut");
		out.expectedMessageCount(2);

		template.sendBody("direct:inB", "s1"); // only one of two: held
		template.sendBody("direct:inModeB", "m1"); // completes the set: emit 1
		template.sendBody("direct:inB", "s2"); // fresh arrival: emit 2 (latest)

		out.assertIsSatisfied(4000);

		Map<?, ?> first = sourcesOf(out.getExchanges().get(0).getMessage().getBody());
		assertEquals("s1", first.get("State"));
		assertEquals("m1", first.get("Mode"));
		Map<?, ?> second = sourcesOf(out.getExchanges().get(1).getMessage().getBody());
		assertEquals("s2", second.get("State"), "latest value wins");
	}

	@Test
	void neverEmitsAPartialSourcesMap() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:allOut");
		// only State delivers; Mode never does → nothing is emitted
		template.sendBody("direct:inB", "lonely");
		Thread.sleep(500);
		assertEquals(0, out.getReceivedCounter(), "a partial map is never emitted");
	}

	@Test
	void syncPollerFeedsTheSameCacheAsAsyncSources() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:periodicOut");
		out.expectedMinimumMessageCount(1);

		template.sendBody("seda:inPAsync", "CACHED");
		template.sendBody("seda:inPSync", "FRESH");
		out.assertIsSatisfied(4000);

		boolean found = out.getReceivedExchanges().stream().anyMatch(e -> {
			Object body = e.getMessage().getBody();
			if (!(body instanceof Map<?, ?>)) {
				return false;
			}
			Map<?, ?> m = sourcesOf(body);
			return "CACHED".equals(m.get("PAsync")) && "FRESH".equals(m.get("PSync"));
		});
		assertTrue(found, "the sync poll tick evaluates the cached async snapshot");
	}

	@Test
	void syncOnlyRouteEmitsPerPollTick() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:syncOnlyOut");
		out.expectedMinimumMessageCount(1);

		template.sendBody("seda:inSOnly", "POLLED");
		out.assertIsSatisfied(4000);

		Map<?, ?> sources = sourcesOf(out.getExchanges().get(0).getMessage().getBody());
		assertEquals("POLLED", sources.get("S"));
	}

	@Test
	void identicalAcquisitionsAreReadOnceAndProjectedToEverySourceId() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:sharedOut");
		out.expectedMessageCount(1);
		template.sendBody("seda:inShared", Map.of("left", "L", "right", "R"));
		out.assertIsSatisfied(3000);
		Map<?, ?> sources = sourcesOf(out.getExchanges().get(0).getMessage().getBody());
		assertEquals("L", sources.get("Left"));
		assertEquals("R", sources.get("Right"));
	}

	@Test
	void sameUriWithDifferentSyncCadencesKeepsSeparatePollRoutes() {
		var fast = context.getRouteDefinition("differentCadence-poll-fastCadence");
		var slow = context.getRouteDefinition("differentCadence-poll-slowCadence");
		assertNotNull(fast);
		assertNotNull(slow);
		assertTrue(fast.getInput().getEndpointUri().endsWith("?period=100"), fast.getInput().getEndpointUri());
		assertTrue(slow.getInput().getEndpointUri().endsWith("?period=200"), slow.getInput().getEndpointUri());
	}

	@Test
	void identicalSubscriptionsFanOneEventOutToEverySourceId() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:sharedEventsOut");
		out.expectedMessageCount(1);
		template.sendBody("seda:inSharedEvents", Map.of("left", "L", "right", "R"));
		out.assertIsSatisfied(3000);
		Map<?, ?> sources = sourcesOf(out.getExchanges().get(0).getMessage().getBody());
		assertEquals("L", sources.get("EventLeft"));
		assertEquals("R", sources.get("EventRight"));
	}

	@Test
	void aSourceThatCannotReadAnEventDoesNotStopTheOthersOfItsAcquisition() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:sharedEventsOut");
		out.expectedMessageCount(2);
		template.sendBody("seda:inSharedEvents", Map.of("left", "L", "right", "R"));
		template.sendBody("seda:inSharedEvents", Map.of("left", "L2"));
		out.assertIsSatisfied(3000);
		Map<?, ?> sources = sourcesOf(out.getExchanges().get(1).getMessage().getBody());
		assertEquals("L2", sources.get("EventLeft"));
		assertEquals("R", sources.get("EventRight"), "the last value of the source that could not read the event");
	}

	@Test
	void managedBuilderInventoriesAreExactAndIndependentForAsyncAndSyncSources() throws Exception {
		DmpRouteBuilder first = managedBuilder("mapping-A", "async-A", "sync-A");
		DmpRouteBuilder second = managedBuilder("mapping-B", "async-B", "sync-B");

		first.prepareBundle(context);
		second.prepareBundle(context);

		Set<String> firstIds = Set.of("mapping-A-source-async-A", "mapping-A-poll-sync-A", "mapping-A");
		Set<String> secondIds = Set.of("mapping-B-source-async-B", "mapping-B-poll-sync-B", "mapping-B");
		assertEquals(firstIds, first.getRouteInventory());
		assertEquals(secondIds, second.getRouteInventory());
		assertTrue(java.util.Collections.disjoint(first.getRouteInventory(), second.getRouteInventory()));
		assertThrows(UnsupportedOperationException.class,
				() -> first.getRouteInventory().add("not-owned"));
		assertEquals(List.of("mapping-A", "mapping-A-source-async-A", "mapping-A-poll-sync-A"),
				first.getPreparedRouteDefinitions().stream().map(org.apache.camel.model.RouteDefinition::getRouteId).toList());
		assertThrows(UnsupportedOperationException.class,
				() -> first.getPreparedRouteDefinitions().clear());
		assertThrows(IllegalStateException.class, () -> first.prepareBundle(context));
	}

	@Test
	void managedBuilderRejectsMissingAndDuplicateAuthoredRouteIds() {
		RouteConfiguration missingId = route(null, List.of("source"), List.of());
		RoutesConfiguration missingBundle = ordinaryBundle(missingId, "source", "async-source", "unused-sync");
		assertThrows(IllegalArgumentException.class,
				() -> DmpRouteBuilder.forManagedBundle(missingBundle, policyFactories()));
		assertNull(missingId.getRouteId());

		RouteConfiguration firstRoute = route("duplicate", List.of("source"), List.of());
		RouteConfiguration secondRoute = route("duplicate", List.of("source"), List.of());
		RoutesConfiguration duplicateBundle = ordinaryBundle(List.of(firstRoute, secondRoute), "source",
				"async-source", "unused-sync");
		assertThrows(IllegalArgumentException.class,
				() -> DmpRouteBuilder.forManagedBundle(duplicateBundle, policyFactories()));

		RouteConfiguration operationRoute = route("operation", List.of(), List.of());
		operationRoute.setRouteTrigger("unsupported-managed-trigger");
		assertThrows(IllegalArgumentException.class, () -> DmpRouteBuilder.forManagedBundle(
				ordinaryBundle(operationRoute, "source", "async-source", "unused-sync"), policyFactories()));
	}

	private static DmpRouteBuilder managedBuilder(String routeId, String asyncId, String syncId) {
		RoutesConfiguration configuration = ordinaryBundle(route(routeId, List.of(asyncId, syncId), List.of()),
				asyncId, asyncId, syncId);
		return DmpRouteBuilder.forManagedBundle(configuration, policyFactories());
	}

	private static RoutesConfiguration ordinaryBundle(RouteConfiguration route, String asyncId,
			String asyncEndpointId, String syncEndpointId) {
		return ordinaryBundle(List.of(route), asyncId, asyncEndpointId, syncEndpointId);
	}

	private static RoutesConfiguration ordinaryBundle(List<RouteConfiguration> routes, String asyncId,
			String asyncEndpointId, String syncEndpointId) {
		return new RoutesConfiguration(
				List.of(new FixedSource(asyncEndpointId, asyncId, "async", "direct:" + asyncEndpointId),
						new FixedSource(syncEndpointId, "Sync", "sync", "direct:" + syncEndpointId)),
				List.of(), List.of(), routes);
	}

	private static Map<String, dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreatorFactory>
			policyFactories() {
		ExecutionPolicyRouteCreatorFactory factory = new ExecutionPolicyRouteCreatorFactory();
		return Map.of(factory.routeTrigger(), factory);
	}
}
