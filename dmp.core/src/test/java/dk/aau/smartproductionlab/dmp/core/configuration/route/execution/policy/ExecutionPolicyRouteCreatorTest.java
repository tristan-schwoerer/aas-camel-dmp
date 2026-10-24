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
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.Evaluation;
import dk.aau.smartproductionlab.dmp.core.plan.MappingRoute;
import dk.aau.smartproductionlab.dmp.core.plan.RoutePlan;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
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
	/** Holds the slow sink until a test releases it. */
	private static final java.util.concurrent.CountDownLatch SLOW_SINK = new java.util.concurrent.CountDownLatch(1);

	/** A source at a fixed URI; a read one is polled every 100 ms. */
	private static Source source(String id, String sourceId, String delivery, String uri) {
		boolean read = "sync".equals(delivery);
		return new Source(id, sourceId, Endpoint.of(uri), read ? Source.Delivery.READ : Source.Delivery.OBSERVED,
				read ? 100 : 0);
	}

	private static Sink sink(String id, String uri) {
		return new Sink(id, null, Endpoint.of(uri));
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() {
				Source nullable = source("nullSource", "Nullable", "async", "direct:inNull").withValuePath("$.value");
				Source sharedLeft = source("sharedLeft", "Left", "sync", "seda:inShared").withValuePath("$.left");
				Source sharedRight = source("sharedRight", "Right", "sync", "seda:inShared").withValuePath("$.right");
				Source fastCadence = source("fastCadence", "Fast", "sync", "seda:inDifferentCadence");
				Source slowCadence = source("slowCadence", "Slow", "sync", "seda:inDifferentCadence").withPollingIntervalMs(200);
				Source eventLeft = source("eventLeft", "EventLeft", "async", "seda:inSharedEvents").withValuePath("$.left");
				Source eventRight = source("eventRight", "EventRight", "async", "seda:inSharedEvents").withValuePath("$.right")
						.withPollingIntervalMs(300);
				Source changing = source("changing", "Changing", "async", "direct:inChanging").withEvaluation(Evaluation.ON_CHANGE);
				Source cached = source("cached", "Cached", "async", "direct:inCached").withEvaluation(Evaluation.CACHED);
				for (Source source : Arrays.asList(
						// single-async route
						source("stateA", "StationState", "async", "direct:inA"),
						nullable,
						// two-async route (gate on the full set)
						source("stateB", "State", "async", "direct:inB"),
						source("modeB", "Mode", "async", "direct:inModeB"),
						// periodic-style route: async cache + sync poller
						source("pAsync", "PAsync", "async", "seda:inPAsync"),
						source("pSync", "PSync", "sync", "seda:inPSync"),
						sharedLeft,
						sharedRight,
						fastCadence,
						slowCadence,
						eventLeft,
						eventRight,
						// sync-only route: the poller drives everything
						source("sOnly", "S", "sync", "seda:inSOnly"),
						// two async sources arriving on threads of their own
						source("serialA", "A", "async", "direct:inSerialA"),
						source("serialB", "B", "async", "direct:inSerialB"),
						// headers of the source's message
						source("headed", "Headed", "async", "direct:inHeaded"),
						source("polledHeaded", "PolledHeaded", "sync", "seda:inPolledHeaded"),
						// an OnChange trigger the gate holds back
						changing,
						cached,
						// a source whose sink blocks
						source("slow", "Slow", "async", "direct:inSlow"))) sources.put(source.id(), source);
				for (Sink sink : List.of(sink("outAny", "mock:anyOut"),
						sink("outNull", "mock:nullOut"),
						sink("outAll", "mock:allOut"),
						sink("outPeriodic", "mock:periodicOut"),
						sink("outShared", "mock:sharedOut"),
						sink("outSharedEvents", "mock:sharedEventsOut"),
						sink("outSyncOnly", "mock:syncOnlyOut"),
						sink("outSerial", "direct:serialSink"),
						sink("outHeaded", "mock:headedOut"),
						sink("outPolledHeaded", "mock:polledHeadedOut"),
						sink("outChanging", "mock:changingOut"),
						sink("outSlow", "direct:slowSink"))) sinks.put(sink.id(), sink);

				add("onAny", List.of("stateA"), List.of("outAny"));
				add("onNull", List.of("nullSource"), List.of("outNull"));
				add("onAll", List.of("stateB", "modeB"), List.of("outAll"));
				add("periodic", List.of("pAsync", "pSync"), List.of("outPeriodic"));
				add("shared", List.of("sharedLeft", "sharedRight"), List.of("outShared"));
				add("differentCadence", List.of("fastCadence", "slowCadence"), List.of());
				add("sharedEvents", List.of("eventLeft", "eventRight"), List.of("outSharedEvents"));
				add("syncOnly", List.of("sOnly"), List.of("outSyncOnly"));
				add("serial", List.of("serialA", "serialB"), List.of("outSerial"));
				add("headed", List.of("headed"), List.of("outHeaded"));
				add("polledHeaded", List.of("polledHeaded"), List.of("outPolledHeaded"));
				add("changingBehindGate", List.of("changing", "cached"), List.of("outChanging"));
				add("slowSink", List.of("slow"), List.of("outSlow"));
				from("direct:slowSink").process(exchange -> SLOW_SINK.await(10, java.util.concurrent.TimeUnit.SECONDS)).to("mock:slowOut");
				// a slow sink that counts the evaluations writing at the same time
				from("direct:serialSink").process(exchange -> {
					MOST_CONCURRENT.accumulateAndGet(RUNNING.incrementAndGet(), Math::max);
					try { Thread.sleep(20); } finally { RUNNING.decrementAndGet(); }
				}).to("mock:serialOut");
			}

			private final Map<String, Source> sources = new java.util.LinkedHashMap<>();
			private final Map<String, Sink> sinks = new java.util.LinkedHashMap<>();

			private void add(String routeId, List<String> sourceIds, List<String> sinkIds) {
				new MappingRoute(routeId, sourceIds.stream().map(sources::get).toList(), null,
						sinkIds.stream().map(sinks::get).toList(), null).build(this);
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
	void aSlowSinkDoesNotHoldTheSourcesThread() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:slowOut");
		out.expectedMessageCount(2);
		long started = System.nanoTime();
		template.sendBody("direct:inSlow", "first");
		template.sendBody("direct:inSlow", "second");
		long sendMillis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
		assertTrue(sendMillis < 2000, "the source's thread only accepts its value; it took " + sendMillis + " ms");
		assertEquals(0, out.getReceivedCounter(), "the sink is still held");
		SLOW_SINK.countDown();
		out.assertIsSatisfied(5000);
		assertEquals(List.of(Map.of("Slow", "first"), Map.of("Slow", "second")),
				out.getExchanges().stream().map(exchange -> exchange.getMessage().getBody()).toList(), "one at a time, in order");
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
		// The source's thread accepts the value and a worker evaluates it: count what every thread allocates.
		long evaluation = Long.MAX_VALUE;
		for (int i = 0; i < 5; i++) {
			out.reset();
			out.expectedMessageCount(1);
			long before = allocatedByAllThreads(threads);
			template.sendBody("direct:inA", payload);
			out.assertIsSatisfied(5000);
			evaluation = Math.min(evaluation, allocatedByAllThreads(threads) - before);
		}
		assertEquals(payload, sourcesOf(out.getExchanges().get(0).getMessage().getBody()).get("StationState"));
		assertTrue(evaluation < 2 * copy, "one evaluation allocated " + evaluation + " bytes, one copy of its source "
				+ copy + ": the value is copied once, when the cache accepts it");
	}

	private static long allocatedByAllThreads(com.sun.management.ThreadMXBean threads) {
		long total = 0;
		for (long bytes : threads.getThreadAllocatedBytes(threads.getAllThreadIds())) if (bytes > 0) total += bytes;
		return total;
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
			// The evaluation's event is committed after its sinks wrote, on the worker.
			Thread.sleep(200);
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
	void managedBuilderRejectsMissingRouteIdsAndDuplicateEndpoints() {
		assertThrows(IllegalArgumentException.class, () -> new MappingRoute(null,
				List.of(source("source", "Source", "async", "direct:source")), null, List.of(), null));
		assertThrows(IllegalArgumentException.class, () -> new MappingRoute("duplicate",
				List.of(source("source", "A", "async", "direct:a"), source("source", "B", "async", "direct:b")),
				null, List.of(), null));
		RoutePlan unnamed = new RoutePlan() {
			@Override public String routeId() { return " "; }
			@Override public void build(RouteBuilder builder) { }
			@Override public List<Endpoint> endpoints() { return List.of(); }
		};
		assertThrows(IllegalArgumentException.class, () -> DmpRouteBuilder.forManagedBundle(unnamed));
	}

	private static DmpRouteBuilder managedBuilder(String routeId, String asyncId, String syncId) {
		return DmpRouteBuilder.forManagedBundle(new MappingRoute(routeId,
				List.of(source(asyncId, asyncId, "async", "direct:" + asyncId),
						source(syncId, "Sync", "sync", "direct:" + syncId)), null, List.of(), null));
	}
}
