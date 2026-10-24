/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SerialLaneTest {
	private final ExecutorService workers = Executors.newFixedThreadPool(4);

	@AfterEach
	void stop() { workers.shutdownNow(); }

	@Test
	void itemsRunOneAtATimeInOrderOnTheSharedWorkers() throws Exception {
		List<Integer> seen = java.util.Collections.synchronizedList(new ArrayList<>());
		AtomicInteger running = new AtomicInteger(), most = new AtomicInteger();
		SerialLane<Integer> lane = new SerialLane<>("test", workers, 1000, item -> {
			most.accumulateAndGet(running.incrementAndGet(), Math::max);
			try { Thread.sleep(1); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
			seen.add(item);
			running.decrementAndGet();
		}, (older, newer) -> newer);
		for (int i = 0; i < 200; i++) lane.submit(i);
		assertTrue(lane.drain(Duration.ofSeconds(10)));
		assertEquals(1, most.get());
		List<Integer> expected = new ArrayList<>();
		for (int i = 0; i < 200; i++) expected.add(i);
		assertEquals(expected, seen);
	}

	@Test
	void aBlockedLaneLeavesTheWorkersToTheOthers() throws Exception {
		CountDownLatch release = new CountDownLatch(1);
		SerialLane<String> blocked = new SerialLane<>("blocked", workers, 10, item -> {
			try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
		}, (older, newer) -> newer);
		blocked.submit("held");
		AtomicInteger done = new AtomicInteger();
		List<SerialLane<String>> others = new ArrayList<>();
		for (int i = 0; i < 20; i++) others.add(new SerialLane<>("other" + i, workers, 10, item -> done.incrementAndGet(), (o, n) -> n));
		for (var lane : others) lane.submit("go");
		for (var lane : others) assertTrue(lane.drain(Duration.ofSeconds(5)));
		assertEquals(20, done.get());
		assertFalse(blocked.drain(Duration.ofMillis(50)));
		release.countDown();
		assertTrue(blocked.drain(Duration.ofSeconds(5)));
	}

	@Test
	void moreBusyLanesThanWorkersWaitForAWorkerInsteadOfBeingDropped() throws Exception {
		try (var context = new org.apache.camel.impl.DefaultCamelContext()) {
			context.start();
			var shared = DmpExecutors.of(context).workers();
			int lanes = DmpExecutors.WORKER_THREADS * 3;
			AtomicInteger done = new AtomicInteger();
			List<SerialLane<Integer>> all = new ArrayList<>();
			for (int i = 0; i < lanes; i++) {
				SerialLane<Integer> lane = new SerialLane<>("busy" + i, shared, 10, item -> {
					try { Thread.sleep(20); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
					done.incrementAndGet();
				}, (o, n) -> n);
				all.add(lane);
				lane.submit(1);
				lane.submit(2);
			}
			for (var lane : all) assertTrue(lane.drain(Duration.ofSeconds(10)));
			assertEquals(2 * lanes, done.get());
		}
	}

	@Test
	void aFullLaneMergesIntoItsNewestItem() throws Exception {
		CountDownLatch release = new CountDownLatch(1);
		List<List<String>> seen = java.util.Collections.synchronizedList(new ArrayList<>());
		SerialLane<List<String>> lane = new SerialLane<>("full", workers, 2, item -> {
			try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
			seen.add(item);
		}, (older, newer) -> { List<String> both = new ArrayList<>(older); both.addAll(newer); return both; });
		lane.submit(List.of("a"));
		Thread.sleep(100); // "a" runs and waits
		lane.submit(List.of("b"));
		lane.submit(List.of("c"));
		lane.submit(List.of("d"));
		lane.submit(List.of("e"));
		assertEquals(2, lane.pending());
		assertEquals(2, lane.merged());
		release.countDown();
		assertTrue(lane.drain(Duration.ofSeconds(5)));
		assertEquals(List.of(List.of("a"), List.of("b"), List.of("c", "d", "e")), seen);
	}

	@Test
	void aClosedLaneDropsWhatWaitsAndWhatComes() throws Exception {
		CountDownLatch release = new CountDownLatch(1);
		AtomicInteger runs = new AtomicInteger();
		SerialLane<String> lane = new SerialLane<>("closed", workers, 10, item -> {
			runs.incrementAndGet();
			try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
		}, (o, n) -> n);
		lane.submit("running");
		Thread.sleep(100);
		lane.submit("waiting");
		lane.close();
		lane.submit("later");
		release.countDown();
		assertTrue(lane.drain(Duration.ofSeconds(5)));
		assertEquals(1, runs.get());
	}
}
