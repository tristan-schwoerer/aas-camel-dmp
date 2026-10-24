/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.execution;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.junit.jupiter.api.Test;

class DmpPollComponentTest extends CamelTestSupport {
	private static final int POLLS = 300;
	private final ConcurrentHashMap<String, AtomicInteger> ticks = new ConcurrentHashMap<>();
	private final AtomicInteger running = new AtomicInteger(), most = new AtomicInteger();

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override public void configure() {
				for (int i = 0; i < POLLS; i++) {
					String name = "poll-" + i;
					from(DmpPollComponent.uri(name, 50) + "&delay=0").routeId(name)
							.process(exchange -> ticks.computeIfAbsent(name, ignored -> new AtomicInteger()).incrementAndGet());
				}
				// a poll slower than its period: its ticks are skipped, never run twice at once
				from(DmpPollComponent.uri("slow", 10) + "&delay=0").routeId("slow").process(exchange -> {
					most.accumulateAndGet(running.incrementAndGet(), Math::max);
					try { Thread.sleep(100); } finally { running.decrementAndGet(); }
				});
			}
		};
	}

	@Test
	void pollsShareOneSchedulerAndTheWorkers() throws Exception {
		int before = ManagementFactory.getThreadMXBean().getThreadCount();
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (System.nanoTime() < deadline && (ticks.size() < POLLS || ticks.values().stream().anyMatch(count -> count.get() < 3)))
			Thread.sleep(50);
		assertTrue(ticks.size() == POLLS && ticks.values().stream().allMatch(count -> count.get() >= 3),
				"every poll ticks at its rate: " + ticks.size() + " polls ticked");
		int threads = ManagementFactory.getThreadMXBean().getThreadCount();
		// timer: kept a thread per poll; these share one scheduler and at most dmp.workerThreads workers
		assertTrue(threads - before <= DmpExecutors.WORKER_THREADS + 1, "threads grew by " + (threads - before));
		assertTrue(most.get() == 1, "a slow poll never runs twice at once");
	}
}
