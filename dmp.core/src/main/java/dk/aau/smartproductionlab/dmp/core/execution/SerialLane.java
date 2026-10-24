/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.execution;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The work of one owner (a mapping's evaluations, one element's events), run one item at a time and in order on
 * shared workers: the owner has no thread of its own, and whoever submits never waits for the work. Each run takes
 * one item and hands the worker back, so a busy owner does not keep the workers from the others.
 *
 * <p>A lane holds at most {@code limit} items. One more is merged into the newest held item, so a lane whose
 * worker is slower than its input stays bounded and still runs the latest input.
 */
public final class SerialLane<T> {
	private static final Logger LOGGER = LoggerFactory.getLogger(SerialLane.class);

	private final String name;
	private final Executor workers;
	private final int limit;
	private final Consumer<T> worker;
	private final BinaryOperator<T> merge;
	private final ArrayDeque<T> queue = new ArrayDeque<>();
	/** A run is handed to the workers or running. */
	private boolean scheduled;
	private boolean closed;
	private long merged;

	public SerialLane(String name, Executor workers, int limit, Consumer<T> worker, BinaryOperator<T> merge) {
		if (limit < 1) throw new IllegalArgumentException("a lane holds at least one item");
		this.name = name;
		this.workers = workers;
		this.limit = limit;
		this.worker = worker;
		this.merge = merge;
	}

	/** Queues {@code item} and returns at once; a closed lane drops it. */
	public void submit(T item) {
		synchronized (this) {
			if (closed) return;
			if (queue.size() >= limit) {
				queue.addLast(merge.apply(queue.pollLast(), item));
				if (merged++ == 0 || merged % 1000 == 0)
					LOGGER.warn("{}: {} items are waiting; {} merged into the newest so far", name, limit, merged);
			} else {
				queue.addLast(item);
			}
			if (scheduled) return;
			scheduled = true;
		}
		dispatch();
	}

	private void dispatch() {
		try {
			workers.execute(this::runOne);
		} catch (RejectedExecutionException stopped) {
			synchronized (this) {
				queue.clear();
				scheduled = false;
				notifyAll();
			}
		}
	}

	private void runOne() {
		T item;
		synchronized (this) {
			item = closed ? null : queue.pollFirst();
			if (item == null) {
				scheduled = false;
				notifyAll();
				return;
			}
		}
		try {
			worker.accept(item);
		} catch (Throwable failure) {
			LOGGER.warn("{}: {}", name, failure.toString());
		} finally {
			boolean more;
			synchronized (this) {
				more = !closed && !queue.isEmpty();
				if (!more) {
					scheduled = false;
					notifyAll();
				}
			}
			if (more) dispatch();
		}
	}

	/** Waits until every item submitted so far has run; false when {@code timeout} passed first. */
	public synchronized boolean drain(Duration timeout) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (scheduled) {
			long left = deadline - System.nanoTime();
			if (left <= 0) return false;
			TimeUnit.NANOSECONDS.timedWait(this, left);
		}
		return true;
	}

	/** Drops the held items and every later one; an item already running ends on its own. */
	public synchronized void close() {
		closed = true;
		queue.clear();
	}

	/** Items waiting to run. */
	public synchronized int pending() { return queue.size(); }

	/** Items merged into a newer one because the lane was full. */
	public synchronized long merged() { return merged; }
}
