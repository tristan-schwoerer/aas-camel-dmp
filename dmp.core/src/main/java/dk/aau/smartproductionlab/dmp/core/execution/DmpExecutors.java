/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.execution;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.camel.CamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The data plane's threads, one set per Camel context and bound in its registry: a bounded pool of workers that
 * runs evaluations, polls and event deliveries ({@link SerialLane} keeps each owner's work in order), and one
 * scheduler thread that only ticks and hands the work to the workers. No mapping, source or consumer has a thread
 * of its own, so the thread count does not grow with the number of mappings.
 *
 * <p>The worker count is {@code dmp.workerThreads} (default 32). Workers are created on demand and end after a
 * minute without work.
 */
public final class DmpExecutors {
	public static final String REGISTRY_NAME = "dmpExecutors";
	public static final int WORKER_THREADS = Integer.getInteger("dmp.workerThreads", 32);
	private static final Logger LOGGER = LoggerFactory.getLogger(DmpExecutors.class);

	private final ExecutorService workers;
	private final ScheduledExecutorService scheduler;

	private DmpExecutors(CamelContext context) {
		// Built here, not by Camel's pool factory: Camel turns an unbounded queue (-1) into a hand-off queue, which
		// rejects work once every worker is busy. Work waits in the queue instead; it ends with the context.
		ThreadPoolExecutor pool = new ThreadPoolExecutor(WORKER_THREADS, WORKER_THREADS, 60L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(), task -> {
					Thread thread = new Thread(task, context.getExecutorServiceManager().resolveThreadName("dmp-worker"));
					thread.setDaemon(true);
					return thread;
				});
		pool.allowCoreThreadTimeOut(true);
		workers = pool;
		scheduler = context.getExecutorServiceManager().newScheduledThreadPool(this, "dmp-scheduler", 1);
		try {
			context.addService(new org.apache.camel.support.service.ServiceSupport() {
				@Override protected void doStop() { pool.shutdownNow(); }
			}, true, true);
		} catch (Exception unexpected) {
			pool.shutdownNow();
			throw new IllegalStateException("could not tie the DMP workers to the Camel context", unexpected);
		}
	}

	/** The context's threads, created on first use (and again after the context stopped and shut them down). */
	public static DmpExecutors of(CamelContext context) {
		synchronized (context) {
			DmpExecutors bound = context.getRegistry().lookupByNameAndType(REGISTRY_NAME, DmpExecutors.class);
			if (bound == null || bound.workers.isShutdown() || bound.scheduler.isShutdown()) {
				bound = new DmpExecutors(context);
				context.getRegistry().bind(REGISTRY_NAME, DmpExecutors.class, bound);
			}
			return bound;
		}
	}

	/** The shared workers; {@link SerialLane} runs an owner's work on them one item at a time. */
	public ExecutorService workers() { return workers; }

	/**
	 * Runs {@code task} on a worker at a fixed rate, the first time after {@code initialDelay}. A tick while the
	 * previous run still lasts is skipped, so one task never runs twice at once and never occupies more than one
	 * worker.
	 */
	public Ticker every(Duration initialDelay, Duration period, Runnable task) {
		return new Ticker(initialDelay, period, task);
	}

	/** Runs {@code task} once on a worker after {@code delay}; a stopped context drops it. */
	public void later(Duration delay, Runnable task) {
		try {
			scheduler.schedule(() -> {
				try {
					workers.execute(task);
				} catch (RejectedExecutionException stopped) { /* the context stopped */ }
			}, delay.toMillis(), TimeUnit.MILLISECONDS);
		} catch (RejectedExecutionException stopped) { /* the context stopped */ }
	}

	/** A task {@link #every} runs; cancel it to stop. */
	public final class Ticker {
		private final AtomicBoolean running = new AtomicBoolean();
		private final ScheduledFuture<?> ticks;
		private volatile boolean cancelled;
		private volatile Thread runner;

		private Ticker(Duration initialDelay, Duration period, Runnable task) {
			ticks = scheduler.scheduleAtFixedRate(() -> {
				if (cancelled || !running.compareAndSet(false, true)) return;
				try {
					workers.execute(() -> {
						runner = Thread.currentThread();
						try {
							if (!cancelled) task.run();
						} catch (Throwable failure) {
							LOGGER.warn("scheduled task failed: {}", failure.toString());
						} finally {
							runner = null;
							synchronized (this) { running.set(false); notifyAll(); }
						}
					});
				} catch (RejectedExecutionException stopped) {
					running.set(false);
				}
			}, initialDelay.toMillis(), Math.max(1, period.toMillis()), TimeUnit.MILLISECONDS);
		}

		/**
		 * No further run starts; waits up to {@code timeout} for a run in progress to end, so what it creates can
		 * be cleaned up after. True when no run is in progress (a run calling this on itself does not wait).
		 */
		public boolean cancel(Duration timeout) throws InterruptedException {
			cancelled = true;
			ticks.cancel(false);
			if (runner == Thread.currentThread()) return false;
			long deadline = System.nanoTime() + timeout.toNanos();
			synchronized (this) {
				while (running.get()) {
					long left = deadline - System.nanoTime();
					if (left <= 0) return false;
					TimeUnit.NANOSECONDS.timedWait(this, left);
				}
			}
			return true;
		}
	}
}
