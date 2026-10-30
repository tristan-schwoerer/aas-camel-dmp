/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.execution;

import java.time.Duration;
import java.util.Map;

import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.support.DefaultEndpoint;

/**
 * {@code dmp-poll:<name>?period=<ms>[&delay=<ms>]}: an empty message every {@code period} milliseconds, at a fixed
 * rate, the first after {@code delay} (default 1000, as {@code timer:}). Unlike {@code timer:}, which keeps a
 * thread per timer name, the ticks come from the context's one scheduler and the route runs on a shared worker
 * ({@link DmpExecutors#every}); a tick while the previous message is still being processed is skipped.
 * {@code dmp-poll:<name>?once=true} sends one message when its route starts.
 */
public final class DmpPollComponent extends DefaultComponent {
	public static final String SCHEME = "dmp-poll";

	/** The endpoint URI of a poll named {@code name} (only letters, digits, {@code _ . -} are kept). */
	public static String uri(String name, long periodMs) {
		return SCHEME + ":" + name.replaceAll("[^A-Za-z0-9_.-]", "-") + "?period=" + periodMs;
	}

	/** The endpoint URI of a single message named {@code name}, sent when its route starts. */
	public static String once(String name) {
		return SCHEME + ":" + name.replaceAll("[^A-Za-z0-9_.-]", "-") + "?once=true";
	}

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		boolean once = getAndRemoveParameter(parameters, "once", Boolean.class, false);
		Long period = getAndRemoveParameter(parameters, "period", Long.class);
		if (once) {
			if (period != null) throw new IllegalArgumentException("dmp-poll endpoint " + remaining + " is sent once and takes no period");
			// The one message is sent at once; the ticker stops itself after it (PollConsumer.poll).
			period = Duration.ofDays(1).toMillis();
		}
		if (period == null || period <= 0) throw new IllegalArgumentException("dmp-poll endpoint " + remaining + " needs a positive period");
		long delay = getAndRemoveParameter(parameters, "delay", Long.class, once ? 0L : 1000L);
		if (delay < 0) throw new IllegalArgumentException("dmp-poll endpoint " + remaining + " has a negative delay");
		return new PollEndpoint(uri, this, Duration.ofMillis(delay), Duration.ofMillis(period), once);
	}

	private static final class PollEndpoint extends DefaultEndpoint {
		private final Duration delay;
		private final Duration period;
		private final boolean once;

		PollEndpoint(String uri, DmpPollComponent component, Duration delay, Duration period, boolean once) {
			super(uri, component);
			this.delay = delay;
			this.period = period;
			this.once = once;
		}

		@Override public Producer createProducer() { throw new UnsupportedOperationException("dmp-poll endpoints only consume"); }

		@Override public Consumer createConsumer(Processor processor) throws Exception {
			Consumer consumer = new PollConsumer(this, processor);
			configureConsumer(consumer);
			return consumer;
		}
	}

	private static final class PollConsumer extends DefaultConsumer {
		private volatile DmpExecutors.Ticker ticker;
		/** A {@code once} endpoint's message was sent; a later tick sends nothing. */
		private final java.util.concurrent.atomic.AtomicBoolean sent = new java.util.concurrent.atomic.AtomicBoolean();

		PollConsumer(PollEndpoint endpoint, Processor processor) { super(endpoint, processor); }

		@Override protected void doStart() throws Exception {
			super.doStart();
			PollEndpoint endpoint = (PollEndpoint) getEndpoint();
			sent.set(false);
			ticker = DmpExecutors.of(endpoint.getCamelContext()).every(endpoint.delay, endpoint.period, this::poll);
		}

		private void poll() {
			if (!isRunAllowed()) return;
			boolean once = ((PollEndpoint) getEndpoint()).once;
			if (once && !sent.compareAndSet(false, true)) {
				stopTicking();
				return;
			}
			Exchange exchange = createExchange(false);
			try {
				getProcessor().process(exchange);
			} catch (Exception failure) {
				exchange.setException(failure);
			}
			if (exchange.getException() != null) getExceptionHandler().handleException("poll failed", exchange, exchange.getException());
			releaseExchange(exchange, false);
			if (once) stopTicking();
		}

		/** Called from the ticker's own run, so it does not wait for itself; before the ticker is stored, a later tick stops it. */
		private void stopTicking() {
			DmpExecutors.Ticker current = ticker;
			if (current == null) return;
			try {
				current.cancel(Duration.ZERO);
			} catch (InterruptedException interrupted) {
				Thread.currentThread().interrupt();
			}
		}

		@Override protected void doStop() throws Exception {
			// A poll in progress ends before the route's downstream may stop (bounded by a read's timeout).
			if (ticker != null) ticker.cancel(Duration.ofSeconds(30));
			ticker = null;
			super.doStop();
		}
	}
}
