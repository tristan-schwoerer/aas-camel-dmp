/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.observe;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.support.DefaultConsumer;

/**
 * Observes one HTTP resource on its own thread until stopped. A failed request or
 * a broken stream is reported and retried with backoff (1 s doubling to 30 s);
 * values missed in between are lost, as with any observation (SPEC §5.2).
 */
final class DmpHttpObserveConsumer extends DefaultConsumer {
	static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
	/** The longest a server may hold a long poll before the DMP asks again. */
	static final Duration LONGPOLL_TIMEOUT = Duration.ofMinutes(5);
	/** Spacing between long polls, so a server that answers at once is not hammered. */
	static final long LONGPOLL_GAP_MS = 100;
	static final long MIN_BACKOFF_MS = 1_000;
	static final long MAX_BACKOFF_MS = 30_000;
	/** The SSE reconnection time until the server sets one with {@code retry:}. */
	static final long DEFAULT_SSE_RETRY_MS = 3_000;
	/** An event stream silent this long (no event, no comment) is taken as dead and reopened. */
	static final Duration SSE_IDLE_TIMEOUT = Duration.ofMinutes(5);

	private final DmpHttpObserveEndpoint endpoint;
	private final SseParser parser = new SseParser();
	private HttpClient client;
	private ExecutorService executor;
	private volatile boolean running;
	private volatile CompletableFuture<?> inFlight;
	private volatile InputStream stream;
	private volatile long lastRead;
	private ScheduledExecutorService watchdog;
	private final Duration idleTimeout;

	DmpHttpObserveConsumer(DmpHttpObserveEndpoint endpoint, Processor processor) {
		super(endpoint, processor);
		this.endpoint = endpoint;
		this.idleTimeout = endpoint.idleTimeout();
	}

	@Override protected void doStart() throws Exception {
		super.doStart();
		client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(CONNECT_TIMEOUT).build();
		running = true;
		executor = getEndpoint().getCamelContext().getExecutorServiceManager().newSingleThreadExecutor(this, "dmp-http-observe");
		executor.execute(this::run);
		if (endpoint.subprotocol() == DmpHttpObserveComponent.Subprotocol.SSE) {
			// A stream whose peer vanished without a FIN never ends a read; closing it does.
			long period = Math.max(10, Math.min(10_000, idleTimeout.toMillis() / 4));
			watchdog = getEndpoint().getCamelContext().getExecutorServiceManager()
					.newSingleThreadScheduledExecutor(this, "dmp-http-observe-idle");
			watchdog.scheduleWithFixedDelay(this::closeIfIdle, period, period, TimeUnit.MILLISECONDS);
		}
	}

	@Override protected void doStop() throws Exception {
		running = false;
		CompletableFuture<?> request = inFlight;
		if (request != null) request.cancel(true);
		InputStream open = stream;
		if (open != null) {
			try { open.close(); } catch (IOException ignored) { /* stopping */ }
		}
		if (executor != null) getEndpoint().getCamelContext().getExecutorServiceManager().shutdownNow(executor);
		executor = null;
		if (watchdog != null) getEndpoint().getCamelContext().getExecutorServiceManager().shutdownNow(watchdog);
		watchdog = null;
		if (client != null) client.shutdownNow();
		client = null;
		super.doStop();
	}

	private void run() {
		long backoff = MIN_BACKOFF_MS;
		while (running) {
			try {
				if (endpoint.subprotocol() == DmpHttpObserveComponent.Subprotocol.SSE) stream();
				else longPoll();
				backoff = MIN_BACKOFF_MS;
			} catch (InterruptedException stopped) {
				Thread.currentThread().interrupt();
				return;
			} catch (Exception failure) {
				if (!running) return;
				getExceptionHandler().handleException("HTTP observation of " + endpoint.url() + " failed", failure);
				try { Thread.sleep(backoff); }
				catch (InterruptedException stopped) { Thread.currentThread().interrupt(); return; }
				backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
			}
		}
	}

	/** One long poll: the answer, if it carries a body, is the next value. */
	private void longPoll() throws Exception {
		HttpRequest request = request().timeout(LONGPOLL_TIMEOUT).header("Accept", endpoint.contentType()).GET().build();
		HttpResponse<String> response;
		try {
			response = await(client.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)));
		} catch (HttpConnectTimeoutException unreachable) {
			throw unreachable; // the server is not there: back off
		} catch (HttpTimeoutException quiet) {
			return; // nothing changed while the server held the request
		}
		if (response.statusCode() < 200 || response.statusCode() >= 300) throw new IOException("HTTP " + response.statusCode());
		String body = response.body();
		if (response.statusCode() != 204 && body != null && !body.isEmpty()) emit(body);
		Thread.sleep(LONGPOLL_GAP_MS);
	}

	/** One event-stream connection, read until it ends; then wait the reconnection time. */
	private void stream() throws Exception {
		HttpRequest.Builder builder = request().header("Accept", "text/event-stream").header("Cache-Control", "no-cache");
		if (parser.lastEventId != null && !parser.lastEventId.isEmpty()) builder.header("Last-Event-ID", parser.lastEventId);
		HttpResponse<InputStream> response = await(client.sendAsync(builder.GET().build(), HttpResponse.BodyHandlers.ofInputStream()));
		try (InputStream body = response.body()) {
			stream = body;
			lastRead = System.nanoTime();
			if (response.statusCode() < 200 || response.statusCode() >= 300) throw new IOException("HTTP " + response.statusCode());
			String type = response.headers().firstValue("Content-Type").orElse("");
			if (!type.toLowerCase(Locale.ROOT).startsWith("text/event-stream"))
				throw new IOException("the response is not an event stream: " + type);
			BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8));
			parser.reset();
			String line;
			boolean first = true;
			while (running && (line = reader.readLine()) != null) {
				lastRead = System.nanoTime();
				if (first && line.startsWith("﻿")) line = line.substring(1);
				first = false;
				String data = parser.line(line);
				if (data != null) emit(data);
			}
		} catch (IOException closed) {
			if (!running) return;
			throw closed;
		} finally {
			stream = null;
		}
		if (running) Thread.sleep(parser.retryMs);
	}

	private void closeIfIdle() {
		InputStream open = stream;
		if (open == null || System.nanoTime() - lastRead < idleTimeout.toNanos()) return;
		try { open.close(); } catch (IOException ignored) { /* the reader fails and reconnects */ }
	}

	private HttpRequest.Builder request() {
		HttpRequest.Builder builder = HttpRequest.newBuilder(endpoint.requestUrl());
		endpoint.headers().forEach(builder::setHeader);
		return builder;
	}

	private <T> T await(CompletableFuture<T> request) throws Exception {
		inFlight = request;
		try {
			return request.get();
		} catch (ExecutionException failed) {
			throw failed.getCause() instanceof Exception cause ? cause : failed;
		} finally {
			inFlight = null;
		}
	}

	private void emit(String value) {
		Exchange exchange = createExchange(false);
		exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, endpoint.contentType());
		exchange.getMessage().setBody(value);
		try { getProcessor().process(exchange); }
		catch (Exception failure) { exchange.setException(failure); }
		if (exchange.getException() != null)
			getExceptionHandler().handleException("HTTP observed value failed", exchange, exchange.getException());
		releaseExchange(exchange, false);
	}

	/** The event-stream interpretation of the HTML Living Standard (§9.2.6), reduced to data, id and retry. */
	static final class SseParser {
		private final StringBuilder data = new StringBuilder();
		private boolean hasData;
		String lastEventId;
		long retryMs = DEFAULT_SSE_RETRY_MS;

		/** Forgets a partial event (a new connection starts afresh); keeps the last event id and retry. */
		void reset() {
			data.setLength(0);
			hasData = false;
		}

		/** Consumes one line; returns the data of the event that this line dispatches, else null. */
		String line(String line) {
			if (line.isEmpty()) {
				if (!hasData) return null;
				String event = data.toString();
				reset();
				return event;
			}
			if (line.startsWith(":")) return null;
			int colon = line.indexOf(':');
			String field = colon < 0 ? line : line.substring(0, colon);
			String value = colon < 0 ? "" : line.substring(colon + 1);
			if (value.startsWith(" ")) value = value.substring(1);
			switch (field) {
			case "data" -> {
				if (hasData) data.append('\n');
				data.append(value);
				hasData = true;
			}
			case "id" -> {
				if (value.indexOf('\0') < 0) lastEventId = value;
			}
			case "retry" -> {
				if (!value.isEmpty() && value.chars().allMatch(c -> c >= '0' && c <= '9')) {
					try { retryMs = Long.parseLong(value); } catch (NumberFormatException tooLarge) { /* ignored */ }
				}
			}
			default -> { /* event types and unknown fields carry no value */ }
			}
			return null;
		}
	}
}
