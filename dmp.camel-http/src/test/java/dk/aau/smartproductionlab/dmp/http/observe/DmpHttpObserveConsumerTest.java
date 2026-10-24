/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.observe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

class DmpHttpObserveConsumerTest {
	private HttpServer server;
	private CamelContext camel;
	private final BlockingQueue<Exchange> received = new LinkedBlockingQueue<>();

	@BeforeEach void start() throws Exception {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
		server.start();
	}

	@AfterEach void stop() throws Exception {
		if (camel != null) camel.stop();
		server.stop(0);
	}

	@Test void parsesEventStreamsAsTheHtmlStandardDescribes() {
		var parser = new DmpHttpObserveConsumer.SseParser();
		assertNull(parser.line(": a comment"));
		assertNull(parser.line("id: 7"));
		assertNull(parser.line("retry: 250"));
		assertNull(parser.line("event: update"));
		assertNull(parser.line("data: {\"a\":"));
		assertNull(parser.line("data:1}"));
		assertEquals("{\"a\":\n1}", parser.line(""));
		assertNull(parser.line(""), "a blank line without data dispatches nothing");
		assertEquals("7", parser.lastEventId);
		assertEquals(250, parser.retryMs);
		assertNull(parser.line("data"));
		assertEquals("", parser.line(""), "a data field without a value is an empty value");
	}

	@Test void eachServerSentEventIsOneValueAndAReconnectResumesFromTheLastId() throws Exception {
		List<String> lastEventIds = new CopyOnWriteArrayList<>();
		AtomicInteger connections = new AtomicInteger();
		server.createContext("/events", exchange -> {
			lastEventIds.add(String.valueOf(exchange.getRequestHeaders().getFirst("Last-Event-ID")));
			exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
			exchange.sendResponseHeaders(200, 0);
			try (OutputStream body = exchange.getResponseBody()) {
				int connection = connections.incrementAndGet();
				String events = connection == 1
						? "retry: 50\r\nid: 1\r\ndata: {\"v\":1}\r\n\r\n: keep-alive\n\nid: 2\ndata: {\"v\":2}\n\n"
						: "data: {\"v\":3}\n\n";
				body.write(events.getBytes(StandardCharsets.UTF_8));
				body.flush();
			}
		});
		observe("sse", "/events");

		assertEquals("{\"v\":1}", next().getMessage().getBody(String.class));
		Exchange second = next();
		assertEquals("{\"v\":2}", second.getMessage().getBody(String.class));
		assertEquals("application/json", second.getMessage().getHeader(Exchange.CONTENT_TYPE));
		assertEquals("{\"v\":3}", next().getMessage().getBody(String.class), "the stream ended and was reopened");
		assertEquals("null", lastEventIds.get(0));
		assertEquals("2", lastEventIds.get(1));
	}

	@Test void aLongPollIsRepeatedAndAnEmptyAnswerIsNoValue() throws Exception {
		AtomicInteger polls = new AtomicInteger();
		server.createContext("/value", exchange -> {
			int poll = polls.incrementAndGet();
			if (poll == 2) {
				exchange.sendResponseHeaders(204, -1);
				exchange.close();
				return;
			}
			byte[] body = Integer.toString(poll).getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream out = exchange.getResponseBody()) { out.write(body); }
		});
		observe("longpoll", "/value");

		assertEquals("1", next().getMessage().getBody(String.class));
		assertEquals("3", next().getMessage().getBody(String.class), "the 204 answer delivered nothing");
	}

	/** A peer that vanished keeps the connection open without a word: the stream is reopened after the idle timeout. */
	@Test void aSilentStreamIsReopened() throws Exception {
		AtomicInteger connections = new AtomicInteger();
		java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
		server.createContext("/silent", exchange -> {
			exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
			exchange.sendResponseHeaders(200, 0);
			OutputStream body = exchange.getResponseBody();
			body.write(("retry: 10\ndata: " + connections.incrementAndGet() + "\n\n").getBytes(StandardCharsets.UTF_8));
			body.flush();
			try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException stopped) { Thread.currentThread().interrupt(); }
			try { body.close(); } catch (java.io.IOException gone) { /* the client closed it */ }
		});
		observe("sse", "/silent", "&idleTimeout=300");
		try {
			assertEquals("1", next().getMessage().getBody(String.class));
			assertEquals("2", next().getMessage().getBody(String.class), "the silent stream was closed and reopened");
		} finally {
			release.countDown();
		}
	}

	@Test void aFailedRequestIsRetried() throws Exception {
		AtomicInteger polls = new AtomicInteger();
		server.createContext("/flaky", exchange -> {
			if (polls.incrementAndGet() == 1) {
				exchange.sendResponseHeaders(503, -1);
				exchange.close();
				return;
			}
			byte[] body = "42".getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream out = exchange.getResponseBody()) { out.write(body); }
		});
		observe("longpoll", "/flaky");

		assertEquals("42", next().getMessage().getBody(String.class));
	}

	private void observe(String subprotocol, String path) throws Exception {
		observe(subprotocol, path, "");
	}

	private void observe(String subprotocol, String path, String options) throws Exception {
		String url = "http://127.0.0.1:" + server.getAddress().getPort() + path;
		camel = new DefaultCamelContext();
		camel.addRoutes(new RouteBuilder() {
			@Override public void configure() {
				from(DmpHttpObserveComponent.uri(subprotocol, url, null) + options).process(received::add);
			}
		});
		camel.start();
	}

	private Exchange next() throws InterruptedException {
		Exchange exchange = received.poll(10, TimeUnit.SECONDS);
		assertTrue(exchange != null, "no observed value within 10 s");
		return exchange;
	}
}
