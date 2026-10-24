/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import static org.junit.jupiter.api.Assertions.*;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.junit.jupiter.api.Test;

/**
 * MQTT 5 request/reply on the shared session, against a peer that withholds selected acknowledgements: every wait
 * ends at the call's deadline, nothing is published before its reply subscription is confirmed on the current
 * connection, and no command is ever published twice (neither after a timeout, nor a reconnect, nor a restart).
 */
class DmpPahoMqtt5RequestReplyTest {
	@Test
	void aWithheldSubackEndsTheRequestAtItsDeadlineWithoutPublishing() throws Exception {
		try (var fixture = new Fixture(1)) {
			fixture.broker.withholdSubscription = 1;
			assertTimesOut(fixture, "withheld");
			assertEquals(0, fixture.broker.publications.get());
			fixture.stop();
		}
	}

	@Test
	void afterAReconnectTheReplySubscriptionIsConfirmedAgainBeforeAnythingIsPublished() throws Exception {
		try (var fixture = new Fixture(1)) {
			assertReply(fixture, "warm");
			fixture.broker.withholdSubscription = 2;
			fixture.broker.subscriber.close();
			assertTrue(fixture.broker.secondSubscription.await(5, TimeUnit.SECONDS), "the session subscribes again after the reconnect");
			assertTimesOut(fixture, "withheld");
			assertEquals(1, fixture.broker.publications.get(), "nothing is published unconfirmed, and the warm command is not replayed");
			fixture.stop();
		}
	}

	@Test
	void aWithheldPubackOrPubrecEndsTheRequestAndTheCommandIsNeverSentAgain() throws Exception {
		for (int qos : List.of(1, 2)) assertNoReplay(qos, false);
	}

	@Test
	void aWithheldPubcompEndsTheRequestAndTheCommandIsNeverSentAgain() throws Exception {
		assertNoReplay(2, true);
	}

	/** The command whose acknowledgement never came is not sent again on the next connection, nor after a restart. */
	private void assertNoReplay(int qos, boolean withholdPubcomp) throws Exception {
		try (var fixture = new Fixture(qos)) {
			fixture.broker.withholdPublicationAck = !withholdPubcomp;
			fixture.broker.withholdPubcomp = withholdPubcomp;
			assertTimesOut(fixture, "unknown-outcome");
			assertEquals(1, fixture.broker.publications.get());
			fixture.broker.withholdPublicationAck = false;
			fixture.broker.withholdPubcomp = false;
			fixture.broker.subscriber.close();
			assertTrue(fixture.broker.secondSubscription.await(5, TimeUnit.SECONDS), "the session reconnected");
			assertReply(fixture, "after-reconnect");
			assertFalse(fixture.broker.thirdPublication.await(1_500, TimeUnit.MILLISECONDS), "no QoS replay after the reconnect");
			assertEquals(2, fixture.broker.publications.get(), "only the new command is published");
			fixture.stop();
			fixture.producer.start();
			assertReply(fixture, "after-restart");
			assertEquals(3, fixture.broker.publications.get(), "no QoS replay after a restart");
			fixture.stop();
		}
	}

	@Test
	void subscriptionPublicationAndReplyShareOneDeadline() throws Exception {
		try (var fixture = new Fixture(1)) {
			fixture.broker.subscriptionDelayMillis = 600;
			fixture.broker.withholdPublicationAck = true;
			long started = System.nanoTime();
			assertTimesOut(fixture, "one-budget");
			assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 2_800);
			assertEquals(1, fixture.broker.subscriptions.get());
			assertEquals(1, fixture.broker.publications.get());
		}
	}

	@Test
	void thirtyTwoConcurrentCallersEachGetTheirOwnReply() throws Exception {
		try (var fixture = new Fixture(1)) {
			var callers = Executors.newFixedThreadPool(32);
			try {
				var calls = new java.util.ArrayList<java.util.concurrent.Future<Object>>();
				var start = new CountDownLatch(1);
				for (int caller = 0; caller < 32; caller++) {
					calls.add(callers.submit(() -> {
						start.await();
						var request = fixture.request();
						fixture.producer.process(request);
						return request.getMessage().getBody();
					}));
				}
				start.countDown();
				for (var call : calls) assertEquals("{\"State\":\"SUCCESS\"}", call.get(10, TimeUnit.SECONDS));
			} finally {
				callers.shutdownNow();
			}
			assertEquals(32, fixture.broker.publications.get(), "each call is published once");
			assertEquals(1, fixture.broker.subscriptions.get(), "concurrent calls share one confirmed subscription");
			assertEquals(0, fixture.endpoint.replies.pendingCount(), "every reply was matched and released");
		}
	}

	@Test
	void theReplySubscriptionIsConfirmedOncePerConnectionNotPerCall() throws Exception {
		try (var fixture = new Fixture(1)) {
			for (int call = 0; call < 3; call++) assertReply(fixture, "call-" + call);
			assertEquals(3, fixture.broker.publications.get());
			assertEquals(1, fixture.broker.subscriptions.get());
			assertEquals(1, fixture.broker.clientIds.size(), "requests and replies share one connection");
		}
	}

	@Test
	void aStopEndsARequestInFlightAtOnceAndARestartedProducerNeverPublishesIt() throws Exception {
		try (var fixture = new Fixture(1, 30)) {
			fixture.broker.subscriptionDelayMillis = 800;
			var request = fixture.executor.submit(() -> assertThrows(Exception.class, () -> fixture.producer.process(fixture.request())));
			Thread.sleep(200);
			long stopping = System.nanoTime();
			fixture.stop();
			assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopping) < 2_000, "the stop did not wait for the request");
			fixture.producer.start();
			request.get(5, TimeUnit.SECONDS);
			Thread.sleep(1_000);
			assertEquals(0, fixture.broker.publications.get(), "a request admitted before the stop is never published");
			fixture.stop();
		}
	}

	@Test
	void aOneWayPublicationAlsoEndsAtItsDeadline() throws Exception {
		try (var fixture = new Fixture(1)) {
			fixture.broker.withholdPublicationAck = true;
			var request = fixture.request();
			request.setPattern(ExchangePattern.InOnly);
			var failure = assertThrows(ConnectorInvocationException.class, () -> fixture.producer.process(request));
			assertEquals("MQTT_REQUEST_TIMEOUT", failure.getConnectorCode());
			assertEquals(0, fixture.broker.subscriptions.get(), "a one-way publication subscribes nothing");
			assertEquals(1, fixture.broker.publications.get());
			fixture.stop();
		}
	}

	private static void assertTimesOut(Fixture fixture, String label) throws Exception {
		var failure = assertThrows(ConnectorInvocationException.class, () -> fixture.producer.process(fixture.request()), label);
		assertEquals("MQTT_REQUEST_TIMEOUT", failure.getConnectorCode(), label);
	}

	private static void assertReply(Fixture fixture, String label) throws Exception {
		var request = fixture.request();
		fixture.producer.process(request);
		assertEquals("{\"State\":\"SUCCESS\"}", request.getMessage().getBody(), label);
	}

	static final class Fixture implements AutoCloseable {
		final PacketBroker broker = new PacketBroker();
		final DefaultCamelContext context = new DefaultCamelContext();
		final java.util.concurrent.ExecutorService executor = Executors.newFixedThreadPool(2);
		final DmpPahoMqtt5Endpoint endpoint;
		final Producer producer;
		Fixture(int qos) throws Exception { this(qos, 1); }
		Fixture(int qos, int replyTimeout) throws Exception {
			context.start();
			endpoint = context.getEndpoint(DmpPahoMqtt5Component.SCHEME + ":command?brokerUrl=" + broker.uri()
					+ "&name=deadline-" + UUID.randomUUID() + "&replyTopic=reply&qos=" + qos
					+ "&replyTimeout=" + replyTimeout, DmpPahoMqtt5Endpoint.class);
			producer = endpoint.createProducer();
			producer.start();
		}
		DefaultExchange request() {
			var exchange = new DefaultExchange(context, ExchangePattern.InOut);
			exchange.getMessage().setBody(Map.of("value", 1));
			return exchange;
		}
		/** Stops the producer: nothing pending, and the session it alone held is closed. */
		void stop() {
			producer.stop();
			assertEquals(0, endpoint.replies.pendingCount());
			assertEquals(0, context.getComponent(DmpPahoMqtt5Component.SCHEME, DmpPahoMqtt5Component.class).sessionCount());
		}
		@Override public void close() throws Exception {
			try { producer.stop(); }
			finally {
				context.stop();
				broker.close();
				executor.shutdownNow();
				assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
				// Paho's threads end shortly after its client closed.
				long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
				for (Thread thread : pahoThreads()) {
					long remaining = deadline - System.nanoTime();
					if (remaining > 0) thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
				}
				assertTrue(pahoThreads().isEmpty(), () -> "the session's Paho threads must end: "
						+ pahoThreads().stream().map(Thread::getName).toList());
			}
		}
		List<Thread> pahoThreads() {
			return Thread.getAllStackTraces().keySet().stream().filter(thread -> thread.isAlive()
					&& thread.getName().startsWith("MQTT ")
					&& broker.clientIds.stream().anyMatch(id -> thread.getName().endsWith(id))).toList();
		}
	}

	/** Minimal peer, intentionally keeps TCP and PINGRESP alive without selected ACKs. */
	static final class PacketBroker implements AutoCloseable {
		final ServerSocket server = new ServerSocket(0, 10, InetAddress.getLoopbackAddress());
		final java.util.concurrent.ScheduledExecutorService workers = Executors.newScheduledThreadPool(5);
		final List<Socket> sockets = new CopyOnWriteArrayList<>();
		final List<Throwable> failures = new CopyOnWriteArrayList<>();
		final java.util.Set<String> clientIds = java.util.concurrent.ConcurrentHashMap.newKeySet();
		final AtomicInteger subscriptions = new AtomicInteger();
		final AtomicInteger publications = new AtomicInteger();
		final AtomicInteger pings = new AtomicInteger();
		final CountDownLatch withheld = new CountDownLatch(1);
		final CountDownLatch thirdPublication = new CountDownLatch(3);
		final CountDownLatch secondSubscription = new CountDownLatch(2);
		/** Withholds the SUBACK of this SUBSCRIBE and of every later one (0: none). */
		volatile int withholdSubscription;
		volatile boolean withholdConnectAck;
		volatile boolean withholdPublicationAck;
		volatile boolean withholdPubcomp;
		volatile long subscriptionDelayMillis;
		volatile Socket subscriber;
		volatile boolean closing;
		PacketBroker() throws IOException {
			workers.submit(() -> {
				try {
					while (!closing) {
						Socket socket = server.accept();
						sockets.add(socket);
						workers.submit(() -> serve(socket));
					}
				} catch (IOException e) { if (!closing) failures.add(e); }
			});
		}
		String uri() { return "tcp://127.0.0.1:" + server.getLocalPort(); }
		void dropClients() throws IOException {
			for (var socket : sockets) socket.close();
		}
		void serve(Socket socket) {
			try (socket; var input = new DataInputStream(socket.getInputStream())) {
				while (!closing) {
					int header = input.readUnsignedByte();
					byte[] body = input.readNBytes(variableInt(input));
					switch (header >> 4) {
					case 1 -> {
						var connect = new DataInputStream(new java.io.ByteArrayInputStream(body));
						connect.skipNBytes(connect.readUnsignedShort() + 2L); // protocol, version, flags
						connect.readUnsignedShort(); // keepalive
						connect.skipNBytes(variableInt(connect));
						clientIds.add(connect.readUTF());
						if (withholdConnectAck) withheld.countDown();
						else send(socket, 0x20, new byte[] {0, 0, 0});
					}
					case 8 -> {
						subscriber = socket;
						int number = subscriptions.incrementAndGet();
						secondSubscription.countDown();
						if (withholdSubscription > 0 && number >= withholdSubscription) withheld.countDown();
						else workers.schedule(() -> {
							try { send(socket, 0x90, new byte[] {body[0], body[1], 0, body[body.length - 1]}); }
							catch (IOException e) { if (!closing && !socket.isClosed()) failures.add(e); }
						}, subscriptionDelayMillis, TimeUnit.MILLISECONDS);
					}
					case 3 -> {
						publications.incrementAndGet();
						thirdPublication.countDown();
						int qos = (header >> 1) & 3;
						int topicLength = ((body[0] & 255) << 8) | (body[1] & 255);
						int offset = 2 + topicLength;
						if (withholdPublicationAck) { withheld.countDown(); break; }
						if (qos > 0) send(socket, qos == 1 ? 0x40 : 0x50, new byte[] {body[offset], body[offset + 1]});
						if (!withholdPubcomp && subscriber != null) reply(body, offset + (qos > 0 ? 2 : 0));
					}
					case 6 -> {
						if (withholdPubcomp) withheld.countDown();
						else send(socket, 0x70, new byte[] {body[0], body[1]});
					}
					case 10 -> send(socket, 0xb0, new byte[] {body[0], body[1], 0, 0});
					case 12 -> { pings.incrementAndGet(); send(socket, 0xd0, new byte[0]); }
					case 14 -> { return; }
					default -> throw new IllegalStateException("Unexpected MQTT packet type " + (header >> 4));
					}
				}
			} catch (EOFException e) { /* Client retirement closes the connection. */ }
			catch (IOException e) { if (!closing && !socket.isClosed()) failures.add(e); }
			catch (Throwable failure) { failures.add(failure); }
		}
		void reply(byte[] request, int offset) throws IOException {
			var input = new DataInputStream(new java.io.ByteArrayInputStream(request, offset, request.length - offset));
			int length = variableInt(input);
			var properties = new DataInputStream(new java.io.ByteArrayInputStream(input.readNBytes(length)));
			byte[] correlation = null;
			while (properties.available() > 0) {
				int type = properties.readUnsignedByte();
				byte[] value = properties.readNBytes(properties.readUnsignedShort());
				if (type == 9) correlation = value;
				else if (type != 8) throw new IOException("Unexpected request property " + type);
			}
			if (correlation == null) return;
			var bytes = new ByteArrayOutputStream();
			bytes.write(new byte[] {0, 5});
			bytes.write("reply".getBytes(StandardCharsets.UTF_8));
			writeVariableInt(bytes, correlation.length + 3);
			bytes.write(9);
			bytes.write(correlation.length >> 8);
			bytes.write(correlation.length & 255);
			bytes.write(correlation);
			bytes.write("{\"State\":\"SUCCESS\"}".getBytes(StandardCharsets.UTF_8));
			send(subscriber, 0x30, bytes.toByteArray());
		}
		static int variableInt(DataInputStream input) throws IOException {
			int value = 0, multiplier = 1, digit;
			do {
				digit = input.readUnsignedByte();
				value += (digit & 127) * multiplier;
				multiplier *= 128;
			} while ((digit & 128) != 0);
			return value;
		}
		static void writeVariableInt(ByteArrayOutputStream output, int value) {
			do {
				int digit = value % 128;
				value /= 128;
				output.write(digit | (value > 0 ? 128 : 0));
			} while (value > 0);
		}
		static void send(Socket socket, int header, byte[] body) throws IOException {
			synchronized (socket) {
				var packet = new ByteArrayOutputStream();
				packet.write(header);
				writeVariableInt(packet, body.length);
				packet.write(body);
				socket.getOutputStream().write(packet.toByteArray());
				socket.getOutputStream().flush();
			}
		}
		@Override public void close() throws Exception {
			closing = true;
			server.close();
			for (var socket : sockets) socket.close();
			workers.shutdownNow();
			assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
			assertTrue(failures.isEmpty(), () -> "Packet broker failures: " + failures);
		}
	}
}
