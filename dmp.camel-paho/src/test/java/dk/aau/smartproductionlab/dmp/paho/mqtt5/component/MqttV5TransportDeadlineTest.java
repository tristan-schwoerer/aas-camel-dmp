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
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.junit.jupiter.api.Test;

/** Real upstream Camel/Paho clients against a loopback MQTT5 packet peer. */
class MqttV5TransportDeadlineTest {
	@Test
	void missingInitialSubackIsFiniteAndStopCannotWaitOnTheProducerMonitor() throws Exception {
		assertSubscriptionDeadline(1, false);
	}

	@Test
	void missingInitialReadinessSubackIsFiniteWithoutPublication() throws Exception {
		assertSubscriptionDeadline(2, false);
	}

	@Test
	void missingSubsequentReadinessSubackDoesNotReplayThePreviousCommand() throws Exception {
		// A warm call confirms nothing again; after a reply reconnect the confirmation is due once more.
		try (var fixture = new Fixture(1)) {
			fixture.producer.process(fixture.request("warm"));
			fixture.broker.withholdSubscription = 4;
			fixture.broker.subscriber.close();
			assertTrue(fixture.broker.thirdSubscription.await(4, TimeUnit.SECONDS), "upstream resubscribes after the reconnect");
			var request = fixture.executor.submit(() -> assertThrows(ConnectorInvocationException.class,
					() -> fixture.producer.process(fixture.request("withheld"))));
			assertTrue(fixture.broker.withheld.await(3, TimeUnit.SECONDS));
			assertEquals("MQTT_REQUEST_TIMEOUT", request.get(4, TimeUnit.SECONDS).getConnectorCode());
			fixture.producer.stop();
			assertEquals(1, fixture.broker.publications.get(), "the previous command is not replayed");
			assertEquals(4, fixture.broker.subscriptions.get());
			fixture.assertRetired();
		}
	}

	private void assertSubscriptionDeadline(int withheldSubscription, boolean warm) throws Exception {
		try (var fixture = new Fixture(1)) {
			fixture.broker.withholdSubscription = withheldSubscription;
			if (warm) fixture.producer.process(fixture.request("warm"));
			var request = fixture.executor.submit(() -> {
				return assertThrows(ConnectorInvocationException.class,
						() -> fixture.producer.process(fixture.request("withheld")));
			});
			assertTrue(fixture.broker.withheld.await(3, TimeUnit.SECONDS));
			var stop = fixture.executor.submit(() -> { fixture.producer.stop(); return null; });
			assertEquals("MQTT_REQUEST_TIMEOUT", request.get(4, TimeUnit.SECONDS).getConnectorCode());
			stop.get(4, TimeUnit.SECONDS);
			assertEquals(warm ? 1 : 0, fixture.broker.publications.get());
			assertEquals(withheldSubscription, fixture.broker.subscriptions.get());
			assertTrue(fixture.broker.pings.get() > 0, "peer answered keepalive while withholding SUBACK");
			fixture.assertRetired();
		}
	}

	@Test
	void missingPubackAndPubrecAreFiniteAndNeverReplayOnStopOrRestart() throws Exception {
		for (int qos : List.of(1, 2)) {
			assertPublicationDeadline(qos, false);
		}
	}

	@Test
	void missingPubcompIsFiniteAndNeverReplaysTheCommand() throws Exception {
		assertPublicationDeadline(2, true);
	}

	private void assertPublicationDeadline(int qos, boolean withholdPubcomp) throws Exception {
		try (var fixture = new Fixture(qos)) {
			fixture.broker.withholdPublicationAck = !withholdPubcomp;
			fixture.broker.withholdPubcomp = withholdPubcomp;
			var request = fixture.executor.submit(() -> assertThrows(ConnectorInvocationException.class,
					() -> fixture.producer.process(fixture.request("unknown-outcome"))));
			assertTrue(fixture.broker.withheld.await(3, TimeUnit.SECONDS));
			var stop = fixture.executor.submit(() -> { fixture.producer.stop(); return null; });
			assertEquals("MQTT_REQUEST_TIMEOUT", request.get(4, TimeUnit.SECONDS).getConnectorCode());
			stop.get(4, TimeUnit.SECONDS);
			assertEquals(1, fixture.broker.publications.get());
			assertTrue(fixture.broker.pings.get() > 0, "peer answered keepalive while withholding publication ACK");
			fixture.assertRetired();
			fixture.producer.start();
			fixture.broker.withholdPublicationAck = false;
			fixture.broker.withholdPubcomp = false;
			fixture.producer.process(fixture.request("after-restart"));
			assertFalse(fixture.broker.thirdPublication.await(1_500, TimeUnit.MILLISECONDS), "no QoS replay after restart");
			assertEquals(2, fixture.broker.publications.get(), "only the new command is published");
		}
	}

	@Test
	void readinessAndPublicationShareOneDeadlineRatherThanResettingTheBudget() throws Exception {
		try (var fixture = new Fixture(1)) {
			fixture.broker.subscriptionDelayMillis = 600;
			fixture.broker.withholdPublicationAck = true;
			long started = System.nanoTime();
			var request = fixture.executor.submit(() -> assertThrows(ConnectorInvocationException.class,
					() -> fixture.producer.process(fixture.request("one-budget"))));
			assertEquals("MQTT_REQUEST_TIMEOUT", request.get(3, TimeUnit.SECONDS).getConnectorCode());
			assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 2_800);
			assertEquals(2, fixture.broker.subscriptions.get());
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
					String label = "caller-" + caller;
					calls.add(callers.submit(() -> {
						start.await();
						var request = fixture.request(label);
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
			assertEquals(2, fixture.broker.subscriptions.get(), "concurrent calls share one confirmed subscription");
			assertEquals(0, fixture.endpoint.replies.pendingCount(), "every reply was matched and released");
		}
	}

	@Test
	void theReplySubscriptionIsConfirmedOncePerConnectionNotPerCall() throws Exception {
		try (var fixture = new Fixture(1)) {
			for (int call = 0; call < 3; call++) {
				var request = fixture.request("call-" + call);
				fixture.producer.process(request);
				assertEquals("{\"State\":\"SUCCESS\"}", request.getMessage().getBody());
			}
			assertEquals(3, fixture.broker.publications.get());
			assertEquals(2, fixture.broker.subscriptions.get(), "upstream's SUBSCRIBE and one confirmation, not one per call");
		}
	}

	@Test
	void replyReconnectRestoresSubscriptionWithoutReplayingAPreviousCommand() throws Exception {
		try (var fixture = new Fixture(1)) {
			fixture.producer.process(fixture.request("before-reconnect"));
			fixture.broker.subscriber.close();
			assertTrue(fixture.broker.thirdSubscription.await(4, TimeUnit.SECONDS));
			assertEquals(1, fixture.broker.publications.get(), "reply reconnect never replays commands");
			var request = fixture.request("after-reconnect");
			fixture.producer.process(request);
			assertEquals("{\"State\":\"SUCCESS\"}", request.getMessage().getBody());
			assertEquals(2, fixture.broker.publications.get());
			fixture.producer.stop();
			fixture.assertRetired();
		}
	}

	@Test
	void stopReportsABoundedCleanupFailureWhileALongerPublishIsStillInFlight() throws Exception {
		try (var fixture = new Fixture(1)) {
			fixture.endpoint.setReplyTimeout(30);
			fixture.broker.withholdPublicationAck = true;
			var request = fixture.executor.submit(() -> assertThrows(Exception.class,
					() -> fixture.producer.process(fixture.request("stop-deadline"))));
			assertTrue(fixture.broker.withheld.await(3, TimeUnit.SECONDS));
			var stop = fixture.executor.submit(() -> assertThrows(Exception.class,
					fixture.producer::stop));
			assertInstanceOf(java.util.concurrent.TimeoutException.class, stop.get(6, TimeUnit.SECONDS).getCause());
			assertFalse(request.isDone(), "stop did not wait for the 30-second transport budget");
			assertEquals(0, fixture.endpoint.replies.pendingCount(), "stop settles its owner's reply immediately");
			assertEquals(1, fixture.broker.publications.get());
			fixture.broker.dropClients();
			request.get(4, TimeUnit.SECONDS);
			// The finite admitted invocation releases ownership even though Camel
			// will not call stop() again on a service whose previous stop failed.
			fixture.assertRetired();
		}
	}

	@Test
	void oneWayPublicationAlsoHasAFinitePahoDeadline() throws Exception {
		try (var fixture = new Fixture(1)) {
			fixture.broker.withholdPublicationAck = true;
			var request = fixture.request("one-way");
			request.setPattern(ExchangePattern.InOnly);
			var result = fixture.executor.submit(() -> assertThrows(Exception.class,
					() -> fixture.producer.process(request)));
			assertTrue(fixture.broker.withheld.await(3, TimeUnit.SECONDS));
			result.get(4, TimeUnit.SECONDS);
			fixture.producer.stop();
			assertEquals(0, fixture.broker.subscriptions.get());
			assertEquals(1, fixture.broker.publications.get());
			fixture.assertRetired();
		}
	}

	static final class Fixture implements AutoCloseable {
		final PacketBroker broker = new PacketBroker();
		final DefaultCamelContext context = new DefaultCamelContext();
		final java.util.concurrent.ExecutorService executor = Executors.newFixedThreadPool(2);
		final MqttV5Endpoint endpoint;
		final MqttV5Producer producer;
		Fixture(int qos) throws Exception {
			context.start();
			endpoint = context.getEndpoint("paho-mqtt5-inout:command?brokerUrl=" + broker.uri()
					+ "&clientId=deadline-" + UUID.randomUUID() + "&replyTopic=reply&qos=" + qos
					+ "&replyTimeout=1", MqttV5Endpoint.class);
			producer = newProducer();
			producer.start();
		}
		MqttV5Producer newProducer() {
			return new MqttV5Producer(endpoint) {
				// Paho schedules its first ping from CONNECT, before processing a
				// CONNACK Server Keep Alive override. Set the actual CONNECT interval.
				@Override protected String upstreamReplyUri() { return super.upstreamReplyUri() + "&keepAliveInterval=1"; }
				@Override protected String upstreamPublishUri() { return super.upstreamPublishUri() + "&keepAliveInterval=1"; }
			};
		}
		DefaultExchange request(String label) {
			var exchange = new DefaultExchange(context, ExchangePattern.InOut);
			exchange.getMessage().setBody(Map.of("value", 1));
			return exchange;
		}
		void assertRetired() {
			assertEquals(0, endpoint.replies.pendingCount());
			assertTrue(context.getRoutes().isEmpty());
			assertTrue(context.getNamedRouteDefinitions().isEmpty());
		}
		@Override public void close() throws Exception {
			try { producer.stop(); }
			finally {
				context.stop();
				broker.close();
				executor.shutdownNow();
				assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
				// Concurrent receiver shutdown may complete after close(true) returns.
				// Join actual owned threads within a finite shared teardown budget.
				long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
				for (Thread thread : pahoThreads()) {
					long remaining = deadline - System.nanoTime();
					if (remaining > 0) thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
				}
				assertTrue(pahoThreads().isEmpty(), () -> "fixture-owned Paho threads must be retired: "
						+ pahoThreads().stream().map(thread -> thread.getName() + " "
								+ java.util.Arrays.toString(thread.getStackTrace())).toList());
			}
		}
		List<Thread> pahoThreads() {
			return Thread.getAllStackTraces().keySet().stream().filter(thread -> thread.isAlive()
					&& (thread.getName().startsWith("MQTT ") || thread.getName().startsWith("dmp-mqtt-reconnect:"))
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
		final CountDownLatch thirdSubscription = new CountDownLatch(3);
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
						assertEquals(1, connect.readUnsignedShort(), "CONNECT keepalive is one second");
						connect.skipNBytes(variableInt(connect));
						clientIds.add(connect.readUTF());
						if (withholdConnectAck) withheld.countDown();
						else send(socket, 0x20, new byte[] {0, 0, 0});
					}
					case 8 -> {
						subscriber = socket;
						int number = subscriptions.incrementAndGet();
						thirdSubscription.countDown();
						if (number == withholdSubscription) withheld.countDown();
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
