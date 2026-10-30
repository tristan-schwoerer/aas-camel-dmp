/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.paho.testing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.datatypes.MqttTopicFilter;

/**
 * An in-process MQTT 5 broker for tests: CONNECT, SUBSCRIBE, UNSUBSCRIBE, PING, DISCONNECT and PUBLISH at QoS 0,
 * 1 and 2 from clients, routed to every matching subscription (delivered at QoS 0, with the publisher's
 * properties) plus retained messages. Sessions are never kept; {@link #sessionPresent} only shapes CONNACK.
 */
public final class Mqtt5TestBroker implements AutoCloseable {
	private final ServerSocket server;
	private final ExecutorService workers = Executors.newCachedThreadPool(task -> {
		Thread thread = new Thread(task, "mqtt5-test-broker");
		thread.setDaemon(true);
		return thread;
	});
	private final Map<Socket, Set<String>> clients = new ConcurrentHashMap<>();
	private final Map<String, byte[][]> retained = new ConcurrentHashMap<>();
	/** Every subscribed filter, by any client. */
	public final Set<String> subscriptions = ConcurrentHashMap.newKeySet();
	/** CONNECT clean-start flags in arrival order. */
	public final List<Boolean> connects = new CopyOnWriteArrayList<>();
	/** Client identifiers in arrival order. */
	public final List<String> clientIds = new CopyOnWriteArrayList<>();
	/** Topics clients published to, in arrival order. */
	public final List<String> published = new CopyOnWriteArrayList<>();
	public volatile boolean sessionPresent;
	private volatile boolean closing;
	private volatile java.util.function.Consumer<Publication> onPublish = publication -> { };

	/** A client's PUBLISH: topic, raw MQTT 5 properties and payload. */
	public record Publication(String topic, byte[] properties, byte[] payload) {
		/** The request's Correlation Data property (0x09), or null. */
		public byte[] correlationData() {
			try {
				var input = new DataInputStream(new ByteArrayInputStream(properties));
				while (input.available() > 0) {
					int id = variableInt(input);
					switch (id) {
					case 0x01 -> input.readUnsignedByte();
					case 0x02 -> input.readInt();
					case 0x23 -> input.readUnsignedShort();
					case 0x0B -> variableInt(input);
					case 0x03, 0x08 -> input.readUTF();
					case 0x26 -> { input.readUTF(); input.readUTF(); }
					case 0x09 -> { return input.readNBytes(input.readUnsignedShort()); }
					default -> { return null; }
					}
				}
			} catch (IOException malformed) { }
			return null;
		}

		public String text() { return new String(payload, StandardCharsets.UTF_8); }
	}

	public Mqtt5TestBroker() throws IOException { this(0); }

	/** A broker on {@code port} (0: any), e.g. to come back where a stopped one was. */
	public Mqtt5TestBroker(int port) throws IOException {
		server = new ServerSocket();
		server.setReuseAddress(true);
		server.bind(new java.net.InetSocketAddress(InetAddress.getLoopbackAddress(), port), 50);
		workers.submit(() -> {
			try {
				while (!closing) {
					Socket socket = server.accept();
					clients.put(socket, ConcurrentHashMap.newKeySet());
					workers.submit(() -> serve(socket));
				}
			} catch (IOException closed) { }
		});
	}

	public String uri() { return "tcp://127.0.0.1:" + server.getLocalPort(); }
	public int port() { return server.getLocalPort(); }

	/** Publishes as the broker itself; returns how many subscriptions received it. */
	public int publish(String topic, String payload) throws IOException {
		return route(topic, new byte[0], payload.getBytes(StandardCharsets.UTF_8), false);
	}

	/** Called for every PUBLISH a client sends, after it was routed (on the broker's thread). */
	public void onPublish(java.util.function.Consumer<Publication> listener) { onPublish = listener; }

	/** Publishes a correlated MQTT 5 reply: the request's Correlation Data echoed as a property. */
	public int reply(String topic, byte[] correlationData, String payload) throws IOException {
		var properties = new ByteArrayOutputStream();
		properties.write(0x09);
		properties.write(correlationData.length >> 8); properties.write(correlationData.length & 255);
		properties.write(correlationData);
		return route(topic, properties.toByteArray(), payload.getBytes(StandardCharsets.UTF_8), false);
	}

	/** Drops every client connection, as a broker restart would. */
	public void dropClients() throws IOException {
		for (Socket socket : clients.keySet()) socket.close();
		clients.clear();
	}

	@Override public void close() throws IOException {
		closing = true;
		server.close();
		dropClients();
		workers.shutdownNow();
	}

	private int route(String topic, byte[] properties, byte[] payload, boolean retain) throws IOException {
		if (retain) {
			if (payload.length == 0) retained.remove(topic);
			else retained.put(topic, new byte[][] { properties, payload });
		}
		int delivered = 0;
		for (var client : clients.entrySet())
			if (client.getValue().stream().anyMatch(filter -> MqttTopicFilter.of(filter).matches(MqttTopic.of(topic)))) {
				try { send(client.getKey(), 0x30, publication(topic, properties, payload)); delivered++; }
				catch (IOException gone) { }
			}
		return delivered;
	}

	private void serve(Socket socket) {
		try (socket; var input = new DataInputStream(socket.getInputStream())) {
			while (!closing) {
				int header = input.readUnsignedByte();
				byte[] packet = input.readNBytes(variableInt(input));
				var body = new DataInputStream(new ByteArrayInputStream(packet));
				switch (header >> 4) {
				case 1 -> {
					body.skipNBytes(body.readUnsignedShort() + 1L);
					boolean clean = (body.readUnsignedByte() & 2) != 0;
					body.readUnsignedShort();
					body.skipNBytes(variableInt(body));
					clientIds.add(body.readUTF());
					connects.add(clean);
					send(socket, 0x20, new byte[] { (byte) (!clean && sessionPresent ? 1 : 0), 0, 0 });
				}
				case 3 -> {
					int qos = (header >> 1) & 3;
					boolean retain = (header & 1) != 0;
					String topic = body.readUTF();
					int id = qos > 0 ? body.readUnsignedShort() : 0;
					byte[] properties = body.readNBytes(variableInt(body));
					byte[] payload = body.readAllBytes();
					published.add(topic);
					if (qos == 1) send(socket, 0x40, new byte[] { (byte) (id >> 8), (byte) id, 0 });
					if (qos == 2) send(socket, 0x50, new byte[] { (byte) (id >> 8), (byte) id, 0 });
					route(topic, properties, payload, retain);
					onPublish.accept(new Publication(topic, properties, payload));
				}
				case 6 -> send(socket, 0x70, new byte[] { packet[0], packet[1], 0 });
				case 8, 10 -> {
					int id = body.readUnsignedShort();
					body.skipNBytes(variableInt(body));
					var acks = new ByteArrayOutputStream();
					acks.write(id >> 8); acks.write(id & 255); acks.write(0);
					Set<String> own = clients.computeIfAbsent(socket, ignored -> ConcurrentHashMap.newKeySet());
					List<String> added = new CopyOnWriteArrayList<>();
					while (body.available() > 0) {
						String filter = body.readUTF();
						if (header >> 4 == 8) {
							body.readUnsignedByte();
							own.add(filter); subscriptions.add(filter); added.add(filter); acks.write(1);
						} else { own.remove(filter); subscriptions.remove(filter); acks.write(0); }
					}
					send(socket, header >> 4 == 8 ? 0x90 : 0xb0, acks.toByteArray());
					for (var message : retained.entrySet())
						if (added.stream().anyMatch(filter -> MqttTopicFilter.of(filter).matches(MqttTopic.of(message.getKey()))))
							send(socket, 0x31, publication(message.getKey(), message.getValue()[0], message.getValue()[1]));
				}
				case 4, 5, 7 -> { }
				case 12 -> send(socket, 0xd0, new byte[0]);
				case 14 -> { return; }
				default -> { }
				}
			}
		} catch (EOFException | java.net.SocketException closed) {
		} catch (IOException failure) {
		} finally {
			clients.remove(socket);
		}
	}

	private static byte[] publication(String topic, byte[] properties, byte[] payload) throws IOException {
		var body = new ByteArrayOutputStream();
		byte[] name = topic.getBytes(StandardCharsets.UTF_8);
		body.write(name.length >> 8); body.write(name.length & 255); body.write(name);
		writeVariableInt(body, properties.length);
		body.write(properties);
		body.write(payload);
		return body.toByteArray();
	}

	private static void send(Socket socket, int header, byte[] body) throws IOException {
		var packet = new ByteArrayOutputStream();
		packet.write(header);
		writeVariableInt(packet, body.length);
		packet.write(body);
		synchronized (socket) {
			OutputStream output = socket.getOutputStream();
			output.write(packet.toByteArray());
			output.flush();
		}
	}

	private static void writeVariableInt(ByteArrayOutputStream output, int value) {
		do { int digit = value % 128; value /= 128; output.write(digit | (value > 0 ? 128 : 0)); } while (value > 0);
	}

	private static int variableInt(DataInputStream input) throws IOException {
		int value = 0, multiplier = 1, digit;
		do { digit = input.readUnsignedByte(); value += (digit & 127) * multiplier; multiplier *= 128; } while ((digit & 128) != 0);
		return value;
	}
}
