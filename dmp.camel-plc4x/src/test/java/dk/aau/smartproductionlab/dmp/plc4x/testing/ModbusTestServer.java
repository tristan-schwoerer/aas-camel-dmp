/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.plc4x.testing;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

/**
 * A minimal Modbus TCP slave for tests: coils and holding registers in memory,
 * function codes 1, 3, 5, 6, 15 and 16. Addresses are protocol addresses
 * (zero-based), which PLC4X tag {@code holding-register:1} reaches as 0.
 */
public final class ModbusTestServer implements AutoCloseable {
	private final ServerSocket server;
	private final ExecutorService workers = Executors.newCachedThreadPool(runnable -> {
		Thread thread = new Thread(runnable, "modbus-test-server");
		thread.setDaemon(true);
		return thread;
	});
	private final List<Socket> clients = new CopyOnWriteArrayList<>();
	private final ConcurrentHashMap<Integer, Boolean> coils = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Integer, Integer> registers = new ConcurrentHashMap<>();
	private final List<String> writes = new CopyOnWriteArrayList<>();
	private volatile BiConsumer<String, Integer> onWrite = (kind, address) -> { };
	private volatile boolean failReads;

	public ModbusTestServer() throws IOException {
		server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
		workers.submit(this::accept);
	}

	public int port() { return server.getLocalPort(); }

	public void coil(int address, boolean value) { coils.put(address, value); }
	public boolean coil(int address) { return coils.getOrDefault(address, false); }
	public void register(int address, int value) { registers.put(address, value & 0xFFFF); }
	public int register(int address) { return registers.getOrDefault(address, 0); }

	/** Every write as {@code coil:<address>=<value>} or {@code register:<address>=<value>}, in order. */
	public List<String> writes() { return List.copyOf(writes); }

	/** Called after a write is applied, with {@code coil} or {@code register} and the address. */
	public void onWrite(BiConsumer<String, Integer> listener) { onWrite = listener; }

	/** TCP connections accepted so far, and those still open. */
	public int acceptedConnections() { return clients.size(); }
	public long openConnections() { return clients.stream().filter(client -> !client.isClosed()).count(); }

	/** Answers reads with Modbus exception 4 (slave device failure). */
	public void failReads(boolean fail) { failReads = fail; }

	private void accept() {
		while (!server.isClosed()) {
			try {
				Socket client = server.accept();
				clients.add(client);
				workers.submit(() -> serve(client));
			} catch (IOException closed) {
				return;
			}
		}
	}

	private void serve(Socket client) {
		try (client; DataInputStream in = new DataInputStream(client.getInputStream());
				DataOutputStream out = new DataOutputStream(client.getOutputStream())) {
			while (true) {
				int transaction = in.readUnsignedShort();
				int protocol = in.readUnsignedShort();
				int length = in.readUnsignedShort();
				int unit = in.readUnsignedByte();
				byte[] pdu = new byte[length - 1];
				in.readFully(pdu);
				byte[] reply = protocol == 0 ? handle(pdu) : exception(pdu[0], 1);
				synchronized (out) {
					out.writeShort(transaction);
					out.writeShort(0);
					out.writeShort(reply.length + 1);
					out.writeByte(unit);
					out.write(reply);
					out.flush();
				}
			}
		} catch (IOException | RuntimeException closed) {
			// client went away or the server stopped
		} finally {
			clients.remove(client);
		}
	}

	private byte[] handle(byte[] pdu) {
		int function = pdu[0] & 0xFF;
		int address = word(pdu, 1);
		switch (function) {
		case 1 -> {
			if (failReads) return exception(pdu[0], 4);
			int quantity = word(pdu, 3);
			byte[] bits = new byte[(quantity + 7) / 8];
			for (int i = 0; i < quantity; i++) if (coil(address + i)) bits[i / 8] |= (byte) (1 << (i % 8));
			return concat(new byte[] { (byte) function, (byte) bits.length }, bits);
		}
		case 3 -> {
			if (failReads) return exception(pdu[0], 4);
			int quantity = word(pdu, 3);
			byte[] reply = new byte[2 + quantity * 2];
			reply[0] = (byte) function;
			reply[1] = (byte) (quantity * 2);
			for (int i = 0; i < quantity; i++) {
				int value = register(address + i);
				reply[2 + i * 2] = (byte) (value >> 8);
				reply[3 + i * 2] = (byte) value;
			}
			return reply;
		}
		case 5 -> {
			writeCoil(address, word(pdu, 3) == 0xFF00);
			return pdu.clone();
		}
		case 6 -> {
			writeRegister(address, word(pdu, 3));
			return pdu.clone();
		}
		case 15 -> {
			int quantity = word(pdu, 3);
			for (int i = 0; i < quantity; i++) writeCoil(address + i, (pdu[6 + i / 8] & (1 << (i % 8))) != 0);
			return new byte[] { pdu[0], pdu[1], pdu[2], pdu[3], pdu[4] };
		}
		case 16 -> {
			int quantity = word(pdu, 3);
			for (int i = 0; i < quantity; i++) writeRegister(address + i, word(pdu, 6 + i * 2));
			return new byte[] { pdu[0], pdu[1], pdu[2], pdu[3], pdu[4] };
		}
		default -> {
			return exception(pdu[0], 1);
		}
		}
	}

	private void writeCoil(int address, boolean value) {
		coils.put(address, value);
		writes.add("coil:" + address + "=" + value);
		onWrite.accept("coil", address);
	}

	private void writeRegister(int address, int value) {
		registers.put(address, value);
		writes.add("register:" + address + "=" + value);
		onWrite.accept("register", address);
	}

	private static byte[] exception(byte function, int code) {
		return new byte[] { (byte) (function | 0x80), (byte) code };
	}

	private static int word(byte[] bytes, int offset) {
		return ((bytes[offset] & 0xFF) << 8) | (bytes[offset + 1] & 0xFF);
	}

	private static byte[] concat(byte[] head, byte[] tail) {
		byte[] result = new byte[head.length + tail.length];
		System.arraycopy(head, 0, result, 0, head.length);
		System.arraycopy(tail, 0, result, head.length, tail.length);
		return result;
	}

	@Override
	public void close() throws IOException {
		server.close();
		for (Socket client : new ArrayList<>(clients)) {
			try { client.close(); } catch (SocketException ignored) { }
		}
		workers.shutdownNow();
	}
}
