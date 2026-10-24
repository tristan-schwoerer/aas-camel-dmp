/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The pending MQTT 5 requests of one {@code dmp-paho-mqtt5:} endpoint: a producer registers each request's
 * correlation and awaits it, the reply subscription completes it. Reservations span registries because identical
 * data on the same broker/reply channel cannot distinguish their callers. Correlations are fresh UUIDs, so a
 * finished call frees its reservation at once.
 *
 * @author aas-camel-dmp
 */
final class MqttReplyRegistry {

	/** In-flight calls JVM-wide. */
	private static final WireState WIRE = new WireState(8_192);
	private final WireState wire;

	MqttReplyRegistry() {
		this(WIRE);
	}

	MqttReplyRegistry(WireState wire) {
		this.wire = wire;
	}

	CompletableFuture<String> register(Object owner, String broker, String topic, String correlation) {
		synchronized (wire) {
			Key key = new Key(broker, topic, correlation);
			if (wire.pending.containsKey(key)) {
				throw new IllegalStateException("MQTT correlation is already pending on the reply channel");
			}
			if (wire.pending.size() >= wire.capacity) {
				throw new IllegalStateException("MQTT reply reservation capacity exhausted");
			}
			CompletableFuture<String> future = new CompletableFuture<>();
			wire.pending.put(key, new Pending(this, owner, future));
			return future;
		}
	}

	void unregister(Object owner, String broker, String topic, String correlation,
			CompletableFuture<String> future) {
		synchronized (wire) {
			Key key = new Key(broker, topic, correlation);
			Pending pending = wire.pending.get(key);
			if (pending != null && pending.registry == this && pending.owner == owner && pending.future == future) {
				wire.pending.remove(key);
			}
		}
	}

	boolean complete(String broker, String topic, String correlation, String payload) {
		synchronized (wire) {
			Key key = new Key(broker, topic, correlation);
			Pending pending = wire.pending.get(key);
			if (pending == null || pending.registry != this) return false;
			wire.pending.remove(key);
			return pending.future.complete(payload);
		}
	}

	void cancelOwner(Object owner) {
		synchronized (wire) {
			for (Key key : java.util.List.copyOf(wire.pending.keySet())) {
				Pending pending = wire.pending.get(key);
				if (pending.registry == this && pending.owner == owner) {
					wire.pending.remove(key);
					pending.future.cancel(false);
				}
			}
		}
	}

	int pendingCount() {
		synchronized (wire) {
			return (int) wire.pending.values().stream().filter(p -> p.registry == this).count();
		}
	}

	private record Key(String broker, String topic, String correlation) { }
	private record Pending(MqttReplyRegistry registry, Object owner, CompletableFuture<String> future) { }

	/** The pending calls, bounded. */
	static final class WireState {
		private final Map<Key, Pending> pending = new HashMap<>();
		private final int capacity;

		WireState(int capacity) {
			this.capacity = capacity;
		}
	}
}
