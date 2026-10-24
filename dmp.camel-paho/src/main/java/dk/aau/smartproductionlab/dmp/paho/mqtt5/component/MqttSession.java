/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.util.MqttTopicValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import dk.aau.smartproductionlab.dmp.core.execution.DmpExecutors;

/**
 * One MQTT 5 connection to a broker with one set of credentials, shared by every source, sink and action of the
 * context that uses them: the DMP holds a connection per broker and user, not per mapping. Sources and actions'
 * reply topics subscribe their topic filters through it; a filter several of them share is subscribed once, at
 * the highest QoS asked for. Each filter's SUBACK is confirmed per connection ({@link #confirm}).
 *
 * <p>Messages arrive on Paho's callback thread, one at a time for the whole connection; a listener only accepts
 * the value there (evaluations run on the shared workers). When a source joins a filter that is already
 * subscribed, the filter is subscribed again so the broker sends its retained messages, and those go only to
 * listeners that have not had a message of that topic yet. The session reconnects by itself (1 s doubling to
 * 30 s), subscribes every filter again and then lets the retained messages through to every listener.
 *
 * <p>Connections start clean, and Paho clears its in-flight state when one ends: a publication whose
 * acknowledgement never came is not sent again on the next connection (actions are never replayed).
 */
final class MqttSession implements MqttCallback {
	private static final Logger LOGGER = LoggerFactory.getLogger(MqttSession.class);
	static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
	static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
	private static final long MAX_BACKOFF_MS = 30_000;

	/** What a session is shared by: the broker and the credentials. */
	record Key(String brokerUrl, String userName, String password) {
		@Override public String toString() {
			return brokerUrl + (userName == null ? "" : " as " + userName);
		}
	}

	/** A subscription: told every message of a topic its filter matches. */
	interface Listener {
		void deliver(String topic, MqttMessage message);
	}

	private record Route(String filter, Listener listener) { }

	private final Key key;
	private final MqttAsyncClient client;
	private final DmpExecutors executors;
	private final Set<Object> owners = Collections.newSetFromMap(new IdentityHashMap<>());
	/** Filter → listener → QoS asked for; changed under {@link #broker}. */
	private final Map<String, Map<Listener, Integer>> filters = new LinkedHashMap<>();
	/** What the callback thread dispatches by, replaced whole on every change: it never takes a lock. */
	private volatile List<Route> routes = List.of();
	/** The topics each listener has had a message of, since it joined or the session reconnected. */
	private final Map<Listener, Set<String>> seen = new ConcurrentHashMap<>();
	/** Serialises connecting and (un)subscribing, which wait for the broker; dispatch never takes it. */
	private final ReentrantLock broker = new ReentrantLock();
	private volatile boolean closed;
	/** A reconnect is scheduled or running; set without the broker lock, so the callback thread never waits. */
	private final java.util.concurrent.atomic.AtomicBoolean reconnecting = new java.util.concurrent.atomic.AtomicBoolean();
	private long backoffMs = 1000;
	/** Counts established connections; a filter is confirmed for the connection it was acknowledged on. */
	private final java.util.concurrent.atomic.AtomicLong connection = new java.util.concurrent.atomic.AtomicLong();
	private final Map<String, Long> confirmed = new ConcurrentHashMap<>();

	MqttSession(Key key, DmpExecutors executors) throws MqttException {
		this.key = key;
		this.executors = executors;
		client = new MqttAsyncClient(key.brokerUrl(), "dmp-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16),
				new MemoryPersistence());
		client.setCallback(this);
	}

	Key key() { return key; }

	/**
	 * Registers {@code owner}; the first one connects. A broker that cannot be reached fails nothing: the session
	 * keeps reconnecting, subscribes once it is connected, and publications fail until then (as an OPC UA or HTTP
	 * source starts and retries, a mapping installs whether or not its device is up).
	 */
	void acquire(Object owner) {
		broker.lock();
		try {
			if (closed) throw new IllegalStateException("MQTT session to " + key + " is closed");
			boolean first = owners.isEmpty();
			owners.add(owner);
			if (!first || client.isConnected() || reconnecting.get()) return;
			try {
				connect();
			} catch (MqttException unreachable) {
				LOGGER.warn("MQTT session to {} is not connected yet (retrying): {}", key, unreachable.getMessage());
				scheduleReconnect();
			}
		} finally {
			broker.unlock();
		}
	}

	/** Unregisters {@code owner}; true when it was the last one and the session is closed now. */
	boolean release(Object owner) {
		broker.lock();
		try {
			owners.remove(owner);
			if (!owners.isEmpty() || closed) return closed;
		} finally {
			broker.unlock();
		}
		close();
		return true;
	}

	/** Disconnects and closes the client, whoever still holds the session. */
	void close() {
		broker.lock();
		try {
			if (closed) return;
			closed = true;
		} finally {
			broker.unlock();
		}
		try {
			if (client.isConnected()) client.disconnectForcibly(0, 1_000, true);
		} catch (MqttException ignored) { /* the broker went away */ }
		try {
			client.close(true);
		} catch (MqttException ignored) { /* closing anyway */ }
	}

	/** Subscribes {@code listener} to {@code filter}; fails when the broker refuses the subscription. */
	void subscribe(Listener listener, String filter, int qos) throws MqttException {
		subscribe(listener, filter, qos, REQUEST_TIMEOUT);
	}

	/**
	 * Subscribes {@code listener} to {@code filter}, waiting at most {@code timeout} for the broker; fails when the
	 * broker refuses or does not answer. While the session reconnects, the reconnect subscribes it.
	 */
	void subscribe(Listener listener, String filter, int qos, Duration timeout) throws MqttException {
		lock(timeout);
		try {
			Map<Listener, Integer> listeners = filters.computeIfAbsent(filter, ignored -> new IdentityHashMap<>());
			listeners.put(listener, qos);
			seen.put(listener, ConcurrentHashMap.newKeySet());
			publishRoutes();
			if (!client.isConnected()) return; // the reconnect subscribes it
			try {
				subscribeNow(filter, Collections.max(listeners.values()), timeout);
			} catch (MqttException | RuntimeException refused) {
				listeners.remove(listener);
				if (listeners.isEmpty()) { filters.remove(filter); confirmed.remove(filter); }
				seen.remove(listener);
				publishRoutes();
				throw refused;
			}
		} finally {
			broker.unlock();
		}
	}

	void unsubscribe(Listener listener, String filter) {
		broker.lock();
		try {
			Map<Listener, Integer> listeners = filters.get(filter);
			if (listeners == null || listeners.remove(listener) == null) return;
			seen.remove(listener);
			if (listeners.isEmpty()) { filters.remove(filter); confirmed.remove(filter); }
			publishRoutes();
			// Best effort and not awaited: a stop never waits for the broker. Messages of the filter reach no listener.
			if (listeners.isEmpty() && client.isConnected() && !closed) {
				try { client.unsubscribe(filter); }
				catch (MqttException failed) { LOGGER.debug("MQTT unsubscribe from {} failed: {}", filter, failed.getMessage()); }
			}
		} finally {
			broker.unlock();
		}
	}

	/**
	 * Makes sure the broker acknowledged {@code filter} on the current connection, subscribing it again when the
	 * connection changed since; waits at most {@code timeout}. Fails when the session is not connected, so nothing
	 * is published that a reply could not reach.
	 */
	void confirm(String filter, Duration timeout) throws MqttException {
		if (confirmed(filter)) return;
		lock(timeout);
		try {
			Map<Listener, Integer> listeners = filters.get(filter);
			if (listeners == null) throw new IllegalStateException("nothing listens to " + filter);
			if (confirmed(filter)) return;
			if (closed || !client.isConnected()) throw notConnected();
			subscribeNow(filter, Collections.max(listeners.values()), timeout);
		} finally {
			broker.unlock();
		}
	}

	/** Starts a publication: it is sent (QoS 0) or acknowledged (QoS 1 and 2) when the token completes. */
	IMqttToken send(String topic, MqttMessage message) throws MqttException {
		if (closed) throw new IllegalStateException("MQTT session to " + key + " is closed");
		if (!client.isConnected()) throw notConnected();
		return client.publish(topic, message);
	}

	boolean isConnected() { return client.isConnected(); }

	private boolean confirmed(String filter) {
		Long at = confirmed.get(filter);
		return at != null && at == connection.get() && client.isConnected();
	}

	private void subscribeNow(String filter, int qos, Duration timeout) throws MqttException {
		long at = connection.get();
		IMqttToken token = client.subscribe(filter, qos);
		token.waitForCompletion(Math.max(1, timeout.toMillis()));
		int[] reasons = token.getReasonCodes();
		if (reasons != null) for (int reason : reasons)
			if (reason >= 0x80) throw new IllegalStateException("the MQTT broker refused the subscription to " + filter + " (reason code " + reason + ")");
		confirmed.put(filter, at);
	}

	private void lock(Duration timeout) throws MqttException {
		try {
			if (broker.tryLock(Math.max(1, timeout.toMillis()), java.util.concurrent.TimeUnit.MILLISECONDS)) return;
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
		}
		throw new MqttException(org.eclipse.paho.mqttv5.client.MqttClientException.REASON_CODE_CLIENT_TIMEOUT);
	}

	private static MqttException notConnected() {
		return new MqttException(org.eclipse.paho.mqttv5.client.MqttClientException.REASON_CODE_CLIENT_NOT_CONNECTED);
	}

	private void publishRoutes() {
		List<Route> next = new ArrayList<>();
		filters.forEach((filter, listeners) -> listeners.keySet().forEach(listener -> next.add(new Route(filter, listener))));
		routes = List.copyOf(next);
	}

	private void connect() throws MqttException {
		MqttConnectionOptions options = new MqttConnectionOptions();
		options.setCleanStart(true);
		options.setAutomaticReconnect(false); // Paho's reconnect timer outlives a closed client; the session reconnects
		options.setConnectionTimeout((int) CONNECT_TIMEOUT.toSeconds());
		options.setKeepAliveInterval(60);
		if (key.userName() != null) options.setUserName(key.userName());
		if (key.password() != null) options.setPassword(key.password().getBytes(StandardCharsets.UTF_8));
		client.connect(options).waitForCompletion(CONNECT_TIMEOUT.toMillis() + 1_000);
		connection.incrementAndGet();
	}

	private void scheduleReconnect() {
		if (closed || !reconnecting.compareAndSet(false, true)) return;
		executors.later(Duration.ofSeconds(1), this::reconnect);
	}

	/**
	 * Connects under the broker lock, then subscribes every filter without it: a broker that withholds a SUBACK
	 * delays no stop and no other subscription. A filter dropped meanwhile may stay subscribed at the broker; its
	 * messages reach no listener.
	 */
	private void reconnect() {
		Map<String, Integer> resubscribe = new LinkedHashMap<>();
		try {
			broker.lock();
			try {
				if (closed) {
					reconnecting.set(false);
					return;
				}
				if (!client.isConnected()) connect();
				// A clean start keeps no subscriptions; the retained messages they bring go to every listener.
				seen.values().forEach(Set::clear);
				filters.forEach((filter, listeners) -> resubscribe.put(filter, Collections.max(listeners.values())));
			} finally {
				broker.unlock();
			}
			for (var filter : resubscribe.entrySet()) {
				if (closed) break;
				subscribeNow(filter.getKey(), filter.getValue(), REQUEST_TIMEOUT);
			}
			reconnecting.set(false);
			backoffMs = 1000;
			LOGGER.info("MQTT session to {} reconnected with {} subscriptions", key, resubscribe.size());
			return;
		} catch (MqttException | RuntimeException failed) {
			if (closed) {
				reconnecting.set(false);
				return;
			}
			LOGGER.warn("MQTT session to {} is not reconnected yet (next attempt in {} ms): {}", key, backoffMs, failed.getMessage());
			if (client.isConnected()) {
				try { client.disconnectForcibly(0, 1_000, false); } catch (MqttException ignored) { /* retried below */ }
			}
		}
		backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
		executors.later(Duration.ofMillis(backoffMs), this::reconnect);
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) {
		// Never throws: Paho drops the connection when a listener does.
		for (Route route : routes) {
			try {
				if (!MqttTopicValidator.isMatched(route.filter(), topic)) continue;
				Set<String> topics = seen.get(route.listener());
				// A retained message is the answer to a subscription: only a listener new to the topic takes it.
				boolean first = topics == null || topics.add(topic);
				if (message.isRetained() && !first) continue;
				route.listener().deliver(topic, message);
			} catch (RuntimeException failed) {
				LOGGER.warn("MQTT message on {} could not be delivered: {}", topic, failed.toString());
			}
		}
	}

	@Override
	public void disconnected(MqttDisconnectResponse response) {
		if (closed) return;
		LOGGER.warn("MQTT session to {} lost its connection: {}", key, response == null ? "" : response.getReasonString());
		scheduleReconnect();
	}

	@Override
	public void mqttErrorOccurred(MqttException failure) {
		LOGGER.warn("MQTT session to {}: {}", key, failure.getMessage());
	}

	@Override public void deliveryComplete(IMqttToken token) { }

	@Override public void connectComplete(boolean reconnect, String serverUri) { }

	@Override public void authPacketArrived(int reasonCode, MqttProperties properties) { }
}
