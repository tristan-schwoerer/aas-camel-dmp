/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.datatypes.MqttTopicFilter;
import com.hivemq.client.mqtt.lifecycle.MqttClientDisconnectedContext;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5SubAckException;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5SimpleAuth;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5SimpleAuthBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import dk.aau.smartproductionlab.dmp.core.execution.DmpExecutors;
import dk.aau.smartproductionlab.dmp.core.execution.SerialLane;

/**
 * One MQTT 5 connection to a broker with one set of credentials, shared by every source, sink and action of the
 * context that uses them: the DMP holds a connection per broker and user, not per mapping. Sources and actions'
 * reply topics subscribe their topic filters through it; a filter several of them share is subscribed once, at
 * the highest QoS asked for. Each filter's SUBACK is confirmed per connection ({@link #confirm}).
 *
 * <p>The HiveMQ client carries the wire protocol on its shared Netty threads. Every message arrives through one
 * global flow, which only queues it; the client ends that flow with every session, so each connection attempt
 * starts a new one ({@link #listen}). The session's {@link SerialLane} hands the messages to the listeners on
 * the shared workers, one at a time and in arrival order for the whole connection, and acknowledges each (QoS 1
 * and 2) once its listeners accepted it. A listener only accepts the value (evaluations run on the shared
 * workers). When a source joins a filter that is already subscribed, the filter is subscribed again so the broker
 * sends its retained messages, and those go only to listeners that have not had a message of that topic yet. The
 * session reconnects by itself (1 s doubling to 30 s), subscribes every filter again and then lets the retained
 * messages through to every listener.
 *
 * <p>Shared sessions start clean and keep no session state: a publication whose acknowledgement never came fails
 * when the connection ends and is not sent again on the next one, and nothing is published while disconnected
 * (actions are never replayed). The client's own automatic reconnect is not used, because it would queue
 * publications until it reconnected.
 *
 * <p>A dedicated session ({@link Key#dedicated()}: a fixed client identifier, optionally persistent) serves one
 * consumer, the repository's event channel. It connects when its first filter is subscribed, so the messages a
 * resumed session delivers right after connecting already have a listener.
 */
final class MqttSession {
	private static final Logger LOGGER = LoggerFactory.getLogger(MqttSession.class);
	static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
	static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
	private static final long MAX_BACKOFF_MS = 30_000;

	/**
	 * What a session is shared by: the broker and the credentials. A dedicated session also names its client
	 * identifier, whether the broker keeps its session ({@code persistent}) and for how long.
	 */
	record Key(String brokerUrl, String userName, String password, String clientId, boolean persistent,
			long sessionExpirySeconds) {
		Key(String brokerUrl, String userName, String password) {
			this(brokerUrl, userName, password, null, false, 0);
		}
		boolean dedicated() { return clientId != null; }
		@Override public String toString() {
			return brokerUrl + (userName == null ? "" : " as " + userName) + (clientId == null ? "" : " (" + clientId + ")");
		}
	}

	/** A subscription: told every message of a topic its filter matches. */
	interface Listener {
		void deliver(String topic, Mqtt5Publish message);
	}

	private record Route(MqttTopicFilter filter, Listener listener) { }

	private final Key key;
	private final Mqtt5AsyncClient client;
	private final DmpExecutors executors;
	/** Hands arrived messages to the listeners one at a time, in order, on the shared workers. */
	private final SerialLane<Mqtt5Publish> arrivals;
	private final Set<Object> owners = Collections.newSetFromMap(new IdentityHashMap<>());
	/** Filter → listener → QoS asked for; changed under {@link #broker}. */
	private final Map<String, Map<Listener, Integer>> filters = new LinkedHashMap<>();
	/** What the arrivals dispatch by, replaced whole on every change: dispatch never takes a lock. */
	private volatile List<Route> routes = List.of();
	/** The topics each listener has had a message of, since it joined or the session reconnected. */
	private final Map<Listener, Set<String>> seen = new ConcurrentHashMap<>();
	/** Serialises connecting and (un)subscribing, which wait for the broker; dispatch never takes it. */
	private final ReentrantLock broker = new ReentrantLock();
	private volatile boolean closed;
	/** A reconnect is scheduled or running; set without the broker lock, so the Netty threads never wait. */
	private final java.util.concurrent.atomic.AtomicBoolean reconnecting = new java.util.concurrent.atomic.AtomicBoolean();
	private long backoffMs = 1000;
	/** Counts established connections; a filter is confirmed for the connection it was acknowledged on. */
	private final java.util.concurrent.atomic.AtomicLong connection = new java.util.concurrent.atomic.AtomicLong();
	private final Map<String, Long> confirmed = new ConcurrentHashMap<>();
	/** The global flow of the current or next session; replaced before every connection attempt. */
	private io.reactivex.disposables.Disposable flow;

	MqttSession(Key key, DmpExecutors executors) {
		this.key = key;
		this.executors = executors;
		this.arrivals = new SerialLane<>("MQTT session to " + key, executors.workers(), Integer.MAX_VALUE, this::dispatch,
				(older, newer) -> { throw new IllegalStateException("an MQTT session never merges messages"); });
		client = builder(key).addDisconnectedListener(this::disconnected).buildAsync();
	}

	/**
	 * Under the broker lock, before a connection attempt: one global flow of every message, manually acknowledged,
	 * so a resumed session's backlog reaches the lane too. The client ends the flow when the session ends; the
	 * previous one is disposed first in any case, so there is never a second.
	 */
	private void listen() {
		if (flow != null) flow.dispose();
		flow = client.toRx().publishes(MqttGlobalPublishFilter.ALL, true)
				.subscribe(arrivals::submit, ended -> LOGGER.debug("MQTT session to {} ended its message flow: {}", key, message(ended)));
	}

	/** The client for the broker URL: {@code tcp}, {@code ssl} (TLS), {@code ws} or {@code wss} (WebSocket). */
	private static Mqtt5ClientBuilder builder(Key key) {
		URI broker = URI.create(key.brokerUrl());
		String scheme = String.valueOf(broker.getScheme()).toLowerCase(Locale.ROOT);
		boolean tls = scheme.equals("ssl") || scheme.equals("wss");
		boolean webSocket = scheme.equals("ws") || scheme.equals("wss");
		if (!tls && !webSocket && !scheme.equals("tcp"))
			throw new IllegalArgumentException("MQTT broker URL must be tcp, ssl, ws or wss: " + key.brokerUrl());
		if (broker.getHost() == null) throw new IllegalArgumentException("MQTT broker URL has no host: " + key.brokerUrl());
		int port = broker.getPort() > 0 ? broker.getPort() : webSocket ? (tls ? 443 : 80) : (tls ? 8883 : 1883);
		Mqtt5ClientBuilder builder = MqttClient.builder().useMqttVersion5()
				.identifier(key.dedicated() ? key.clientId()
						: "dmp-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16))
				.serverHost(broker.getHost()).serverPort(port)
				.transportConfig()
					.serverHost(broker.getHost()).serverPort(port)
					.socketConnectTimeout(CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
					.mqttConnectTimeout(CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
					.applyTransportConfig();
		if (tls) builder = builder.sslWithDefaultConfig();
		if (webSocket) {
			String path = broker.getRawPath() == null || broker.getRawPath().isEmpty() ? "mqtt" : broker.getRawPath().substring(1);
			builder = builder.webSocketConfig().serverPath(path).applyWebSocketConfig();
		}
		return builder;
	}

	Key key() { return key; }

	/**
	 * Registers {@code owner}; for a shared session the first one connects. A broker that cannot be reached fails
	 * nothing: the session keeps reconnecting, subscribes once it is connected, and publications fail until then
	 * (as an OPC UA or HTTP source starts and retries, a mapping installs whether or not its device is up). A
	 * dedicated session connects when its first filter is subscribed.
	 */
	void acquire(Object owner) {
		broker.lock();
		try {
			if (closed) throw new IllegalStateException("MQTT session to " + key + " is closed");
			boolean first = owners.isEmpty();
			owners.add(owner);
			if (!first || key.dedicated()) return;
			connectOrRetry();
		} finally {
			broker.unlock();
		}
	}

	/** Under the broker lock: connects unless connected or reconnecting; a failure starts the reconnects. */
	private void connectOrRetry() {
		if (isConnected() || reconnecting.get()) return;
		try {
			connect();
		} catch (Exception unreachable) {
			LOGGER.warn("MQTT session to {} is not connected yet (retrying): {}", key, message(unreachable));
			scheduleReconnect();
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

	/** Disconnects the client, whoever still holds the session. */
	void close() {
		broker.lock();
		try {
			if (closed) return;
			closed = true;
		} finally {
			broker.unlock();
		}
		arrivals.close();
		broker.lock();
		try {
			if (flow != null) flow.dispose();
		} finally {
			broker.unlock();
		}
		if (client.getState() != MqttClientState.DISCONNECTED) {
			try {
				client.disconnect().get(1, TimeUnit.SECONDS);
			} catch (InterruptedException interrupted) {
				Thread.currentThread().interrupt();
			} catch (ExecutionException | TimeoutException | RuntimeException ignored) { /* the broker went away */ }
		}
	}

	/** Subscribes {@code listener} to {@code filter}; fails when the broker refuses the subscription. */
	void subscribe(Listener listener, String filter, int qos) throws TimeoutException {
		subscribe(listener, filter, qos, REQUEST_TIMEOUT);
	}

	/**
	 * Subscribes {@code listener} to {@code filter}, waiting at most {@code timeout} for the broker; fails when the
	 * broker refuses or does not answer. While the session reconnects, the reconnect subscribes it.
	 */
	void subscribe(Listener listener, String filter, int qos, Duration timeout) throws TimeoutException {
		MqttTopicFilter.of(filter); // a malformed filter fails here, before anything is registered
		lock(timeout);
		try {
			Map<Listener, Integer> listeners = filters.computeIfAbsent(filter, ignored -> new IdentityHashMap<>());
			listeners.put(listener, qos);
			seen.put(listener, ConcurrentHashMap.newKeySet());
			publishRoutes();
			if (key.dedicated() && !closed) connectOrRetry();
			if (!isConnected()) return; // the reconnect subscribes it
			try {
				subscribeNow(filter, Collections.max(listeners.values()), timeout);
			} catch (TimeoutException | RuntimeException refused) {
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
			if (listeners.isEmpty() && isConnected() && !closed) {
				client.unsubscribeWith().topicFilter(filter).send().whenComplete((ack, failed) -> {
					if (failed != null) LOGGER.debug("MQTT unsubscribe from {} failed: {}", filter, message(failed));
				});
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
	void confirm(String filter, Duration timeout) throws TimeoutException {
		if (confirmed(filter)) return;
		lock(timeout);
		try {
			Map<Listener, Integer> listeners = filters.get(filter);
			if (listeners == null) throw new IllegalStateException("nothing listens to " + filter);
			if (confirmed(filter)) return;
			if (closed || !isConnected()) throw notConnected();
			subscribeNow(filter, Collections.max(listeners.values()), timeout);
		} finally {
			broker.unlock();
		}
	}

	/**
	 * Starts a publication: its future completes when it was sent (QoS 0) or acknowledged (QoS 1 and 2), and
	 * fails when the broker refused it or the connection ended first.
	 */
	CompletableFuture<Mqtt5PublishResult> send(Mqtt5Publish message) {
		if (closed) throw new IllegalStateException("MQTT session to " + key + " is closed");
		if (!isConnected()) throw notConnected();
		return client.publish(message);
	}

	boolean isConnected() { return client.getState() == MqttClientState.CONNECTED; }

	/** {@code filter} was acknowledged by the broker on the current connection. */
	boolean confirmed(String filter) {
		Long at = confirmed.get(filter);
		return at != null && at == connection.get() && isConnected();
	}

	private void subscribeNow(String filter, int qos, Duration timeout) throws TimeoutException {
		long at = connection.get();
		Mqtt5SubAck ack;
		try {
			ack = client.subscribeWith().topicFilter(filter).qos(qos(qos)).send()
					.get(Math.max(1, timeout.toMillis()), TimeUnit.MILLISECONDS);
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("interrupted while subscribing to " + filter, interrupted);
		} catch (ExecutionException failed) {
			if (failed.getCause() instanceof Mqtt5SubAckException refused) ack = refused.getMqttMessage();
			else throw new IllegalStateException("the MQTT subscription to " + filter + " failed: " + message(failed.getCause()),
					failed.getCause());
		}
		for (Mqtt5SubAckReasonCode reason : ack.getReasonCodes())
			if (reason.isError()) throw new IllegalStateException("the MQTT broker refused the subscription to " + filter
					+ " (reason code " + reason.getCode() + ")");
		confirmed.put(filter, at);
	}

	static MqttQos qos(int qos) {
		MqttQos value = MqttQos.fromCode(qos);
		if (value == null) throw new IllegalArgumentException("MQTT QoS must be 0, 1 or 2");
		return value;
	}

	private void lock(Duration timeout) throws TimeoutException {
		try {
			if (broker.tryLock(Math.max(1, timeout.toMillis()), TimeUnit.MILLISECONDS)) return;
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
		}
		throw new TimeoutException("MQTT session to " + key + " is busy");
	}

	private IllegalStateException notConnected() {
		return new IllegalStateException("MQTT session to " + key + " is not connected");
	}

	private void publishRoutes() {
		List<Route> next = new ArrayList<>();
		filters.forEach((filter, listeners) -> {
			MqttTopicFilter parsed = MqttTopicFilter.of(filter);
			listeners.keySet().forEach(listener -> next.add(new Route(parsed, listener)));
		});
		routes = List.copyOf(next);
	}

	/** Under the broker lock: one connection attempt, waiting at most {@link #CONNECT_TIMEOUT} and a second. */
	private void connect() throws Exception {
		listen();
		Mqtt5ConnectBuilder.Send<CompletableFuture<Mqtt5ConnAck>> connect = client.connectWith()
				.cleanStart(!key.persistent()).sessionExpiryInterval(key.persistent() ? key.sessionExpirySeconds() : 0)
				.keepAlive(60).simpleAuth(simpleAuth(key));
		try {
			Mqtt5ConnAck connAck = connect.send().get(CONNECT_TIMEOUT.toMillis() + 1_000, TimeUnit.MILLISECONDS);
			// A broker that lost a persistent session (a restart without its store) makes the client end the session
			// it still held, and the flow with it. Nothing is subscribed yet and there is no backlog: listen afresh.
			if (key.persistent() && !connAck.isSessionPresent() || flow.isDisposed()) listen();
		} catch (ExecutionException failed) {
			throw failed.getCause() instanceof Exception cause ? cause : failed;
		} catch (TimeoutException timedOut) {
			client.disconnect(); // ends the attempt, so the next one starts from a disconnected client
			throw timedOut;
		}
		connection.incrementAndGet();
	}

	/** The CONNECT's user name and password, either of them alone, or none. */
	private static Mqtt5SimpleAuth simpleAuth(Key key) {
		if (key.userName() == null && key.password() == null) return null;
		byte[] password = key.password() == null ? null : key.password().getBytes(StandardCharsets.UTF_8);
		Mqtt5SimpleAuthBuilder.Complete auth = key.userName() != null ? Mqtt5SimpleAuth.builder().username(key.userName())
				: Mqtt5SimpleAuth.builder().password(password);
		if (key.userName() != null && password != null) auth = auth.password(password);
		return auth.build();
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
				if (!isConnected()) connect();
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
		} catch (Exception failed) {
			if (closed) {
				reconnecting.set(false);
				return;
			}
			LOGGER.warn("MQTT session to {} is not reconnected yet (next attempt in {} ms): {}", key, backoffMs, message(failed));
			if (client.getState() != MqttClientState.DISCONNECTED) client.disconnect();
		}
		backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
		executors.later(Duration.ofMillis(backoffMs), this::reconnect);
	}

	/** On a shared worker, one message at a time: the listeners whose filter matches, then the acknowledgement. */
	private void dispatch(Mqtt5Publish message) {
		String topic = message.getTopic().toString();
		MqttTopic parsed = message.getTopic();
		for (Route route : routes) {
			try {
				if (!route.filter().matches(parsed)) continue;
				Set<String> topics = seen.get(route.listener());
				// A retained message is the answer to a subscription: only a listener new to the topic takes it.
				boolean first = topics == null || topics.add(topic);
				if (message.isRetain() && !first) continue;
				route.listener().deliver(topic, message);
			} catch (RuntimeException failed) {
				LOGGER.warn("MQTT message on {} could not be delivered: {}", topic, failed.toString());
			}
		}
		try {
			message.acknowledge();
		} catch (RuntimeException alreadyGone) {
			LOGGER.debug("MQTT message on {} was not acknowledged: {}", topic, alreadyGone.toString());
		}
	}

	/** On a Netty thread when the connection ends or an attempt fails: never waits. */
	private void disconnected(MqttClientDisconnectedContext context) {
		if (closed) return;
		if (!reconnecting.get()) LOGGER.warn("MQTT session to {} lost its connection: {}", key, message(context.getCause()));
		scheduleReconnect();
	}

	private static String message(Throwable failure) {
		return failure == null ? "" : failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
	}
}
