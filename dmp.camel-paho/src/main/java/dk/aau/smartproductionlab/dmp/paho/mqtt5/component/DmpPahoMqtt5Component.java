/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.camel.Endpoint;
import org.apache.camel.support.DefaultComponent;
import dk.aau.smartproductionlab.dmp.core.execution.DmpExecutors;

/**
 * {@code dmp-paho-mqtt5:<topic>?brokerUrl=tcp://host:port[&name=…][&qos=0|1|2][&retained=true|false]
 * [&replyTopic=…][&replyTimeout=<seconds>][&userName=#…&password=#…]}: MQTT 5 sources (a subscription to the topic
 * filter), sinks (a publish to the topic) and actions (a publish, with {@code replyTopic} an MQTT 5 request/reply)
 * on the context's {@link MqttSession} for the broker and credentials, one connection however many mappings use
 * them, carried by the HiveMQ MQTT client.
 *
 * <p>{@code name} only tells endpoints apart (each mapping's endpoint is its own). Credentials are registry
 * references (bound by the owning bundle), never values. QoS defaults to 0, the WoT MQTT binding's.
 * {@code replyTimeout} (default 30 s) bounds a publish and, for a request, its subscription and reply.
 */
public final class DmpPahoMqtt5Component extends DefaultComponent {
	public static final String SCHEME = "dmp-paho-mqtt5";
	private final Map<MqttSession.Key, MqttSession> sessions = new ConcurrentHashMap<>();

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		if (remaining == null || remaining.isBlank()) throw new IllegalArgumentException(SCHEME + " endpoint needs a topic");
		String brokerUrl = getAndRemoveParameter(parameters, "brokerUrl", String.class);
		if (brokerUrl == null || brokerUrl.isBlank()) throw new IllegalArgumentException(SCHEME + " endpoint " + remaining + " needs a brokerUrl");
		getAndRemoveParameter(parameters, "name", String.class);
		int qos = getAndRemoveParameter(parameters, "qos", Integer.class, 0);
		if (qos < 0 || qos > 2) throw new IllegalArgumentException(SCHEME + " qos must be 0, 1 or 2");
		boolean retained = getAndRemoveParameter(parameters, "retained", Boolean.class, false);
		String replyTopic = getAndRemoveParameter(parameters, "replyTopic", String.class);
		if (replyTopic != null && (replyTopic.isBlank() || replyTopic.contains("+") || replyTopic.contains("#")))
			throw new IllegalArgumentException(SCHEME + " request/reply needs a concrete reply topic");
		long replyTimeout = getAndRemoveParameter(parameters, "replyTimeout", Long.class, 30L);
		if (replyTimeout < 1) throw new IllegalArgumentException(SCHEME + " replyTimeout must be at least one second");
		String userName = getAndRemoveOrResolveReferenceParameter(parameters, "userName", String.class);
		String password = getAndRemoveOrResolveReferenceParameter(parameters, "password", String.class);
		return new DmpPahoMqtt5Endpoint(uri, this, remaining, new MqttSession.Key(brokerUrl, userName, password), qos, retained,
				replyTopic, replyTimeout);
	}

	/**
	 * A subscription on a session of its own: a fixed client identifier and, when {@code persistent}, a session the
	 * broker keeps for {@code sessionExpirySeconds}, so messages published while it was disconnected arrive when it
	 * reconnects. The repository's MQTT event channel uses it; the URI only names the endpoint (no credentials).
	 */
	public DmpPahoMqtt5Endpoint dedicatedEndpoint(String name, String topic, String brokerUrl, String clientId,
			String userName, String password, int qos, boolean persistent, long sessionExpirySeconds) {
		if (clientId == null || clientId.isBlank()) throw new IllegalArgumentException(SCHEME + " dedicated session needs a client identifier");
		MqttSession.qos(qos);
		return new DmpPahoMqtt5Endpoint(SCHEME + ":" + name, this, topic,
				new MqttSession.Key(brokerUrl, userName, password, clientId, persistent, sessionExpirySeconds), qos, false, null, 30L);
	}

	/** The session for {@code key}, held by {@code owner}; a new one connects, or keeps trying in the background. */
	synchronized MqttSession acquire(MqttSession.Key key, Object owner) throws Exception {
		MqttSession session = sessions.get(key);
		if (session == null) {
			session = new MqttSession(key, DmpExecutors.of(getCamelContext()));
			sessions.put(key, session);
		}
		session.acquire(owner);
		return session;
	}

	/** Closes the session once its last owner released it. */
	synchronized void release(MqttSession session, Object owner) {
		if (session.release(owner)) sessions.remove(session.key(), session);
	}

	/** The open session for {@code key}, if any; never creates or connects one. */
	MqttSession session(MqttSession.Key key) { return sessions.get(key); }

	/** Open sessions, one per broker and credentials. */
	public int sessionCount() { return sessions.size(); }

	@Override
	protected void doStop() throws Exception {
		synchronized (this) {
			// Endpoints stop before their component; whatever is left has lost its owners.
			for (MqttSession session : sessions.values()) session.close();
			sessions.clear();
		}
		super.doStop();
	}
}
