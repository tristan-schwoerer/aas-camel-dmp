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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.camel.ExchangePattern;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriPath;

/**
 * Endpoint for the {@code paho-mqtt5-inout:} scheme.
 *
 * <p>
 * InOut options: {@code replyTopic} (the DECLARED {@code mqv_response_topic}),
 * {@code replyTimeout} (seconds). Each request carries a fresh UUID as its
 * correlation data; the first reply with it is the terminal reply.
 * The publish itself rides the upstream {@code paho-mqtt5:} producer derived
 * from the shared transport options (brokerUrl, clientId, qos, retained, and
 * credentials: the registry name of bound credentials, which the upstream
 * endpoints then reference instead of carrying the secret).
 *
 * @author aas-camel-dmp
 */
@UriEndpoint(firstVersion = "1.0.0-SNAPSHOT", scheme = "paho-mqtt5-inout", title = "Paho MQTT v5 InOut",
		syntax = "paho-mqtt5-inout:topic", producerOnly = true, category = { org.apache.camel.Category.MESSAGING })
public class MqttV5Endpoint extends org.apache.camel.support.DefaultEndpoint
		implements dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource.AccountedEndpoint {

	@UriPath(description = "MQTT topic to publish the request on (absolute topic path)")
	@Metadata(required = true)
	private String topic;

	@Metadata(required = true, description = "MQTT broker URL (tcp://host:port)")
	private String brokerUrl = "tcp://localhost:1883";

	@Metadata(required = true, description = "Client id of the request/reply connection")
	private String clientId;

	@Metadata(defaultValue = "2", description = "Request QoS (reply subscription uses the same QoS)")
	private int qos = 2;

	@Metadata(defaultValue = "false", description = "Retain the published request message")
	private boolean retained = false;

	@Metadata(description = "Reply topic (the declared mqv_response_topic); REQUIRED for InOut")
	private String replyTopic;

	@Metadata(defaultValue = "30", description = "Request budget in seconds including subscription, publication and reply wait")
	private long replyTimeout = 30;

	@Metadata(description = "Registry name of bound credentials (BoundCredential); keeps them out of every URI")
	private String credentials;

	final MqttV5ReplyRegistry replies = new MqttV5ReplyRegistry();
	final ReentrantLock transportLock = new ReentrantLock(true);
	static final long STOP_TIMEOUT_MILLIS = 5_000L;
	private final Set<Object> replyOwners = Collections.newSetFromMap(new IdentityHashMap<>());
	private final Set<Object> publishOwners = Collections.newSetFromMap(new IdentityHashMap<>());
	/** The upstream endpoint's producer that publishes commands; owned by this endpoint. */
	private org.apache.camel.Producer publisher;
	/** Identity of the current reply subscription (survives a failed cleanup until it is retried). */
	private String replySubscriptionId;
	/** The reply listener: a consumer this endpoint owns, never a route, so stopping a producer never stops routes. */
	private org.apache.camel.Consumer replyConsumer;
	private String replyClientId;
	private org.apache.camel.Endpoint replyEndpoint;
	private volatile org.eclipse.paho.mqttv5.client.MqttClient replyClient;
	private volatile org.eclipse.paho.mqttv5.client.MqttClient publishClient;
	private org.apache.camel.component.paho.mqtt5.PahoMqtt5Endpoint publishEndpoint;
	/** The reply client and connection whose subscription the broker acknowledged. */
	private org.eclipse.paho.mqttv5.client.MqttClient confirmedClient;
	private long confirmedConnection = -1;

	/** Local observations only; does not acquire the endpoint monitor or Paho locks. */
	@Override public java.util.List<dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource.AccountingEntry> accounting() {
		var entries = new java.util.ArrayList<dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource.AccountingEntry>(2);
		for (var client : new org.eclipse.paho.mqttv5.client.MqttClient[] { replyClient, publishClient }) {
			if (client instanceof TransportClient owned) entries.add(
					new dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource.AccountingEntry(client,
							java.util.Map.of("mqttClients", 1L, "mqttConnectedClients", owned.connected ? 1L : 0L,
									"mqttObservedOpenTcpSockets", owned.sockets.openSocketCount(),
									"mqttSocketUnobservableClients", owned.socketObservable ? 0L : 1L)));
		}
		return java.util.List.copyOf(entries);
	}

	public MqttV5Endpoint(String endpointUri, MqttV5Component component) {
		super(endpointUri, component);
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getBrokerUrl() {
		return brokerUrl;
	}

	public void setBrokerUrl(String brokerUrl) {
		this.brokerUrl = brokerUrl;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public int getQos() {
		return qos;
	}

	public void setQos(int qos) {
		this.qos = qos;
	}

	public boolean isRetained() {
		return retained;
	}

	public void setRetained(boolean retained) {
		this.retained = retained;
	}

	public String getReplyTopic() {
		return replyTopic;
	}

	public void setReplyTopic(String replyTopic) {
		this.replyTopic = replyTopic;
	}

	public long getReplyTimeout() {
		return replyTimeout;
	}

	public void setReplyTimeout(long replyTimeout) {
		this.replyTimeout = replyTimeout;
	}

	public String getCredentials() {
		return credentials;
	}

	public void setCredentials(String credentials) {
		this.credentials = credentials;
	}

	@Override
	public ExchangePattern getExchangePattern() {
		return ExchangePattern.InOut;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public org.apache.camel.Consumer createConsumer(org.apache.camel.Processor processor) throws Exception {
		throw new UnsupportedOperationException("The paho-mqtt5 component is producer-only "
				+ "(InOut request/reply leg); replies are correlated by the producer.");
	}

	@Override
	public org.apache.camel.Producer createProducer() throws Exception {
		return new MqttV5Producer(this);
	}

	synchronized void acquirePublisher(Object owner) throws Exception {
		if ((publisher != null || publishClient != null) && publishOwners.isEmpty()) {
			// A failed last-owner stop must finish before the transport is reused.
			discardPublisher();
		}
		publishOwners.add(owner);
	}

	synchronized void releasePublisher(Object owner) throws Exception {
		try {
			if ((publisher != null || publishClient != null) && (publishOwners.isEmpty()
					|| publishOwners.size() == 1 && publishOwners.contains(owner))) {
				discardPublisher();
			}
		} finally {
			publishOwners.remove(owner);
		}
	}

	long invocationDeadline() {
		long seconds = Math.min(Math.max(0, replyTimeout), Integer.MAX_VALUE);
		return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(seconds * 1_000L
				+ MqttV5ReplyRegistry.WAIT_MARGIN_MILLIS);
	}

	static long remainingMillis(long deadline) throws TimeoutException {
		long remaining = deadline - System.nanoTime();
		if (remaining <= 0) throw new TimeoutException("MQTT invocation deadline expired");
		// Paho treats zero as an INFINITE wait.
		return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining));
	}

	/** The started producer of the upstream publish endpoint, with a connected transport. */
	synchronized org.apache.camel.Producer preparePublisher(String uri, long deadline) throws Exception {
		org.apache.camel.Endpoint target = getCamelContext().getEndpoint(uri);
		if (target instanceof org.apache.camel.component.paho.mqtt5.PahoMqtt5Endpoint paho) {
			publishEndpoint = paho;
			if (publishClient == null) {
				publishClient = newTransportClient(paho, deadline);
				try {
					connectTransport(paho, publishClient, deadline, false);
					paho.setClient(publishClient);
				} catch (Exception failure) {
					try { discardPublisher(); }
					catch (Exception cleanup) { failure.addSuppressed(cleanup); }
					throw failure;
				}
			}
			publishClient.setTimeToWait(remainingMillis(deadline));
		}
		if (publisher == null) {
			publisher = target.createProducer();
			org.apache.camel.support.service.ServiceHelper.startService(publisher);
		}
		return publisher;
	}

	synchronized void discardPublisher() throws Exception {
		if (publishClient != null) {
			closeTransport(publishClient);
			publishClient = null;
		}
		if (publishEndpoint != null) publishEndpoint.setClient(null);
		if (publisher != null) {
			org.apache.camel.support.service.ServiceHelper.stopAndShutdownService(publisher);
			publisher = null;
		}
	}

	private org.eclipse.paho.mqttv5.client.MqttClient newTransportClient(
			org.apache.camel.component.paho.mqtt5.PahoMqtt5Endpoint paho, long deadline) throws Exception {
		long wait = remainingMillis(deadline);
		var config = paho.getConfiguration();
		var client = new TransportClient(config.getBrokerUrl(),
				config.getClientId() == null ? "dmp-publish-" + UUID.randomUUID() : config.getClientId());
		client.setTimeToWait(wait);
		return client;
	}

	private void connectTransport(org.apache.camel.component.paho.mqtt5.PahoMqtt5Endpoint paho,
			org.eclipse.paho.mqttv5.client.MqttClient client, long deadline, boolean reconnect) throws Exception {
		// Reuse upstream's complete connection-option translation, without changing
		// the shared component defaults or letting it allocate an unbounded client.
		// The endpoint is never started, so it holds nothing to close.
		@SuppressWarnings("resource")
		var optionsAccess = new org.apache.camel.component.paho.mqtt5.PahoMqtt5Endpoint(
				paho.getEndpointUri(), paho.getTopic(), paho.getComponent(), paho.getConfiguration()) {
			org.eclipse.paho.mqttv5.client.MqttConnectionOptions options() {
				return createMqttConnectionOptions();
			}
		};
		var options = optionsAccess.options();
		TransportClient owned = (TransportClient) client;
		String[] servers = options.getServerURIs();
		// Preserve Paho's SSL/WebSocket option translation. Only plain TCP has an
		// instrumented factory; unknown transports are not reported as zero sockets.
		if (paho.getConfiguration().getBrokerUrl().startsWith("tcp://")
				&& (servers == null || java.util.Arrays.stream(servers).allMatch(server -> server.startsWith("tcp://")))
				&& options.getSocketFactory() == null) {
			options.setSocketFactory(owned.sockets);
			owned.socketObservable = true;
		}
		((TransportClient) client).reconnectEnabled = reconnect && options.isAutomaticReconnect();
		// Paho 1.2.5 does not cancel its private automatic-reconnect Timer on close.
		// Reply reconnects use an owned, cancellable scheduler instead.
		options.setAutomaticReconnect(false);
		((TransportClient) client).reconnectOptions = options;
		options.setConnectionTimeout((int) Math.max(1, Math.min(Integer.MAX_VALUE,
				(remainingMillis(deadline) + 999) / 1_000L)));
		client.connect(options);
		client.setTimeToWait(remainingMillis(deadline));
	}

	private static void closeTransport(org.eclipse.paho.mqttv5.client.MqttClient client) throws Exception {
		// disconnect() ignores setTimeToWait and can quiesce pending QoS tokens forever.
		((TransportClient) client).retireReconnects();
		try { client.disconnectForcibly(0, 1_000, false); }
		finally { client.close(true); ((TransportClient) client).connected = false; }
	}

	/** Counts established, not locally closed Java TCP sockets, not endpoint URIs or clients. */
	static final class ObservedSocketFactory extends javax.net.SocketFactory {
		private final java.util.concurrent.CopyOnWriteArrayList<ObservedSocket> sockets = new java.util.concurrent.CopyOnWriteArrayList<>();
		@Override public java.net.Socket createSocket() {
			var socket = new ObservedSocket();
			sockets.removeIf(previous -> previous.closed);
			sockets.add(socket);
			return socket;
		}
		long openSocketCount() {
			return sockets.stream().filter(socket -> socket.connected && !socket.closed).count();
		}
		@Override public java.net.Socket createSocket(String host, int port) throws java.io.IOException {
			return connected(new java.net.InetSocketAddress(host, port), null);
		}
		@Override public java.net.Socket createSocket(String host, int port, java.net.InetAddress local, int localPort) throws java.io.IOException {
			return connected(new java.net.InetSocketAddress(host, port), new java.net.InetSocketAddress(local, localPort));
		}
		@Override public java.net.Socket createSocket(java.net.InetAddress host, int port) throws java.io.IOException {
			return connected(new java.net.InetSocketAddress(host, port), null);
		}
		@Override public java.net.Socket createSocket(java.net.InetAddress host, int port, java.net.InetAddress local, int localPort) throws java.io.IOException {
			return connected(new java.net.InetSocketAddress(host, port), new java.net.InetSocketAddress(local, localPort));
		}
		private java.net.Socket connected(java.net.SocketAddress remote, java.net.SocketAddress local) throws java.io.IOException {
			java.net.Socket socket = createSocket();
			try {
				if (local != null) socket.bind(local);
				socket.connect(remote);
				return socket;
			} catch (java.io.IOException | RuntimeException failure) {
				try { socket.close(); } catch (java.io.IOException cleanup) { failure.addSuppressed(cleanup); }
				throw failure;
			}
		}
		private static final class ObservedSocket extends java.net.Socket {
			private volatile boolean connected, closed;
			@Override public void connect(java.net.SocketAddress remote, int timeout) throws java.io.IOException {
				super.connect(remote, timeout);
				connected = true;
			}
			@Override public void close() throws java.io.IOException {
				super.close();
				closed = true;
			}
		}
	}

	private static final class TransportClient extends org.eclipse.paho.mqttv5.client.MqttClient {
		private final java.util.concurrent.ScheduledExecutorService reconnects;
		private volatile boolean retired;
		private boolean reconnectEnabled;
		private org.eclipse.paho.mqttv5.client.MqttConnectionOptions reconnectOptions;
		private long reconnectDelayMillis = 1_000;
		private final ObservedSocketFactory sockets = new ObservedSocketFactory();
		private volatile boolean socketObservable;
		private volatile boolean connected;
		/** Counts established connections, so a reconnect is told apart from the connection before it. */
		private volatile long connections;

		TransportClient(String broker, String id) throws org.eclipse.paho.mqttv5.common.MqttException {
			super(broker, id, new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());
			reconnects = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
					task -> new Thread(task, "dmp-mqtt-reconnect:" + id));
		}

		void retireReconnects() {
			retired = true;
			reconnects.shutdownNow();
		}

		@Override public void connect(org.eclipse.paho.mqttv5.client.MqttConnectionOptions options)
				throws org.eclipse.paho.mqttv5.common.MqttException {
			super.connect(options);
			connections++;
			if (!retired) connected = true;
		}

		private void reconnectLater() {
			if (retired) return;
			try {
				reconnects.schedule(() -> {
					if (retired) return;
					try {
						connect(reconnectOptions);
						reconnectDelayMillis = 1_000;
					} catch (org.eclipse.paho.mqttv5.common.MqttException failure) {
						reconnectDelayMillis = Math.min(reconnectDelayMillis * 2,
								Math.max(1_000, reconnectOptions.getMaxReconnectDelay()));
						reconnectLater();
					}
				}, reconnectDelayMillis, TimeUnit.MILLISECONDS);
			} catch (java.util.concurrent.RejectedExecutionException stopped) {
				if (!retired) throw stopped;
			}
		}

		@Override public void setCallback(org.eclipse.paho.mqttv5.client.MqttCallback callback) {
			if (callback == null) { super.setCallback(null); return; }
			super.setCallback(new org.eclipse.paho.mqttv5.client.MqttCallback() {
				@Override public void disconnected(org.eclipse.paho.mqttv5.client.MqttDisconnectResponse response) {
					connected = false;
					callback.disconnected(response);
					if (reconnectEnabled) reconnectLater();
				}
				@Override public void connectComplete(boolean reconnect, String server) {
					// Initial connect precedes consumer callback installation. Subsequent
					// connects must retain upstream's resubscription callback contract.
					if (!retired) {
						connected = true;
						if (reconnect) connections++;
						callback.connectComplete(reconnectEnabled || reconnect, server);
					}
				}
				@Override public void messageArrived(String topic, org.eclipse.paho.mqttv5.common.MqttMessage message)
						throws Exception { callback.messageArrived(topic, message); }
				@Override public void deliveryComplete(org.eclipse.paho.mqttv5.client.IMqttToken token) {
					callback.deliveryComplete(token);
				}
				@Override public void mqttErrorOccurred(org.eclipse.paho.mqttv5.common.MqttException failure) {
					callback.mqttErrorOccurred(failure);
				}
				@Override public void authPacketArrived(int reason,
						org.eclipse.paho.mqttv5.common.packet.MqttProperties properties) {
					callback.authPacketArrived(reason, properties);
				}
			});
		}
	}

	synchronized String replyClientId() {
		if (replyClientId == null) replyClientId = "dmp-reply-" + UUID.randomUUID();
		return replyClientId;
	}

	synchronized void acquireReplySubscription(Object owner, String replyUri) throws Exception {
		acquireReplySubscription(owner, replyUri, invocationDeadline());
	}

	synchronized void acquireReplySubscription(Object owner, String replyUri, long deadline) throws Exception {
		if (replyOwners.contains(owner)) {
			assertReplyReady(deadline);
			return;
		}
		if (replySubscriptionId == null) {
			replySubscriptionId = "dmp-mqtt-reply-" + UUID.randomUUID();
			try {
				replyEndpoint = getCamelContext().getEndpoint(replyUri);
				if (replyEndpoint instanceof org.apache.camel.component.paho.mqtt5.PahoMqtt5Endpoint paho) {
					replyClient = newTransportClient(paho, deadline);
					connectTransport(paho, replyClient, deadline, true);
					paho.setClient(replyClient);
				}
				replyConsumer = replyEndpoint.createConsumer(new MqttV5ReplyMatcher(replies, getCamelContext(),
						brokerUrl, replyTopic));
				org.apache.camel.support.service.ServiceHelper.startService(replyConsumer);
				assertReplyReady(deadline);
			} catch (Exception failure) {
				try {
					removeReplySubscription();
				} catch (Exception cleanup) {
					failure.addSuppressed(cleanup);
				}
				throw failure;
			}
		} else {
			// A failed cleanup must not turn into a successful registration.
			if (replyOwners.isEmpty()) {
				removeReplySubscription();
				acquireReplySubscription(owner, replyUri, deadline);
				return;
			}
			assertReplyReady(deadline);
		}
		replyOwners.add(owner);
	}

	private void assertReplyReady(long deadline) throws Exception {
		if (!(replyConsumer instanceof org.apache.camel.support.service.ServiceSupport service) || !service.isStarted()) {
			throw new IllegalStateException("MQTT reply subscription is not started");
		}
		// Paho's synchronous consumer start waits for SUBACK before returning.
		if (replyConsumer instanceof org.apache.camel.component.paho.mqtt5.PahoMqtt5Consumer consumer) {
			replyClient = consumer.getClient();
			if (replyClient == null || !replyClient.isConnected()) {
				throw new IllegalStateException("MQTT reply subscription is not connected");
			}
			// Upstream discards its SUBACK token. Confirm broker acceptance once per
			// connection (again after a reconnect), not once per call: a call that
			// waited for a SUBACK held the transport lock for a broker round trip.
			long connection = replyClient instanceof TransportClient owned ? owned.connections : -1;
			if (replyClient == confirmedClient && connection == confirmedConnection && connection >= 0) return;
			replyClient.setTimeToWait(remainingMillis(deadline));
			int[] reasons = replyClient.subscribe(replyTopic, qos).getReasonCodes();
			if (reasons == null || reasons.length != 1 || reasons[0] < 0 || reasons[0] > 2) {
				throw new IllegalStateException("MQTT broker rejected the reply subscription");
			}
			confirmedClient = replyClient;
			confirmedConnection = connection;
		}
	}

	synchronized void releaseReplySubscription(Object owner) throws Exception {
		try {
			if (replyOwners.isEmpty() || replyOwners.size() == 1 && replyOwners.contains(owner)) {
				removeReplySubscription();
			}
		} finally {
			replyOwners.remove(owner);
		}
	}

	private void removeReplySubscription() throws Exception {
		if (replySubscriptionId == null) return;
		if (replyClient != null) {
			closeTransport(replyClient);
			replyClient = null;
		}
		if (replyConsumer != null) {
			org.apache.camel.support.service.ServiceHelper.stopAndShutdownService(replyConsumer);
			replyConsumer = null;
		}
		if (replyEndpoint != null) {
			getCamelContext().removeEndpoint(replyEndpoint);
			replyEndpoint = null;
		}
		replySubscriptionId = null;
		replyClientId = null;
		confirmedClient = null;
		confirmedConnection = -1;
	}

	/** The current reply subscription's identity, or null (for lifecycle checks). */
	synchronized String replySubscriptionId() { return replySubscriptionId; }

	/** True while the reply consumer is started. */
	synchronized boolean replySubscribed() {
		return replyConsumer instanceof org.apache.camel.support.service.ServiceSupport service && service.isStarted();
	}

	/** The reply endpoint in use or awaiting cleanup, or null. */
	synchronized org.apache.camel.Endpoint replyEndpoint() { return replyEndpoint; }
}
