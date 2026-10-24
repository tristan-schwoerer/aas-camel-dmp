/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.MultipleConsumersSupport;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.component.paho.mqtt5.PahoMqtt5Constants;
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.camel.support.DefaultProducer;
import org.eclipse.paho.mqttv5.client.MqttClientException;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;

/**
 * A topic on a shared {@link MqttSession}.
 *
 * <p>A consumer subscribes the topic filter and receives each message as its payload bytes with the headers
 * camel-paho-mqtt5 sets ({@code CamelMqttTopic}, {@code CamelMqttQoS}, the MQTT 5 properties) and the message's
 * content type as {@link Exchange#CONTENT_TYPE}.
 *
 * <p>A producer publishes the body (a map, list, number or boolean as JSON) and returns once the broker took it
 * (QoS 1 and 2) or it was sent (QoS 0); the topic, QoS, retain flag and properties may be set per message with
 * camel-paho-mqtt5's headers. With a {@code replyTopic}, an exchange that expects a reply is an MQTT 5 request:
 * the reply topic is subscribed (confirmed again on every new connection, before anything is published), the
 * request carries the ResponseTopic and fresh CorrelationData, and the first message on the reply topic with that
 * correlation data is the reply; the device publishes anything else (progress) without it. One deadline,
 * {@code replyTimeout} plus a second, bounds the subscription, the publication and the reply. A request is
 * published at most once: neither a timeout nor a reconnect sends it again, and a request admitted before the
 * producer stopped is not published after.
 */
public final class DmpPahoMqtt5Endpoint extends DefaultEndpoint implements MultipleConsumersSupport,
		ManagedRouteResource.AccountedEndpoint {
	private static final Logger LOGGER = LoggerFactory.getLogger(DmpPahoMqtt5Endpoint.class);
	/** Scheduling margin beyond {@code replyTimeout}. */
	static final long WAIT_MARGIN_MILLIS = 1_000L;

	private final DmpPahoMqtt5Component component;
	private final String topic;
	private final MqttSession.Key key;
	private final int qos;
	private final boolean retained;
	private final String replyTopic;
	private final long replyTimeout;
	final MqttReplyRegistry replies = new MqttReplyRegistry();

	DmpPahoMqtt5Endpoint(String uri, DmpPahoMqtt5Component component, String topic, MqttSession.Key key, int qos,
			boolean retained, String replyTopic, long replyTimeout) {
		super(uri, component);
		this.component = component;
		this.topic = topic;
		this.key = key;
		this.qos = qos;
		this.retained = retained;
		this.replyTopic = replyTopic;
		this.replyTimeout = replyTimeout;
	}

	public String getTopic() { return topic; }
	public String getBrokerUrl() { return key.brokerUrl(); }
	public int getQos() { return qos; }
	public String getReplyTopic() { return replyTopic; }
	public long getReplyTimeout() { return replyTimeout; }
	/** The resolved credentials the session connects with; the URI only names them. */
	public String getUserName() { return key.userName(); }
	public String getPassword() { return key.password(); }

	/** Mappings that read one topic each have a consumer of their own. */
	@Override public boolean isMultipleConsumersSupported() { return true; }

	@Override public Consumer createConsumer(Processor processor) throws Exception {
		Consumer consumer = new SubscriptionConsumer(processor);
		configureConsumer(consumer);
		return consumer;
	}

	@Override public Producer createProducer() { return new PublishProducer(); }

	/** The session this endpoint's broker and credentials share, if one is open; no I/O and no lock. */
	@Override public List<ManagedRouteResource.AccountingEntry> accounting() {
		MqttSession session = component.session(key);
		return session == null ? List.of() : List.of(new ManagedRouteResource.AccountingEntry(session,
				Map.of("mqttClients", 1L, "mqttConnectedClients", session.isConnected() ? 1L : 0L)));
	}

	private long deadline() {
		return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(replyTimeout * 1_000L + WAIT_MARGIN_MILLIS);
	}

	private static Duration remaining(long deadline) throws TimeoutException {
		long left = deadline - System.nanoTime();
		if (left <= 0) throw new TimeoutException("MQTT invocation deadline expired");
		return Duration.ofNanos(left);
	}

	private final class SubscriptionConsumer extends DefaultConsumer implements MqttSession.Listener {
		private MqttSession session;

		SubscriptionConsumer(Processor processor) { super(DmpPahoMqtt5Endpoint.this, processor); }

		@Override protected void doStart() throws Exception {
			super.doStart();
			MqttSession acquired = component.acquire(key, this);
			try {
				acquired.subscribe(this, topic, qos);
			} catch (Exception refused) {
				component.release(acquired, this);
				throw refused;
			}
			session = acquired;
		}

		@Override protected void doStop() throws Exception {
			MqttSession current = session;
			session = null;
			if (current != null) {
				current.unsubscribe(this, topic);
				component.release(current, this);
			}
			super.doStop();
		}

		/** On the session's callback thread: the route only accepts the value there. */
		@Override public void deliver(String messageTopic, MqttMessage message) {
			if (!isRunAllowed()) return;
			Exchange exchange = createExchange(false);
			exchange.getMessage().setBody(message.getPayload());
			exchange.getMessage().setHeader(PahoMqtt5Constants.MQTT_TOPIC, messageTopic);
			exchange.getMessage().setHeader(PahoMqtt5Constants.MQTT_QOS, message.getQos());
			MqttProperties properties = message.getProperties();
			if (properties != null) {
				exchange.getMessage().setHeader(PahoMqtt5Constants.CAMEL_PAHO_MSG_PROPERTIES, properties);
				if (properties.getContentType() != null) exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, properties.getContentType());
			}
			try {
				getProcessor().process(exchange);
			} catch (Exception failure) {
				exchange.setException(failure);
			}
			if (exchange.getException() != null) getExceptionHandler().handleException("MQTT message on " + messageTopic + " failed", exchange, exchange.getException());
			releaseExchange(exchange, false);
		}
	}

	private final class PublishProducer extends DefaultProducer implements MqttSession.Listener {
		private MqttSession session;
		/** Incremented by every stop: a request admitted before it never publishes after it. */
		private long generation;
		private boolean replySubscribed;
		private final java.util.concurrent.locks.ReentrantLock replySubscription = new java.util.concurrent.locks.ReentrantLock();

		PublishProducer() { super(DmpPahoMqtt5Endpoint.this); }

		@Override protected void doStart() throws Exception {
			super.doStart();
			MqttSession acquired = component.acquire(key, this);
			synchronized (this) { session = acquired; }
		}

		@Override protected void doStop() throws Exception {
			MqttSession current;
			boolean subscribed;
			synchronized (this) {
				current = session;
				session = null;
				generation++;
				subscribed = replySubscribed;
				replySubscribed = false;
				replies.cancelOwner(this);
			}
			if (current != null) {
				if (subscribed) current.unsubscribe(this, replyTopic);
				component.release(current, this);
			}
			super.doStop();
		}

		@Override public void process(Exchange exchange) throws Exception {
			long deadline = deadline();
			MqttSession current;
			long admitted;
			synchronized (this) {
				if (session == null) throw new IllegalStateException("the MQTT producer is not started");
				current = session;
				admitted = generation;
			}
			var in = exchange.getMessage();
			MqttMessage message = new MqttMessage(payload(exchange));
			message.setQos(in.getHeader(PahoMqtt5Constants.CAMEL_PAHO_MSG_QOS, qos, Integer.class));
			message.setRetained(in.getHeader(PahoMqtt5Constants.CAMEL_PAHO_MSG_RETAINED, retained, Boolean.class));
			MqttProperties properties = in.getHeader(PahoMqtt5Constants.CAMEL_PAHO_MSG_PROPERTIES, MqttProperties.class);
			if (properties != null) message.setProperties(properties);
			String target = in.getHeader(PahoMqtt5Constants.CAMEL_PAHO_OVERRIDE_TOPIC, topic, String.class);
			try {
				if (replyTopic == null || !exchange.getPattern().isOutCapable()) {
					publish(current, admitted, target, message, deadline);
					return;
				}
				exchange.getMessage().setBody(request(current, admitted, target, message, deadline));
			} catch (TimeoutException timedOut) {
				throw timeout();
			} catch (MqttException failed) {
				if (failed.getReasonCode() == MqttClientException.REASON_CODE_CLIENT_TIMEOUT) throw timeout();
				throw failed;
			}
		}

		private String request(MqttSession current, long admitted, String target, MqttMessage message, long deadline)
				throws Exception {
			awaitReplySubscription(current, deadline);
			String correlation = UUID.randomUUID().toString();
			CompletableFuture<String> reply;
			synchronized (this) {
				if (generation != admitted) throw stopped();
				reply = replies.register(this, key.brokerUrl(), replyTopic, correlation);
			}
			try {
				MqttProperties properties = message.getProperties() == null ? new MqttProperties() : message.getProperties();
				properties.setResponseTopic(replyTopic);
				properties.setCorrelationData(correlation.getBytes(StandardCharsets.UTF_8));
				message.setProperties(properties);
				publish(current, admitted, target, message, deadline);
				LOGGER.debug("MQTT request on {} (replyTopic={}, correlation={})", target, replyTopic, correlation);
				return reply.get(remaining(deadline).toNanos(), TimeUnit.NANOSECONDS);
			} catch (CancellationException cancelled) {
				throw stopped();
			} catch (ExecutionException failed) {
				throw new IllegalStateException("MQTT correlated reply failed", failed.getCause());
			} catch (InterruptedException interrupted) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("interrupted while awaiting the correlated MQTT reply", interrupted);
			} finally {
				replies.unregister(this, key.brokerUrl(), replyTopic, correlation, reply);
			}
		}

		/** The reply topic is subscribed once, and confirmed on the current connection before every request. */
		private void awaitReplySubscription(MqttSession current, long deadline) throws Exception {
			if (!replySubscription.tryLock(remaining(deadline).toNanos(), TimeUnit.NANOSECONDS)) throw new TimeoutException("reply subscription");
			try {
				boolean subscribed;
				synchronized (this) { subscribed = replySubscribed; }
				if (!subscribed) {
					current.subscribe(this, replyTopic, qos, remaining(deadline));
					synchronized (this) { replySubscribed = true; }
				}
				current.confirm(replyTopic, remaining(deadline));
			} finally {
				replySubscription.unlock();
			}
		}

		/** Starts the publication unless the producer stopped since the call was admitted, then waits for it. */
		private void publish(MqttSession current, long admitted, String target, MqttMessage message, long deadline) throws Exception {
			org.eclipse.paho.mqttv5.client.IMqttToken token;
			synchronized (this) {
				if (generation != admitted) throw stopped();
				token = current.send(target, message);
			}
			token.waitForCompletion(Math.max(1, remaining(deadline).toMillis()));
		}

		/** A reply: the first one carrying a pending request's correlation data completes it. */
		@Override public void deliver(String messageTopic, MqttMessage message) {
			MqttProperties properties = message.getProperties();
			if (properties == null || properties.getCorrelationData() == null) {
				LOGGER.debug("MQTT reply on {} without correlation data ignored", messageTopic);
				return;
			}
			String correlation = new String(properties.getCorrelationData(), StandardCharsets.UTF_8);
			if (!replies.complete(key.brokerUrl(), replyTopic, correlation, new String(message.getPayload(), StandardCharsets.UTF_8)))
				LOGGER.debug("MQTT reply with unknown correlation {} ignored", correlation);
		}

		private ConnectorInvocationException timeout() {
			return new ConnectorInvocationException(ConnectorInvocationException.Category.TIMEOUT, "MQTT_REQUEST_TIMEOUT",
					replyTopic == null ? "the MQTT broker did not acknowledge the publication in time"
							: "Timed out awaiting correlated MQTT reply",
					replyTopic == null ? Map.of("topic", topic, "timeoutSeconds", replyTimeout)
							: Map.of("topic", topic, "responseTopic", replyTopic, "timeoutSeconds", replyTimeout));
		}

		private IllegalStateException stopped() {
			return new IllegalStateException("the MQTT producer stopped");
		}
	}

	/** The body as MQTT payload: bytes as they are, a JSON value as JSON text, anything else as its text. */
	private static byte[] payload(Exchange exchange) {
		Object body = exchange.getMessage().getBody();
		if (body == null) return new byte[0];
		if (body instanceof byte[] bytes) return bytes;
		String text = body == JsonPointer.JSON_NULL || body instanceof Map<?, ?> || body instanceof List<?>
				|| body instanceof Number || body instanceof Boolean ? JsonCodec.toJson(body)
				: exchange.getMessage().getBody(String.class);
		return text.getBytes(StandardCharsets.UTF_8);
	}
}
