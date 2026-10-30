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
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.camel.support.DefaultProducer;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishBuilder;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.core.util.ConnectorJsonPayload;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;

/**
 * A topic on a shared {@link MqttSession}.
 *
 * <p>A consumer subscribes the topic filter and receives each message as its payload bytes with the topic and
 * QoS headers ({@link #MQTT_TOPIC}, {@link #MQTT_QOS}, named as camel-paho-mqtt5 named them) and the message's
 * content type as {@link Exchange#CONTENT_TYPE}.
 *
 * <p>A producer publishes the body (a map, list, number or boolean as JSON) and returns once the broker took it
 * (QoS 1 and 2) or it was sent (QoS 0); the topic, QoS and retain flag may be set per message with
 * {@link #OVERRIDE_TOPIC}, {@link #MSG_QOS} and {@link #MSG_RETAINED}. With a {@code replyTopic}, an exchange that
 * expects a reply is an MQTT 5 request:
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
	/** A received message's topic and QoS. */
	public static final String MQTT_TOPIC = "CamelMqttTopic";
	public static final String MQTT_QOS = "CamelMqttQoS";
	/** Per-message overrides of the endpoint's topic, QoS and retain flag on a publication. */
	public static final String OVERRIDE_TOPIC = "CamelPahoMqtt5OverrideTopic";
	public static final String MSG_QOS = "CamelPahoMqtt5Qos";
	public static final String MSG_RETAINED = "CamelPahoMqtt5Retained";
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

	/** The broker acknowledged this endpoint's topic filter on its session's current connection. */
	public boolean isSubscribed() {
		MqttSession session = component.session(key);
		return session != null && session.confirmed(topic);
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

		/** On the session's lane, one message at a time: the route only accepts the value there. */
		@Override public void deliver(String messageTopic, Mqtt5Publish message) {
			if (!isRunAllowed()) return;
			Exchange exchange = createExchange(false);
			exchange.getMessage().setBody(message.getPayloadAsBytes());
			exchange.getMessage().setHeader(MQTT_TOPIC, messageTopic);
			exchange.getMessage().setHeader(MQTT_QOS, message.getQos().getCode());
			message.getContentType().ifPresent(type -> exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, type.toString()));
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
			Mqtt5PublishBuilder.Complete message = Mqtt5Publish.builder()
					.topic(in.getHeader(OVERRIDE_TOPIC, topic, String.class))
					.qos(MqttSession.qos(in.getHeader(MSG_QOS, qos, Integer.class)))
					.retain(in.getHeader(MSG_RETAINED, retained, Boolean.class))
					.payload(payload(exchange));
			try {
				if (replyTopic == null || !exchange.getPattern().isOutCapable()) {
					publish(current, admitted, message.build(), deadline);
					return;
				}
				exchange.getMessage().setBody(request(current, admitted, message, deadline));
			} catch (TimeoutException timedOut) {
				throw timeout();
			}
		}

		private String request(MqttSession current, long admitted, Mqtt5PublishBuilder.Complete message, long deadline)
				throws Exception {
			awaitReplySubscription(current, deadline);
			String correlation = UUID.randomUUID().toString();
			CompletableFuture<String> reply;
			synchronized (this) {
				if (generation != admitted) throw stopped();
				reply = replies.register(this, key.brokerUrl(), replyTopic, correlation);
			}
			try {
				Mqtt5Publish request = message.responseTopic(replyTopic)
						.correlationData(correlation.getBytes(StandardCharsets.UTF_8)).build();
				publish(current, admitted, request, deadline);
				LOGGER.debug("MQTT request on {} (replyTopic={}, correlation={})", request.getTopic(), replyTopic, correlation);
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
		private void publish(MqttSession current, long admitted, Mqtt5Publish message, long deadline) throws Exception {
			CompletableFuture<Mqtt5PublishResult> sent;
			synchronized (this) {
				if (generation != admitted) throw stopped();
				sent = current.send(message);
			}
			try {
				sent.get(Math.max(1, remaining(deadline).toMillis()), TimeUnit.MILLISECONDS);
			} catch (ExecutionException failed) {
				// The broker refused it (a PUBACK/PUBREC error) or the connection ended before it was acknowledged.
				throw failed.getCause() instanceof Exception cause ? cause : failed;
			}
		}

		/** A reply: the first one carrying a pending request's correlation data completes it. */
		@Override public void deliver(String messageTopic, Mqtt5Publish message) {
			if (message.getCorrelationData().isEmpty()) {
				LOGGER.debug("MQTT reply on {} without correlation data ignored", messageTopic);
				return;
			}
			String correlation = StandardCharsets.UTF_8.decode(message.getCorrelationData().get()).toString();
			if (!replies.complete(key.brokerUrl(), replyTopic, correlation, new String(message.getPayloadAsBytes(), StandardCharsets.UTF_8)))
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
		// A string for a JSON sink: the JSON string, quoted, never the wrapper's toString().
		if (body instanceof ConnectorJsonPayload json) return JsonCodec.toJson(json.value()).getBytes(StandardCharsets.UTF_8);
		String text = body == JsonPointer.JSON_NULL || body instanceof Map<?, ?> || body instanceof List<?>
				|| body instanceof Number || body instanceof Boolean ? JsonCodec.toJson(body)
				: exchange.getMessage().getBody(String.class);
		return text.getBytes(StandardCharsets.UTF_8);
	}
}
