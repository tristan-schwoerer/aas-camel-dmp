/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.opcua.component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.milo.client.MiloClientConnection;
import org.apache.camel.support.DefaultConsumer;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaSubscription;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import dk.aau.smartproductionlab.dmp.core.execution.DmpExecutors;
import java.util.Map;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observes one variable (OPC 10101 {@code observeproperty}), or the events of one notifier
 * ({@code subscribeevent}, each delivered as a map of its selected fields, see
 * {@link EventSelection}), with a subscription of its own: the
 * endpoint's {@code requestedPublishingInterval} and {@code samplingInterval} apply to this
 * variable, and the monitored item keeps a queue of samples so a short state between two
 * publishes (e.g. a program's Running) is still delivered. camel-milo shares one subscription
 * per server connection, created with the first endpoint's publishing interval, and keeps only
 * the latest sample. Each message body is the sampled {@code DataValue}, or an event's field map.
 *
 * <p>The subscription is made on the endpoint's shared connection and made again whenever that
 * connection's client changes (first connect, reconnect).
 */
final class DmpMiloSubscriptionConsumer extends DefaultConsumer {
	private static final Logger LOGGER = LoggerFactory.getLogger(DmpMiloSubscriptionConsumer.class);
	private static final long WATCH_PERIOD_MS = 500;
	private static final long MAX_BACKOFF_MS = 30_000;
	private static final double DEFAULT_PUBLISHING_INTERVAL_MS = 1000;
	private static final UInteger QUEUE_SIZE = UInteger.valueOf(16);

	private final DmpMiloClientEndpoint endpoint;
	private MiloClientConnection connection;
	/** Checks the subscription every 500 ms on the shared workers. */
	private DmpExecutors.Ticker watch;
	private volatile OpcUaClient subscribedClient;
	private volatile OpcUaSubscription subscription;
	private volatile java.util.Optional<Map<String, Object>> deviceSchema;
	/** After a failed setup, no new attempt before this time (System.nanoTime), doubling up to 30 s. */
	private long retryAt;
	private long backoffMs = WATCH_PERIOD_MS;

	DmpMiloSubscriptionConsumer(DmpMiloClientEndpoint endpoint, Processor processor) {
		super(endpoint, processor);
		this.endpoint = endpoint;
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		connection = endpoint.createConnection();
		watch = DmpExecutors.of(endpoint.getCamelContext()).every(Duration.ZERO, Duration.ofMillis(WATCH_PERIOD_MS),
				this::ensureSubscription);
	}

	@Override
	protected void doStop() throws Exception {
		// A setup in progress ends first, so the subscription it creates is deleted below (bounded by the request timeout).
		if (watch != null) watch.cancel(Duration.ofSeconds(35));
		watch = null;
		deleteSubscription();
		if (connection != null) endpoint.releaseConnection(connection);
		connection = null;
		super.doStop();
	}

	/** (Re)creates the subscription when the shared connection has a client this consumer is not subscribed with. */
	private void ensureSubscription() {
		OpcUaSubscription created = null;
		try {
			OpcUaClient client = connection == null ? null : connection.getOpcUaClient();
			if (client == null || client == subscribedClient || System.nanoTime() - retryAt < 0) return;
			deleteSubscription();
			Double publishing = endpoint.getConfiguration().getRequestedPublishingInterval();
			created = new OpcUaSubscription(client,
					publishing == null ? DEFAULT_PUBLISHING_INTERVAL_MS : publishing);
			created.create();
			deviceSchema = null;
			NodeId node = endpoint.node(client);
			EventSelection events = endpoint.getEventSelection();
			OpcUaMonitoredItem item;
			if (events == null) {
				item = OpcUaMonitoredItem.newDataItem(node);
				Double sampling = endpoint.getSamplingInterval();
				if (sampling != null) item.setSamplingInterval(sampling);
				item.setDataValueListener((monitored, value) -> {
					// The variable's own type, once per subscription and from a Good sample (a Bad one has no
					// value to tell it): the AID schema is checked against it.
					if (deviceSchema == null && value.getStatusCode() != null && value.getStatusCode().isGood())
						deviceSchema = java.util.Optional.ofNullable(OpcUaStructures.deviceSchema(value, client));
					var schema = deviceSchema;
					deliver(OpcUaStructures.decoded(value, client), schema == null ? null : schema.orElse(null));
				});
			} else {
				// An event item (OPC 10000-4 §5.12.1.4): the server queues each event the filter passes.
				item = OpcUaMonitoredItem.newEventItem(node, events.filter(client));
				item.setEventValueListener((monitored, values) ->
						deliver(events.notification((Variant[]) OpcUaStructures.decoded(values, client)), null));
			}
			item.setQueueSize(QUEUE_SIZE);
			item.setDiscardOldest(true);
			created.addMonitoredItem(item);
			created.synchronizeMonitoredItems();
			subscription = created;
			subscribedClient = client;
			backoffMs = WATCH_PERIOD_MS;
		} catch (Exception failure) {
			LOGGER.warn("OPC UA subscription to {} is not established yet (next attempt in {} ms): {}",
					endpoint.getBrowsePath() == null ? endpoint.getNode() : endpoint.getBrowsePath(), backoffMs, failure.getMessage());
			// The subscription this attempt created lives on the shared session until deleted.
			if (created != null && created != subscription) {
				try { created.delete(); }
				catch (Exception ignored) { /* the session may already be gone */ }
			}
			deleteSubscription();
			retryAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(backoffMs);
			backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
		}
	}

	private void deliver(Object value, Map<String, Object> schema) {
		Exchange exchange = createExchange(true);
		exchange.getMessage().setBody(value);
		if (schema != null) exchange.getMessage().setHeader(dk.aau.smartproductionlab.dmp.core.plan.Source.DEVICE_SCHEMA_HEADER, schema);
		try {
			getProcessor().process(exchange);
		} catch (Exception failure) {
			exchange.setException(failure);
		} finally {
			if (exchange.getException() != null)
				getExceptionHandler().handleException("OPC UA sample processing failed", exchange, exchange.getException());
			releaseExchange(exchange, true);
		}
	}

	private void deleteSubscription() {
		OpcUaSubscription current = subscription;
		subscription = null;
		subscribedClient = null;
		if (current == null) return;
		try { current.delete(); }
		catch (Exception ignored) { /* the session may already be gone */ }
	}
}
