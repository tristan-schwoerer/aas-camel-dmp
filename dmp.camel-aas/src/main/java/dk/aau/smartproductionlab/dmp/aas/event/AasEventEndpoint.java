/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas.event;

import java.util.Map;

import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.support.DefaultEndpoint;
import dk.aau.smartproductionlab.dmp.core.execution.DmpExecutors;
import dk.aau.smartproductionlab.dmp.core.execution.SerialLane;

/** Consumer-only endpoint for one repository element; each consumer processes its events in order on the shared workers. */
final class AasEventEndpoint extends DefaultEndpoint implements org.apache.camel.MultipleConsumersSupport {
	private final AasEventComponent component;
	private final String submodelId;
	private final String idShortPath;

	AasEventEndpoint(String uri, AasEventComponent component, String submodelId, String idShortPath) {
		super(uri, component);
		this.component = component;
		this.submodelId = submodelId;
		this.idShortPath = idShortPath;
	}

	/** Each consumer is a listener of its own on the hub, so mappings may follow the same element. */
	@Override public boolean isMultipleConsumersSupported() { return true; }

	@Override public Producer createProducer() {
		throw new UnsupportedOperationException("aas-event endpoints only consume repository events");
	}

	@Override public Consumer createConsumer(Processor processor) throws Exception {
		Consumer consumer = new EventConsumer(this, processor);
		configureConsumer(consumer);
		return consumer;
	}

	private final class EventConsumer extends DefaultConsumer implements AasEventHub.Listener {
		/** Events this element may have waiting; one more replaces the newest (it carries the newer value). */
		private static final int PENDING_EVENTS = 1024;
		private volatile SerialLane<Delivery> lane;

		/** One event with the headers it arrived with. */
		private record Delivery(com.google.gson.JsonObject event, Map<String, Object> headers) { }

		EventConsumer(AasEventEndpoint endpoint, Processor processor) { super(endpoint, processor); }

		@Override public String submodelId() { return submodelId; }
		@Override public String idShortPath() { return idShortPath; }

		/** Returns at once: the event is processed on a shared worker, after the ones before it. */
		@Override public void deliver(com.google.gson.JsonObject event, Map<String, Object> headers) {
			SerialLane<Delivery> current = lane;
			if (current != null) current.submit(new Delivery(event, headers));
		}

		private void process(Delivery delivery) {
			Exchange exchange = createExchange(false);
			exchange.getMessage().setHeaders(new java.util.HashMap<>(delivery.headers()));
			exchange.getMessage().setBody(delivery.event());
			try { getProcessor().process(exchange); }
			catch (Exception failure) { exchange.setException(failure); }
			if (exchange.getException() != null) getExceptionHandler().handleException("AAS element event failed", exchange, exchange.getException());
			releaseExchange(exchange, false);
		}

		@Override protected void doStart() throws Exception {
			super.doStart();
			lane = new SerialLane<>("AAS events of " + submodelId + " " + idShortPath,
					DmpExecutors.of(getEndpoint().getCamelContext()).workers(), PENDING_EVENTS, this::process,
					(older, newer) -> newer);
			component.hub().register(this);
		}

		@Override protected void doStop() throws Exception {
			component.hub().unregister(this);
			SerialLane<Delivery> current = lane;
			lane = null;
			// An event being processed ends before the route's downstream may stop; waiting ones are dropped.
			if (current != null) {
				current.close();
				current.drain(java.time.Duration.ofSeconds(30));
			}
			super.doStop();
		}
	}
}
