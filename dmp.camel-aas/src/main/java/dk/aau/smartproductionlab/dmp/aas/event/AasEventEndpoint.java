/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas.event;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.support.DefaultEndpoint;

/** Consumer-only endpoint for one repository element; events are delivered in order on one thread. */
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
		private ExecutorService executor;

		EventConsumer(AasEventEndpoint endpoint, Processor processor) { super(endpoint, processor); }

		@Override public String submodelId() { return submodelId; }
		@Override public String idShortPath() { return idShortPath; }

		@Override public void deliver(com.google.gson.JsonObject event, Map<String, Object> headers) {
			ExecutorService current = executor;
			if (current == null || current.isShutdown()) return;
			current.execute(() -> {
				Exchange exchange = createExchange(false);
				exchange.getMessage().setHeaders(new java.util.HashMap<>(headers));
				exchange.getMessage().setBody(event);
				try { getProcessor().process(exchange); }
				catch (Exception failure) { exchange.setException(failure); }
				if (exchange.getException() != null) getExceptionHandler().handleException("AAS element event failed", exchange, exchange.getException());
				releaseExchange(exchange, false);
			});
		}

		@Override protected void doStart() throws Exception {
			super.doStart();
			executor = getEndpoint().getCamelContext().getExecutorServiceManager().newSingleThreadExecutor(this, "aas-event");
			component.hub().register(this);
		}

		@Override protected void doStop() throws Exception {
			component.hub().unregister(this);
			if (executor != null) getEndpoint().getCamelContext().getExecutorServiceManager().shutdown(executor);
			executor = null;
			super.doStop();
		}
	}
}
