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
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.core.configuration.route.execution.policy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.ProcessorDefinition;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreator;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreatorHelper;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.FanIn;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.FanOut;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.ResponseHalf;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.SourceReads;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.Evaluation;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.routebuilder.DmpRouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ONE mapping-route creator: every MappingConfiguration compiles to
 * {@code sources[] + sinks[] + transformer(s)} and this creator wires exactly
 * that, with acquisition, trigger selection and readiness kept independent:
 *
 * <ul>
 * <li>every source contributes its LATEST value to a per-route cache —
 * async sources on message arrival, sync sources polled by their own
 * declared {@code pollingIntervalMs} (the AIMC's IDTA polling interval, MC
 * level or per-source override);</li>
 * <li>every valid source update refreshes the cached contribution; an update
 * evaluates the mapping only when the source's {@link Evaluation} triggers
 * ({@code OnUpdate}, or {@code OnChange} with a changed value) and once all
 * cached sources have delivered;</li>
 * <li>a source read at evaluation has no consumer or poll of its own: each
 * evaluation reads it once, after the readiness gate.</li>
 * </ul>
 *
 * <p>
 * A trigger never changes how an endpoint is read or observed. Triggering
 * contributions still wait for a complete sources snapshot; the others
 * update the cache without executing Lua.
 *
 * <p>
 * Fan-out reuses the shared transform-once multicast {@link FanOut}.
 */
public class ExecutionPolicyRouteCreator implements RouteCreator {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionPolicyRouteCreator.class);

	private final RouteBuilder routeBuilder;
	private final RoutesConfiguration routesConfiguration;

	public ExecutionPolicyRouteCreator(RouteBuilder routeBuilder, RoutesConfiguration routesConfiguration) {
		this.routeBuilder = routeBuilder;
		this.routesConfiguration = routesConfiguration;
	}

	@Override
	public void addRouteToRouteBuilder(RouteConfiguration routeConfig) {
		String routeId = routeConfig.getRouteId() != null ? routeConfig.getRouteId() : "<unnamed-policy>";

		for (String endpointId : routeConfig.getDatasources()) {
			RouteCreatorHelper.getDataSourceEndpoint(routesConfiguration, endpointId, routeId);
		}
		String[] dataSinkEndpoints = RouteCreatorHelper.getDataSinkEndpoints(
				routesConfiguration, routeConfig.getDatasinks(), routeId);
		String[] dataTransformerEndpoints = RouteCreatorHelper.getDataTransformerEndpoints(
				routesConfiguration, routeConfig.getTransformers(), routeId);

		FanIn fanIn = new FanIn(routeConfig, routesConfiguration);
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerManagedResource(fanIn);
		List<String> readAtEvaluation = routeConfig.getDatasources().stream()
				.filter(id -> evaluation(id) == Evaluation.READ_AT_EVALUATION).toList();
		String collectUri = "direct:policy-" + routeId;

		// Install the collector before source/timer ingress routes so it is ready
		// before any source can contribute during dynamic route startup. Sources reach it
		// through FanIn.evaluate, which runs one evaluation at a time on a fresh snapshot.
		registerRouteId(routeId);
		if (routeConfig.getDatasources().isEmpty()) registerIngressRouteId(routeId);
		ProcessorDefinition<?> cycle = routeBuilder.from(collectUri)
				.routeId(routeId)
				.filter(fanIn.gatePredicate())
				.process(fanIn.merge());
		ResponseHalf response = ResponseHalf.of(routeBuilder, routesConfiguration, routeConfig, routeId);
		if (response == null) {
			readAtEvaluation(cycle, fanIn, readAtEvaluation, routeId);
			FanOut.apply(cycle, routesConfiguration, routeConfig,
					dataTransformerEndpoints, dataSinkEndpoints, routeId);
		} else {
			// The response half runs after the sinks, also when the evaluation failed, and inside the
			// evaluation: its writes finish before the next evaluation starts.
			var attempt = cycle.doTry();
			readAtEvaluation(attempt, fanIn, readAtEvaluation, routeId);
			attempt.process(response::keepRequestSources);
			FanOut.apply(attempt, routesConfiguration, routeConfig,
					dataTransformerEndpoints, dataSinkEndpoints, routeId);
			attempt.process(response::succeeded).enrich(response.uri(), ResponseHalf::keepOriginal);
			attempt.doCatch(Exception.class)
					.process(response::failed)
					.enrich(response.uri(), ResponseHalf::keepOriginal)
					.process(ResponseHalf::rethrow)
					.end();
		}

		Map<Acquisition, List<String>> acquisitions = new LinkedHashMap<>();
		for (String endpointId : routeConfig.getDatasources()) {
			if (readAtEvaluation.contains(endpointId)) continue;
			DataSourceConfiguration config = routesConfiguration.getDatasources().get(endpointId);
			acquisitions.computeIfAbsent(Acquisition.of(config), ignored -> new java.util.ArrayList<>()).add(endpointId);
		}
		for (List<String> endpointIds : acquisitions.values()) {
			String endpointId = endpointIds.get(0);
			DataSourceConfiguration config = routesConfiguration.getDatasources().get(endpointId);
			if (config != null && "sync".equalsIgnoreCase(config.getDelivery())) {
				// Sync sources issue a fresh Read when the connector has a read
				// adapter; consumer-only sources retain their polling consumer path.
				long periodMs = effectivePollingIntervalMs(config);
				LOGGER.info("route {}: {} sync source(s) polled every {} ms", routeId, endpointIds.size(), periodMs);
				String pollRouteId = routeId + "-poll-" + endpointId;
				registerRouteId(pollRouteId);
				registerIngressRouteId(pollRouteId);
				ProcessorDefinition<?> poll = routeBuilder
						.from("timer:" + timerName(routeId, endpointId) + "?period=" + periodMs)
						.routeId(pollRouteId);
				RouteCreatorHelper.configureSyncRead(poll, config.getConnectionURI(), SYNC_POLL_TIMEOUT_MS);
				poll.filter(exchange -> exchange.getMessage().getBody() != null)
						.process(fanIn.disassemble(endpointIds))
						.filter(fanIn::contributed)
						.filter(fanIn::selectedTrigger)
						.process(fanIn.evaluate(collectUri));
			} else {
				// async source: the consumer feeds the cache and drives an
				// evaluation on every arrival
				String sourceRouteId = routeId + "-source-" + endpointId;
				registerRouteId(sourceRouteId);
				registerIngressRouteId(sourceRouteId);
				ProcessorDefinition<?> sourceRoute = routeBuilder
						.from(routesConfiguration.getDatasources().get(endpointId).getConnectionURI())
						.routeId(sourceRouteId)
						;
				sourceRoute = RouteCreatorHelper.normalizeSourceEvents(sourceRoute, config, routeId);
				sourceRoute
						.process(fanIn.disassemble(endpointIds))
						.filter(fanIn::contributed)
						.filter(fanIn::selectedTrigger)
						.process(fanIn.evaluate(collectUri));
			}
		}

	}

	/** One fresh Read per evaluation of each source read at evaluation, into the sources map. */
	private void readAtEvaluation(ProcessorDefinition<?> evaluation, FanIn fanIn, List<String> endpointIds, String routeId) {
		for (int i = 0; i < endpointIds.size(); i++) {
			String endpointId = endpointIds.get(i);
			evaluation.enrich(SourceReads.route(routeBuilder, routesConfiguration, routeId, i, endpointId, fanIn),
					SourceReads.into(FanIn.resolveSourceId(routesConfiguration, endpointId)));
		}
	}

	private Evaluation evaluation(String endpointId) {
		DataSourceConfiguration config = routesConfiguration.getDatasources().get(endpointId);
		return config == null ? Evaluation.ON_UPDATE : config.getEvaluation();
	}

	private void registerRouteId(String routeId) {
		if (routeBuilder instanceof DmpRouteBuilder dmpRouteBuilder) {
			dmpRouteBuilder.registerRouteId(routeId);
		}
	}

	private void registerIngressRouteId(String routeId) {
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerIngressRouteId(routeId);
	}

	/** Per-source poll ceiling for one sync read. */
	public static final long SYNC_POLL_TIMEOUT_MS = Long.getLong("dmp.policy.syncPollTimeoutMs", 5000L);

	private static String timerName(String routeId, String endpointId) {
		return ("policy-" + routeId + "-" + endpointId).replaceAll("[^A-Za-z0-9_.-]", "-");
	}

	/** A read source is polled only at an authored interval; none is assumed. */
	private static long effectivePollingIntervalMs(DataSourceConfiguration config) {
		long authored = config.getPollingIntervalMs();
		if (authored <= 0) throw new IllegalArgumentException("read source " + config.getUniqueId()
				+ " has no polling interval; it needs a positive PollingInterval or DefaultPollingInterval");
		return authored;
	}

	/**
	 * What makes two sources share one acquisition: the same consumer or poll, delivered
	 * and normalized the same way. The normalizer is built from the group's first source,
	 * so its sources must also agree on whether their schema decodes the payload as JSON.
	 */
	private record Acquisition(Class<?> type, String uri, String delivery, long pollingIntervalMs,
			String sourceKind, Map<String, Object> eventFilter, boolean jsonSchema) {
		static Acquisition of(DataSourceConfiguration config) {
			if (config == null) return new Acquisition(null, null, null, 0, null, null, false);
			String delivery = config.getDelivery() == null ? null : config.getDelivery().toLowerCase(java.util.Locale.ROOT);
			long pollingIntervalMs = "sync".equals(delivery) ? effectivePollingIntervalMs(config) : 0;
			return new Acquisition(config.getClass(), config.getConnectionURI(), delivery, pollingIntervalMs,
					config.getSourceKind(), config.getEventFilter(),
					FanIn.jsonSchema(config.getSchema()));
		}
	}
}
