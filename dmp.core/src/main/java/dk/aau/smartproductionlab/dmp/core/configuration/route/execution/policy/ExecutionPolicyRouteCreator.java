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
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreatorHelper;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.FanIn;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.FanOut;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.ResponseHalf;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.SourceReads;
import dk.aau.smartproductionlab.dmp.core.execution.DmpPollComponent;
import dk.aau.smartproductionlab.dmp.core.plan.Evaluation;
import dk.aau.smartproductionlab.dmp.core.plan.MappingRoute;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import dk.aau.smartproductionlab.dmp.core.routebuilder.DmpRouteBuilder;
import dk.aau.smartproductionlab.dmp.core.routebuilder.PlanBuild;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ONE mapping-route creator: every {@link MappingRoute} is
 * {@code sources[] + sinks[] + transformer} and this creator wires exactly
 * that, with acquisition, trigger selection and readiness kept independent:
 *
 * <ul>
 * <li>every source contributes its LATEST value to a per-route cache —
 * observed sources on message arrival, read sources polled by their own
 * {@code pollingIntervalMs} (the AIMC's IDTA polling interval, MC
 * level or per-source override) through {@code dmp-poll:}, whose ticks share
 * one scheduler;</li>
 * <li>the consumer's thread only accepts the value: evaluations run on the
 * shared workers, one at a time per mapping ({@link FanIn#evaluate});</li>
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
public class ExecutionPolicyRouteCreator {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionPolicyRouteCreator.class);

	private final RouteBuilder routeBuilder;
	private final PlanBuild build;

	public ExecutionPolicyRouteCreator(RouteBuilder routeBuilder) {
		this.routeBuilder = routeBuilder;
		this.build = PlanBuild.of(routeBuilder);
	}

	public void create(MappingRoute mapping) {
		String routeId = mapping.routeId();
		List<Sink> sinks = mapping.sinks();
		String[] dataSinkEndpoints = build.uris(sinks.stream().map(Sink::endpoint).toList());
		String[] dataTransformerEndpoints = mapping.transformation() == null ? new String[0]
				: new String[] { build.uri(mapping.transformation()) };

		Map<String, String> sinkUris = new LinkedHashMap<>();
		for (int i = 0; i < sinks.size(); i++)
			if (sinks.get(i).sinkId() != null) sinkUris.put(sinks.get(i).sinkId(), dataSinkEndpoints[i]);
		FanIn fanIn = new FanIn(routeId, mapping.sources(), sinkUris);
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerManagedResource(fanIn);
		List<Source> readAtEvaluation = mapping.sources().stream()
				.filter(source -> source.evaluation() == Evaluation.READ_AT_EVALUATION).toList();
		String collectUri = "direct:policy-" + routeId;

		// Install the collector before source/poll ingress routes so it is ready
		// before any source can contribute during dynamic route startup. Sources reach it
		// through FanIn.evaluate, which queues one evaluation at a time on the shared workers.
		registerRouteId(routeId);
		if (mapping.sources().isEmpty()) registerIngressRouteId(routeId);
		ProcessorDefinition<?> cycle = routeBuilder.from(collectUri)
				.routeId(routeId)
				.filter(fanIn.gatePredicate())
				.process(fanIn.merge());
		ResponseHalf response = mapping.response() == null ? null
				: new ResponseHalf(routeBuilder, build, mapping.response(), routeId);
		if (response == null) {
			readAtEvaluation(cycle, fanIn, readAtEvaluation, routeId);
			FanOut.apply(cycle, sinks, dataTransformerEndpoints, dataSinkEndpoints, routeId);
		} else {
			// The response half runs after the sinks, also when the evaluation failed, and inside the
			// evaluation: its writes finish before the next evaluation starts.
			var attempt = cycle.doTry();
			readAtEvaluation(attempt, fanIn, readAtEvaluation, routeId);
			attempt.process(response::keepRequestSources);
			FanOut.apply(attempt, sinks, dataTransformerEndpoints, dataSinkEndpoints, routeId);
			attempt.process(response::succeeded).enrich(response.uri(), ResponseHalf::keepOriginal);
			attempt.doCatch(Exception.class)
					.process(response::failed)
					.enrich(response.uri(), ResponseHalf::keepOriginal)
					.process(ResponseHalf::rethrow)
					.end();
		}

		Map<Acquisition, List<Source>> acquisitions = new LinkedHashMap<>();
		for (Source source : mapping.sources()) {
			if (source.evaluation() == Evaluation.READ_AT_EVALUATION) continue;
			acquisitions.computeIfAbsent(Acquisition.of(source, build.uri(source.endpoint())),
					ignored -> new java.util.ArrayList<>()).add(source);
		}
		for (List<Source> group : acquisitions.values()) {
			Source source = group.get(0);
			List<String> endpointIds = group.stream().map(Source::id).toList();
			String endpointId = source.id();
			String uri = build.uri(source.endpoint());
			if (source.read()) {
				// Read sources issue a fresh Read when the connector has a read
				// adapter; consumer-only sources retain their polling consumer path.
				long periodMs = effectivePollingIntervalMs(source);
				LOGGER.info("route {}: {} sync source(s) polled every {} ms", routeId, endpointIds.size(), periodMs);
				String pollRouteId = routeId + "-poll-" + endpointId;
				registerRouteId(pollRouteId);
				registerIngressRouteId(pollRouteId);
				ProcessorDefinition<?> poll = routeBuilder
						.from(DmpPollComponent.uri("policy-" + routeId + "-" + endpointId, periodMs))
						.routeId(pollRouteId);
				RouteCreatorHelper.configureSyncRead(poll, uri, SYNC_POLL_TIMEOUT_MS);
				poll.filter(exchange -> exchange.getMessage().getBody() != null)
						.process(fanIn.disassemble(endpointIds))
						.filter(fanIn::contributed)
						.filter(fanIn::selectedTrigger)
						.process(fanIn.evaluate(collectUri));
			} else {
				// observed source: the consumer feeds the cache and drives an
				// evaluation on every arrival
				String sourceRouteId = routeId + "-source-" + endpointId;
				registerRouteId(sourceRouteId);
				registerIngressRouteId(sourceRouteId);
				ProcessorDefinition<?> sourceRoute = routeBuilder
						.from(uri)
						.routeId(sourceRouteId)
						;
				sourceRoute = RouteCreatorHelper.normalizeSourceEvents(sourceRoute, source);
				sourceRoute
						.process(fanIn.disassemble(endpointIds))
						.filter(fanIn::contributed)
						.filter(fanIn::selectedTrigger)
						.process(fanIn.evaluate(collectUri));
			}
		}

	}

	/** One fresh Read per evaluation of each source read at evaluation, into the sources map. */
	private void readAtEvaluation(ProcessorDefinition<?> evaluation, FanIn fanIn, List<Source> sources, String routeId) {
		for (int i = 0; i < sources.size(); i++) {
			Source source = sources.get(i);
			evaluation.enrich(SourceReads.route(routeBuilder, build, routeId, i, source, fanIn),
					SourceReads.into(source.sourceId()));
		}
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

	/** A read source is polled only at an authored interval; none is assumed. */
	private static long effectivePollingIntervalMs(Source source) {
		long authored = source.pollingIntervalMs();
		if (authored <= 0) throw new IllegalArgumentException("read source " + source.id()
				+ " has no polling interval; it needs a positive PollingInterval or DefaultPollingInterval");
		return authored;
	}

	/**
	 * What makes two sources share one acquisition: the same consumer or poll, delivered
	 * and normalized the same way. The normalizer is built from the group's first source,
	 * so its sources must also agree on whether their schema decodes the payload as JSON.
	 */
	private record Acquisition(Class<?> type, String uri, Source.Delivery delivery, long pollingIntervalMs,
			boolean jsonSchema) {
		static Acquisition of(Source source, String uri) {
			long pollingIntervalMs = source.read() ? effectivePollingIntervalMs(source) : 0;
			return new Acquisition(source.endpoint().getClass(), uri, source.delivery(), pollingIntervalMs,
					FanIn.jsonSchema(source.schema()));
		}
	}
}
