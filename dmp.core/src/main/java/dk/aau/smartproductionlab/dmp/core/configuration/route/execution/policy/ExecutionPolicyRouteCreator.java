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
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.FanIn;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.FanOut;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.ResponseHalf;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.SourceAcquisition;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.SourceReads;
import dk.aau.smartproductionlab.dmp.core.plan.Evaluation;
import dk.aau.smartproductionlab.dmp.core.plan.MappingRoute;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import dk.aau.smartproductionlab.dmp.core.routebuilder.DmpRouteBuilder;
import dk.aau.smartproductionlab.dmp.core.routebuilder.PlanBuild;

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
 * evaluation reads it once, after the readiness gate;</li>
 * <li>an observed {@code Cached} source with an initial read is read once when
 * its route starts ({@link SourceAcquisition}).</li>
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
		if (mapping.sources().isEmpty() && routeBuilder instanceof DmpRouteBuilder managed) managed.registerIngressRouteId(routeId);
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

		SourceAcquisition.install(routeBuilder, build, routeId, fanIn, mapping.sources(), collectUri);
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

	/** Per-source poll ceiling for one sync read. */
	public static final long SYNC_POLL_TIMEOUT_MS = Long.getLong("dmp.policy.syncPollTimeoutMs", 5000L);
}
