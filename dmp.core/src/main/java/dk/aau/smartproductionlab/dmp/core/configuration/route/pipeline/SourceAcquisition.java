/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.ProcessorDefinition;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreatorHelper;
import dk.aau.smartproductionlab.dmp.core.configuration.route.execution.policy.ExecutionPolicyRouteCreator;
import dk.aau.smartproductionlab.dmp.core.execution.DmpPollComponent;
import dk.aau.smartproductionlab.dmp.core.plan.Evaluation;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import dk.aau.smartproductionlab.dmp.core.routebuilder.DmpRouteBuilder;
import dk.aau.smartproductionlab.dmp.core.routebuilder.PlanBuild;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * How a mapping's sources acquire their values (SPEC §6), into its {@link FanIn} cache: an observed source through
 * its consumer, a read source by a poll at its interval ({@code dmp-poll:}), and an observed source with an
 * {@link Source#initialRead()} also by one read when its route starts, so it has a value before its first update.
 * A source read at evaluation acquires nothing here.
 *
 * <p>An ordinary mapping passes its collector: a contribution then evaluates the mapping when its source triggers
 * ({@link FanIn#selectedTrigger}). An Operation passes none: its sources only keep their latest value for the calls.
 */
public final class SourceAcquisition {

	private static final Logger LOGGER = LoggerFactory.getLogger(SourceAcquisition.class);

	private SourceAcquisition() { }

	/**
	 * Adds the acquisition routes of {@code sources} to {@code routeBuilder}, as ingress routes of {@code routeId}'s
	 * bundle; {@code collectUri} is the collector evaluations are sent to.
	 */
	public static void install(RouteBuilder routeBuilder, PlanBuild build, String routeId, FanIn fanIn,
			List<Source> sources, String collectUri) {
		Map<Acquisition, List<Source>> acquisitions = new LinkedHashMap<>();
		for (Source source : sources) {
			if (source.evaluation() == Evaluation.READ_AT_EVALUATION) continue;
			acquisitions.computeIfAbsent(Acquisition.of(source, build.uri(source.endpoint())),
					ignored -> new ArrayList<>()).add(source);
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
				register(routeBuilder, pollRouteId);
				ProcessorDefinition<?> poll = routeBuilder
						.from(DmpPollComponent.uri("policy-" + routeId + "-" + endpointId, periodMs))
						.routeId(pollRouteId);
				RouteCreatorHelper.configureSyncRead(poll, uri, ExecutionPolicyRouteCreator.SYNC_POLL_TIMEOUT_MS);
				accept(poll.filter(exchange -> exchange.getMessage().getBody() != null)
						.process(fanIn.disassemble(endpointIds)), fanIn, collectUri);
			} else {
				// observed source: the consumer feeds the cache and drives an
				// evaluation on every arrival
				String sourceRouteId = routeId + "-source-" + endpointId;
				register(routeBuilder, sourceRouteId);
				ProcessorDefinition<?> sourceRoute = routeBuilder
						.from(uri)
						.routeId(sourceRouteId);
				sourceRoute = RouteCreatorHelper.normalizeSourceEvents(sourceRoute, source);
				accept(sourceRoute.process(fanIn.disassemble(endpointIds)), fanIn, collectUri);
			}
		}
		for (Source source : sources)
			if (source.initialRead() != null && source.evaluation() != Evaluation.READ_AT_EVALUATION)
				initialRead(routeBuilder, build, routeId, fanIn, source, collectUri);
	}

	/** After a contribution: an ordinary mapping evaluates when the source triggers. */
	private static void accept(ProcessorDefinition<?> contribution, FanIn fanIn, String collectUri) {
		contribution.filter(fanIn::contributed)
				.filter(fanIn::selectedTrigger)
				.process(fanIn.evaluate(collectUri));
	}

	/**
	 * One read of {@code source}'s affordance when its route starts. It fills only an empty cache, so an update that
	 * arrived first wins; a failed read leaves the source waiting for its first update.
	 */
	private static void initialRead(RouteBuilder routeBuilder, PlanBuild build, String routeId, FanIn fanIn,
			Source source, String collectUri) {
		String endpointId = source.id();
		String readRouteId = routeId + "-initial-" + endpointId;
		register(routeBuilder, readRouteId);
		Processor seed = fanIn.seed(List.of(endpointId));
		Processor evaluate = fanIn.evaluate(collectUri);
		var read = routeBuilder.from(DmpPollComponent.once("initial-" + routeId + "-" + endpointId))
				.routeId(readRouteId)
				.doTry();
		RouteCreatorHelper.configureSyncRead(read, build.uri(source.initialRead()),
				ExecutionPolicyRouteCreator.SYNC_POLL_TIMEOUT_MS);
		read.process(exchange -> {
					seed.process(exchange);
					// A trigger the readiness gate held back for this value evaluates now.
					if (fanIn.contributed(exchange) && fanIn.selectedTrigger(exchange))
						evaluate.process(exchange);
				})
				.doCatch(Exception.class)
					.process(exchange -> LOGGER.warn("route {}: the initial read of source {} failed; it waits for its"
							+ " first update: {}", routeId, source.sourceId(), failure(exchange)))
				.end();
	}

	private static String failure(Exchange exchange) {
		Throwable cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
		return cause == null ? "unknown" : String.valueOf(cause.getMessage());
	}

	/** An acquisition route starts with its bundle's ingress. */
	private static void register(RouteBuilder routeBuilder, String routeId) {
		if (routeBuilder instanceof DmpRouteBuilder managed) {
			managed.registerRouteId(routeId);
			managed.registerIngressRouteId(routeId);
		}
	}

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
