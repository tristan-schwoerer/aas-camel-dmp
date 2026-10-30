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
package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.Objects;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.model.ProcessorDefinition;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.EndpointIdentityProvider;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;
import dk.aau.smartproductionlab.dmp.core.util.ConnectorJsonPayload;
import dk.aau.smartproductionlab.dmp.core.diagnostics.SinkWriteEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fan-out: the mirror image of the fan-in half (SPEC §6).
 *
 * <p>Both halves follow the same shape and share one vocabulary:
 *
 * <pre>
 *   fan-in    N source contributions ─▶ ONE {@link KeyedSnapshot} keyed by SourceId
 *             (disassembled at the source, gated on completeness)
 *   transform ONE evaluation of the shared snapshot
 *   fan-out   the shared result      ─▶ ONE {@link KeyedSnapshot} keyed by SinkId
 *             (parsed once)            ─▶ one payload per device affordance
 * </pre>
 *
 * <p>The invariants are explicit here, not inherited from a framework detail:
 *
 * <ol>
 * <li>the transform result is parsed <b>once</b> into a single immutable
 * {@link KeyedSnapshot};</li>
 * <li>each sink contribution is derived from that snapshot, never from a
 * sibling's output;</li>
 * <li>members of a device affordance are staged in the current exchange and
 * validated together before its single send.</li>
 * </ol>
 *
 * <p>Sinks are emitted in configured order through one sequential Split — no
 * {@code multicast} (whose contract is "the <i>same</i> message to N endpoints",
 * not "N differently shaped messages") and no per-sink {@code direct:} branch
 * routes, so route count stays a function of mappings rather than of sinks.
 */
public final class FanOut {

	private static final Logger LOGGER = LoggerFactory.getLogger(FanOut.class);

	/** Exchange property carrying the one shared, immutable result snapshot. */
	private static final String RESULT_PROPERTY = "dmp.sharedResult";
	/** Exchange property carrying the sinks that failed, if any. */
	private static final String SINK_FAILURES_PROPERTY = "dmp.sinkFailures";
	private static final String GROUP_PAYLOADS_PROPERTY = "dmp.groupPayloads";
	private static final String GROUP_FAILURES_PROPERTY = "dmp.groupFailures";
	private static final String SINK_RESULT_PROPERTY = "dmp.sinkResult";
	private static final String BRANCH_BASE_PROPERTIES = "dmp.branchBaseProperties";
	private static final String BRANCH_BASE_HEADERS = "dmp.branchBaseHeaders";
	private static final String DISPATCH_JOBS_PROPERTY = "dmp.dispatchJobs";
	private static final String DISPATCH_OUTCOMES_PROPERTY = "dmp.dispatchOutcomes";
	private static final String CURRENT_DISPATCH_JOB = "dmp.currentDispatchJob";
	private static final String DISPATCH_ENDPOINT_HEADER = "dmp.dispatchEndpoint";
	private static final String SINK_WRITE_EVENT = "dmp.sinkWriteEvent";
	/** Exchange property carrying each SinkId's value as staged for its sink (a response's ResponseSources). */
	private static final String SINK_VALUES_PROPERTY = "dmp.sinkValues";
	/** Exchange property holding the first sink failure, the cause of the fan-out's own. */
	private static final String FIRST_FAILURE_PROPERTY = "dmp.firstSinkFailure";

	private FanOut() {
	}

	/**
	 * Appends transform + fan-out to {@code route}.
	 *
	 * @param transformerEndpoints
	 *            the mapping's transformers (run once)
	 * @param sinkEndpoints
	 *            the endpoint of each of {@code sinks}, in order
	 */
	public static void apply(ProcessorDefinition<?> route, List<Sink> sinks, String[] transformerEndpoints,
			String[] sinkEndpoints, String routeId) {
		apply(route, sinks, transformerEndpoints, sinkEndpoints, routeId, AffordanceValidation.configuredMode());
	}

	static void apply(ProcessorDefinition<?> route, List<Sink> sinks, String[] transformerEndpoints,
			String[] sinkEndpoints, String routeId, AffordanceValidation.Mode mode) {
		String[] configIds = sinks.stream().map(Sink::id).toArray(String[]::new);
		if (configIds.length != sinkEndpoints.length)
			throw new IllegalArgumentException("route " + routeId + ": one endpoint per sink is required");

		if (transformerEndpoints != null && transformerEndpoints.length > 0) {
			route.process(exchange -> {
				if (LOGGER.isDebugEnabled()) LOGGER.debug("route {}: body before transformer: {}",
						routeId, exchange.getMessage().getBody());
			});
			route.to(transformerEndpoints);
		}

		if (sinkEndpoints.length == 0) {
			LOGGER.warn("route {}: no sink endpoints", routeId);
			return;
		}

		String[] groupKeys = new String[sinkEndpoints.length];
		Map<String, Integer> groupLast = new LinkedHashMap<>();
		Map<String, List<String>> groupMembers = new LinkedHashMap<>();
		Map<String, String> valuePaths = new LinkedHashMap<>();
		Map<String, String> groupEndpoints = new LinkedHashMap<>();
		Map<String, Object> groupSchemas = new LinkedHashMap<>();
		Map<String, AffordanceValidation> payloadValidators = new LinkedHashMap<>();
		Map<String, AffordanceValidation> memberValidators = new LinkedHashMap<>();
		for (int i = 0; i < sinkEndpoints.length; i++) {
			Sink config = sinks.get(i);
			valuePaths.put(configIds[i], config.valuePath());
			memberValidators.put(configIds[i], new AffordanceValidation(config.schema(), "sink " + configIds[i], mode));
			if (!config.endpoint().appliesValuePath()
					&& config.payloadGroup() != null && !config.payloadGroup().isBlank()) {
				String group = config.payloadGroup();
				groupKeys[i] = group;
				String previousEndpoint = groupEndpoints.putIfAbsent(group, sinkEndpoints[i]);
				if (previousEndpoint != null
						&& !sameDestination(previousEndpoint, sinkEndpoints[i])) {
					throw new IllegalArgumentException("payloadGroup " + group + " has different endpoint URIs");
				}
				if (groupSchemas.containsKey(group) && !Objects.equals(groupSchemas.get(group), config.payloadSchema())) {
					throw new IllegalArgumentException("payloadGroup " + group + " has different payload schemas");
				}
				groupSchemas.put(group, config.payloadSchema());
				groupLast.put(group, i);
				groupMembers.computeIfAbsent(group, ignored -> new ArrayList<>()).add(configIds[i]);
				payloadValidators.putIfAbsent(group, new AffordanceValidation(config.payloadSchema(),
						"payloadGroup " + group, mode));
			}
		}

		// invariant 1: the shared result is frozen ONCE into one immutable
		// snapshot; every sink below shares that same value, uncopied
		route.process(exchange -> {
			KeyedSnapshot result = KeyedSnapshot.parse(exchange.getMessage().getBody());
			exchange.setProperty(RESULT_PROPERTY, result);
			exchange.setProperty(SINK_FAILURES_PROPERTY, new java.util.ArrayList<String>());
			exchange.setProperty(GROUP_PAYLOADS_PROPERTY, new LinkedHashMap<String, Map<String, Object>>());
			exchange.setProperty(GROUP_FAILURES_PROPERTY, new java.util.HashSet<String>());
			exchange.setProperty(DISPATCH_JOBS_PROPERTY, new ArrayList<DispatchJob>());
			exchange.setProperty(SINK_VALUES_PROPERTY, new LinkedHashMap<String, Object>());
			exchange.setProperty(FIRST_FAILURE_PROPERTY, new java.util.concurrent.atomic.AtomicReference<Exception>());
			exchange.setProperty(BRANCH_BASE_PROPERTIES, baseProperties(exchange.getProperties()));
			exchange.setProperty(BRANCH_BASE_HEADERS, new LinkedHashMap<>(exchange.getMessage().getHeaders()));
			LOGGER.debug("route {}: body after transformer: {}",
					routeId, exchange.getMessage().getBody());
		});

		// invariant 2 + 3: each sink's body is derived from the shared value
		// (never from a sibling's output) and no per-branch message copying is
		// relied upon. Sinks are emitted in configured order from one route, so
		// route count is a function of mappings, not of sinks.
		for (int i = 0; i < sinkEndpoints.length; i++) {
			final String configId = configIds[i];
			final Sink sink = sinks.get(i);
			final String group = groupKeys[i];
			final boolean sendGroup = group != null && groupLast.get(group) == i;
			final String endpoint = group == null ? sinkEndpoints[i] : groupEndpoints.get(group);
			// Selection and transformation failures belong to this sink/group, not the mapping.
			org.apache.camel.model.TryDefinition attempt = route.doTry();
			// start every sink from the shared result
			attempt.process(exchange -> {
				restoreBranchState(exchange);
				KeyedSnapshot result = snapshotOf(exchange);
				exchange.setProperty(SINK_RESULT_PROPERTY, result);
				exchange.getMessage().setBody(selectedValue(result, sink));
			});
			// every sink is attempted; failures are collected and reported once
			// at the end rather than aborting the remaining sinks
			attempt.process(e -> {
						Object member = e.getMessage().getBody();
						memberValidators.get(configId).outgoing(member == JsonPointer.JSON_NULL ? null : member);
						if (sink.sinkId() != null) sinkValues(e).put(sink.sinkId(), member);
						if (group != null) {
							if (sendGroup && groupFailures(e).contains(group)) {
								throw new IllegalArgumentException("payloadGroup " + group + " has a failed member");
							}
							Map<String, Object> staged = groupPayloads(e)
									.computeIfAbsent(group, ignored -> new LinkedHashMap<>());
							staged.put(configId, member);
							if (sendGroup) {
								Object merged = PayloadAssembler.assemble(staged, groupMembers.get(group), valuePaths);
								groupPayloads(e).remove(group);
								payloadValidators.get(group).outgoing(merged);
								e.getMessage().setBody(merged);
							}
						} else {
							e.getMessage().setBody(assemble(member,
									e.getProperty(SINK_RESULT_PROPERTY, KeyedSnapshot.class), sink));
						}
					});
			if (group == null || sendGroup) {
				boolean jsonPayload = sink.jsonPayload();
				attempt.process(e -> {
					dispatchJobs(e).add(new DispatchJob(routeId, configId, endpoint,
							e.getMessage().getBody(), jsonPayload));
				});
			}
			attempt.doCatch(Exception.class)
					.process(e -> {
						if (group != null) {
							groupFailures(e).add(group);
						}
						sinkFailures(e).add(endpoint);
						firstFailure(e).compareAndSet(null, e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class));
						LOGGER.warn("route {}: sink {} ({}) failed",
								routeId, configId, endpoint, e);
					})
					.end();
		}

		var split = route.split(org.apache.camel.builder.Builder.exchangeProperty(DISPATCH_JOBS_PROPERTY));
		split.aggregationStrategy(new DispatchOutcomeAggregationStrategy());
		split.onPrepare(FanOut::restoreBranchMetadata);
		split.process(exchange -> {
			DispatchJob job = (DispatchJob) exchange.getMessage().getBody();
			stripFanOutState(exchange);
			exchange.setProperty(CURRENT_DISPATCH_JOB, job);
			SinkWriteEvent write = new SinkWriteEvent();
			write.begin();
			exchange.setProperty(SINK_WRITE_EVENT, write);
			exchange.getMessage().setHeader(DISPATCH_ENDPOINT_HEADER, job.endpoint());
			if (job.jsonPayload()) exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
			else exchange.getMessage().removeHeader(Exchange.CONTENT_TYPE);
			Object payload = job.payload();
			if (job.jsonPayload() && payload instanceof String) {
				payload = new ConnectorJsonPayload(payload);
			}
			exchange.getMessage().setBody(payload);
		});
		var send = split.doTry();
		var endpointChoice = send.choice();
		for (String dispatchEndpoint : Arrays.stream(sinkEndpoints).distinct().toList()) {
			endpointChoice.when(exchange -> dispatchEndpoint.equals(
					exchange.getMessage().getHeader(DISPATCH_ENDPOINT_HEADER, String.class)))
					.to(dispatchEndpoint);
		}
		endpointChoice.otherwise().process(exchange -> {
			throw new IllegalStateException("fan-out job references an unconfigured sink endpoint");
		});
		endpointChoice.end();
		send.process(exchange -> {
			commitWrite(exchange, true);
			exchange.getMessage().setBody(
					new DispatchOutcome((DispatchJob) exchange.getProperty(CURRENT_DISPATCH_JOB), null));
		});
		send.doCatch(Exception.class)
				.process(exchange -> {
					commitWrite(exchange, false);
					exchange.getMessage().setBody(new DispatchOutcome(
							(DispatchJob) exchange.getProperty(CURRENT_DISPATCH_JOB),
							exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class)));
				})
				.end();
		split.end();
		route.process(FanOut::collectDispatchOutcomes);
		route.process(exchange -> {
			restoreBranchState(exchange);
			exchange.removeProperty(BRANCH_BASE_PROPERTIES);
			exchange.removeProperty(BRANCH_BASE_HEADERS);
			reportFailures(exchange);
		});
	}

	private static final class DispatchOutcomeAggregationStrategy implements AggregationStrategy {
		@Override
		@SuppressWarnings("unchecked")
		public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
			Exchange aggregate = oldExchange == null ? newExchange : oldExchange;
			List<DispatchOutcome> outcomes = aggregate.getProperty(DISPATCH_OUTCOMES_PROPERTY, List.class);
			if (outcomes == null) {
				outcomes = new ArrayList<>();
				aggregate.setProperty(DISPATCH_OUTCOMES_PROPERTY, outcomes);
			}
			outcomes.add(newExchange.getMessage().getBody(DispatchOutcome.class));
			return aggregate;
		}
	}

	private record DispatchJob(String routeId, String configId, String endpoint, Object payload,
			boolean jsonPayload) { }

	/** Ends a sink write's {@link SinkWriteEvent} once its connector answered. */
	private static void commitWrite(Exchange exchange, boolean succeeded) {
		SinkWriteEvent write = exchange.getProperty(SINK_WRITE_EVENT, SinkWriteEvent.class);
		if (write == null || !write.shouldCommit()) return;
		DispatchJob job = (DispatchJob) exchange.getProperty(CURRENT_DISPATCH_JOB);
		write.mapping = job.routeId();
		write.sink = job.configId();
		int colon = job.endpoint().indexOf(':');
		write.kind = colon <= 0 ? "unknown" : job.endpoint().substring(0, colon);
		write.succeeded = succeeded;
		write.commit();
	}

	private record DispatchOutcome(DispatchJob job, Exception failure) { }

	@SuppressWarnings("unchecked")
	private static java.util.concurrent.atomic.AtomicReference<Exception> firstFailure(Exchange exchange) {
		return (java.util.concurrent.atomic.AtomicReference<Exception>) exchange.getProperty(FIRST_FAILURE_PROPERTY);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> sinkValues(Exchange exchange) {
		return (Map<String, Object>) exchange.getProperty(SINK_VALUES_PROPERTY);
	}

	/**
	 * After the fan-out: each SinkId's value as it was sent (a member of an assembled payload is its own
	 * value), for the ResponseSources that project it. Empty before the fan-out.
	 */
	public static Map<String, Object> writtenValues(Exchange exchange) {
		Map<String, Object> values = sinkValues(exchange);
		return values == null ? Map.of() : java.util.Collections.unmodifiableMap(values);
	}

	@SuppressWarnings("unchecked")
	private static List<DispatchJob> dispatchJobs(Exchange exchange) {
		return (List<DispatchJob>) exchange.getProperty(DISPATCH_JOBS_PROPERTY);
	}

	private static void collectDispatchOutcomes(Exchange exchange) {
		List<?> outcomes = exchange.getProperty(DISPATCH_OUTCOMES_PROPERTY, List.class);
		if (outcomes == null) return;
		for (Object value : outcomes) {
			DispatchOutcome outcome = (DispatchOutcome) value;
			if (outcome.failure() == null) continue;
			sinkFailures(exchange).add(outcome.job().endpoint());
			firstFailure(exchange).compareAndSet(null, outcome.failure());
			LOGGER.warn("route {}: sink {} ({}) failed",
					outcome.job().routeId(), outcome.job().configId(), outcome.job().endpoint(), outcome.failure());
		}
	}

	private static void restoreBranchState(org.apache.camel.Exchange exchange) {
		restoreBranchMetadata(exchange);
		exchange.getMessage().setBody(snapshotOf(exchange).root());
	}

	@SuppressWarnings("unchecked")
	private static void restoreBranchMetadata(org.apache.camel.Exchange exchange) {
		Map<String, Object> properties = (Map<String, Object>) exchange.getProperty(BRANCH_BASE_PROPERTIES);
		Map<String, Object> headers = (Map<String, Object>) exchange.getProperty(BRANCH_BASE_HEADERS);
		if (properties == null || headers == null) return;
		exchange.getProperties().clear();
		exchange.getProperties().putAll(properties);
		exchange.setProperty(BRANCH_BASE_PROPERTIES, properties);
		exchange.setProperty(BRANCH_BASE_HEADERS, headers);
		exchange.getMessage().setHeaders(new LinkedHashMap<>(headers));
	}

	private static void stripFanOutState(Exchange exchange) {
		exchange.removeProperty(RESULT_PROPERTY);
		exchange.removeProperty(SINK_FAILURES_PROPERTY);
		exchange.removeProperty(GROUP_PAYLOADS_PROPERTY);
		exchange.removeProperty(GROUP_FAILURES_PROPERTY);
		exchange.removeProperty(DISPATCH_JOBS_PROPERTY);
		exchange.removeProperty(SINK_VALUES_PROPERTY);
		exchange.removeProperty(FIRST_FAILURE_PROPERTY);
		exchange.removeProperty(BRANCH_BASE_PROPERTIES);
		exchange.removeProperty(BRANCH_BASE_HEADERS);
	}

	/**
	 * The properties every sink starts from. Map and list values are frozen once (a value already frozen, like
	 * the sources, is shared as it is), so a sink can never change what its siblings see; the fan-out's own
	 * state stays shared and writable.
	 */
	private static Map<String, Object> baseProperties(Map<String, Object> properties) {
		Map<String, Object> base = new LinkedHashMap<>();
		properties.forEach((key, value) -> base.put(key, isFanOutState(key)
				|| !(value instanceof Map<?, ?> || value instanceof List<?>) ? value : JsonCodec.freeze(value)));
		return base;
	}

	private static boolean isFanOutState(String key) {
		return RESULT_PROPERTY.equals(key) || SINK_FAILURES_PROPERTY.equals(key)
				|| GROUP_PAYLOADS_PROPERTY.equals(key) || GROUP_FAILURES_PROPERTY.equals(key)
				|| DISPATCH_JOBS_PROPERTY.equals(key) || SINK_VALUES_PROPERTY.equals(key)
				|| FIRST_FAILURE_PROPERTY.equals(key) || BRANCH_BASE_PROPERTIES.equals(key)
				|| BRANCH_BASE_HEADERS.equals(key);
	}

	private static Object selectedValue(KeyedSnapshot result, Sink config) {
		if (config.sinkId() != null && !config.sinkId().isBlank()) {
			if (!result.has(config.sinkId())) {
				throw new IllegalArgumentException("Sink '" + config.id()
						+ "': SinkId '" + config.sinkId() + "' not present in the transform result");
			}
			Object value = result.value(config.sinkId());
			return value == null ? JsonPointer.JSON_NULL : value;
		}
		return result.root();
	}

	static boolean sameDestination(String first, String second) {
		if (Objects.equals(first, second)) return true;
		if (first == null || second == null) return false;
		List<EndpointIdentityProvider> providers = java.util.ServiceLoader
				.load(EndpointIdentityProvider.class, Thread.currentThread().getContextClassLoader())
				.stream().map(java.util.ServiceLoader.Provider::get).toList();
		EndpointIdentityProvider left = providers.stream().filter(provider -> provider.supports(first))
				.findFirst().orElse(null);
		EndpointIdentityProvider right = providers.stream().filter(provider -> provider.supports(second))
				.findFirst().orElse(null);
		return left != null && right != null && left.getClass().equals(right.getClass())
				&& left.identity(first).equals(right.identity(second));
	}

	@SuppressWarnings("unchecked")
	private static java.util.Set<String> groupFailures(org.apache.camel.Exchange exchange) {
		return (java.util.Set<String>) exchange.getProperty(GROUP_FAILURES_PROPERTY);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Map<String, Object>> groupPayloads(org.apache.camel.Exchange exchange) {
		return (Map<String, Map<String, Object>>) exchange.getProperty(GROUP_PAYLOADS_PROPERTY);
	}

	private static KeyedSnapshot snapshotOf(org.apache.camel.Exchange exchange) {
		return exchange.getProperty(RESULT_PROPERTY, KeyedSnapshot.class);
	}

	@SuppressWarnings("unchecked")
	private static List<String> sinkFailures(org.apache.camel.Exchange exchange) {
		return (List<String>) exchange.getProperty(SINK_FAILURES_PROPERTY);
	}

	/**
	 * Makes a partial fan-out visible: every sink was attempted, and a sink that
	 * failed must not pass as a successful write. Throwing here (after the fact)
	 * keeps both properties — all sinks attempted, and the failure reported.
	 * Change this to a debug log to make fan-out best-effort.
	 */
	private static void reportFailures(org.apache.camel.Exchange exchange) {
		List<String> failures = sinkFailures(exchange);
		if (failures.isEmpty()) {
			return;
		}
		throw new IllegalStateException("fan-out could not deliver to sinks: " + failures, firstFailure(exchange).get());
	}

	/** The same shaping step, over an arbitrary (possibly transformed) value. */
	private static Object assemble(Object value, KeyedSnapshot result, Sink config) {
		String valuePath = config.valuePath();
		if (config.endpoint().appliesValuePath()) {
			return valuePath == null || valuePath.isBlank() ? value : result.root();
		}
		return PayloadAssembler.assemble(java.util.Collections.singletonMap(config.id(), value),
				List.of(config.id()), java.util.Collections.singletonMap(config.id(), valuePath));
	}
}
