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
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas.operation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.AffordanceValidation;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.FanIn;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.ResponseSinkFanOutProcessor;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.ResponseSinkTargets;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.SourceReads;
import dk.aau.smartproductionlab.dmp.core.plan.Response;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import dk.aau.smartproductionlab.dmp.core.routebuilder.DmpRouteBuilder;
import dk.aau.smartproductionlab.dmp.core.routebuilder.PlanBuild;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generalized interactive-bridge creator (ADR-020 ProtocolBridge, DMP v3).
 *
 * <p>
 * Applies to every operation-delegation mapping: the descriptor derives the
 * caller-facing REST surface by convention ({@link OperationRoute.Identity#restPath()},
 * served with POST). This AAS connector emits
 * the REST consumer and completes the accepted Operation invocation. Target
 * request/reply behavior is delegated to registered connector adapters:
 * <pre>
 * rest → flatten → fan-in → request Lua → named command → Invoke adapter [InOut]
 *      → response Lua (ResponseSources, requestSources) → reply envelope (OperationVariable[] for BaSyx callers)
 * </pre>
 * <p>Target connectors own endpoint addressing, request encoding,
 * correlation, reply normalization and native error/status conversion.
 *
 * <p>
 * The interactive consumer joins the DMP's consolidated REST surface
 * (camel-rest with jetty, same port as {@code /health}).
 */
public class ProtocolBridgeRouteCreator {

	private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolBridgeRouteCreator.class);
	private static final String CONTENT_TYPE_JSON = "application/json";

	private final RouteBuilder routeBuilder;
	private final PlanBuild build;

	public ProtocolBridgeRouteCreator(RouteBuilder routeBuilder) {
		this.routeBuilder = routeBuilder;
		this.build = PlanBuild.of(routeBuilder);
	}

	public void create(OperationRoute bridge) {
		String routeId = bridge.routeId();
		String restPath = bridge.identity().restPath();
		String action = bridge.identity().action();
		OperationRoute.Command command = bridge.command();
		OperationRoute.Caller caller = bridge.caller();
		Response response = bridge.response();

		String requestLuaUri = build.uri(bridge.requestTransformation());
		String responseLuaUri = response == null ? null : build.uri(response.transformation());
		String commandSinkUri = build.uri(command.sink().endpoint());
		AffordanceValidation commandValidation = commandValidation(command.sink());
		List<ResponseSinkFanOutProcessor.Target> responseSinkTargets = response == null ? List.of()
				: ResponseSinkTargets.resolve(routeBuilder, build, routeId, response.sinks());
		List<Response.ResponseSource> responseSourceBindings = response == null ? List.of() : response.sources();
		List<String> commandSourceIds = command.members().stream().map(OperationRoute.Command.Member::sinkId).toList();
		Map<String, String> commandValuePaths = new LinkedHashMap<>();
		for (OperationRoute.Command.Member member : command.members())
			if (member.valuePath() != null) commandValuePaths.put(member.sinkId(), member.valuePath());
		String method = "POST";

		// 1. Interactive consumer (consolidated camel-rest surface), emitted
		// from the DERIVED operation path. Camel merges the rest consumer and
		// the direct route it targets into a single route; the direct route's
		// id wins, so it carries the canonical bridge id.
		org.apache.camel.model.rest.RestDefinition rest = routeBuilder.rest();
		org.apache.camel.model.rest.RestDefinition restVerb = rest.post(restPath);
		String restRouteId = routeBuilder instanceof DmpRouteBuilder ? routeId + "-rest-consumer" : null;
		if (restRouteId != null) restVerb.getVerbs().get(restVerb.getVerbs().size() - 1).setRouteId(restRouteId);
		restVerb
				.consumes(CONTENT_TYPE_JSON)
				.produces(CONTENT_TYPE_JSON)
				.to("direct:" + routeId);

		OperationContext operationContext = OperationContext.builder(action, commandSinkUri)
				.responseLuaUri(responseLuaUri)
				.parameterTypes(command.parameterTypes())
				.responseTypes(command.responseTypes())
				.callerResponseTypes(caller.responseTypes())
				.callerOutputOrder(caller.outputOrder())
				.callerInputTemplates(templates(caller.inputTemplates()))
				.callerOutputTemplates(templates(caller.outputTemplates()))
				.callerInputSourcePaths(caller.inputSourcePaths())
				.callerReplyBindings(caller.replyBindings())
				.callerReplySinkId(caller.replySinkId())
				.commandSinkIds(commandSourceIds)
				.commandSinkValuePaths(commandValuePaths)
				.explicitResponseMapping(response != null)
				.parameterRequired(command.parameterRequired())
				.responseRequired(command.responseRequired())
				.propertyWrite(command.propertyWrite())
				.jsonPayload(command.sink().jsonPayload())
				.targetOptions(command.connectorOptions())
				.build();
		SyncOperationProcessor operation = new SyncOperationProcessor(routeBuilder.getContext(), operationContext,
				commandValidation, new AffordanceValidation(command.responseSchema(),
						"bridge output " + action), responseSourceBindings, responseSinkTargets, routeId);
		operation.setOperationIdentity(bridge.identity().tags());
		// Fan-in: AAS elements read at invocation, decoded like ordinary sources (SPEC §5.4).
		List<Source> fanIn = bridge.fanIn();
		FanIn decoder = fanIn.isEmpty() ? null : new FanIn(routeId, fanIn, Map.of());
		if (decoder != null && routeBuilder instanceof DmpRouteBuilder managed) managed.registerManagedResource(decoder);
		registerManagedResource(operation);
		String responseWriteRouteId = routeId + "-response-write";
		String responseWriteUri = "direct:" + responseWriteRouteId;
		if (!responseSinkTargets.isEmpty()) {
			registerRouteId(responseWriteRouteId);
			operation.configureResponseWrite(routeBuilder.from(responseWriteUri).routeId(responseWriteRouteId));
		}

		// The device call: the connector's invocation steps (SynchronousInvocationAdapter).
		String commandRouteId = routeId + "-command";
		String commandRouteUri = "direct:" + commandRouteId;
		registerRouteId(commandRouteId);
		operation.configureInvoke(routeBuilder.from(commandRouteUri).routeId(commandRouteId).removeHeaders("*"));

		registerRouteId(routeId);
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerIngressRouteId(routeId);
		var operationRoute = routeBuilder.from("direct:" + routeId).routeId(routeId);
		var flow = operationRoute.doTry();
		flow.process(operation::prepareRequest);
		for (int i = 0; i < fanIn.size(); i++) {
			Source source = fanIn.get(i);
			flow.enrich(SourceReads.route(routeBuilder, build, routeId + "-fan-in", i, source, decoder),
					SourceReads.into(source.sourceId()));
		}
		flow.process(operation::keepRequestSources)
				.to(requestLuaUri)
				.process(operation::keepRequestResult);
		// The device call: the connector's steps run in the command route on a copy of the
		// exchange (Content Enricher), so caller headers never reach the device. The
		// connector correlates the terminal reply (OPC UA Call, MQTT 5 correlation data, HTTP).
		flow.process(operation::prepareCommand)
				.enrich(commandRouteUri, (original, reply) -> {
					original.getMessage().setBody(reply.getMessage().getBody());
					return original;
				})
				.process(operation::acceptReply);
		// Response: reply projection, response Lua, best-effort AAS writes, caller reply.
		flow.process(operation::collectResponseSources);
		if (responseLuaUri != null) flow.to(responseLuaUri);
		flow.process(operation::prepareResponseWrites);
		if (!responseSinkTargets.isEmpty()) operation.configureResponseDispatch(flow, responseWriteUri);
		flow.process(operation::completeReply);
		var failed = flow.doCatch(Exception.class);
		// A failed call still runs the response transformation for its log sinks, in a route of its own whose
		// failure is only logged; the caller's error reply is built afterwards.
		if (operation.answersFailures()) {
			String failureRouteId = routeId + "-failure-response";
			registerRouteId(failureRouteId);
			var failureFlow = routeBuilder.from("direct:" + failureRouteId).routeId(failureRouteId).doTry();
			failureFlow.to(responseLuaUri).process(operation::prepareFailureWrites);
			operation.configureResponseDispatch(failureFlow, responseWriteUri);
			failureFlow.doCatch(Exception.class).process(operation::failureResponseFailed).end();
			failed.process(operation::prepareFailureResponse)
					.enrich("direct:" + failureRouteId, (original, ignored) -> original);
		}
		failed.process(operation::handleFailure).end();
		operationRoute.process(ProtocolBridgeRouteCreator::encodeRestResponse);
		registerRestReservation(method, restPath, rest, restRouteId);
		LOGGER.info("bridge route {}: {} {} (callerOutputOrder {})",
				routeId, method, restPath,
				operationContext.callerOutputOrder);
	}

	/** Declared variables as fresh model elements: each build gets copies of its own. */
	private static Map<String, SubmodelElement> templates(Map<String, String> json) {
		Map<String, SubmodelElement> templates = new LinkedHashMap<>();
		json.forEach((name, element) -> templates.put(name, Aas.read(element, SubmodelElement.class)));
		return templates;
	}

	private static void encodeRestResponse(org.apache.camel.Exchange exchange) {
		if (Boolean.TRUE.equals(exchange.getProperty(OperationPayloads.OPERATION_DELEGATION_FAILED))) return;
		exchange.getMessage().setBody(OperationPayloads.encodeReply(exchange.getMessage().getBody()));
	}

	private void registerRouteId(String routeId) {
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerRouteId(routeId);
	}

	private void registerRestReservation(String method, String path,
			org.apache.camel.model.rest.RestDefinition definition, String restRouteId) {
		if (routeBuilder instanceof DmpRouteBuilder managed) {
			managed.registerRestReservation(method, path, definition, restRouteId);
		}
	}

	private void registerManagedResource(SyncOperationProcessor operation) {
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerManagedResource(operation);
	}

	private static AffordanceValidation commandValidation(Sink sink) {
		return new AffordanceValidation(sink.payloadSchema() != null ? sink.payloadSchema() : sink.schema(),
				"bridge sink " + sink.id());
	}
}
