/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.aas.operation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.Response;
import dk.aau.smartproductionlab.dmp.core.plan.RoutePlan;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.plan.Source;

/**
 * A mapping an AAS Operation call triggers (SPEC §7): the caller's request through the request transformation to
 * the device's command, and the reply through the response half back to the caller.
 *
 * @param requestTransformation the request Lua
 * @param fanIn                 further AAS elements, read at each call and passed to the request Lua
 * @param response              the response half; {@code null} when the Operation answers with no outputs
 */
public record OperationRoute(String routeId, Identity identity, Endpoint requestTransformation, List<Source> fanIn,
		Command command, Caller caller, Response response) implements RoutePlan {

	public OperationRoute {
		if (routeId == null || routeId.isBlank()) throw new IllegalArgumentException("an Operation route needs a route ID");
		Objects.requireNonNull(identity, "identity");
		Objects.requireNonNull(requestTransformation, "requestTransformation");
		Objects.requireNonNull(command, "command");
		Objects.requireNonNull(caller, "caller");
		fanIn = List.copyOf(fanIn);
	}

	/**
	 * The Operation: where BaSyx calls it ({@code restPath}, served with POST), the skill ({@code action}) and its
	 * AAS identity, the submodel and the Operation's idShortPath.
	 */
	public record Identity(String restPath, String action, String submodelId, String path) {
		public Identity {
			if (restPath == null || restPath.isBlank()) throw new IllegalArgumentException("an Operation needs its delegation path");
			if (action == null || action.isBlank()) throw new IllegalArgumentException("an Operation needs its action");
		}

		/** Lua's {@code meta.invocation} tags: submodel, path, skill, REST path and, where the path names it, the AAS. */
		public Map<String, String> tags() {
			Map<String, String> tags = new LinkedHashMap<>();
			tags.put("submodel", submodelId);
			tags.put("path", path);
			tags.put("skill", action);
			tags.put("restPath", restPath);
			if (restPath.matches("/operations/[^/]+/[^/]+"))
				tags.put("aas", java.net.URLDecoder.decode(restPath.split("/")[2], java.nio.charset.StandardCharsets.UTF_8));
			tags.values().removeIf(Objects::isNull);
			return tags;
		}
	}

	/**
	 * The device's command: the action invoked or the property written through {@code sink}, assembled from the
	 * request Lua's SinkIds ({@code members}). The types and required names come from the action's input and output
	 * DataSchemas; {@code connectorOptions} are the connector's ({@code SynchronousInvocationAdapter}).
	 *
	 * @param propertyWrite  the Operation writes a property instead of invoking an action
	 * @param responseSchema the action's output DataSchema, or {@code null}
	 */
	public record Command(Sink sink, List<Member> members, boolean propertyWrite, Map<String, String> parameterTypes,
			List<String> parameterRequired, Map<String, String> responseTypes, List<String> responseRequired,
			Map<String, Object> responseSchema, Map<String, Object> connectorOptions) {
		public Command {
			Objects.requireNonNull(sink, "sink");
			members = List.copyOf(members);
			parameterTypes = Map.copyOf(parameterTypes);
			parameterRequired = List.copyOf(parameterRequired);
			responseTypes = Map.copyOf(responseTypes);
			responseRequired = List.copyOf(responseRequired);
			connectorOptions = Map.copyOf(connectorOptions);
		}

		/** A SinkId of the request Lua and where its value goes in the command ({@code null} for the whole). */
		public record Member(String sinkId, String valuePath) {
			public Member {
				Objects.requireNonNull(sinkId, "sinkId");
			}
		}
	}

	/**
	 * The caller's side: the Operation's Sources ({@code inputSourcePaths}, SourceId to a path in the caller's
	 * variables, {@code $} for all), the declared variables as AAS JSON by idShort, the output and InOut variables'
	 * types and order, and the ResponseSinks answering the caller: the whole Operation ({@code replySinkId}) or
	 * variables ({@code replyBindings}, SinkId to path).
	 */
	public record Caller(Map<String, String> inputSourcePaths, Map<String, String> inputTemplates,
			Map<String, String> outputTemplates, Map<String, String> responseTypes, List<String> outputOrder,
			Map<String, String> replyBindings, String replySinkId) {
		public Caller {
			inputSourcePaths = Map.copyOf(inputSourcePaths);
			inputTemplates = Map.copyOf(inputTemplates);
			outputTemplates = Map.copyOf(outputTemplates);
			responseTypes = Map.copyOf(responseTypes);
			outputOrder = List.copyOf(outputOrder);
			replyBindings = Map.copyOf(replyBindings);
		}
	}

	@Override
	public List<Endpoint> endpoints() {
		List<Endpoint> endpoints = new ArrayList<>();
		fanIn.forEach(source -> endpoints.add(source.endpoint()));
		endpoints.add(requestTransformation);
		endpoints.add(command.sink().endpoint());
		if (response != null) endpoints.addAll(response.endpoints());
		return endpoints;
	}

	@Override
	public void build(RouteBuilder builder) {
		new ProtocolBridgeRouteCreator(Objects.requireNonNull(builder, "builder")).create(this);
	}
}
