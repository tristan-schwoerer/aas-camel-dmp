/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.management.compiler;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataTransformerConfiguration;
import dk.aau.smartproductionlab.dmp.core.security.BasicCredential;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.Evaluation;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.execution.policy.ExecutionPolicyRouteConfiguration;
import dk.aau.smartproductionlab.dmp.aas.configuration.AASDatasinkConfiguration;
import dk.aau.smartproductionlab.dmp.aas.configuration.AASPollingDataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.aas.configuration.AasEventDataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.aas.operation.OperationVariableValues;
import dk.aau.smartproductionlab.dmp.aas.operation.OptionalQualifier;
import dk.aau.smartproductionlab.dmp.aas.operation.ProtocolBridgeRouteConfiguration;
import dk.aau.smartproductionlab.dmp.http.configuration.HttpDataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.http.configuration.HttpObserveDataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.influxdb2.InfluxSettings;
import dk.aau.smartproductionlab.dmp.influxdb2.configuration.InfluxDb2DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.influxdb2.configuration.InfluxDb2DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.management.model.TimeSeriesProjection;
import dk.aau.smartproductionlab.dmp.snmp.configuration.SnmpDataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.snmp.configuration.SnmpDataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.http.configuration.HttpPollingDataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.lua.configuration.LuaTransformerConfiguration;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource;
import dk.aau.smartproductionlab.dmp.paho.configuration.MqttDataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.paho.configuration.MqttDataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.opcua.configuration.OpcuaDataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.opcua.configuration.OpcuaDataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.plc4x.configuration.Option;
import dk.aau.smartproductionlab.dmp.plc4x.configuration.Plc4xDataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.plc4x.configuration.Plc4xDataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.plc4x.configuration.Tag;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.AidTarget;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.DirectTarget;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.EndpointPlan;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.MappingPlan;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.Plan;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.SeriesTarget;
import org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements;
import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.Operation;
import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.Qualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.QualifierKind;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementList;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.management.model.WireModel;
import dk.aau.smartproductionlab.dmp.management.model.WireModel.DataSchema;
import dk.aau.smartproductionlab.dmp.management.model.WireModel.Presence;
import dk.aau.smartproductionlab.dmp.management.model.WireModel.Form;
import dk.aau.smartproductionlab.dmp.management.configuration.MqttV5OperationDataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;

/** Compiles resolved wire plans into existing runtime connector configuration classes. */
public final class ConnectorConfigurationFactory {
	private static final long MAX_LUA_BYTES = 1_048_576;
	// Two scripts per mapping, covering installed, desired and staged generations at the 8192-mapping limit.
	private static final int MAX_CACHED_LUA_SOURCES = 65_536;
	private record ScriptIdentity(String diagnostic, String content) { }
	private static final Map<ScriptIdentity, java.util.concurrent.CompletableFuture<java.lang.ref.WeakReference<LuaCompiledSource>>> LUA_SOURCES =
			new LinkedHashMap<>();

	private static LuaCompiledSource compiledLua(byte[] bytes, String identity) {
		if (bytes.length > MAX_LUA_BYTES) throw new IllegalArgumentException("Lua source exceeds maxScriptBytes");
		String content;
		try { content = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)); }
		catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
		// Cache immutable prototypes only, not bindings or evaluation state. Retired sources remain collectible.
		ScriptIdentity key = new ScriptIdentity(identity, content);
		while (true) {
			java.util.concurrent.CompletableFuture<java.lang.ref.WeakReference<LuaCompiledSource>> pending;
			boolean compile = false;
			synchronized (LUA_SOURCES) {
				pending = LUA_SOURCES.get(key);
				if (pending != null && pending.isDone() && pending.join().get() == null) {
					LUA_SOURCES.remove(key);
					pending = null;
				}
				if (pending == null) {
					if (LUA_SOURCES.size() >= MAX_CACHED_LUA_SOURCES)
						LUA_SOURCES.values().removeIf(value -> value.isDone() && value.join().get() == null);
					if (LUA_SOURCES.size() >= MAX_CACHED_LUA_SOURCES)
						throw new IllegalStateException("managed Lua compilation capacity exceeded");
					pending = new java.util.concurrent.CompletableFuture<>();
					LUA_SOURCES.put(key, pending);
					compile = true;
				}
			}
			// Deduplicate one source without serializing independent AIMC compilation.
			if (compile) {
				try {
					LuaCompiledSource source = LuaCompiledSource.fromBytes(bytes, identity, MAX_LUA_BYTES);
					pending.complete(new java.lang.ref.WeakReference<>(source));
					return source;
				} catch (RuntimeException | Error failure) {
					synchronized (LUA_SOURCES) { LUA_SOURCES.remove(key, pending); }
					pending.completeExceptionally(failure);
					throw failure;
				}
			}
			LuaCompiledSource source = pending.join().get();
			if (source != null) return source;
		}
	}

	public record Issue(String path, String mappingId, String message) { }
	public record Result(RoutesConfiguration configuration, List<Issue> issues) {
		public Result { issues = List.copyOf(issues); }
	}
	public record CompiledBundle(String mappingIdentity,int listIndex,String bundleId,String routeId,
			RoutesConfiguration configuration, EffectiveFingerprint fingerprint) { }
	public record BundleResult(List<CompiledBundle> bundles,List<Issue> issues) {
		public BundleResult { bundles=List.copyOf(bundles);issues=List.copyOf(issues); }
	}
	public record DesiredBundle(MappingPlanCompiler.DesiredMapping desired,CompiledBundle bundle,List<Issue> issues,
			EffectiveFingerprint fingerprint) {
		public DesiredBundle { issues=List.copyOf(issues); }
		public boolean valid() { return desired.valid() && bundle!=null && issues.isEmpty(); }
	}
	/** {@code influx} is the InfluxDB TimeSeries segments must name, or {@code null} when none is configured. */
	/** The DMP's deployment as compilation input: the repository, the InfluxDB, and the credentials AID schemes need. */
	public record RuntimeSettings(URI aasRepositoryBaseUri, InfluxSettings influx, DmpCredentials credentials) {
		public RuntimeSettings(URI aasRepositoryBaseUri) { this(aasRepositoryBaseUri, null, null); }
		public RuntimeSettings(URI aasRepositoryBaseUri, InfluxSettings influx) { this(aasRepositoryBaseUri, influx, null); }
		public RuntimeSettings {
			Objects.requireNonNull(aasRepositoryBaseUri,"aasRepositoryBaseUri");
			if(credentials==null)credentials=DmpCredentials.none();
			if(aasRepositoryBaseUri.getUserInfo()!=null || aasRepositoryBaseUri.getQuery()!=null
					|| aasRepositoryBaseUri.getFragment()!=null || aasRepositoryBaseUri.getHost()==null
					|| !("http".equalsIgnoreCase(aasRepositoryBaseUri.getScheme()) || "https".equalsIgnoreCase(aasRepositoryBaseUri.getScheme()))) {
				throw new IllegalArgumentException("AAS runtime base must be a credential-free HTTP(S) URI");
			}
		}
	}

	/** routeId is an ownership identity supplied by the mapping-bundle caller. */
	public Result create(Plan plan, String routeId) {
		return create(plan,routeId,null);
	}

	public Result create(Plan plan, String routeId, RuntimeSettings runtime) {
		Objects.requireNonNull(plan, "plan");
		if (routeId == null || routeId.isBlank()) throw new IllegalArgumentException("routeId is required");
		RoutesConfiguration output = new RoutesConfiguration();
		List<Issue> issues = new ArrayList<>();
		for (MappingPlan mapping : plan.mappings()) {
			int issueStart = issues.size();
			validateCapabilities(mapping,issues);
			if(issues.size()!=issueStart)continue;
			if (operationMapping(mapping)) {
				compileBridgeMapping(mapping,plan,routeId,runtime,output,issues);
				continue;
			}
			List<DataSourceConfiguration> sources = new ArrayList<>();
			List<DataSinkConfiguration> sinks = new ArrayList<>();
			List<String> sourceIds = new ArrayList<>();
			List<String> sinkIds = new ArrayList<>();
			Map<DataSourceConfiguration,Integer> sourcePositions = new java.util.IdentityHashMap<>();
			for (int i = 0; i < mapping.sources().size(); i++) {
				EndpointPlan endpoint = mapping.sources().get(i);
				Evaluation evaluation = evaluation(endpoint);
				DataSourceConfiguration source = evaluation == Evaluation.READ_AT_EVALUATION
						? readSource(endpoint, mapping, i, plan, routeId, runtime, issues)
						: source(endpoint, mapping, i, plan, routeId, runtime, issues);
				if (source != null) {
					source.setEvaluation(evaluation);
					sources.add(source); sourceIds.add(source.getUniqueId()); sourcePositions.put(source, i);
				}
			}
			if (mapping.sources().stream().noneMatch(endpoint -> evaluation(endpoint).triggers())) issue(issues, mapping,
					"MappingConfigurations/value/" + mapping.listIndex() + "/Sources",
					"no Source triggers the mapping; one needs the Evaluation OnUpdate (the default) or OnChange");
			for (int i = 0; i < mapping.sinks().size(); i++) {
				EndpointPlan endpoint = mapping.sinks().get(i);
				DataSinkConfiguration sink = sink(endpoint, mapping, i, plan, routeId, runtime, issues);
				if(sink!=null)payloadBinding(sink,endpoint,mapping,issues);
				if (sink != null) { sinks.add(sink); sinkIds.add(sink.getUniqueId()); }
			}
			// A read source is polled at its PollingInterval, else at the mapping's DefaultPollingInterval
			// (IDTA AIMC; ignored for asynchronous sources). Without a positive one (none, or 0) the source
			// is event-based, and one its binding can only read blocks the mapping: no interval is assumed.
			java.util.function.Predicate<DataSourceConfiguration> unpaced = source -> source.getEvaluation() != Evaluation.READ_AT_EVALUATION
					&& "sync".equalsIgnoreCase(source.getDelivery()) && source.getPollingIntervalMs()<=0;
			java.util.function.Predicate<DataSourceConfiguration> ownInterval = source ->
					mapping.sources().get(sourcePositions.get(source)).pollingInterval().present();
			boolean fallback = sources.stream().anyMatch(unpaced.and(ownInterval.negate()));
			OptionalLong pollingMillis = fallback ? pollingMillis(mapping, issues) : OptionalLong.empty();
			if (pollingMillis.isPresent()) sources.stream().filter(unpaced.and(ownInterval.negate()))
					.forEach(source -> source.setPollingIntervalMs(pollingMillis.getAsLong()));
			sources.stream().filter(unpaced).forEach(source -> issue(issues, mapping,
					endpointPath(mapping, "Sources", sourcePositions.get(source)),
					"the source can only be read, not observed; it needs a positive PollingInterval or DefaultPollingInterval"));
			if (mapping.qualifiers().stream().anyMatch(ConnectorConfigurationFactory::behaviorQualifier)) issue(issues, mapping,
					"MappingConfigurations/value/" + mapping.listIndex() + "/qualifiers",
					"mapping qualifier semantics are not represented by the runtime route API");

			List<DataTransformerConfiguration> transformers = new ArrayList<>();
			if (mapping.transformation().present()) {
				WireModel.Blob blob = mapping.transformation().value();
				if (blob == null) issue(issues, mapping,
						"MappingConfigurations/value/" + mapping.listIndex() + "/Transformation",
						"authored Lua Blob is null");
				else {
					try {
						String id = entityId(mapping, "transform", 0);
						LuaCompiledSource compiled = compiledLua(blob.bytes(), mapping.identity());
						transformers.add(new LuaTransformerConfiguration(id,compiled));
					} catch (RuntimeException failure) {
						issue(issues, mapping, "MappingConfigurations/value/" + mapping.listIndex() + "/Transformation",
								"Lua source cannot be compiled: " + failure.getMessage());
					}
				}
			}

			Map<String, Object> triggerData = new LinkedHashMap<>();
			List<DataSinkConfiguration> responseDestinations = new ArrayList<>();
			if (mapping.responseSources().present() || mapping.responseSinks().present()
					|| mapping.responseTransformation().present())
				compileOrdinaryResponse(mapping, plan, routeId, runtime, triggerData, responseDestinations, transformers, issues);

			// A mapping is atomic: never return a route with only a subset of its endpoints.
			if (issues.size() != issueStart) continue;
			output.addDatasources(sources);
			output.addDatasinks(sinks);
			output.addDatasinks(responseDestinations);
			output.addTransformers(transformers);
			// The route's transformers are the request side's; the response Lua is named in the trigger data.
			Object responseTransformer = triggerData.get(RouteConfiguration.RESPONSE_TRANSFORMER);
			RouteConfiguration route = new RouteConfiguration(ExecutionPolicyRouteConfiguration.ROUTE_TRIGGER,
					sourceIds, transformers.stream().map(DataTransformerConfiguration::getUniqueId)
							.filter(id -> !id.equals(responseTransformer)).toList(), sinkIds);
			route.setRouteId(routeId + ":" + mapping.listIndex());
			if (!triggerData.isEmpty()) route.setTriggerData(triggerData);
			output.addRoute(new ExecutionPolicyRouteConfiguration(route));
		}
		return new Result(output, issues);
	}

	private static boolean operationMapping(MappingPlan mapping) {
		return mapping.sources().stream().anyMatch(endpoint -> operationModel(endpoint) != null);
	}

	private static SubmodelElement operationModel(EndpointPlan endpoint) {
		if (!(endpoint.target() instanceof DirectTarget direct)) return null;
		SubmodelElement element=direct.resolved().element();
		return element instanceof Operation ? element : direct.resolved().operation();
	}

	private static boolean isWholeOperation(EndpointPlan endpoint) {
		return endpoint.target() instanceof DirectTarget direct && direct.resolved().element() instanceof Operation;
	}

	/** The reference's keys up to and including its Operation, or all keys outside an Operation. */
	private static List<Key> operationKeys(EndpointPlan endpoint) {
		List<Key> keys=Aas.keys(endpoint.reference());
		int operation=AasReferenceResolver.operationKeyIndex(keys);
		return operation<0 ? keys : List.copyOf(keys.subList(0,operation+1));
	}

	private static String operationIdentity(EndpointPlan endpoint) {
		List<Key> keys=operationKeys(endpoint);
		String submodel=keys.isEmpty()?"":keys.get(0).getValue();
		return submodel+"/"+keys.subList(Math.min(1,keys.size()),keys.size()).stream()
				.map(key -> Aas.keyType(key)+":"+key.getValue()).collect(java.util.stream.Collectors.joining("/"));
	}

	/** A caller value path relative to the flattened OperationVariable map. */
	private static String operationVariablePath(EndpointPlan endpoint) {
		List<Key> keys=Aas.keys(endpoint.reference());
		int operation=AasReferenceResolver.operationKeyIndex(keys);
		if(operation<0 || operation+1>=keys.size()) return null;
		StringBuilder path=new StringBuilder("$");
		for(int i=operation+1;i<keys.size();i++) {
			Key key=keys.get(i);
			if(i>operation+1 && "SubmodelElementList".equals(Aas.keyType(keys.get(i-1)))) path.append('[').append(key.getValue()).append(']');
			else path.append('.').append(key.getValue());
		}
		return path.toString();
	}

	/** The idShort of the Operation variable the reference selects or lies in. */
	private static String operationVariableRoot(EndpointPlan endpoint) {
		List<Key> keys=Aas.keys(endpoint.reference());
		int operation=AasReferenceResolver.operationKeyIndex(keys);
		return operation>=0 && operation+1<keys.size() ? keys.get(operation+1).getValue() : null;
	}

	private static Set<String> callerInputNames(SubmodelElement element) {
		if (!(element instanceof Operation operation)) return Set.of();
		return variableNames(operation.getInputVariables(),operation.getInoutputVariables());
	}

	private static Set<String> callerOutputNames(SubmodelElement element) {
		if (!(element instanceof Operation operation)) return Set.of();
		return variableNames(operation.getInoutputVariables(),operation.getOutputVariables());
	}

	@SafeVarargs
	private static Set<String> variableNames(List<OperationVariable>... categories) {
		Set<String> names=new LinkedHashSet<>();
		for(List<OperationVariable> variables:categories) for(OperationVariable variable:Aas.nonNull(variables))
			if(variable!=null && variable.getValue()!=null) names.add(variable.getValue().getIdShort());
		return names;
	}

	private void compileBridgeMapping(MappingPlan mapping,Plan plan,String routeId,RuntimeSettings runtime,
			RoutesConfiguration output,List<Issue> issues) {
		int issueStart=issues.size();
		String root="MappingConfigurations/value/"+mapping.listIndex();
		Map<String, EndpointPlan> operationAnchors=new LinkedHashMap<>();
		for(EndpointPlan endpoint:mapping.sources()) if(operationModel(endpoint)!=null)
			operationAnchors.putIfAbsent(operationIdentity(endpoint),endpoint);
		if(operationAnchors.size()!=1) {
			issue(issues,mapping,root+"/Sources","bridge mappings require Sources from exactly one declared CCI Operation");
			return;
		}
		EndpointPlan operation=operationAnchors.values().iterator().next();
		Operation cci=(Operation)operationModel(operation);
		// Fan-in: further AAS elements, read at invocation and passed to the request Lua with the Operation's variables.
		List<EndpointPlan> fanIn=mapping.sources().stream().filter(endpoint -> operationModel(endpoint)==null).toList();
		for(EndpointPlan extra:fanIn)
			if(!(extra.target() instanceof DirectTarget))
				issue(issues,mapping,root+"/Sources/"+extra.id(),"an operation mapping fans in AAS elements only; the device is reached through its Sinks");
		String action=operationAction(operation.reference());
		if(action==null || action.isBlank()) { issue(issues,mapping,root+"/Sources","Operation reference does not identify an exact parent skill/action");return; }
		if(mapping.sinks().isEmpty()) { issue(issues,mapping,root+"/Sinks","bridge Operation requires at least one declared command binding");return; }
		if(mapping.qualifiers().stream().anyMatch(ConnectorConfigurationFactory::behaviorQualifier)) issue(issues,mapping,root+"/qualifiers","mapping qualifiers are not represented by the bridge route schema");
		// The Operation is its mapping's only trigger; its further sources are read at the call.
		for(EndpointPlan source:mapping.sources()) {
			if(operationModel(source)!=null && source.evaluation().present())
				issue(issues,mapping,root+"/Sources/"+source.id()+"/Evaluation","an Operation is its mapping's only trigger; it takes no Evaluation");
			if(operationModel(source)==null && evaluation(source,Evaluation.READ_AT_EVALUATION)!=Evaluation.READ_AT_EVALUATION)
				issue(issues,mapping,root+"/Sources/"+source.id()+"/Evaluation","an Operation mapping reads its further sources at the call (ReadAtEvaluation)");
		}
		// The Operation is bound 1:1 to the AID action of its skill, or to one property affordance it writes.
		AidTarget actionTarget=null,propertyTarget=null;
		for(EndpointPlan sink:mapping.sinks()) {
			if(sink.target() instanceof AidTarget target && "property".equalsIgnoreCase(target.resolved().affordance().kind())) {
				if(mapping.sinks().size()!=1 || !target.resolved().schemaPath().isEmpty())
					issue(issues,mapping,root+"/Sinks/"+sink.id(),"an Operation bound to a property writes exactly one whole property affordance");
				else propertyTarget=target;
				continue;
			}
			if(!(sink.target() instanceof AidTarget target) || !"action".equalsIgnoreCase(target.resolved().affordance().kind())
					|| !action.equals(target.resolved().affordance().idShort().value())) {
				issue(issues,mapping,root+"/Sinks/"+sink.id(),"command binding must address the same exact AID action as the CCI Operation");
				continue;
			}
			// The request writes the action's input; its output is the reply, named by the ResponseSources.
			if(target.resolved().part()!=AasReferenceResolver.SchemaPart.INPUT) {
				issue(issues,mapping,root+"/Sinks/"+sink.id(),"a command Sink addresses the action's input DataSchema, or a member of it");
				continue;
			}
			if(actionTarget==null) actionTarget=target;
			else if(!sameAction(actionTarget,target)) issue(issues,mapping,root+"/Sinks/"+sink.id(),
					"grouped command members must share one AID action endpoint");
		}
		if(actionTarget==null && propertyTarget==null || issues.size()!=issueStart) return;
		boolean write=propertyTarget!=null;
		AidTarget commandTarget=write?propertyTarget:actionTarget;
		if(mapping.responseTransformation().present() && mapping.responseTransformation().value()==null) {
			issue(issues,mapping,root+"/ResponseTransformation","response transformation Blob is explicitly null");return;
		}
		String commandId=entityId(mapping,"command",0);
		Map<String,Object> targetOptions=new LinkedHashMap<>();
		DataSinkConfiguration commandSink=write
				?propertyWriteSink(mapping.sinks().get(0),mapping,plan,routeId,runtime,targetOptions,issues)
				:bridgeCommandSink(mapping,action,actionTarget,plan,routeId,commandId,targetOptions,runtime,issues,root+"/Sinks");
		if(commandSink==null || issues.size()!=issueStart) return;
		List<DataSourceConfiguration> fanInReads=new ArrayList<>();
		List<Map<String,Object>> fanInBindings=new ArrayList<>();
		for(int i=0;i<fanIn.size();i++) {
			DataSourceConfiguration read=aasReadSource(fanIn.get(i),mapping,500000+i,(DirectTarget)fanIn.get(i).target(),runtime,issues);
			if(read==null)continue;
			read.setEvaluation(Evaluation.READ_AT_EVALUATION);
			fanInReads.add(read);
			fanInBindings.add(Map.of("sourceId",fanIn.get(i).id(),"dataSourceId",read.getUniqueId()));
		}
		if(issues.size()!=issueStart) return;
		DataSchema input=write?null:actionTarget.resolved().affordance().input().present()
				?actionTarget.resolved().affordance().input().value():null;
		DataSchema response=write?null:actionTarget.resolved().affordance().output().present()
				?actionTarget.resolved().affordance().output().value():null;
		Map<String,String> parameterTypes=schemaTypes(input);
		Map<String,String> responseTypes=schemaTypes(response);
		List<String> parameterRequired=schemaRequired(input);
		List<String> responseRequired=schemaRequired(response);
		String restPath=delegationPath(cci);
		if(restPath==null) { issue(issues,mapping,root+"/Sources","CCI Operation needs an invocationDelegation qualifier whose URL names the DMP path");return; }
		Map<String,Object> trigger=new LinkedHashMap<>();
		trigger.put(ProtocolBridgeRouteConfiguration.REST_PATH,restPath);
		trigger.put("action",action);
		// The Operation's AAS identity (submodel ID, idShort path), passed to Lua as meta.invocation.
		List<Key> operationKeys=operationKeys(operation);
		if(!operationKeys.isEmpty()) {
			trigger.put(ProtocolBridgeRouteConfiguration.OPERATION_SUBMODEL_ID,operationKeys.get(0).getValue());
			trigger.put(ProtocolBridgeRouteConfiguration.OPERATION_PATH,String.join(".",
					operationKeys.subList(1,operationKeys.size()).stream().map(Key::getValue).toList()));
		}
		Map<String,String> callerInputSourcePaths=new LinkedHashMap<>();
		for(EndpointPlan endpoint:mapping.sources()) if(operationModel(endpoint)!=null) {
			if(isWholeOperation(endpoint)) callerInputSourcePaths.put(endpoint.id(), "$");
			else {
				String valuePath=operationVariablePath(endpoint);
				if(valuePath==null) issue(issues,mapping,root+"/Sources/"+endpoint.id(),"Operation Source must select an input or InOut variable");
				else callerInputSourcePaths.put(endpoint.id(),valuePath);
			}
		}
		trigger.put(ProtocolBridgeRouteConfiguration.CALLER_INPUT_SOURCE_PATHS,Map.copyOf(callerInputSourcePaths));
		trigger.put(ProtocolBridgeRouteConfiguration.FAN_IN_SOURCES,List.copyOf(fanInBindings));
		trigger.put(ProtocolBridgeRouteConfiguration.COMMAND_SINK_IDS,List.of(commandSink.getUniqueId()));
		List<Map<String,Object>> bindings=new ArrayList<>();
		for(EndpointPlan sink:mapping.sinks()) {
			AidTarget endpointTarget=(AidTarget)sink.target();
			Map<String,Object> binding=new LinkedHashMap<>();
			binding.put("sinkId",sink.id());
			binding.put("dataSinkId",commandSink.getUniqueId());
			binding.put("valuePath",jsonPointer(endpointTarget.resolved().schemaPath()));
			bindings.add(binding);
		}
		trigger.put(ProtocolBridgeRouteConfiguration.COMMAND_SINK_BINDINGS,List.copyOf(bindings));
		trigger.put(ProtocolBridgeRouteConfiguration.PARAMETER_TYPES,parameterTypes);
		trigger.put("parameterRequired",parameterRequired);
		trigger.put(ProtocolBridgeRouteConfiguration.RESPONSE_TYPES,responseTypes);
		trigger.put("responseRequired",responseRequired);
		Map<String,String> callerTypes=new LinkedHashMap<>();
		Map<String,String> inputTemplates=new LinkedHashMap<>();
		Map<String,String> outputTemplates=new LinkedHashMap<>();
		List<String> callerOrder=new ArrayList<>();
		// The output and InOut variables not marked Optional: the reply must answer them.
		List<String> requiredOutputs=new ArrayList<>();
		Set<String> callerNames=new LinkedHashSet<>();
		// Inputs reach the caller request; InOut variables are both request and reply, outputs reply only.
		Map<String,List<OperationVariable>> categories=new LinkedHashMap<>();
		categories.put("input",Aas.nonNull(cci.getInputVariables()));
		categories.put("inoutput",Aas.nonNull(cci.getInoutputVariables()));
		categories.put("output",Aas.nonNull(cci.getOutputVariables()));
		Set<String> callerInputNames=callerInputNames(cci);
		for(EndpointPlan endpoint:mapping.sources()) if(operationModel(endpoint)!=null && !isWholeOperation(endpoint)) {
			String variable=operationVariableRoot(endpoint);
			if(variable==null || !callerInputNames.contains(variable))
				issue(issues,mapping,root+"/Sources/"+endpoint.id(),"Operation Sources must select a declared input or InOut variable");
		}
		categories.forEach((category,variables) -> {
			for(OperationVariable variable:variables) {
				SubmodelElement value=variable==null?null:variable.getValue();
				if(value==null || !callerElementSupported(value,true) || !callerNames.add(value.getIdShort())) {
					issue(issues,mapping,root+"/Sources","CCI "+category+" variables must be uniquely named Properties with"
							+" supported valueTypes, or collections and lists of them");
					continue;
				}
				for(String problem:OptionalQualifier.problems(value))
					issue(issues,mapping,root+"/Sources","CCI "+category+" variable "+problem);
				String template=Aas.write(value);
				if(!"output".equals(category))inputTemplates.put(value.getIdShort(),template);
				if(!"input".equals(category)) {
					outputTemplates.put(value.getIdShort(),template);
					if(value instanceof Property property)callerTypes.put(value.getIdShort(),Aas.xsd(property.getValueType()));
					callerOrder.add(value.getIdShort());
					if(!OptionalQualifier.optional(value))requiredOutputs.add(value.getIdShort());
				}
			}
		});
		trigger.put("callerResponseTypes",Map.copyOf(callerTypes));
		trigger.put(ProtocolBridgeRouteConfiguration.CALLER_INPUT_TEMPLATES,Map.copyOf(inputTemplates));
		trigger.put(ProtocolBridgeRouteConfiguration.CALLER_OUTPUT_TEMPLATES,Map.copyOf(outputTemplates));
		if(!callerOrder.isEmpty())trigger.put("callerOutputOrder",List.copyOf(callerOrder));
		if(response!=null) trigger.put(ProtocolBridgeRouteConfiguration.RESPONSE_SCHEMA,schema(Presence.of(response)));
		// The caller's outputs are only what the response mapping makes of the reply; nothing is
		// matched by name. Without a response mapping the Operation answers with no outputs, which
		// only an Operation whose outputs are all Optional (or that has none) may do.
		boolean explicit=mapping.responseSources().present() || mapping.responseSinks().present()
				|| mapping.responseTransformation().present();
		if(!explicit && !requiredOutputs.isEmpty()) {
			issue(issues,mapping,root+"/response","the Operation's required output and InOut variables "+requiredOutputs
					+" need ResponseSources, a ResponseTransformation and a ResponseSink selecting the Operation");return;
		}
		trigger.put(ProtocolBridgeRouteConfiguration.EXPLICIT_RESPONSE_MAPPING,explicit);
		List<DataSinkConfiguration> responseDestinations=new ArrayList<>();
		if(explicit)compileResponseBindings(mapping,operation,commandTarget,write,!callerOrder.isEmpty(),
				requiredOutputs,plan,routeId,runtime,trigger,responseDestinations,issues);
		trigger.put("targetOptions",Map.copyOf(targetOptions));

		List<DataTransformerConfiguration> transformers=new ArrayList<>();
		String requestTransformerId=entityId(mapping,"request-transform",0);
		if(!mapping.transformation().present() || mapping.transformation().value()==null) {
			issue(issues,mapping,root+"/Transformation","Operation mappings require an authored request Lua transform");return;
		}
		byte[] requestBytes=mapping.transformation().value().bytes();
		addLua(transformers,requestTransformerId,requestBytes,mapping.identity()+":request",mapping,root+"/Transformation",issues);
		List<String> transformerIds=new ArrayList<>();
		if(!transformers.isEmpty()) transformerIds.add(requestTransformerId);
		if(mapping.responseTransformation().present() && mapping.responseTransformation().value()!=null) {
			String responseTransformerId=entityId(mapping,"response-transform",0);
			addLua(transformers,responseTransformerId,mapping.responseTransformation().value().bytes(),
					mapping.identity()+":response",mapping,root+"/ResponseTransformation",issues);
			if(issues.size()==issueStart) trigger.put(ProtocolBridgeRouteConfiguration.RESPONSE_TRANSFORMER,responseTransformerId);
		}
		if(issues.size()!=issueStart) return;
		output.addDatasources(fanInReads);
		output.addDatasink(commandSink);
		output.addDatasinks(responseDestinations);
		output.addTransformers(transformers);
		RouteConfiguration base=new RouteConfiguration(ProtocolBridgeRouteConfiguration.ROUTE_TRIGGER,
				List.of(),transformerIds,java.util.stream.Stream.concat(java.util.stream.Stream.of(commandSink.getUniqueId()),
						responseDestinations.stream().map(DataSinkConfiguration::getUniqueId)).toList());
		ProtocolBridgeRouteConfiguration bridge=new ProtocolBridgeRouteConfiguration(base);
		bridge.setRouteId(routeId+":"+mapping.listIndex());
		bridge.setTriggerData(trigger);
		output.addRoute(bridge);
	}

	/**
	 * The command of an Operation bound to a property: an ordinary write of that property,
	 * acknowledged by its connector (PLC4X write response, OPC UA Write status, HTTP response;
	 * MQTT reaches the broker only).
	 */
	private static DataSinkConfiguration propertyWriteSink(EndpointPlan endpoint,MappingPlan mapping,Plan plan,String routeId,
			RuntimeSettings runtime,Map<String,Object> targetOptions,List<Issue> issues) {
		DataSinkConfiguration sink=sink(endpoint,mapping,0,plan,routeId,runtime,issues);
		if(sink==null)return null;
		targetOptions.put("write",true);
		targetOptions.put("jsonPayload",sink.requiresJsonWireEncoding());
		return sink;
	}

	/** An AAS element read once per invocation (fan-in), whatever the source's authored interval. */
	private static DataSourceConfiguration aasReadSource(EndpointPlan endpoint,MappingPlan mapping,int position,
			DirectTarget target,RuntimeSettings runtime,List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		if(runtime==null) { issue(issues,mapping,path,"AAS source compilation requires explicit repository runtime settings");return null; }
		String elementPath=aasElementPath(endpoint,mapping,path,issues);
		if(elementPath==null)return null;
		String submodelEndpoint=submodelEndpoint(target.resolved().submodel().getId(),runtime,mapping,path,issues);
		if(submodelEndpoint==null)return null;
		URI base=runtime.aasRepositoryBaseUri();
		AASPollingDataSourceConfiguration source=new AASPollingDataSourceConfiguration(entityId(mapping,"source",position),
				base.getHost(),effectivePort(base),elementPath,submodelEndpoint);
		source.setSourceId(endpoint.id());
		source.setElementType(Aas.modelType(target.resolved().element()));
		source.setSchema(schemaFromAasElement(target.resolved().element(),mapping,path,issues));
		source.setDelivery("sync");
		return hasIssue(issues,mapping)?null:source;
	}

	private DataSinkConfiguration bridgeCommandSink(MappingPlan mapping,String action,AidTarget target,Plan plan,
			String routeId,String uniqueId,Map<String,Object> targetOptions,RuntimeSettings runtime,List<Issue> issues,String path) {
		List<WireModel.ProtocolFamily> families=target.resolved().owner().protocolFamilies();
		if(families.size()!=1) { issue(issues,mapping,path,"Operation command requires one unambiguous AID binding, found "+families);return null; }
		DataSchema input=target.resolved().affordance().input().present()?target.resolved().affordance().input().value():null;
		WireModel.ProtocolFamily family=families.get(0);
		if(family==WireModel.ProtocolFamily.HTTP || family==WireModel.ProtocolFamily.IOLINK) {
			if(!selectHttpForm(target,mapping,path,issues))return null;
			Form form=selectedForm(target);
			String method=httpMethod(form,"invoke",mapping,path,issues);
			if(method==null)return null;
			URI uri=formUri(target,form,mapping,path,issues);
			if(uri==null || !httpUri(uri,mapping,path,issues))return null;
			AidSecurity.Applied security=security(target,form,uri,runtime,mapping,path,issues);
			if(security==null)return null;
			HttpDataSinkConfiguration sink=new HttpDataSinkConfiguration(uniqueId,uri.getScheme(),uri.getHost(),uri.getPath(),effectivePort(uri));
			sink.setQuery(uri.getQuery());sink.setSinkId(action);sink.setSchema(schemaOptional(input));
			if(security.basic())sink.setBasicCredentials(security.username(),security.password());
			sink.setSecretQuery(security.query());
			sink.setHeaders(httpHeaders(form,false,security,mapping,path,issues));
			targetOptions.put("httpMethod",method);targetOptions.put("timeoutSeconds",30);
			return sink;
		}
		if(family==WireModel.ProtocolFamily.OPCUA) {
			Form form=protocolForm(target,mapping,path,issues);if(form==null)return null;
			ProtocolBase base=protocolBase(target,"opc.tcp",mapping,path,issues);if(base==null)return null;
			String method=opcuaNode(form,mapping,path,issues);
			String componentOf=componentOf(target,mapping,path,issues);
			if(method==null || componentOf==null)return null;
			Map<String,String> options=new LinkedHashMap<>();
			options.put("method","RAW("+method+")");
			OpcuaDataSinkConfiguration sink=new OpcuaDataSinkConfiguration(uniqueId,base.host(),base.port(),base.servicePath(),
					componentOf,base.username(),base.password(),options);
			sink.setSinkId(action);sink.setSchema(schemaOptional(input));
			targetOptions.put("timeoutSeconds",30);
			return sink;
		}
		if(family==WireModel.ProtocolFamily.MQTT) {
			Form form=mqttForm(target,mapping,path,"sink",true,issues);if(form==null)return null;
			MqttConnection base=mqttConnection(target,form,runtime,mapping,path,issues);if(base==null)return null;
			String topic=mqttTopic(base,form,mapping,path,issues);if(topic==null)return null;
			String reply=form.responseTopic().present()?form.responseTopic().value():null;
			// An action without an output only invokes: the command is published and the call
			// returns once the broker accepted it, so it needs QoS 1 or 2 authored (the default is the
			// WoT MQTT binding's 0, which the broker never acknowledges). An action with an output
			// answers through MQTT 5 request/reply on its mqv_responseTopic.
			boolean invokeOnly=reply==null || reply.isBlank();
			if(invokeOnly && target.resolved().affordance().output().present()) {
				issue(issues,mapping,path+"/forms/mqv_responseTopic","an MQTT action with an output needs its mqv_responseTopic (MQTT 5 request/reply)");return null;
			}
			String replyTopic=invokeOnly?null:mqttTopic(base,reply,mapping,path+"/forms/mqv_responseTopic",issues);
			Integer qos=mqttQos(form,mapping,path,issues);
			Boolean retained=mqttRetain(form,mapping,path,issues);
			if(!invokeOnly && replyTopic==null || hasIssue(issues,mapping))return null;
			if(invokeOnly && (qos==null || qos==0)) {
				issue(issues,mapping,path+"/forms/mqv_qos","an invoke-only MQTT action needs mqv_qos 1 or 2 so the broker acknowledges the command");return null;
			}
			long timeoutSeconds=30;
			MqttV5OperationDataSinkConfiguration sink=new MqttV5OperationDataSinkConfiguration(uniqueId,
					"tcp://"+base.host()+":"+base.port(),topic,stableMqttClientId(plan,routeId,mapping,action,"operation"),
					qos==null?MQTT_DEFAULT_QOS:qos,Boolean.TRUE.equals(retained),replyTopic,timeoutSeconds,base.username(),base.password());
			sink.setSinkId(action);sink.setSchema(schemaOptional(input));
			targetOptions.put("requestReply",!invokeOnly);
			if(!invokeOnly)targetOptions.put("responseTopic",replyTopic);
			targetOptions.put("timeoutSeconds",timeoutSeconds);
			return sink;
		}
		issue(issues,mapping,path,"unsupported Operation connector family "+families);
		return null;
	}

	/** The path of the Operation's BaSyx invocationDelegation URL: where BaSyx will call this DMP. */
	private static String delegationPath(Operation operation) {
		List<String> values=Aas.nonNull(operation.getQualifiers()).stream()
				.filter(qualifier -> "invocationDelegation".equals(qualifier.getType())).map(Qualifier::getValue).toList();
		if(values.size()!=1 || values.get(0)==null || values.get(0).isBlank())return null;
		try {
			URI uri=new URI(values.get(0).trim());
			String path=uri.getRawPath();
			if(!uri.isAbsolute() || uri.getHost()==null || path==null || path.isBlank() || "/".equals(path)
					|| uri.getRawQuery()!=null || uri.getRawFragment()!=null || path.contains("{"))return null;
			return path;
		} catch(java.net.URISyntaxException invalid) { return null; }
	}

	private static String operationAction(Reference reference) {
		List<Key> keys=Aas.keys(reference);
		for(int i=keys.size()-1;i>0;i--)if("Operation".equals(Aas.keyType(keys.get(i))))return keys.get(i-1).getValue();
		return null;
	}

	private static boolean sameAction(AidTarget left,AidTarget right) {
		return Objects.equals(left.resolved().submodelId(),right.resolved().submodelId())
				&& left.resolved().owner().idShort().equals(right.resolved().owner().idShort())
				&& left.resolved().affordance().equals(right.resolved().affordance());
	}

	private void compileResponseBindings(MappingPlan mapping,EndpointPlan operation,AidTarget command,boolean write,
			boolean callerOutputs,List<String> requiredOutputs,Plan plan,String routeId,RuntimeSettings runtime,Map<String,Object> trigger,
			List<DataSinkConfiguration> destinations,List<Issue> issues) {
		String path="MappingConfigurations/value/"+mapping.listIndex()+"/response";
		requireResponseUnit(mapping,path,issues);
		List<Map<String,Object>> sourceBindings=new ArrayList<>();
		Set<String> sourcePaths=new LinkedHashSet<>();
		if(mapping.responseSources().present()) {
			if(mapping.responseSources().value()==null)issue(issues,mapping,path,"ResponseSources must not be null");
			else for(EndpointPlan endpoint:mapping.responseSources().value()) {
				// The reply of an action is its output; the reply of a property write is the written value.
				if(!(endpoint.target() instanceof AidTarget target) || !sameAction(command,target)
						|| !write && target.resolved().part()!=AasReferenceResolver.SchemaPart.OUTPUT) {
					issue(issues,mapping,path,write?"ResponseSource must select the written property or a member of it"
							:"ResponseSource must select output of the exact command ACTION anchor");continue;
				}
				String pointer=jsonPointer(target.resolved().schemaPath());
				sourcePaths.add(pointer==null?"":pointer);
				Map<String,Object> binding=new LinkedHashMap<>();binding.put("sourceId",endpoint.id());binding.put("valuePath",pointer);
				sourceBindings.add(binding);
			}
		}
		if(sourcePaths.contains("") && sourcePaths.size()>1)issue(issues,mapping,path,"whole and member response sources overlap");
		trigger.put(ProtocolBridgeRouteConfiguration.RESPONSE_SOURCE_BINDINGS,List.copyOf(sourceBindings));
		String callerId=null;
		Map<String,String> callerBindings=new LinkedHashMap<>();
		List<Map<String,Object>> sinkBindings=new ArrayList<>();
		if(mapping.responseSinks().present()) {
			if(mapping.responseSinks().value()==null)issue(issues,mapping,path,"ResponseSinks must not be null");
			else for(int i=0;i<mapping.responseSinks().value().size();i++) {
				EndpointPlan endpoint=mapping.responseSinks().value().get(i);
				if(operationModel(endpoint)!=null) {
					if(!operationIdentity(endpoint).equals(operationIdentity(operation))) {
						issue(issues,mapping,path,"ResponseSink must select the command's exact caller Operation");
					} else if(isWholeOperation(endpoint)) {
						if(callerId!=null || !callerBindings.isEmpty())
							issue(issues,mapping,path,"whole Operation ResponseSink cannot overlap variable ResponseSinks");
						else callerId=endpoint.id();
					} else {
						if(callerId!=null) issue(issues,mapping,path,"whole Operation ResponseSink cannot overlap variable ResponseSinks");
						else {
							String variable=operationVariableRoot(endpoint);
							String valuePath=operationVariablePath(endpoint);
							if(variable==null || valuePath==null || !callerOutputNames(operationModel(operation)).contains(variable))
								issue(issues,mapping,path,"ResponseSink must select a declared output or InOut variable");
							else if(!valuePath.equals("$."+variable))
								issue(issues,mapping,path,"ResponseSink must select the whole output or InOut variable");
							else if(callerBindings.containsValue(valuePath))
								issue(issues,mapping,path,"only one ResponseSink may select the caller variable "+variable);
							else callerBindings.put(endpoint.id(),valuePath);
						}
					}
					continue;
				}
				Map<String,Object> binding=responseSinkBinding(endpoint,mapping,i,plan,routeId,runtime,destinations,issues);
				if(binding!=null)sinkBindings.add(binding);
			}
		}
		if(callerId==null && requiredOutputs.stream().anyMatch(name -> !callerBindings.containsValue("$."+name)))issue(issues,mapping,path,
				"the Operation's required output and InOut variables need a ResponseSink selecting the Operation or each required variable");
		if(!callerOutputs && (callerId!=null || !callerBindings.isEmpty()))issue(issues,mapping,path,
				"a ResponseSink selects the Operation, which declares no output or InOut variables");
		if(callerId!=null)trigger.put(ProtocolBridgeRouteConfiguration.CALLER_REPLY_SINK_ID,callerId);
		trigger.put(ProtocolBridgeRouteConfiguration.CALLER_REPLY_BINDINGS,Map.copyOf(callerBindings));
		trigger.put(ProtocolBridgeRouteConfiguration.RESPONSE_SINK_BINDINGS,List.copyOf(sinkBindings));
	}

	/**
	 * ResponseSources, ResponseTransformation and ResponseSinks are one unit: the sources name the reply's
	 * values, the transformation maps them, the sinks receive the result.
	 */
	private static void requireResponseUnit(MappingPlan mapping,String path,List<Issue> issues) {
		if(mapping.responseSources().value()!=null) for(EndpointPlan source:mapping.responseSources().value())
			if(source.evaluation().present())issue(issues,mapping,path,"ResponseSource "+source.id()+" takes no Evaluation: it is the reply, not an input");
		if(!mapping.responseTransformation().present() || mapping.responseTransformation().value()==null)
			issue(issues,mapping,path,"a response mapping requires a ResponseTransformation");
		if(!mapping.responseSources().present() || mapping.responseSources().value()==null
				|| mapping.responseSources().value().isEmpty())
			issue(issues,mapping,path,"a response mapping requires ResponseSources naming the reply's values");
		if(!mapping.responseSinks().present() || mapping.responseSinks().value()==null
				|| mapping.responseSinks().value().isEmpty())
			issue(issues,mapping,path,"a response mapping requires ResponseSinks receiving its result");
	}

	/** A ResponseSink written by the DMP (an AAS element, a TimeSeries segment, a device property), or null. */
	private Map<String,Object> responseSinkBinding(EndpointPlan endpoint,MappingPlan mapping,int position,Plan plan,
			String routeId,RuntimeSettings runtime,List<DataSinkConfiguration> destinations,List<Issue> issues) {
		DataSinkConfiguration sink=sink(endpoint,mapping,RESPONSE_SINK_POSITION+position,plan,routeId,runtime,issues);
		if(sink==null)return null;
		payloadBinding(sink,endpoint,mapping,issues);
		destinations.add(sink);
		Map<String,Object> binding=new LinkedHashMap<>();binding.put("sinkId",endpoint.id());
		binding.put("dataSinkId",sink.getUniqueId());binding.put("schema",sink.getSchema());
		boolean aas=endpoint.target() instanceof DirectTarget;
		binding.put("aas",aas);binding.put("jsonPayload",!aas && (sink instanceof HttpDataSinkConfiguration || sink instanceof MqttDataSinkConfiguration));
		if(!aas && endpoint.target() instanceof AidTarget target) {
			binding.put("valuePath",jsonPointer(target.resolved().schemaPath()));
			binding.put("payloadGroup",sink.getPayloadGroup());
			binding.put("payloadSchema",sink.getPayloadSchema());
		}
		return binding;
	}

	/**
	 * An ordinary mapping's response half (SPEC §6): each ResponseSource projects the value one of the mapping's
	 * Sinks was sent, or a member of it; the ResponseSinks receive the ResponseTransformation's result. No
	 * Operation is answered: only the mapping an Operation triggers answers it.
	 */
	private void compileOrdinaryResponse(MappingPlan mapping,Plan plan,String routeId,RuntimeSettings runtime,
			Map<String,Object> trigger,List<DataSinkConfiguration> destinations,List<DataTransformerConfiguration> transformers,
			List<Issue> issues) {
		String root="MappingConfigurations/value/"+mapping.listIndex();
		String path=root+"/response";
		requireResponseUnit(mapping,path,issues);
		List<Map<String,Object>> sourceBindings=new ArrayList<>();
		if(mapping.responseSources().value()!=null) for(EndpointPlan endpoint:mapping.responseSources().value()) {
			Map<String,Object> binding=writtenValueBinding(endpoint,mapping.sinks());
			if(binding==null)issue(issues,mapping,path,"ResponseSource "+endpoint.id()
					+" must select one of the mapping's Sinks, or a member of it: its value is what that Sink was sent");
			else sourceBindings.add(binding);
		}
		trigger.put(RouteConfiguration.RESPONSE_SOURCE_BINDINGS,List.copyOf(sourceBindings));
		List<Map<String,Object>> sinkBindings=new ArrayList<>();
		if(mapping.responseSinks().value()!=null) for(int i=0;i<mapping.responseSinks().value().size();i++) {
			EndpointPlan endpoint=mapping.responseSinks().value().get(i);
			if(operationModel(endpoint)!=null) {
				issue(issues,mapping,path,"ResponseSink "+endpoint.id()+" selects an Operation; only the mapping it triggers answers it");continue;
			}
			Map<String,Object> binding=responseSinkBinding(endpoint,mapping,i,plan,routeId,runtime,destinations,issues);
			if(binding!=null)sinkBindings.add(binding);
		}
		trigger.put(RouteConfiguration.RESPONSE_SINK_BINDINGS,List.copyOf(sinkBindings));
		if(!mapping.responseTransformation().present() || mapping.responseTransformation().value()==null)return;
		String id=entityId(mapping,"response-transform",0);
		int before=transformers.size();
		addLua(transformers,id,mapping.responseTransformation().value().bytes(),mapping.identity()+":response",
				mapping,root+"/ResponseTransformation",issues);
		if(transformers.size()==before)return;
		trigger.put(RouteConfiguration.RESPONSE_TRANSFORMER,id);
	}

	/** The Sink whose sent value a ResponseSource selects, the most specific when Sinks nest; null when none or two. */
	private static Map<String,Object> writtenValueBinding(EndpointPlan source,List<EndpointPlan> sinks) {
		EndpointPlan match=null;List<String> member=null;boolean ambiguous=false;
		for(EndpointPlan sink:sinks) {
			List<String> remainder=memberPath(sink,source);
			if(remainder==null)continue;
			if(match==null || remainder.size()<member.size()) { match=sink;member=remainder;ambiguous=false; }
			else if(remainder.size()==member.size())ambiguous=true;
		}
		if(match==null || ambiguous)return null;
		Map<String,Object> binding=new LinkedHashMap<>();
		binding.put("sourceId",source.id());binding.put("sinkId",match.id());binding.put("valuePath",jsonPointer(member));
		return binding;
	}

	/** The path from a Sink's target down to the source's, or null when the source does not lie within it. */
	private static List<String> memberPath(EndpointPlan sink,EndpointPlan source) {
		if(sink.target() instanceof AidTarget written && source.target() instanceof AidTarget selected) {
			if(!sameAction(written,selected))return null;
			return remainder(written.resolved().schemaPath(),selected.resolved().schemaPath());
		}
		boolean series=sink.target() instanceof SeriesTarget;
		if(series!=source.target() instanceof SeriesTarget || !series && !(sink.target() instanceof DirectTarget && source.target() instanceof DirectTarget))
			return null;
		List<Key> sinkKeys=Aas.keys(sink.reference()),sourceKeys=Aas.keys(source.reference());
		if(sourceKeys.size()<sinkKeys.size() || series && sourceKeys.size()!=sinkKeys.size())return null;
		for(int i=0;i<sinkKeys.size();i++)
			if(!Objects.equals(sinkKeys.get(i).getValue(),sourceKeys.get(i).getValue())
					|| !Objects.equals(Aas.keyType(sinkKeys.get(i)),Aas.keyType(sourceKeys.get(i))))return null;
		return sourceKeys.subList(sinkKeys.size(),sourceKeys.size()).stream().map(Key::getValue).toList();
	}

	private static List<String> remainder(List<String> prefix,List<String> path) {
		return path.size()>=prefix.size() && path.subList(0,prefix.size()).equals(prefix) ? path.subList(prefix.size(),path.size()) : null;
	}

	/** A Source's authored {@code Evaluation/Mode}; without one, {@link Evaluation#ON_UPDATE}. */
	private static Evaluation evaluation(EndpointPlan source) {
		return evaluation(source,Evaluation.ON_UPDATE);
	}

	private static Evaluation evaluation(EndpointPlan source,Evaluation absent) {
		if(!source.evaluation().present())return absent;
		Evaluation authored=Evaluation.ofAuthored(source.evaluation().value());
		return authored==null?absent:authored;
	}

	/**
	 * A source read once per evaluation ({@code ReadAtEvaluation}): a Read, never an observation, at no interval
	 * of its own. Its affordance must be readable; an MQTT topic is only received by subscription.
	 */
	private static DataSourceConfiguration readSource(EndpointPlan endpoint,MappingPlan mapping,int position,Plan plan,
			String routeId,RuntimeSettings runtime,List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		if(polled(endpoint)) { issue(issues,mapping,path+"/PollingInterval","a source read at evaluation is not polled; remove its PollingInterval");return null; }
		if(endpoint.target() instanceof SeriesTarget series)return seriesSource(endpoint,mapping,position,series,runtime,true,issues);
		if(endpoint.target() instanceof DirectTarget direct) {
			if(direct.resolved().element() instanceof Operation)return source(endpoint,mapping,position,plan,routeId,runtime,issues);
			return aasReadSource(endpoint,mapping,position,direct,runtime,issues);
		}
		AidTarget target=(AidTarget)endpoint.target();
		WireModel.Affordance affordance=target.resolved().affordance();
		if(!"property".equalsIgnoreCase(affordance.kind())) { issue(issues,mapping,path,"only a property can be read at evaluation");return null; }
		if(affordance.forms().stream().noneMatch(form -> AidForms.operations(affordance,form).contains(AidForms.READ))) {
			issue(issues,mapping,path,"the property is not readable (op readproperty), so it cannot be read at evaluation");return null;
		}
		List<WireModel.ProtocolFamily> families=target.resolved().owner().protocolFamilies();
		if(families.equals(List.of(WireModel.ProtocolFamily.MQTT))) {
			issue(issues,mapping,path,"an MQTT property is received by subscription and cannot be read at evaluation");return null;
		}
		DataSourceConfiguration source=families.equals(List.of(WireModel.ProtocolFamily.OPCUA))
				?opcuaSource(endpoint,mapping,position,target,true,issues)
				:source(endpoint,mapping,position,plan,routeId,runtime,issues);
		if(source==null)return null;
		if(!"sync".equalsIgnoreCase(source.getDelivery())) {
			issue(issues,mapping,path,"the form observes (e.g. an HTTP subprotocol) and cannot be read at evaluation");return null;
		}
		source.setPollingIntervalMs(0);
		return source;
	}

	private static void validateCapabilities(MappingPlan mapping,List<Issue> issues) {
		if(mapping.sources().isEmpty() || mapping.sinks().isEmpty())issue(issues,mapping,
				"MappingConfigurations/value/"+mapping.listIndex(),"mapping requires nonempty sources and sinks");
		List<EndpointPlan> endpoints=new ArrayList<>(mapping.sources());endpoints.addAll(mapping.sinks());
		if(mapping.responseSources().value()!=null)endpoints.addAll(mapping.responseSources().value());
		if(mapping.responseSinks().value()!=null)endpoints.addAll(mapping.responseSinks().value());
		for(EndpointPlan endpoint:endpoints)if(endpoint.target() instanceof AidTarget target) {
			// A ResponseSource naming the command's own affordance projects its reply (an action's
			// output, a written value); it is no device read, and the affordance is checked as the Sink.
			if(mapping.responseSources().value()!=null && mapping.responseSources().value().contains(endpoint)
					&& mapping.sinks().stream().anyMatch(sink -> sink.target() instanceof AidTarget command && sameAction(command,target)))
				continue;
			String path="MappingConfigurations/value/"+mapping.listIndex()+"/endpoint/"+endpoint.id();
			List<WireModel.ProtocolFamily> families=target.resolved().owner().protocolFamilies();
			if(families.size()!=1) { issue(issues,mapping,path,"ambiguous protocol bindings require an exact single binding");continue; }
			WireModel.ProtocolFamily family=families.get(0);
			boolean read=mapping.sources().contains(endpoint) || mapping.responseSources().value()!=null && mapping.responseSources().value().contains(endpoint);
			WireModel.Affordance affordance=target.resolved().affordance();
			String kind=affordance.kind();
			for(SubmodelElement child:Aas.children(affordance.element())) {
				String name=child.getIdShort();
				if(!List.of("readOnly","writeOnly","synchronous","observable").contains(name==null?"":name) || Aas.value(child)==null)continue;
				String text=Aas.value(child);
				if(!List.of("true","false","1","0").contains(text.toLowerCase(java.util.Locale.ROOT))) {
					issue(issues,mapping,path+"/"+name,"capability must be a declared boolean");continue;
				}
				boolean enabled="true".equalsIgnoreCase(text) || "1".equals(text);
				if("synchronous".equals(name) && !enabled && "action".equals(kind)
						&& !(family==WireModel.ProtocolFamily.MQTT && affordance.forms().stream().anyMatch(form -> form.responseTopic().present())))
					issue(issues,mapping,path+"/"+name,"non-synchronous actions are not supported by the request/reply bridge");
			}
			if("event".equals(kind))
				for(SubmodelElement child:Aas.children(affordance.element()))
					if(List.of("subscription","dataResponse","cancellation").contains(child.getIdShort()==null?"":child.getIdShort()))
						issue(issues,mapping,path+"/"+child.getIdShort(),"event subscription, response and cancellation payloads are not sent by the DMP");
			Set<String> terms=switch(family) {
				case MQTT -> Set.of("mqv_qos","mqv_retain","mqv_controlPacket","mqv_responseTopic");
				case OPCUA -> Set.of("uav_browsePath");
				case SNMP -> Set.of("snmpv_pduType","snmpv_version");
				case HTTP, IOLINK -> Set.of("htv_headers");
				case MODBUS -> Set.of("modv_entity","modv_function","modv_type","modv_zeroBasedAddressing","modv_mostSignificantByte",
						"modv_mostSignificantWord","modv_pollingTime","modv_timeout");
				default -> Set.of();
			};
			for(Form form:affordance.forms()) {
				for(SubmodelElement child:Aas.children(form.element())) {
					String name=child.getIdShort()==null?"":child.getIdShort();
					boolean authored=Aas.value(child)!=null || !Aas.children(child).isEmpty();
					if("security".equals(name))continue; // read with the interface's selection below (AidSecurity)
					if("subprotocol".equals(name) && authored) {
						// WoT HTTP binding: a subprotocol observes a property or subscribes an event.
						String subprotocol=httpSubprotocol(form);
						if(family!=WireModel.ProtocolFamily.HTTP && family!=WireModel.ProtocolFamily.IOLINK)
							issue(issues,mapping,path+"/forms/subprotocol","a subprotocol belongs to an HTTP form");
						else if(!Set.of("longpoll","sse").contains(subprotocol))
							issue(issues,mapping,path+"/forms/subprotocol","subprotocol "+subprotocol+" is not supported (longpoll and sse are)");
						else if(!read)
							issue(issues,mapping,path+"/forms/subprotocol","a subprotocol form observes; it cannot be a writable destination or an action");
					}
					else if("response".equals(name) && !"action".equals(kind))
						issue(issues,mapping,path+"/forms/response","an expected response belongs to an action form");
					// additionalResponses describe replies other than the regular one; the regular
					// reply is validated against the affordance's schema, so they change nothing here.
					else if(!Set.of("href","contentType","op","htv_methodName","security","response","additionalResponses").contains(name)
							&& !terms.contains(name) && authored)
						issue(issues,mapping,path+"/forms/"+name,"unknown consumed form option");
				}
				for(String term:form.bindingTerms().keySet())if(!terms.contains(term))issue(issues,mapping,path+"/forms/"+term,"authored protocol option is not represented by this connector");
				// The schemes the form must satisfy exist and the DMP applies them to this protocol (PLAN H4).
				List<AidSecurity.Problem> security=new ArrayList<>();
				AidSecurity.required(target.resolved().owner(),form,target.resolved().submodelId(),family,security);
				for(AidSecurity.Problem problem:security)issue(issues,mapping,path+problem.path(),problem.message());
				for(String operation:form.operations())
					if(!AidForms.KNOWN.contains(operation))issue(issues,mapping,path+"/forms/op","unknown WoT operation type "+operation);
				Set<String> operations=AidForms.operations(affordance,form);
				// The mapping role needs an operation the form serves (op, else the TD defaults from
				// readOnly/writeOnly/observable; aas-model docs/aid-extension.md §3.4).
				if(read && "property".equals(kind) && !operations.contains(AidForms.READ) && !operations.contains(AidForms.OBSERVE))
					issue(issues,mapping,path,"the property is neither readable nor observable (op, writeOnly, observable)");
				if(!read && "property".equals(kind) && !operations.contains(AidForms.WRITE))
					issue(issues,mapping,path,"the property is not writable (op, readOnly)");
				if(!read && "action".equals(kind) && !operations.contains(AidForms.INVOKE))
					issue(issues,mapping,path+"/forms/op","the action form does not serve invokeaction");
				if(read && "event".equals(kind) && !operations.contains(AidForms.SUBSCRIBE))
					issue(issues,mapping,path+"/forms/op","the event form does not serve subscribeevent");
				if(!read && "event".equals(kind))issue(issues,mapping,path,"event affordances cannot be writable destinations");
				if(family==WireModel.ProtocolFamily.HTTP || family==WireModel.ProtocolFamily.IOLINK) {
					boolean observed=httpSubprotocol(form)!=null;
					if(read && "event".equals(kind) && !observed)
						issue(issues,mapping,path+"/forms/subprotocol","an HTTP event is subscribed through subprotocol longpoll or sse");
					if(read && "property".equals(kind) && observed && !operations.contains(AidForms.OBSERVE))
						issue(issues,mapping,path+"/forms/op","the form's subprotocol observes, but the form does not serve observeproperty (op, observable)");
					if(observed && mapping.sources().contains(endpoint) && polled(endpoint))
						issue(issues,mapping,path,"an observed HTTP source is not polled; remove its PollingInterval");
				}
				for(SubmodelElement child:Aas.children(form.element()))if("response".equals(child.getIdShort()))
					for(SubmodelElement option:Aas.children(child)) {
						String value=Aas.value(option);
						if(!"contentType".equals(option.getIdShort()))
							issue(issues,mapping,path+"/forms/response","an expected response declares only its contentType");
						else if(value!=null && !"application/json".equalsIgnoreCase(value))
							issue(issues,mapping,path+"/forms/response","action results are decoded as JSON, not "+value);
					}
				if(family==WireModel.ProtocolFamily.MQTT) {
					// WoT MQTT binding: the control packet is the consumer's (subscribe to read and observe,
					// publish to write and invoke), as AID's "in relation to the broker".
					String packet=mqttTerm(form,"mqv_controlPacket");
					String expected=read?"subscribe":"publish";
					if(packet!=null && !expected.equalsIgnoreCase(packet.trim()))
						issue(issues,mapping,path+"/forms/mqv_controlPacket","mqv_controlPacket "+packet+" does not serve this mapping role (expected "+expected+")");
					mqttRetain(form,mapping,path,issues);
					if(form.contentType().present() && form.contentType().value()!=null && !"application/json".equalsIgnoreCase(form.contentType().value()))
						issue(issues,mapping,path+"/forms/contentType","MQTT payloads are decoded as JSON, not "+form.contentType().value());
					if(mapping.sources().contains(endpoint) && "property".equals(kind) && !operations.contains(AidForms.OBSERVE)
							&& !Boolean.TRUE.equals(mqttRetain(form,mapping,path,new ArrayList<>())))
						issue(issues,mapping,path,"an MQTT property is read by subscription: declare it observable, or retained (mqv_retain)");
					if(mapping.sources().contains(endpoint) && polled(endpoint))
						issue(issues,mapping,path,"MQTT polling cannot be implemented as fresh Read");
				}
				if(family!=WireModel.ProtocolFamily.HTTP && family!=WireModel.ProtocolFamily.IOLINK
						&& family!=WireModel.ProtocolFamily.MQTT) {
					// OPC 10101 §6.5.3: the default UA serialization is declared as application/octet-stream.
					if(form.contentType().present() && form.contentType().value()!=null
							&& !"application/json".equalsIgnoreCase(form.contentType().value())
							&& !((family==WireModel.ProtocolFamily.OPCUA || family==WireModel.ProtocolFamily.MODBUS)
									&& "application/octet-stream".equalsIgnoreCase(form.contentType().value())))
						issue(issues,mapping,path,"native connector supports decoded JSON values, not the authored content type");
					if(form.httpMethod().present())issue(issues,mapping,path+"/forms/htv_methodName","an HTTP method on a non-HTTP form");
					if("action".equals(kind) && family!=WireModel.ProtocolFamily.OPCUA)
						issue(issues,mapping,path,"actions of this protocol are not invoked by the DMP (a write reports no result)");
					if("event".equals(kind) && family!=WireModel.ProtocolFamily.OPCUA)
						issue(issues,mapping,path,"events of this protocol are not consumed by the DMP");
				}
			}
		}
	}

	private static void payloadBinding(DataSinkConfiguration sink,EndpointPlan endpoint,MappingPlan mapping,List<Issue> issues) {
		if(!(endpoint.target() instanceof AidTarget target))return;
		WireModel.Affordance affordance=target.resolved().affordance();
		var root=new AidTarget(new dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver.AidTarget(
				target.resolved().owner(),affordance,affordance.schema(),List.of(),target.resolved().submodelId()));
		sink.setPayloadSchema(schemaForAidTarget(root,mapping,"MappingConfigurations/value/"+mapping.listIndex()+"/Sinks",issues));
		sink.setPayloadGroup(target.resolved().submodelId()+":"+target.resolved().owner().idShort()+":"+affordance.idShort().value());
	}

	private static String pathSegment(String value) {
		return java.net.URLEncoder.encode(value,StandardCharsets.UTF_8).replace("+","%20");
	}

	private static Map<String,String> schemaTypes(DataSchema schema) {
		if(schema==null)return Map.of();
		Map<String,String> types=new LinkedHashMap<>();
		schema.properties().forEach((name,child)->{
			String type=argumentType(child);
			if(type!=null)types.put(name,type);
		});
		return Map.copyOf(types);
	}
	private static String argumentType(DataSchema schema) {
		String type=schema.type().value();
		if(type==null)type=schemaType(schema.valueType().value());
		if(!"array".equals(type))return type;
		String item=schema.items().value()==null?null:argumentType(schema.items().value());
		return "array<"+(item==null?"unknown":item)+">";
	}

	private static List<String> schemaRequired(DataSchema schema) {
		return schema!=null && schema.required().present() && schema.required().value()!=null
				? schema.required().value():List.of();
	}

	private void addLua(List<DataTransformerConfiguration> transformers,String id,byte[] bytes,String identity,
			MappingPlan mapping,String path,List<Issue> issues) {
		try {
			LuaCompiledSource compiled=compiledLua(bytes,identity);
			transformers.add(new LuaTransformerConfiguration(id,compiled));
		} catch(RuntimeException failure) {
			issue(issues,mapping,path,"Lua source cannot be compiled: "+failure.getMessage());
		}
	}

	/** Compiles isolated per-mapping configurations suitable for DmpComponent.installBundle. */
	public BundleResult createBundles(Plan plan,RuntimeSettings runtime) {
		List<CompiledBundle> bundles=new ArrayList<>();
		List<Issue> issues=new ArrayList<>();
		for(MappingPlan mapping:plan.mappings()) {
			String bundleId=stableBundleId(plan,mapping,runtime);
			Plan single=new Plan(plan.aimcSubmodelId(),List.of(mapping));
			Result compiled;
			try { compiled=create(single,bundleId,runtime); }
			catch(RuntimeException invalid) {
				issue(issues,mapping,"MappingConfigurations/value/"+mapping.listIndex(),"mapping configuration preflight failed");continue;
			}
			if(compiled.issues().isEmpty()) {
				compiled.configuration().getRoutes().get(0).setRouteId(bundleId+":mapping");
				bundles.add(new CompiledBundle(mapping.identity(),mapping.listIndex(),bundleId,
						compiled.configuration().getRoutes().get(0).getRouteId(),compiled.configuration(),
						EffectiveFingerprint.of(mapping,runtime)));
			} else {
				issues.addAll(compiled.issues());
			}
		}
		return new BundleResult(bundles,issues);
	}

	/** Blocked resolution and connector outcomes keep their dependency sets beside valid bundles. */
	public List<DesiredBundle> createDesiredBundles(MappingPlanCompiler.DesiredPlan desired,RuntimeSettings runtime) {
		List<DesiredBundle> result=new ArrayList<>();
		for(var mapping:desired.mappings()) {
			if(!mapping.valid()) {
				result.add(new DesiredBundle(mapping,null,List.of(),EffectiveFingerprint.ofDesired(mapping,runtime,List.of())));
				continue;
			}
			var compiled=createBundles(new Plan(desired.aimcSubmodelId(),List.of(mapping.plan())),runtime);
			CompiledBundle bundle=compiled.bundles().isEmpty()?null:compiled.bundles().get(0);
			result.add(new DesiredBundle(mapping,bundle,compiled.issues(),bundle==null
					? EffectiveFingerprint.ofDesired(mapping,runtime,compiled.issues()) : bundle.fingerprint()));
		}
		return List.copyOf(result);
	}

	private static String stableBundleId(Plan plan,MappingPlan mapping,RuntimeSettings runtime) {
		String aimc=plan.aimcSubmodelId()==null?"":plan.aimcSubmodelId();
		String repository=runtime==null?"":runtime.aasRepositoryBaseUri().normalize().toString();
		String identity=repository+"\u0000"+aimc+"\u0000"+mapping.identity();
		try {
			byte[] digest=java.security.MessageDigest.getInstance("SHA-256").digest(identity.getBytes(StandardCharsets.UTF_8));
			return "dmp-bundle-"+java.util.HexFormat.of().formatHex(digest);
		} catch(java.security.NoSuchAlgorithmException impossible) {
			throw new IllegalStateException("SHA-256 unavailable",impossible);
		}
	}

	private static DataSourceConfiguration source(EndpointPlan endpoint, MappingPlan mapping, int position,
			Plan plan, String routeId, RuntimeSettings runtime, List<Issue> issues) {
		String path = endpointPath(mapping, "Sources", position);
		if (endpoint.target() instanceof SeriesTarget series) return seriesSource(endpoint,mapping,position,series,runtime,false,issues);
		if (endpoint.target() instanceof DirectTarget) {
			if(((DirectTarget)endpoint.target()).resolved().element() instanceof Operation) {
				issue(issues,mapping,path,"AAS Operation targets require a ProtocolBridge route and declared ingress contract");
				return null;
			}
			return aasSource(endpoint,mapping,position,(DirectTarget)endpoint.target(),runtime,issues);
		}
		AidTarget target = (AidTarget) endpoint.target();
		if("action".equalsIgnoreCase(target.resolved().affordance().kind())) {
			issue(issues,mapping,path,"AID Action sources require Operation bridge compilation");
			return null;
		}
		List<WireModel.ProtocolFamily> families=target.resolved().owner().protocolFamilies();
		if (families.equals(List.of(WireModel.ProtocolFamily.MQTT))) {
			return mqttSource(endpoint, mapping, position, target, plan, routeId, runtime, issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.OPCUA))) {
			return opcuaSource(endpoint,mapping,position,target,false,issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.MODBUS))) {
			return plc4xSource(endpoint,mapping,position,target,issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.BACNET))) {
			return plc4xSource(endpoint,mapping,position,target,issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.SNMP))) {
			return snmpSource(endpoint,mapping,position,target,issues);
		}
		if (!families.equals(List.of(WireModel.ProtocolFamily.HTTP))
				&& !families.equals(List.of(WireModel.ProtocolFamily.IOLINK))) {
			issue(issues,mapping,path,"unsupported or ambiguous source protocol family "+families);
			return null;
		}
		if (!selectHttpForm(target, mapping, path, issues)) return null;
		Form form = selectedForm(target);
		if (httpMethod(form, "read", mapping, path, issues) == null) return null;
		URI uri = formUri(target,form, mapping, path, issues);
		if (uri == null) return null;
		if (!httpUri(uri, mapping, path, issues)) return null;
		AidSecurity.Applied security = security(target, form, uri, runtime, mapping, path, issues);
		if (security == null) return null;
		String username = security.username();
		String password = security.password();
		String subprotocol = httpSubprotocol(form);
		if (subprotocol != null) {
			// Observed (property) or subscribed (event) through the WoT HTTP binding's subprotocol.
			HttpObserveDataSourceConfiguration observed = new HttpObserveDataSourceConfiguration(entityId(mapping, "source", position),
					uri.toString(), effectivePort(uri), subprotocol,
					form.contentType().present() && form.contentType().value() != null ? form.contentType().value() : "application/json",
					username, password);
			observed.setSourceId(endpoint.id());
			observed.setSecretQuery(security.query());
			observed.setHeaders(httpHeaders(form, true, security, mapping, path, issues));
			observed.setSchema(schemaForAidTarget(target,mapping,path,issues));
			setValuePath(observed,target);
			return observed;
		}
		HttpPollingDataSourceConfiguration result = new HttpPollingDataSourceConfiguration(
				entityId(mapping, "source", position), uri.toString(), effectivePort(uri), username, password);
		result.setSourceId(endpoint.id());
		result.setSecretQuery(security.query());
		result.setHeaders(httpHeaders(form, false, security, mapping, path, issues));
		result.setSchema(schemaForAidTarget(target,mapping,path,issues));
		result.setDelivery("sync");
		setValuePath(result,target);
		if (endpoint.pollingInterval().present()) {
			OptionalLong polling = endpointPollingMillis(endpoint.pollingInterval(), mapping, position, issues);
			if (polling.isPresent()) {
				result.setDelivery("sync");
				result.setPollingIntervalMs(polling.getAsLong());
			}
		}
		return result;
	}

	/** Whether a source authors a PollingInterval other than 0 (0 is event-based, like none). */
	private static boolean polled(EndpointPlan endpoint) {
		if (!endpoint.pollingInterval().present()) return false;
		try { return Double.parseDouble(String.valueOf(endpoint.pollingInterval().value())) != 0; }
		catch (NumberFormatException invalid) { return true; }
	}

	private static OptionalLong endpointPollingMillis(WireModel.Presence<String> presence,
			MappingPlan mapping, int position, List<Issue> issues) {
		Object value = presence.value();
		double seconds;
		try { seconds = value instanceof Number number ? number.doubleValue() : Double.parseDouble(String.valueOf(value)); }
		catch (RuntimeException invalid) {
			issue(issues, mapping, endpointPath(mapping,"Sources",position)+"/PollingInterval", "expected a numeric interval in authored seconds");
			return OptionalLong.empty();
		}
		if (seconds == 0) return OptionalLong.empty();
		if (!Double.isFinite(seconds) || seconds < 0 || seconds * 1000d > Long.MAX_VALUE || seconds * 1000d < 1) {
			issue(issues, mapping, endpointPath(mapping,"Sources",position)+"/PollingInterval", "poll interval is outside the positive runtime millisecond range");
			return OptionalLong.empty();
		}
		return OptionalLong.of((long)(seconds*1000d));
	}

	private static DataSinkConfiguration sink(EndpointPlan endpoint, MappingPlan mapping, int position,
			Plan plan, String routeId, RuntimeSettings runtime, List<Issue> issues) {
		String path = endpointPath(mapping, "Sinks", position);
		if (endpoint.target() instanceof SeriesTarget series) return seriesSink(endpoint,mapping,position,series,runtime,issues);
		if (endpoint.target() instanceof DirectTarget) {
			if(operationModel(endpoint)!=null) {
				issue(issues,mapping,path,"AAS Operation sinks are not supported (chained actions are unsafe); an Operation is answered through its ResponseSinks");
				return null;
			}
			return aasSink(endpoint,mapping,position,(DirectTarget)endpoint.target(),runtime,issues);
		}
		AidTarget target = (AidTarget) endpoint.target();
		if("action".equalsIgnoreCase(target.resolved().affordance().kind())) {
			issue(issues,mapping,path,"AID Action sinks require Operation bridge compilation");
			return null;
		}
		List<WireModel.ProtocolFamily> families=target.resolved().owner().protocolFamilies();
		if (families.equals(List.of(WireModel.ProtocolFamily.MQTT))) {
			return mqttSink(endpoint, mapping, position, target, plan, routeId, runtime, issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.OPCUA))) {
			return opcuaSink(endpoint,mapping,position,target,issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.MODBUS))) {
			return plc4xSink(endpoint,mapping,position,target,issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.BACNET))) {
			return plc4xSink(endpoint,mapping,position,target,issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.SNMP))) {
			return snmpSink(endpoint,mapping,position,target,issues);
		}
		if (!families.equals(List.of(WireModel.ProtocolFamily.HTTP))
				&& !families.equals(List.of(WireModel.ProtocolFamily.IOLINK))) {
			issue(issues,mapping,path,"unsupported or ambiguous sink protocol family "+families);
			return null;
		}
		if (!selectHttpForm(target, mapping, path, issues)) return null;
		Form form = selectedForm(target);
		String method = httpMethod(form, "write", mapping, path, issues);
		if (method == null) return null;
		URI uri = formUri(target,form, mapping, path, issues);
		if (uri == null) return null;
		if (!httpUri(uri, mapping, path, issues)) return null;
		AidSecurity.Applied security = security(target, form, uri, runtime, mapping, path, issues);
		if (security == null) return null;
		HttpDataSinkConfiguration result = new HttpDataSinkConfiguration(entityId(mapping, "sink", position),
				uri.getScheme(), uri.getHost(), uri.getPath(), effectivePort(uri));
		result.setQuery(uri.getQuery());
		result.setMethod(method);
		if (security.basic()) result.setBasicCredentials(security.username(), security.password());
		result.setSecretQuery(security.query());
		result.setHeaders(httpHeaders(form, false, security, mapping, path, issues));
		result.setSinkId(endpoint.id());
		result.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(result,target);
		return result;
	}

	/** A TimeSeries LinkedSegment written one point per row on the configured InfluxDB. */
	private static DataSinkConfiguration seriesSink(EndpointPlan endpoint,MappingPlan mapping,int position,
			SeriesTarget target,RuntimeSettings runtime,List<Issue> issues) {
		String path=endpointPath(mapping,"Sinks",position);
		if(!seriesOnConfiguredInflux(target,mapping,path,runtime,issues))return null;
		TimeSeriesProjection.Series series=target.projection();
		if(!series.open()) {
			issue(issues,mapping,path,"the segment is Completed or has an EndTime; only an open segment can be written");return null;
		}
		InfluxDb2DataSinkConfiguration sink=new InfluxDb2DataSinkConfiguration(entityId(mapping,"sink",position),
				series.measurement(),series.bucket(),series.timePath(),series.tags(),series.columns());
		sink.setPeriod(series.start(),series.end());
		sink.setSinkId(endpoint.id());
		return sink;
	}

	/**
	 * A TimeSeries LinkedSegment read by polling: its latest row. InfluxDB has no subscriptions, so the source
	 * needs an authored PollingInterval (its own or the mapping's DefaultPollingInterval); none is assumed.
	 */
	private static DataSourceConfiguration seriesSource(EndpointPlan endpoint,MappingPlan mapping,int position,
			SeriesTarget target,RuntimeSettings runtime,boolean readAtEvaluation,List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		if(!seriesOnConfiguredInflux(target,mapping,path,runtime,issues))return null;
		OptionalLong interval=readAtEvaluation ? OptionalLong.empty() : endpoint.pollingInterval().present()
				? endpointPollingMillis(endpoint.pollingInterval(),mapping,position,issues) : pollingMillis(mapping,issues);
		if(interval.isEmpty() && !readAtEvaluation) {
			if(!hasIssue(issues,mapping))issue(issues,mapping,path,"a TimeSeries segment source is polled; it needs a PollingInterval or the mapping's DefaultPollingInterval");
			return null;
		}
		TimeSeriesProjection.Series series=target.projection();
		URI influx=runtime.influx().url();
		InfluxDb2DataSourceConfiguration source=new InfluxDb2DataSourceConfiguration(entityId(mapping,"source",position),
				influx.getHost(),effectivePort(influx),series.measurement(),series.bucket(),series.timePath(),series.tags(),series.columns());
		source.setPeriod(series.start(),series.end());
		source.setSourceId(endpoint.id());
		if(interval.isPresent())source.setPollingIntervalMs(interval.getAsLong());
		return hasIssue(issues,mapping)?null:source;
	}

	private static boolean seriesOnConfiguredInflux(SeriesTarget target,MappingPlan mapping,String path,
			RuntimeSettings runtime,List<Issue> issues) {
		if(runtime==null || runtime.influx()==null) {
			issue(issues,mapping,path,"TimeSeries segments need a configured InfluxDB ("+InfluxSettings.URL+")");return false;
		}
		if(!runtime.influx().serves(target.projection().endpoint())) {
			issue(issues,mapping,path,"the segment's Endpoint "+target.projection().endpoint()
					+" does not name the configured InfluxDB and org");return false;
		}
		return true;
	}

	private static DataSourceConfiguration aasSource(EndpointPlan endpoint,MappingPlan mapping,int position,
			DirectTarget target,RuntimeSettings runtime,List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		if(runtime==null) { issue(issues,mapping,path,"AAS source compilation requires explicit repository runtime settings");return null; }
		String elementPath=aasElementPath(endpoint,mapping,path,issues);
		if(elementPath==null)return null;
		String submodelEndpoint=submodelEndpoint(target.resolved().submodel().getId(),runtime,mapping,path,issues);
		if(submodelEndpoint==null)return null;
		OptionalLong interval=endpoint.pollingInterval().present()
				? endpointPollingMillis(endpoint.pollingInterval(),mapping,position,issues) : pollingMillis(mapping,new ArrayList<>());
		if(target.resolved().element() instanceof Property && interval.isEmpty()) {
			// Without a positive PollingInterval (none, or 0) a Property follows the repository's change events.
			String idShortPath=AasReferenceResolver.idShortPath(Aas.keys(endpoint.reference()));
			AasEventDataSourceConfiguration source=new AasEventDataSourceConfiguration(entityId(mapping,"source",position),
					target.resolved().submodel().getId(),idShortPath,dk.aau.smartproductionlab.dmp.aas.http.AasHttp.elementUrl(submodelEndpoint,idShortPath));
			source.setSourceId(endpoint.id());
			source.setSchema(schemaFromAasElement(target.resolved().element(),mapping,path,issues));
			return hasIssue(issues,mapping)?null:source;
		}
		URI base=runtime.aasRepositoryBaseUri();
		AASPollingDataSourceConfiguration source=new AASPollingDataSourceConfiguration(entityId(mapping,"source",position),
				base.getHost(),effectivePort(base),elementPath,submodelEndpoint);
		source.setSourceId(endpoint.id());
		source.setElementType(Aas.modelType(target.resolved().element()));
		source.setSchema(schemaFromAasElement(target.resolved().element(),mapping,path,issues));
		source.setDelivery("sync");
		if(endpoint.pollingInterval().present() && interval.isPresent())source.setPollingIntervalMs(interval.getAsLong());
		return hasIssue(issues,mapping)?null:source;
	}

	private static DataSinkConfiguration aasSink(EndpointPlan endpoint,MappingPlan mapping,int position,
			DirectTarget target,RuntimeSettings runtime,List<Issue> issues) {
		String path=endpointPath(mapping,"Sinks",position);
		if(runtime==null) { issue(issues,mapping,path,"AAS sink compilation requires explicit repository runtime settings");return null; }
		String elementType=Aas.modelType(target.resolved().element());
		// SPEC §5.3: a Property or a collection is written; a list's $value write is broken in BaSyx (it keeps
		// the length and reorders the items), so a list is written item by item (Items[0]).
		if(!"Property".equals(elementType) && !"SubmodelElementCollection".equals(elementType)) {
			issue(issues,mapping,path,"an AAS sink must be a Property or a SubmodelElementCollection, not a "+elementType);return null;
		}
		String elementPath=aasElementPath(endpoint,mapping,path,issues);
		if(elementPath==null)return null;
		String submodelEndpoint=submodelEndpoint(target.resolved().submodel().getId(),runtime,mapping,path,issues);
		if(submodelEndpoint==null)return null;
		AASDatasinkConfiguration sink=new AASDatasinkConfiguration(submodelEndpoint,elementPath,
				entityId(mapping,"sink",position));
		sink.setSinkId(endpoint.id());
		sink.setSchema(schemaFromAasElement(target.resolved().element(),mapping,path,issues));
		sink.setValuePath(endpoint.id());
		sink.setElementType(elementType);
		return hasIssue(issues,mapping)?null:sink;
	}

	private static String aasElementPath(EndpointPlan endpoint,MappingPlan mapping,String path,List<Issue> issues) {
		List<Key> keys=Aas.keys(endpoint.reference());
		if(keys.size()<2 || !"Submodel".equals(Aas.keyType(keys.get(0)))) {
			issue(issues,mapping,path,"AAS reference must address an exact submodel element");return null;
		}
		// The repository addresses a nested element by its idShortPath (Location.x, Items[0]).
		return AasReferenceResolver.idShortPath(keys);
	}

	private static String submodelEndpoint(String submodelId,RuntimeSettings runtime,MappingPlan mapping,
			String path,List<Issue> issues) {
		try {
			String base=runtime.aasRepositoryBaseUri().toString().replaceAll("/+$","");
			return base+"/submodels/"+AasRepositoryClient.base64UrlIdentifier(submodelId);
		} catch(RuntimeException invalid) {
			issue(issues,mapping,path,"AAS submodel endpoint cannot be encoded safely");return null;
		}
	}

	private static Map<String,Object> schemaFromAasElement(SubmodelElement element,MappingPlan mapping,
			String path,List<Issue> issues) {
		switch(Aas.modelType(element)) {
		case "Property": {
			String name=Aas.xsd(((Property)element).getValueType());
			String type=schemaType(name);
			if(type==null) { issue(issues,mapping,path,"unsupported AAS Property valueType "+name);return Map.of(); }
			return Map.of("type",type);
		}
		case "SubmodelElementCollection": {
			Map<String,Object> properties=new LinkedHashMap<>();
			for(SubmodelElement child:Aas.children(element)) {
				if(child.getIdShort()==null) {
					issue(issues,mapping,path,"AAS collection child has no authored idShort");return Map.of();
				}
				Map<String,Object> childSchema=schemaFromAasElement(child,mapping,path+"/"+child.getIdShort(),issues);
				properties.put(child.getIdShort(),childSchema);
			}
			return Map.of("type","object","properties",properties);
		}
		case "SubmodelElementList": {
			SubmodelElementList list=(SubmodelElementList)element;
			if(list.getTypeValueListElement()==AasSubmodelElements.PROPERTY && list.getValueTypeListElement()!=null) {
				String name=Aas.xsd(list.getValueTypeListElement());
				String type=schemaType(name);
				if(type==null) { issue(issues,mapping,path,"unsupported AAS list valueTypeListElement "+name);return Map.of(); }
				return Map.of("type","array","items",Map.of("type",type));
			}
			if(Aas.children(element).isEmpty()) { issue(issues,mapping,path,"AAS list schema has no typed member");return Map.of(); }
			return Map.of("type","array","items",schemaFromAasElement(Aas.children(element).get(0),mapping,path+"/items",issues));
		}
		default:
			issue(issues,mapping,path,"AAS element type cannot be represented as a JSON schema: "+Aas.modelType(element));
			return Map.of();
		}
	}

	private static String schemaType(String valueType) {
		if(valueType==null)return null;
		return switch(valueType) {
			case "xs:string","xs:normalizedString","xs:token","xs:anyURI" -> "string";
			case "xs:boolean" -> "boolean";
			case "xs:byte","xs:short","xs:int","xs:integer","xs:long","xs:unsignedByte","xs:unsignedShort","xs:unsignedInt","xs:unsignedLong" -> "integer";
			case "xs:float","xs:double","xs:decimal" -> "number";
			default -> null;
		};
	}

	/**
	 * Operation variables are Properties with a supported valueType, collections
	 * of uniquely named variables, or lists whose members all have the declared
	 * list type (the first member, or valueTypeListElement, is the member shape).
	 */
	private static boolean callerElementSupported(SubmodelElement element,boolean named) {
		if(named && (element.getIdShort()==null || element.getIdShort().isBlank()))return false;
		if(element instanceof Property property)return schemaType(Aas.xsd(property.getValueType()))!=null;
		if(element instanceof SubmodelElementCollection collection) {
			Set<String> names=new HashSet<>();
			for(SubmodelElement child:Aas.nonNull(collection.getValue()))
				if(!callerElementSupported(child,true) || !names.add(child.getIdShort()))return false;
			return true;
		}
		if(element instanceof SubmodelElementList list) {
			SubmodelElement item=OperationVariableValues.itemTemplate(list);
			if(item==null)return false;
			List<SubmodelElement> members=new ArrayList<>(Aas.nonNull(list.getValue()));
			if(members.isEmpty())members.add(item);
			for(SubmodelElement member:members)
				if(!listMemberMatches(list,member) || !callerElementSupported(member,false))return false;
			return true;
		}
		return false;
	}

	/** AASd-108/109: a member has the list's declared element type and, for Properties, value type. */
	private static boolean listMemberMatches(SubmodelElementList list,SubmodelElement member) {
		AasSubmodelElements declared=list.getTypeValueListElement();
		boolean kind=declared==null || switch(declared) {
			case PROPERTY -> member instanceof Property;
			case SUBMODEL_ELEMENT_COLLECTION -> member instanceof SubmodelElementCollection;
			case SUBMODEL_ELEMENT_LIST -> member instanceof SubmodelElementList;
			default -> false;
		};
		return kind && (!(member instanceof Property property) || list.getValueTypeListElement()==null
				|| list.getValueTypeListElement()==property.getValueType());
	}

	private static boolean hasIssue(List<Issue> issues, MappingPlan mapping) {
		return issues.stream().anyMatch(issue -> issue.mappingId().equals(mapping.identity()));
	}

	/**
	 * The HTTP method of a form for a mapping role: {@code htv_methodName} (mandatory in AID 1.2), else
	 * the WoT HTTP binding default (GET to read, PUT to write, POST to invoke). A read is a GET poll.
	 * Null after an issue.
	 */
	private static String httpMethod(Form form, String role, MappingPlan mapping, String path, List<Issue> issues) {
		String fallback=switch(role) { case "read" -> "GET"; case "write" -> "PUT"; default -> "POST"; };
		String method=form.httpMethod().present() && form.httpMethod().value()!=null
				? form.httpMethod().value().trim().toUpperCase(java.util.Locale.ROOT) : fallback;
		Set<String> allowed=switch(role) {
			case "read" -> Set.of("GET");
			case "write" -> Set.of("PUT","POST","PATCH");
			default -> Set.of("POST","PUT","PATCH","GET","DELETE");
		};
		if(!allowed.contains(method)) {
			issue(issues,mapping,path+"/forms/htv_methodName","HTTP "+method+" cannot "+role+" (expected one of "+new java.util.TreeSet<>(allowed)+")");
			return null;
		}
		if (form.contentType().present() && form.contentType().value()!=null
				&& !"application/json".equalsIgnoreCase(form.contentType().value())) {
			issue(issues,mapping,path+"/forms/contentType","HTTP connector only binds JSON request/response bodies");
			return null;
		}
		if(!form.mqttTerms().isEmpty() || form.responseTopic().present()) {
			issue(issues,mapping,path+"/forms","HTTP form contains MQTT-only binding terms");
			return null;
		}
		return method;
	}

	private static boolean selectHttpForm(AidTarget target, MappingPlan mapping, String path, List<Issue> issues) {
		List<WireModel.ProtocolFamily> families = target.resolved().owner().protocolFamilies();
		if (families.size() != 1 || (families.get(0) != WireModel.ProtocolFamily.HTTP
				&& families.get(0) != WireModel.ProtocolFamily.IOLINK)) {
			issue(issues, mapping, path, "unsupported or ambiguous protocol family " + families);
			return false;
		}
		if (target.resolved().affordance().forms().size() != 1) {
			issue(issues, mapping, path + "/forms", "expected exactly one authored form; missing or ambiguous forms are rejected");
			return false;
		}
		return true;
	}

	private static Form selectedForm(AidTarget target) { return target.resolved().affordance().forms().get(0); }

	/** An SNMP object as the AID 1.2 SNMP binding (Annex E.1) addresses it. */
	private record SnmpForm(String host, int port, String oid, String version, String community) { }

	private static DataSourceConfiguration snmpSource(EndpointPlan endpoint, MappingPlan mapping, int position,
			AidTarget target, List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		SnmpForm form=snmpForm(target,true,mapping,path,issues);
		if(form==null)return null;
		// SNMP has no observation in the binding: a source is read (GET) at its polling interval.
		SnmpDataSourceConfiguration source=new SnmpDataSourceConfiguration(entityId(mapping,"source",position),
				form.host(),form.port(),form.oid(),form.version(),form.community());
		source.setSourceId(endpoint.id());
		source.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(source,target);
		if(endpoint.pollingInterval().present()) {
			OptionalLong interval=endpointPollingMillis(endpoint.pollingInterval(),mapping,position,issues);
			if(interval.isPresent())source.setPollingIntervalMs(interval.getAsLong());
		}
		return hasIssue(issues,mapping)?null:source;
	}

	private static DataSinkConfiguration snmpSink(EndpointPlan endpoint, MappingPlan mapping, int position,
			AidTarget target, List<Issue> issues) {
		String path=endpointPath(mapping,"Sinks",position);
		SnmpForm form=snmpForm(target,false,mapping,path,issues);
		if(form==null)return null;
		SnmpDataSinkConfiguration sink=new SnmpDataSinkConfiguration(entityId(mapping,"sink",position),
				form.host(),form.port(),form.oid(),form.version(),form.community());
		sink.setSinkId(endpoint.id());
		sink.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(sink,target);
		return hasIssue(issues,mapping)?null:sink;
	}

	/**
	 * The resolved form {@code snmp://{address}:{port}/{oid}} (port 161 by default), {@code snmpv_pduType}
	 * (GetRequestPDU to read, SetRequestPDU to write; the binding's defaults) and {@code snmpv_version}
	 * (SNMPv1 or SNMPv2c, default SNMPv2c). The community is the URI's user information, else {@code public}.
	 */
	private static SnmpForm snmpForm(AidTarget target, boolean read, MappingPlan mapping, String path, List<Issue> issues) {
		if(target.resolved().affordance().forms().size()!=1) {
			issue(issues,mapping,path+"/forms","expected exactly one authored form; missing or ambiguous forms are rejected");return null;
		}
		Form form=selectedForm(target);
		URI uri=formUri(target,form,mapping,path,issues);
		if(uri==null)return null;
		String oid=uri.getPath()==null?"":uri.getPath().replaceFirst("^/","");
		if(!"snmp".equalsIgnoreCase(uri.getScheme()) || uri.getHost()==null || !oid.matches("\\d+(\\.\\d+)+")) {
			issue(issues,mapping,path+"/forms/href","an SNMP form resolves to snmp://{address}:{port}/{oid}");return null;
		}
		String pdu=snmpTerm(form,"snmpv_pduType");
		String expected=read?"GetRequestPDU":"SetRequestPDU";
		if(pdu!=null && !expected.equalsIgnoreCase(pdu)) {
			issue(issues,mapping,path+"/forms/snmpv_pduType","snmpv_pduType "+pdu+" does not serve this mapping role (expected "+expected+")");return null;
		}
		String version=snmpTerm(form,"snmpv_version");
		if(version==null)version="SNMPv2c";
		if(!List.of("snmpv1","snmpv2c").contains(version.toLowerCase(java.util.Locale.ROOT))) {
			issue(issues,mapping,path+"/forms/snmpv_version","SNMP version "+version+" is not supported (SNMPv1, SNMPv2c)");return null;
		}
		return new SnmpForm(uri.getHost(),uri.getPort()>0?uri.getPort():161,oid,version,
				uri.getUserInfo()==null || uri.getUserInfo().isBlank()?null:uri.getUserInfo());
	}

	/** An SNMP form term, without the {@code snmpv:} prefix a value may carry. */
	private static String snmpTerm(Form form, String name) {
		String value=form.bindingTerms().get(name);
		return value==null || value.isBlank()?null:value.trim().replaceFirst("^snmpv:","");
	}

	/** Headers the connector sets itself: authoring them would break the request's framing or content. */
	private static final Set<String> MANAGED_HTTP_HEADERS=Set.of("content-type","content-length","host","transfer-encoding",
			"connection","expect","upgrade","te","trailer","keep-alive","proxy-connection");
	private static final java.util.regex.Pattern HTTP_TOKEN=java.util.regex.Pattern.compile("[!#$%&'*+.^_`|~0-9A-Za-z-]+");

	/**
	 * The form's static {@code htv_headers} (WoT HTTP binding; AID: a list of {@code htv_fieldName}/{@code htv_fieldValue}
	 * collections), sent with every request of the form. Names must be HTTP tokens and unique; values one line.
	 * Headers the connector manages are rejected, so is {@code Accept} on an observed form (the subprotocol needs
	 * its own) and {@code Authorization} next to credentials in the href. Empty when absent; null after an issue.
	 */
	/**
	 * What the form's security selection makes the connector send to {@code target}: the schemes from the AID,
	 * the credentials from the DMP's deployment (AidSecurity, PLAN H4). Null after an issue. Credentials in the
	 * URI's user information are refused: they would be secrets in the AAS.
	 */
	private static AidSecurity.Applied security(AidTarget target,Form form,URI uri,RuntimeSettings runtime,
			MappingPlan mapping,String path,List<Issue> issues) {
		if(uri.getRawUserInfo()!=null) {
			issue(issues,mapping,path+"/forms/href","credentials do not belong in the AID: the DMP holds them (DMP_CREDENTIALS_FILE), "
					+ "the AID names the scheme in securityDefinitions");
			return null;
		}
		WireModel.ProtocolFamily family=target.resolved().owner().protocolFamilies().get(0);
		List<AidSecurity.Problem> problems=new ArrayList<>();
		List<AidSecurity.Scheme> required=AidSecurity.required(target.resolved().owner(),form,target.resolved().submodelId(),family,problems);
		AidSecurity.Applied applied=required==null?null:AidSecurity.apply(required,uri,
				runtime==null?DmpCredentials.none():runtime.credentials(),problems);
		for(AidSecurity.Problem problem:problems)issue(issues,mapping,path+problem.path(),problem.message());
		return problems.isEmpty()?applied:null;
	}

	/** The form's static headers with the secret ones of its security scheme; null after an issue. */
	private static Map<String,String> httpHeaders(Form form,boolean observed,AidSecurity.Applied security,MappingPlan mapping,
			String path,List<Issue> issues) {
		boolean authorization=security.basic() || security.headers().keySet().stream().anyMatch("authorization"::equalsIgnoreCase);
		Map<String,String> headers=httpHeaders(form,observed,authorization,mapping,path,issues);
		if(headers==null)return null;
		for(var secret:security.headers().entrySet()) {
			if(headers.keySet().stream().anyMatch(secret.getKey()::equalsIgnoreCase)) {
				issue(issues,mapping,path+"/forms/htv_headers","HTTP header "+secret.getKey()+" is also set by the security scheme");return null;
			}
			headers.put(secret.getKey(),secret.getValue());
		}
		return headers;
	}


	private static Map<String,String> httpHeaders(Form form, boolean observed, boolean credentials, MappingPlan mapping,
			String path, List<Issue> issues) {
		SubmodelElement list=Aas.child(form.element(),"htv_headers");
		Map<String,String> headers=new LinkedHashMap<>();
		if(list==null || Aas.children(list).isEmpty())return headers;
		String at=path+"/forms/htv_headers";
		if(!(list instanceof org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementList)) {
			issue(issues,mapping,at,"htv_headers is a list of htv_fieldName/htv_fieldValue collections");return null;
		}
		Set<String> seen=new java.util.HashSet<>();
		for(SubmodelElement header:Aas.children(list)) {
			SubmodelElement nameElement=Aas.child(header,"htv_fieldName"), valueElement=Aas.child(header,"htv_fieldValue");
			String name=nameElement==null?null:Aas.value(nameElement), value=valueElement==null?null:Aas.value(valueElement);
			if(name==null || !HTTP_TOKEN.matcher(name.trim()).matches()) {
				issue(issues,mapping,at,"an HTTP header needs a valid htv_fieldName, not "+name);return null;
			}
			name=name.trim();
			String key=name.toLowerCase(java.util.Locale.ROOT);
			if(value==null || value.indexOf('\r')>=0 || value.indexOf('\n')>=0) {
				issue(issues,mapping,at,"HTTP header "+name+" needs a one-line htv_fieldValue");return null;
			}
			if(MANAGED_HTTP_HEADERS.contains(key)) { issue(issues,mapping,at,"HTTP header "+name+" is set by the connector");return null; }
			if(observed && key.equals("accept")) { issue(issues,mapping,at,"an observed form's Accept header is set by its subprotocol");return null; }
			if(credentials && key.equals("authorization")) {
				issue(issues,mapping,at,"an Authorization header next to credentials in the href");return null;
			}
			if(!seen.add(key)) { issue(issues,mapping,at,"HTTP header "+name+" is declared twice");return null; }
			headers.put(name,value);
		}
		return headers;
	}

	/** The form's WoT {@code subprotocol} (longpoll, sse, websub), lower case; null when absent. */
	private static String httpSubprotocol(Form form) {
		SubmodelElement child=Aas.child(form.element(),"subprotocol");
		String value=child==null?null:Aas.value(child);
		return value==null || value.isBlank()?null:value.trim().toLowerCase(java.util.Locale.ROOT);
	}
	/** The form's {@code href} resolved against the interface's {@code base} (AID 1.2 Annex B, RFC 3986). */
	private static URI formUri(AidTarget target, Form form, MappingPlan mapping, String path, List<Issue> issues) {
		return formUri(target, form.href().present() ? form.href().value() : null, mapping, path, issues);
	}

	private static URI formUri(AidTarget target, String href, MappingPlan mapping, String path, List<Issue> issues) {
		WireModel.Presence<SubmodelElement> metadata=target.resolved().owner().endpointMetadata();
		String base=metadata.present() && metadata.value()!=null ? elementString(metadata.value(),"base") : null;
		if((href==null || href.isBlank()) && (base==null || base.isBlank())) {
			issue(issues, mapping, path + "/forms/href", "required endpoint href is absent"); return null;
		}
		try {
			return AidForms.resolve(base, href);
		} catch (IllegalArgumentException malformed) {
			issue(issues, mapping, path + "/forms/href", "href cannot be resolved against the endpoint base: " + malformed.getMessage());
			return null;
		}
	}

	private static boolean httpUri(URI uri, MappingPlan mapping, String path, List<Issue> issues) {
		if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
				|| uri.getHost() == null || uri.getHost().isBlank() || uri.getRawFragment()!=null) {
			issue(issues, mapping, path + "/forms/href", "HTTP binding requires an absolute http(s) URI with an endpoint host");
			return false;
		}
		return true;
	}

	private static String elementString(SubmodelElement parent,String idShort) {
		SubmodelElement child=Aas.child(parent,idShort);
		return child==null?null:Aas.value(child);
	}

	private record MqttConnection(String host,int port,String base,String username,String password) { }

	/** The broker of the interface's base, with the credentials the form's security scheme asks for. */
	private static MqttConnection mqttConnection(AidTarget target,Form form,RuntimeSettings runtime,MappingPlan mapping,
			String path,List<Issue> issues) {
		WireModel.Presence<SubmodelElement> metadata=target.resolved().owner().endpointMetadata();
		String base=metadata.present() && metadata.value()!=null ? elementString(metadata.value(),"base") : null;
		if(base==null || base.isBlank()) { issue(issues,mapping,path+"/EndpointMetadata/base","MQTT broker base URI is required"); return null; }
		URI uri;
		try { uri=URI.create(base); } catch(IllegalArgumentException malformed) {
			issue(issues,mapping,path+"/EndpointMetadata/base","invalid MQTT broker base URI"); return null;
		}
		if(!"mqtt".equalsIgnoreCase(uri.getScheme()) || uri.getHost()==null || uri.getPort()<1
				|| uri.getRawQuery()!=null || uri.getRawFragment()!=null) {
			issue(issues,mapping,path+"/EndpointMetadata/base","MQTT requires an mqtt://host:port base without query or fragment");
			return null;
		}
		AidSecurity.Applied security=security(target,form,uri,runtime,mapping,path+"/EndpointMetadata/base",issues);
		if(security==null)return null;
		return new MqttConnection(uri.getHost(),uri.getPort(),base,security.username(),security.password());
	}

	private static String mqttTopic(MqttConnection base,Form form,MappingPlan mapping,String path,List<Issue> issues) {
		String href=form.href().present()?form.href().value():null;
		return mqttTopic(base,href,mapping,path,issues);
	}

	/**
	 * The MQTT topic of an {@code href} (AID 1.2 Annex B.2): the href resolved against the base, its
	 * path without the leading {@code /}, percent-decoded. A topic with a leading {@code /} is written
	 * {@code .//topic}, a {@code #} wildcard {@code %23}.
	 */
	private static String mqttTopic(MqttConnection base,String href,MappingPlan mapping,String path,List<Issue> issues) {
		if(href==null || href.isBlank()) {
			issue(issues,mapping,path+"/forms/href","MQTT topic href is required"); return null;
		}
		URI resolved;
		try { resolved=AidForms.resolve(base.base(),href); } catch(IllegalArgumentException malformed) {
			issue(issues,mapping,path+"/forms/href","invalid MQTT topic href"); return null;
		}
		if(!"mqtt".equalsIgnoreCase(resolved.getScheme()) || resolved.getRawQuery()!=null || resolved.getRawFragment()!=null) {
			issue(issues,mapping,path+"/forms/href","an MQTT href is a topic on the interface's broker"); return null;
		}
		String raw=resolved.getRawPath()==null?"":resolved.getRawPath();
		String topic=AidForms.percentDecode(raw.startsWith("/")?raw.substring(1):raw);
		if(topic.isEmpty()) { issue(issues,mapping,path+"/forms/href","MQTT topic must not be empty"); return null; }
		return topic;
	}

	private static Form mqttForm(AidTarget target,MappingPlan mapping,String path,String leg,List<Issue> issues) {
		return mqttForm(target,mapping,path,leg,false,issues);
	}
	private static Form mqttForm(AidTarget target,MappingPlan mapping,String path,String leg,boolean bridge,List<Issue> issues) {
		List<WireModel.ProtocolFamily> families=target.resolved().owner().protocolFamilies();
		if(!families.equals(List.of(WireModel.ProtocolFamily.MQTT))) {
			issue(issues,mapping,path,"MQTT requires a single unambiguous MQTT binding, found "+families); return null;
		}
		List<Form> forms=target.resolved().affordance().forms();
		if(forms.size()!=1) { issue(issues,mapping,path+"/forms","MQTT connector requires exactly one form"); return null; }
		return forms.get(0);
	}

	private static boolean hasMqttReplyTerms(Form form) {
		return form.responseTopic().present();
	}

	private static String mqttTerm(Form form,String name) {
		return form.mqttTerms().get(name);
	}

	/** Without {@code mqv_qos}, the WoT MQTT binding's default QoS 0 (decided 2026-10-24); camel-paho's is 2. */
	static final int MQTT_DEFAULT_QOS=0;

	private static Integer mqttQos(Form form,MappingPlan mapping,String path,List<Issue> issues) {
		String value=mqttTerm(form,"mqv_qos");
		if(value==null)return null;
		try { int qos=Integer.parseInt(value); if(qos>=0 && qos<=2)return qos; } catch(NumberFormatException ignored) { }
		issue(issues,mapping,path+"/forms/mqv_qos","MQTT QoS must be between 0 and 2");
		return null;
	}

	private static Boolean mqttRetain(Form form,MappingPlan mapping,String path,List<Issue> issues) {
		String value=mqttTerm(form,"mqv_retain"); if(value==null)return null;
		if("true".equalsIgnoreCase(value) || "1".equals(value))return true;
		if("false".equalsIgnoreCase(value) || "0".equals(value))return false;
		issue(issues,mapping,path+"/forms/mqv_retain","MQTT retain must be true, false, 1 or 0"); return null;
	}

	private static String stableMqttClientId(Plan plan,String routeId,MappingPlan mapping,String endpoint,String leg) {
		String aimc=plan.aimcSubmodelId()==null?routeId:plan.aimcSubmodelId();
		String identity=routeId+"\u0000"+aimc+"\u0000"+mapping.identity()+"\u0000"+leg+"\u0000"+endpoint;
		try { return "dmp-"+Base64.getUrlEncoder().withoutPadding().encodeToString(
				java.security.MessageDigest.getInstance("SHA-256").digest(identity.getBytes(StandardCharsets.UTF_8))); }
		catch(java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 unavailable",impossible); }
	}

	private static DataSourceConfiguration mqttSource(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,Plan plan,String routeId,RuntimeSettings runtime,List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		Form form=mqttForm(target,mapping,path,"source",issues);
		if(form==null)return null;
		if(hasMqttReplyTerms(form)) { issue(issues,mapping,path+"/forms","MQTT request/reply sources require an Operation route"); return null; }
		MqttConnection connection=mqttConnection(target,form,runtime,mapping,path,issues);
		String topic=connection==null?null:mqttTopic(connection,form,mapping,path,issues);
		Integer qos=mqttQos(form,mapping,path,issues);
		if(connection==null || topic==null || hasIssue(issues,mapping))return null;
		MqttDataSourceConfiguration source=new MqttDataSourceConfiguration(entityId(mapping,"source",position),
				connection.host(),connection.port(),stableMqttClientId(plan,routeId,mapping,endpoint.id(),"source"),
				topic,connection.username()==null?null:new BasicCredential(connection.username(),connection.password()));
		source.setSourceId(endpoint.id()); source.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(source,target);
		source.setQos(qos==null?MQTT_DEFAULT_QOS:qos);
		if(endpoint.pollingInterval().present()) {
			OptionalLong interval=endpointPollingMillis(endpoint.pollingInterval(),mapping,position,issues);
			if(interval.isPresent()) { source.setDelivery("sync"); source.setPollingIntervalMs(interval.getAsLong()); }
		}
		return hasIssue(issues,mapping)?null:source;
	}

	private static DataSinkConfiguration mqttSink(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,Plan plan,String routeId,RuntimeSettings runtime,List<Issue> issues) {
		String path=endpointPath(mapping,"Sinks",position);
		Form form=mqttForm(target,mapping,path,"sink",issues); if(form==null)return null;
		if("action".equalsIgnoreCase(target.resolved().affordance().kind())) { issue(issues,mapping,path,"MQTT InvokeAction needs ProtocolBridge command compilation"); return null; }
		if(hasMqttReplyTerms(form)) { issue(issues,mapping,path+"/forms","MQTT request/reply sink needs an Operation route"); return null; }
		MqttConnection connection=mqttConnection(target,form,runtime,mapping,path,issues);
		String topic=connection==null?null:mqttTopic(connection,form,mapping,path,issues);
		Integer qos=mqttQos(form,mapping,path,issues); Boolean retain=mqttRetain(form,mapping,path,issues);
		if(connection==null || topic==null || hasIssue(issues,mapping))return null;
		MqttDataSinkConfiguration sink=new MqttDataSinkConfiguration(entityId(mapping,"sink",position),connection.host(),
				connection.port(),topic,stableMqttClientId(plan,routeId,mapping,endpoint.id(),"sink"),
				connection.username()==null?"":connection.username(),connection.password()==null?"":connection.password());
		sink.setSinkId(endpoint.id()); sink.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(sink,target);
		sink.setQos(qos==null?MQTT_DEFAULT_QOS:qos); if(retain!=null)sink.setRetained(retain);
		return sink;
	}

	private record ProtocolBase(String host,int port,String servicePath,String username,String password,URI uri) { }

	private static DataSourceConfiguration opcuaSource(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,boolean readAtEvaluation,List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		if("event".equalsIgnoreCase(target.resolved().affordance().kind()))return opcuaEventSource(endpoint,mapping,position,target,issues);
		if(!"property".equalsIgnoreCase(target.resolved().affordance().kind())) {
			issue(issues,mapping,path,"OPC UA actions require a Method bridge");return null;
		}
		Form form=protocolForm(target,mapping,path,issues);if(form==null)return null;
		ProtocolBase base=protocolBase(target,"opc.tcp",mapping,path,issues);if(base==null)return null;
		String node=opcuaNode(form,mapping,path,issues);if(node==null)return null;
		Map<String,String> configuration=new LinkedHashMap<>();
		OptionalLong interval=endpoint.pollingInterval().present()?endpointPollingMillis(endpoint.pollingInterval(),mapping,position,issues):OptionalLong.empty();
		// observeproperty (OPC 10101 §6.4.1) is a monitored item: the server samples the variable at the
		// sampling interval and reports changes; PollingInterval sets both sampling and publishing interval.
		// A property that is observable is observed; otherwise it is read.
		boolean observed=!readAtEvaluation && AidForms.operations(target.resolved().affordance(),form).contains(AidForms.OBSERVE);
		if(interval.isPresent())configuration.put("requestedPublishingInterval",Long.toString(interval.getAsLong()));
		if(interval.isPresent() && observed)configuration.put("samplingInterval",Long.toString(interval.getAsLong()));
		OpcuaDataSourceConfiguration source=new OpcuaDataSourceConfiguration(entityId(mapping,"source",position),
				base.host(),base.port(),base.servicePath(),node,base.username(),base.password(),configuration);
		source.setSourceId(endpoint.id()); source.setDelivery(observed?"async":"sync");
		source.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(source,target);
		if(interval.isPresent() && !observed) source.setPollingIntervalMs(interval.getAsLong());
		return hasIssue(issues,mapping)?null:source;
	}

	/** A field name of an event notification, and one qualified name of a browse path ({@code 2:Name}). */
	private static final java.util.regex.Pattern EVENT_FIELD_NAME=java.util.regex.Pattern.compile("[A-Za-z][A-Za-z0-9_-]*");
	private static final java.util.regex.Pattern EVENT_PATH_SEGMENT=java.util.regex.Pattern.compile("(?:\\d+:)?[^/:,=()]+");

	/**
	 * The events of an OPC UA notifier (DMP extension; OPC 10101 v1.00 has no event vocabulary):
	 * the form {@code href} names the notifier, each property of the {@code data} schema is one
	 * select clause (its {@code uav_eventField} browse path, or its own name in namespace 0), and
	 * the affordance-level {@code uav_eventType} is the where clause (OfType). PollingInterval sets
	 * the publishing interval. Delivered like an MQTT event: one field map per notification.
	 */
	private static DataSourceConfiguration opcuaEventSource(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		Form form=protocolForm(target,mapping,path,issues);if(form==null)return null;
		ProtocolBase base=protocolBase(target,"opc.tcp",mapping,path,issues);if(base==null)return null;
		String notifier=opcuaNode(form,mapping,path,issues);if(notifier==null)return null;
		Presence<WireModel.DataSchema> data=target.resolved().affordance().schema();
		if(!data.present() || data.value().properties().isEmpty()
				|| data.value().type().present() && !"object".equals(data.value().type().value())) {
			issue(issues,mapping,path+"/data","an OPC UA event needs an object data schema whose properties select the event fields");return null;
		}
		List<String> fields=new ArrayList<>();
		for(Map.Entry<String,WireModel.DataSchema> field:data.value().properties().entrySet()) {
			String name=field.getKey();
			String browsePath=Aas.children(field.getValue().element()).stream()
					.filter(child -> "uav_eventField".equals(child.getIdShort())).map(Aas::value).findFirst().orElse(name);
			boolean valid=EVENT_FIELD_NAME.matcher(name).matches() && browsePath!=null && !browsePath.isBlank()
					&& java.util.Arrays.stream(browsePath.replaceFirst("^/","").split("/",-1)).allMatch(segment -> EVENT_PATH_SEGMENT.matcher(segment).matches());
			if(!valid) { issue(issues,mapping,path+"/data/properties/"+name,"an event field needs a browse path of qualified names, e.g. 0:ToState/0:Id");return null; }
			fields.add(name+"="+browsePath.replaceFirst("^/",""));
		}
		String eventType=Aas.children(target.resolved().affordance().element()).stream()
				.filter(child -> "uav_eventType".equals(child.getIdShort())).map(Aas::value).findFirst().orElse(null);
		if(eventType!=null && (eventType.isBlank() || eventType.contains(")"))) {
			issue(issues,mapping,path+"/uav_eventType","uav_eventType must be an EventType NodeId");return null;
		}
		Map<String,String> configuration=new LinkedHashMap<>();
		configuration.put("eventFields","RAW("+String.join(",",fields)+")");
		if(eventType!=null)configuration.put("eventType","RAW("+eventType.trim()+")");
		OptionalLong interval=endpoint.pollingInterval().present()?endpointPollingMillis(endpoint.pollingInterval(),mapping,position,issues):OptionalLong.empty();
		if(interval.isPresent())configuration.put("requestedPublishingInterval",Long.toString(interval.getAsLong()));
		OpcuaDataSourceConfiguration source=new OpcuaDataSourceConfiguration(entityId(mapping,"source",position),
				base.host(),base.port(),base.servicePath(),notifier,base.username(),base.password(),configuration);
		source.setSourceId(endpoint.id()); source.setDelivery("async");
		source.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(source,target);
		return hasIssue(issues,mapping)?null:source;
	}

	/**
	 * The Object owning a UA Method: {@code uav_componentOf} on the action affordance (OPC 10101
	 * §6.5.4, §7.1.3), a NodeId or a list holding exactly one; a Call addresses one owner.
	 */
	private static String componentOf(AidTarget target,MappingPlan mapping,String path,List<Issue> issues) {
		SubmodelElement term=Aas.children(target.resolved().affordance().element()).stream()
				.filter(child -> "uav_componentOf".equals(child.getIdShort())).findFirst().orElse(null);
		List<String> owners=new ArrayList<>();
		if(term instanceof org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementList list) {
			for(SubmodelElement member:Aas.children(list))if(Aas.value(member)!=null)owners.add(Aas.value(member));
		} else if(term!=null && Aas.value(term)!=null) owners.add(Aas.value(term));
		if(owners.size()!=1 || owners.get(0).isBlank()) {
			issue(issues,mapping,path,"OPC UA Method needs uav_componentOf on the action naming its one owning Object");return null;
		}
		return owners.get(0);
	}

	private static DataSinkConfiguration opcuaSink(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,List<Issue> issues) {
		String path=endpointPath(mapping,"Sinks",position);
		if(!"property".equalsIgnoreCase(target.resolved().affordance().kind())) {
			issue(issues,mapping,path,"OPC UA Method invocation requires ProtocolBridge command compilation");return null;
		}
		Form form=protocolForm(target,mapping,path,issues);if(form==null)return null;
		ProtocolBase base=protocolBase(target,"opc.tcp",mapping,path,issues);if(base==null)return null;
		String node=opcuaNode(form,mapping,path,issues);if(node==null)return null;
		OpcuaDataSinkConfiguration sink=new OpcuaDataSinkConfiguration(entityId(mapping,"sink",position),base.host(),
				base.port(),base.servicePath(),node,base.username(),base.password(),new LinkedHashMap<>());
		sink.setSinkId(endpoint.id()); sink.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(sink,target);
		return sink;
	}

	private static DataSourceConfiguration plc4xSource(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		if(!"property".equalsIgnoreCase(target.resolved().affordance().kind())) {
			issue(issues,mapping,path,"Modbus sources must address a property endpoint");return null;
		}
		Form form=protocolForm(target,mapping,path,issues);if(form==null)return null;
		PlcTarget plc=plcTarget(target,form,false,mapping,path,issues);if(plc==null)return null;
		List<Tag> tags=List.of(new Tag(target.resolved().affordance().idShort().value(),plc.address()));
		Plc4xDataSourceConfiguration source=new Plc4xDataSourceConfiguration(entityId(mapping,"source",position),
				plc.base().host(),plc.base().port(),plc.driver(),plc.base().servicePath(),plc.options(),tags);
		source.setSourceId(endpoint.id()); source.setDelivery("sync"); source.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(source,target);
		// modv_pollingTime is the device's maximum polling rate: a lower limit for the AIMC's interval
		// (its own or the mapping's DefaultPollingInterval), never an interval of its own.
		OptionalLong interval=endpoint.pollingInterval().present()
				? endpointPollingMillis(endpoint.pollingInterval(),mapping,position,issues) : pollingMillis(mapping,new ArrayList<>());
		if(interval.isPresent())source.setPollingIntervalMs(plc.pollingMs()==null?interval.getAsLong():Math.max(interval.getAsLong(),plc.pollingMs()));
		return hasIssue(issues,mapping)?null:source;
	}

	private static DataSinkConfiguration plc4xSink(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,List<Issue> issues) {
		String path=endpointPath(mapping,"Sinks",position);
		if(!"property".equalsIgnoreCase(target.resolved().affordance().kind())) {
			issue(issues,mapping,path,"Modbus command invocation is not represented by the PLC4X tag-write connector");return null;
		}
		Plc4xDataSinkConfiguration sink=plc4xTagSink(target,entityId(mapping,"sink",position),
				target.resolved().affordance().idShort().value(),mapping,path,issues);
		if(sink==null)return null;
		sink.setSinkId(endpoint.id()); sink.setSchema(schemaForAidTarget(target,mapping,path,issues)); setValuePath(sink,target);
		return sink;
	}

	/** A PLC4X write of the one tag that the affordance's form addresses. */
	private static Plc4xDataSinkConfiguration plc4xTagSink(AidTarget target,String uniqueId,String tagName,
			MappingPlan mapping,String path,List<Issue> issues) {
		Form form=protocolForm(target,mapping,path,issues);if(form==null)return null;
		PlcTarget plc=plcTarget(target,form,true,mapping,path,issues);if(plc==null)return null;
		return new Plc4xDataSinkConfiguration(uniqueId,plc.base().host(),plc.base().port(),plc.driver(),plc.base().servicePath(),
				plc.options(),List.of(new Tag(tagName,plc.address())));
	}

	/** A PLC4X endpoint: base, driver, driver options, the tag address and a form-declared polling interval. */
	private record PlcTarget(ProtocolBase base,String driver,List<Option> options,String address,Long pollingMs) { }

	private static PlcTarget plcTarget(AidTarget target,Form form,boolean write,MappingPlan mapping,String path,List<Issue> issues) {
		if(target.resolved().owner().protocolFamilies().contains(WireModel.ProtocolFamily.BACNET)) {
			// PLC4X 0.13.1's bacnet-ip driver neither reads nor writes (it only listens for COV notifications).
			issue(issues,mapping,path,"BACnet is not supported: the PLC4X BACnet/IP driver neither reads nor writes");return null;
		}
		ProtocolBase base=protocolBase(target,"modbus+tcp",mapping,path,issues);if(base==null)return null;
		if(base.uri().getRawQuery()!=null) {
			issue(issues,mapping,path+"/EndpointMetadata/base","a Modbus base is modbus+tcp://host:port[/unitID/]");return null;
		}
		return modbusTarget(target,base,form,write,mapping,path,issues);
	}

	// ---- WoT Modbus binding (w3c/wot-binding-templates, bindings/protocols/modbus) → PLC4X tags ----

	/** The resolved path {@code /{unitID}/{address}} and query {@code quantity={n}} (AID 1.2 Annex B). */
	private static final java.util.regex.Pattern MODBUS_PATH=java.util.regex.Pattern.compile("/(\\d+)/(\\d+)");
	private static final java.util.regex.Pattern MODBUS_QUERY=java.util.regex.Pattern.compile("quantity=(\\d+)");
	/** modv:entity → PLC4X memory area. */
	private static final Map<String,String> MODBUS_ENTITIES=Map.of("coil","coil","discreteinput","discrete-input",
			"holdingregister","holding-register","inputregister","input-register");
	/** modv:function → PLC4X memory area; reads and writes per function. */
	private static final Map<String,String> MODBUS_READ_FUNCTIONS=Map.of("readcoil","coil","readcoils","coil",
			"readdiscreteinput","discrete-input","readdiscreteinputs","discrete-input","readholdingregisters","holding-register",
			"readinputregisters","input-register");
	private static final Map<String,String> MODBUS_WRITE_FUNCTIONS=Map.of("writesinglecoil","coil","writemultiplecoils","coil",
			"writesingleholdingregister","holding-register","writemultipleholdingregisters","holding-register");
	/** modv:type (xsd payload type) → PLC4X data type and the 16-bit registers one value takes. */
	private static final Map<String,Map.Entry<String,Integer>> MODBUS_TYPES=Map.ofEntries(
			Map.entry("boolean",Map.entry("BOOL",1)),Map.entry("byte",Map.entry("SINT",1)),Map.entry("unsignedbyte",Map.entry("USINT",1)),
			Map.entry("short",Map.entry("INT",1)),Map.entry("unsignedshort",Map.entry("UINT",1)),Map.entry("integer",Map.entry("INT",1)),
			Map.entry("int",Map.entry("DINT",2)),Map.entry("unsignedint",Map.entry("UDINT",2)),Map.entry("float",Map.entry("REAL",2)),
			Map.entry("long",Map.entry("LINT",4)),Map.entry("unsignedlong",Map.entry("ULINT",4)),Map.entry("double",Map.entry("LREAL",4)));

	/**
	 * A Modbus form as the WoT Modbus binding describes it, translated into a PLC4X tag: the memory
	 * area from {@code modv_entity}, else {@code modv_function}, else the binding's default per
	 * operation (readCoil, writeSingleCoil); the address from the href (PLC4X counts from 1, the
	 * binding uses the protocol address); {@code modv_type} and the quantity into the PLC4X data
	 * type and element count; the unit ID, byte and word order and timeout into driver options.
	 */
	private static PlcTarget modbusTarget(AidTarget target,ProtocolBase base,Form form,boolean write,MappingPlan mapping,String path,List<Issue> issues) {
		// The href resolved against the base: AID writes the unit ID in the base
		// (modbus+tcp://host:port/{unitID}/) and the address in the href ({address}?quantity={n}).
		String href=form.href().present()?form.href().value():null;
		URI resolved;
		try { resolved=href==null || href.isBlank()?null:AidForms.resolve(base.uri().toString(),href.trim()); }
		catch(IllegalArgumentException malformed) { resolved=null; }
		java.util.regex.Matcher parsed=resolved==null?null:MODBUS_PATH.matcher(resolved.getRawPath()==null?"":resolved.getRawPath());
		java.util.regex.Matcher quantityQuery=resolved==null || resolved.getRawQuery()==null?null:MODBUS_QUERY.matcher(resolved.getRawQuery());
		if(parsed==null || !parsed.matches() || quantityQuery!=null && !quantityQuery.matches()) {
			issue(issues,mapping,path+"/forms/href","a Modbus href resolves to /{unitID}/{address}?quantity={n} against the base");return null;
		}
		int unit=Integer.parseInt(parsed.group(1));
		int address=Integer.parseInt(parsed.group(2));
		int quantity=quantityQuery==null?1:Integer.parseInt(quantityQuery.group(1));
		// modv_zeroBasedAddressing=false: the href counts from 1, the protocol address is one less.
		String zeroBased=formTerm(form,"modv_zeroBasedAddressing");
		if(zeroBased!=null && !List.of("true","false").contains(zeroBased.trim().toLowerCase(java.util.Locale.ROOT))) {
			issue(issues,mapping,path+"/forms/modv_zeroBasedAddressing","modv_zeroBasedAddressing must be true or false");return null;
		}
		if(zeroBased!=null && "false".equalsIgnoreCase(zeroBased.trim())) {
			if(address<1) { issue(issues,mapping,path+"/forms/href","with one-based addressing the first address is 1");return null; }
			address--;
		}
		if(unit>255 || address>65535 || quantity<1) { issue(issues,mapping,path+"/forms/href","Modbus unit ID, address or quantity out of range");return null; }
		String entity=modbusTerm(form,"modv_entity"),function=modbusTerm(form,"modv_function");
		String area;
		if(entity!=null) {
			area=MODBUS_ENTITIES.get(entity);
			if(area==null) { issue(issues,mapping,path+"/forms/modv_entity","unknown Modbus entity "+formTerm(form,"modv_entity"));return null; }
		} else if(function!=null) {
			area=(write?MODBUS_WRITE_FUNCTIONS:MODBUS_READ_FUNCTIONS).get(function);
			if(area==null) { issue(issues,mapping,path+"/forms/modv_function","Modbus function "+formTerm(form,"modv_function")
					+" does not "+(write?"write":"read"));return null; }
		} else area="coil";
		if(write && List.of("discrete-input","input-register").contains(area)) {
			issue(issues,mapping,path+"/forms","Modbus discrete inputs and input registers are read-only");return null;
		}
		String type=modbusTerm(form,"modv_type");
		String plcType=null;int count=quantity;
		if(type!=null) {
			Map.Entry<String,Integer> mapped=MODBUS_TYPES.get(type.replaceFirst("^xsd:",""));
			if(mapped==null) { issue(issues,mapping,path+"/forms/modv_type","Modbus payload type "+formTerm(form,"modv_type")+" is not supported");return null; }
			boolean bits=List.of("coil","discrete-input").contains(area);
			if(bits!=mapped.getKey().equals("BOOL")) { issue(issues,mapping,path+"/forms/modv_type","coils and discrete inputs carry booleans, registers carry numbers");return null; }
			plcType=mapped.getKey();
			if(!bits) {
				if(quantity%mapped.getValue()!=0) { issue(issues,mapping,path+"/forms/href","quantity is not a multiple of the registers one "+type+" takes");return null; }
				count=quantity/mapped.getValue();
			}
		}
		List<Option> options=new ArrayList<>(List.of(new Option("default-unit-identifier",String.valueOf(unit))));
		// The interface's EndpointMetadata may set the byte and word order for all its forms; a form overrides it.
		SubmodelElement metadata=target.resolved().owner().endpointMetadata().present()?target.resolved().owner().endpointMetadata().value():null;
		String msb=formTerm(form,"modv_mostSignificantByte"),msw=formTerm(form,"modv_mostSignificantWord");
		if(msb==null && metadata!=null)msb=elementString(metadata,"modv_mostSignificantByte");
		if(msw==null && metadata!=null)msw=elementString(metadata,"modv_mostSignificantWord");
		if(msb!=null || msw!=null) {
			boolean byteFirst=msb==null || "true".equalsIgnoreCase(msb), wordFirst=msw==null || "true".equalsIgnoreCase(msw);
			options.add(new Option("default-payload-byte-order",wordFirst?(byteFirst?"BIG_ENDIAN":"BIG_ENDIAN_BYTE_SWAP")
					:(byteFirst?"LITTLE_ENDIAN_BYTE_SWAP":"LITTLE_ENDIAN")));
		}
		Long timeout=modbusMillis(form,"modv_timeout",mapping,path,issues);
		if(timeout!=null)options.add(new Option("request-timeout",String.valueOf(timeout)));
		Long polling=modbusMillis(form,"modv_pollingTime",mapping,path,issues);
		if(hasIssue(issues,mapping))return null;
		String tag=area+":"+(address+1)+(plcType==null?"":":"+plcType)+(count>1?"["+count+"]":"");
		return new PlcTarget(base,"modbus-tcp",List.copyOf(options),tag,polling);
	}

	/** A Modbus vocabulary value, lower-cased and without a {@code modv:} prefix; {@code null} when absent. */
	private static String modbusTerm(Form form,String name) {
		String value=formTerm(form,name);
		return value==null || value.isBlank()?null:value.trim().replaceFirst("^modv:","").toLowerCase(java.util.Locale.ROOT);
	}

	private static Long modbusMillis(Form form,String name,MappingPlan mapping,String path,List<Issue> issues) {
		String value=formTerm(form,name);
		if(value==null || value.isBlank())return null;
		try { long millis=Long.parseLong(value.trim()); if(millis>0)return millis; } catch(NumberFormatException ignored) { }
		issue(issues,mapping,path+"/forms/"+name,name+" must be a positive number of milliseconds");return null;
	}

	private static ProtocolBase protocolBase(AidTarget target,String requiredScheme,MappingPlan mapping,String path,
			List<Issue> issues) {
		WireModel.Presence<SubmodelElement> metadata=target.resolved().owner().endpointMetadata();
		String base=metadata.present() && metadata.value()!=null?elementString(metadata.value(),"base"):null;
		if(base==null || base.isBlank()) { issue(issues,mapping,path+"/EndpointMetadata/base","endpoint base URI is required");return null; }
		try {
			URI uri=URI.create(base);
			if(!requiredScheme.equalsIgnoreCase(uri.getScheme()) || uri.getHost()==null || uri.getPort()<1) {
				issue(issues,mapping,path+"/EndpointMetadata/base","unsupported endpoint scheme or missing host/port");return null;
			}
			if(uri.getRawFragment()!=null || "opc.tcp".equals(requiredScheme) && uri.getRawQuery()!=null) {
				issue(issues,mapping,path+"/EndpointMetadata/base","endpoint query/fragment options are not represented by this connector");return null;
			}
			if(uri.getRawUserInfo()!=null) {
				// OPC UA users come with opcua_authentication_sc, from the DMP's credentials (PLAN H4, not applied yet).
				issue(issues,mapping,path+"/EndpointMetadata/base","credentials do not belong in the AID: the DMP holds them, "
						+ "the AID names the scheme in securityDefinitions");return null;
			}
			return new ProtocolBase(uri.getHost(),uri.getPort(),uri.getPath()==null?"":uri.getPath().replaceAll("^/+|/+$",""),null,null,uri);
		} catch(IllegalArgumentException malformed) {
			issue(issues,mapping,path+"/EndpointMetadata/base","endpoint base URI is malformed");return null;
		}
	}

	private static Form protocolForm(AidTarget target,MappingPlan mapping,String path,List<Issue> issues) {
		List<Form> forms=target.resolved().affordance().forms();
		if(forms.size()!=1) { issue(issues,mapping,path+"/forms","expected exactly one authored protocol form");return null; }
		return forms.get(0);
	}

	/**
	 * The node an OPC UA form names: the NodeId of its {@code href} ({@code /?id=<nodeId>}, OPC 10101 and AID
	 * 1.2 Annex B, {@code #} and {@code &} percent-encoded), else its {@code uav_browsePath}, an absolute path
	 * from the Root node that the connector resolves (TranslateBrowsePathsToNodeIds). AID makes {@code href}
	 * mandatory, so a form naming its node by browse path carries an href without an id (e.g. {@code /}).
	 */
	private static String opcuaNode(Form form,MappingPlan mapping,String path,List<Issue> issues) {
		String href=form.href().present() && form.href().value()!=null?form.href().value().trim():"";
		int id=href.toLowerCase(java.util.Locale.ROOT).indexOf("?id=");
		String nodeId=id>=0?href.substring(id+4).replace("%23","#").replace("%26","&")
				:!href.isEmpty() && !href.startsWith("/")?href:null;
		if(nodeId!=null && !nodeId.isBlank())return nodeId;
		String browse=formTerm(form,"uav_browsePath");
		if(browse!=null && !browse.isBlank()) {
			String trimmed=browse.trim();
			if(!trimmed.startsWith("/") || trimmed.length()<2 || trimmed.contains("//")) {
				issue(issues,mapping,path+"/forms/uav_browsePath","uav_browsePath is an absolute path from the Root node, e.g. /Root/Objects/2:Machine");return null;
			}
			return trimmed;
		}
		issue(issues,mapping,path+"/forms/href","an OPC UA form names its node by href /?id=<NodeId> or by uav_browsePath");return null;
	}

	private static String formTerm(Form form,String name) {
		return form.bindingTerms().get(name);
	}

	private static void setValuePath(DataSourceConfiguration configuration,AidTarget target) {
		String pointer=jsonPointer(target.resolved().schemaPath()); if(pointer!=null)configuration.setValuePath(pointer);
	}
	private static void setValuePath(DataSinkConfiguration configuration,AidTarget target) {
		String pointer=jsonPointer(target.resolved().schemaPath()); if(pointer!=null)configuration.setValuePath(pointer);
	}
	private static String jsonPointer(List<String> tokens) {
		if(tokens.isEmpty())return null;
		if(tokens.stream().anyMatch(token -> token.isEmpty() || token.chars().anyMatch(c -> ".[]/ ~".indexOf(c)>=0)))
			return "/"+tokens.stream().map(token -> token.replace("~","~0").replace("/","~1")).collect(java.util.stream.Collectors.joining("/"));
		return "$."+String.join(".",tokens);
	}
	private static int effectivePort(URI uri) {
		return uri.getPort() >= 0 ? uri.getPort() : ("https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80);
	}
	private static OptionalLong pollingMillis(MappingPlan mapping, List<Issue> issues) {
		if (!mapping.defaultPollingInterval().present()) return OptionalLong.empty();
		Object value = mapping.defaultPollingInterval().value();
		double seconds;
		try { seconds = value instanceof Number number ? number.doubleValue() : Double.parseDouble(String.valueOf(value)); }
		catch (RuntimeException invalid) {
			issue(issues, mapping, "MappingConfigurations/value/" + mapping.listIndex() + "/DefaultPollingInterval",
					"expected a numeric interval in authored seconds");
			return OptionalLong.empty();
		}
		if (seconds == 0) return OptionalLong.empty();
		double millis = seconds * 1000d;
		if (!Double.isFinite(seconds) || seconds < 0 || millis > Long.MAX_VALUE || millis < 1) {
			issue(issues, mapping, "MappingConfigurations/value/" + mapping.listIndex() + "/DefaultPollingInterval",
					"poll interval is outside the runtime millisecond range");
			return OptionalLong.empty();
		}
		return OptionalLong.of((long) millis);
	}
	private static Object schema(WireModel.Presence<DataSchema> presence) {
		return presence.present() && presence.value() != null ? schema(presence.value()) : null;
	}

	private static Object schemaOptional(DataSchema schema) { return schema == null ? null : schema(schema); }

	private static Object schemaForAidTarget(AidTarget target,MappingPlan mapping,String path,List<Issue> issues) {
		Object base=schema(target.resolved().schema());
		Map<String,Object> result=new LinkedHashMap<>();
		if(base instanceof Map<?,?> map)map.forEach((key,value) -> result.put(String.class.cast(key),value));
		// required is a data schema element (jsonschema:required), part of the schema above;
		// an affordance qualifier with behaviour has no meaning the DMP could honour.
		for(Qualifier qualifier:Aas.nonNull(target.resolved().affordance().element().getQualifiers()))
			if(behaviorQualifier(qualifier))
				issue(issues,mapping,path+"/qualifiers/"+qualifier.getType(),"unsupported AID qualifier semantics");
		return result.isEmpty()?null:java.util.Collections.unmodifiableMap(result);
	}
	private static boolean behaviorQualifier(Qualifier qualifier) {
		return qualifier.getKind()!=QualifierKind.TEMPLATE_QUALIFIER;
	}
	private static Map<String, Object> schema(DataSchema schema) {
		Map<String, Object> out = new LinkedHashMap<>();
		put(out, "type", schema.type()); put(out, "valueType", schema.valueType());
		if(!schema.type().present() && schema.valueType().value()!=null) {
			String type=schemaType(schema.valueType().value());
			if(type==null)throw new IllegalArgumentException("unsupported typed schema Property valueType");
			out.put("type",type);
		}
		put(out, "contentType", schema.contentType()); put(out, "valueFormat", schema.valueFormat());
		if (!schema.properties().isEmpty()) {
			Map<String, Object> properties = new LinkedHashMap<>();
			schema.properties().forEach((key, value) -> properties.put(key, schema(value)));
			out.put("properties", properties);
		}
		if (schema.items().present() && schema.items().value() != null) out.put("items", schema(schema.items().value()));
		if (schema.required().present()) out.put("required", schema.required().value());
		if (!schema.oneOf().isEmpty()) out.put("oneOf", schema.oneOf().stream().map(ConnectorConfigurationFactory::schema).toList());
		out.putAll(schema.constraints());
		return out;
	}
	private static void put(Map<String, Object> map, String key, WireModel.Presence<String> value) {
		if (value.present()) map.put(key, value.value());
	}
	/**
	 * Response sinks compile as sinks at {@code RESPONSE_SINK_POSITION + i}, which keeps their entity ids apart
	 * from the sinks'; their path is {@code ResponseSinks/value/i}.
	 */
	private static final int RESPONSE_SINK_POSITION = 100000;

	private static String endpointPath(MappingPlan mapping, String collection, int position) {
		if ("Sinks".equals(collection) && position >= RESPONSE_SINK_POSITION) {
			collection = "ResponseSinks";
			position -= RESPONSE_SINK_POSITION;
		}
		return "MappingConfigurations/value/" + mapping.listIndex() + "/" + collection + "/value/" + position;
	}
	private static String entityId(MappingPlan mapping, String kind, int position) {
		try {
			return "managed:"+java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(
					(mapping.identity()+"\u0000"+kind+"\u0000"+position).getBytes(StandardCharsets.UTF_8)));
		} catch(java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
	}
	private static void issue(List<Issue> issues, MappingPlan mapping, String path, String message) {
		issues.add(new Issue(path, mapping.identity(), message));
	}
}
