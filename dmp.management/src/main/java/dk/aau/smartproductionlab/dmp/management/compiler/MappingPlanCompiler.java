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
package dk.aau.smartproductionlab.dmp.management.compiler;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.LinkedHashSet;

import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.ReferenceTypes;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultReference;
import org.eclipse.digitaltwin.aas4j.v3.model.Qualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.QualifierKind;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.ReferenceElement;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.management.model.TimeSeriesProjection;
import dk.aau.smartproductionlab.dmp.management.model.WireModel;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;

/** Resolves immutable repository inputs into an ordered, connector-neutral mapping plan. */
public final class MappingPlanCompiler {
	public sealed interface Target permits DirectTarget, AidTarget, SeriesTarget { }
	public record DirectTarget(AasReferenceResolver.ResolvedElement resolved) implements Target { }
	public record AidTarget(AasReferenceResolver.AidTarget resolved) implements Target { }
	/** A TimeSeries LinkedSegment; its series container (at {@code seriesPath}) holds the Record, the row shape. */
	public record SeriesTarget(AasReferenceResolver.ResolvedElement segment, SubmodelElement series, String seriesPath,
			TimeSeriesProjection.Series projection) implements Target { }
	/** An endpoint with its target; a Source also with its {@code PollingInterval} and {@code Evaluation/Mode}. */
	public record EndpointPlan(String id, Reference reference, Target target,
			WireModel.Presence<String> pollingInterval, WireModel.Presence<String> evaluation) {
		public EndpointPlan(String id, Reference reference, Target target) {
			this(id, reference, target, WireModel.Presence.absent());
		}
		public EndpointPlan(String id, Reference reference, Target target, WireModel.Presence<String> pollingInterval) {
			this(id, reference, target, pollingInterval, WireModel.Presence.absent());
		}
		@Override public String toString() { return "EndpointPlan[id="+id+"]"; }
	}
	public record MappingPlan(String identity, int listIndex, List<EndpointPlan> sources,
			List<EndpointPlan> sinks, WireModel.Presence<String> defaultPollingInterval,
			WireModel.Presence<WireModel.Blob> transformation,
			WireModel.Presence<List<EndpointPlan>> responseSources,
			WireModel.Presence<List<EndpointPlan>> responseSinks,
			WireModel.Presence<WireModel.Blob> responseTransformation, List<Qualifier> qualifiers) {
		public MappingPlan {
			sources=List.copyOf(sources); sinks=List.copyOf(sinks); qualifiers=List.copyOf(qualifiers);
			if(responseSources.present() && responseSources.value()!=null) responseSources=WireModel.Presence.of(List.copyOf(responseSources.value()));
			if(responseSinks.present() && responseSinks.value()!=null) responseSinks=WireModel.Presence.of(List.copyOf(responseSinks.value()));
		}
		@Override public String toString() { return "MappingPlan[identity="+identity+", listIndex="+listIndex+"]"; }
	}
	public record Plan(String aimcSubmodelId, List<MappingPlan> mappings) {
		public Plan(List<MappingPlan> mappings) { this(null, mappings); }
		public Plan { mappings=List.copyOf(mappings); }
	}
	/**
	 * A submodel whose change can change the mapping; with a {@code path}, only that element subtree of it. Of a
	 * {@code submodel} dependency only the structure counts; of a {@code series} (a TimeSeries container) the
	 * values, too.
	 */
	public record Dependency(String kind, String id, String path) {
		public Dependency(String kind, String id) { this(kind, id, null); }
	}
	/**
	 * {@code failurePaths} names, per failure, the endpoint it concerns relative to the mapping
	 * ({@code Sources/value/0}); empty when unknown.
	 */
	public record DesiredMapping(String identity, int listIndex, MappingPlan plan,
			List<AasRepositoryClient.Result<?>> failures, Set<Dependency> dependencies,
			WireModel.Mapping input, WireModel.MappingFailure projectionFailure, List<String> failurePaths) {
		public DesiredMapping(String identity, int listIndex, MappingPlan plan,
				List<AasRepositoryClient.Result<?>> failures, Set<Dependency> dependencies) {
			this(identity, listIndex, plan, failures, dependencies, null, null);
		}
		public DesiredMapping(String identity, int listIndex, MappingPlan plan,
				List<AasRepositoryClient.Result<?>> failures, Set<Dependency> dependencies,
				WireModel.Mapping input, WireModel.MappingFailure projectionFailure) {
			this(identity, listIndex, plan, failures, dependencies, input, projectionFailure, List.of());
		}
		public DesiredMapping { failures=List.copyOf(failures); dependencies=Set.copyOf(dependencies); failurePaths=List.copyOf(failurePaths); }
		/** The path of failure {@code index}, or {@code ""}. */
		public String failurePath(int index) { return index < failurePaths.size() ? failurePaths.get(index) : ""; }
		public boolean valid() { return plan!=null && failures.isEmpty(); }
		@Override public String toString() { return "DesiredMapping[identity="+identity+", valid="+valid()+"]"; }
	}
	public record DesiredPlan(String aimcSubmodelId, List<DesiredMapping> mappings) {
		public DesiredPlan { mappings=List.copyOf(mappings); }
		public Plan validPlan() { return new Plan(aimcSubmodelId,mappings.stream()
				.filter(DesiredMapping::valid).map(DesiredMapping::plan).toList()); }
	}

	private final AasReferenceResolver references;

	public MappingPlanCompiler(AasReferenceResolver references) { this.references=references; }

	public AasRepositoryClient.Result<Plan> compile(WireModel.Aimc aimc) {
		DesiredPlan desired=compileDesired(aimc);
		for(DesiredMapping mapping:desired.mappings()) if(!mapping.valid())return propagate(mapping.failures().get(0));
		return found(desired.validPlan());
	}

	/** Every endpoint reference of an AIMC, for grouped prefetching before compilation. */
	public static List<Reference> endpointReferences(WireModel.Aimc aimc) {
		List<Reference> references=new ArrayList<>();
		for(WireModel.Mapping mapping:aimc.mappings())
			for(WireModel.Endpoint endpoint:endpoints(mapping))references.add(endpoint.reference());
		return List.copyOf(references);
	}

	/** Isolated desired outcomes retain every authored anchor even when an earlier fetch fails. */
	public DesiredPlan compileDesired(WireModel.Aimc aimc) {
		List<DesiredMapping> outcomes=new ArrayList<>();
		for(WireModel.Mapping mapping:aimc.mappings()) {
			String identity=mapping.authoredId().filter(id -> !id.isBlank()).orElse("mapping_"+mapping.listIndex());
			Set<Dependency> dependencies=new LinkedHashSet<>();
			dependencies.add(new Dependency("submodel",aimc.submodel().getId()));
			for(WireModel.Endpoint endpoint:endpoints(mapping)) anchorDependency(endpoint.reference(),dependencies);
			List<AasRepositoryClient.Result<?>> failures=new ArrayList<>();
			List<String> failurePaths=new ArrayList<>();
			List<EndpointPlan> sources=new ArrayList<>(),sinks=new ArrayList<>();
			for(int i=0;i<mapping.sources().size();i++) {
				var compiled=compileEndpoint(mapping.sources().get(i));
				if(compiled.found())sources.add(compiled.value());
				else { failures.add(compiled); failurePaths.add("Sources/value/"+i); }
			}
			for(int i=0;i<mapping.sinks().size();i++) {
				var compiled=compileEndpoint(mapping.sinks().get(i));
				if(compiled.found())sinks.add(compiled.value());
				else { failures.add(compiled); failurePaths.add("Sinks/value/"+i); }
			}
			var responseSourcesResult=compileOptional(mapping.responseSources(),"ResponseSources",failurePaths);
			if(!responseSourcesResult.found())failures.add(responseSourcesResult);
			var responseSinksResult=compileOptional(mapping.responseSinks(),"ResponseSinks",failurePaths);
			if(!responseSinksResult.found())failures.add(responseSinksResult);
			List<EndpointPlan> resolved=new ArrayList<>(sources);resolved.addAll(sinks);
			if(responseSourcesResult.found() && responseSourcesResult.value().value()!=null)resolved.addAll(responseSourcesResult.value().value());
			if(responseSinksResult.found() && responseSinksResult.value().value()!=null)resolved.addAll(responseSinksResult.value().value());
			for(EndpointPlan endpoint:resolved) {
				if(endpoint.target() instanceof DirectTarget target) metadataDependencies(target.resolved().element(),dependencies);
				// The Record and InfluxDb2Target lie beside the referenced segment: the series container is read, too.
				if(endpoint.target() instanceof SeriesTarget series) {
					// Its values count, too: the segments' StartTime, EndTime and State are the period.
					dependencies.add(new Dependency("series",series.segment().submodel().getId(),series.seriesPath()));
					// Elements the Record references are part of its row shape.
					for(Reference referenced:series.projection().references()) {
						List<Key> keys=Aas.keys(referenced);
						if(keys.size()>=2) dependencies.add(new Dependency("submodel",keys.get(0).getValue(),AasReferenceResolver.idShortPath(keys)));
					}
				}
			}
			MappingPlan plan=failures.isEmpty()?new MappingPlan(identity,mapping.listIndex(),sources,sinks,mapping.defaultPollingInterval(),
					mapping.transformation(),responseSourcesResult.value(),responseSinksResult.value(),mapping.responseTransformation(),Aas.nonNull(mapping.element().getQualifiers())):null;
			outcomes.add(new DesiredMapping(identity,mapping.listIndex(),plan,failures,dependencies,mapping,null,failurePaths));
		}
		for (WireModel.MappingFailure failure : aimc.mappingFailures()) {
			Set<Dependency> dependencies = new LinkedHashSet<>();
			dependencies.add(new Dependency("submodel", aimc.submodel().getId()));
			for (WireModel.RejectedReference rejected : failure.references()) anchorDependency(rejected.reference(), dependencies);
			outcomes.add(new DesiredMapping(failure.identity(), failure.listIndex(), null,
					List.of(invalid(failure.pointer() + ": invalid AIMC mapping structure: " + failure.message())), dependencies,null,failure));
		}
		if (!aimc.mappingFailures().isEmpty()) outcomes.sort(java.util.Comparator.comparingInt(DesiredMapping::listIndex));
		return new DesiredPlan(aimc.submodel().getId(),outcomes);
	}

	private static List<WireModel.Endpoint> endpoints(WireModel.Mapping mapping) {
		List<WireModel.Endpoint> all=new ArrayList<>(mapping.sources());all.addAll(mapping.sinks());
		if(mapping.responseSources().value()!=null)all.addAll(mapping.responseSources().value());
		if(mapping.responseSinks().value()!=null)all.addAll(mapping.responseSinks().value());
		return all;
	}

	private static void anchorDependency(Reference reference,Set<Dependency> dependencies) {
		List<Key> keys=Aas.keys(reference);
		if(!keys.isEmpty() && "Submodel".equals(Aas.keyType(keys.get(0))))dependencies.add(new Dependency("submodel",keys.get(0).getValue()));
	}

	private static void metadataDependencies(SubmodelElement element,Set<Dependency> dependencies) {
		if(element instanceof ReferenceElement reference)anchorDependency(reference.getValue(),dependencies);
		for(SubmodelElement child:Aas.children(element))metadataDependencies(child,dependencies);
	}

	/** On a failure, adds the failing endpoint's path ({@code <collection>/value/<i>}) to {@code failurePaths}. */
	private AasRepositoryClient.Result<WireModel.Presence<List<EndpointPlan>>> compileOptional(
			WireModel.Presence<List<WireModel.Endpoint>> endpoints, String collection, List<String> failurePaths) {
		if(!endpoints.present())return found(WireModel.Presence.absent());
		if(endpoints.value()==null)return found(WireModel.Presence.of(null));
		List<EndpointPlan> result=new ArrayList<>();
		for(int i=0;i<endpoints.value().size();i++) {
			var compiled=compileEndpoint(endpoints.value().get(i));
			if(!compiled.found()) { failurePaths.add(collection+"/value/"+i); return propagate(compiled); }
			result.add(compiled.value());
		}
		return found(WireModel.Presence.of(List.copyOf(result)));
	}

	private AasRepositoryClient.Result<EndpointPlan> compileEndpoint(WireModel.Endpoint endpoint) {
		if(Aas.nonNull(endpoint.element().getQualifiers()).stream().anyMatch(qualifier -> qualifier.getKind()!=QualifierKind.TEMPLATE_QUALIFIER))
			return invalid("endpoint qualifier semantics are unsupported; refusing to discard them");
		var resolved=references.resolve(endpoint.reference());
		if(!resolved.found())return propagate(resolved);
		if(resolved.value() instanceof AasReferenceResolver.ResolvedElement element
				&& TimeSeriesProjection.isLinkedSegment(element.element()))
			return compileSeries(endpoint,element);
		Target target=switch(resolved.value()) {
			case AasReferenceResolver.ResolvedElement element -> new DirectTarget(element);
			case AasReferenceResolver.AidTarget aid -> new AidTarget(aid);
		};
		return found(new EndpointPlan(endpoint.id(),endpoint.reference(),target,endpoint.pollingInterval(),endpoint.evaluation()));
	}

	/** A LinkedSegment target: {@code …/<series>/<Segments>/<segment>}, with its series container read beside it. */
	private AasRepositoryClient.Result<EndpointPlan> compileSeries(WireModel.Endpoint endpoint,
			AasReferenceResolver.ResolvedElement segment) {
		List<Key> keys=Aas.keys(endpoint.reference());
		if(keys.size()<4)return invalid("a LinkedSegment must lie in a TimeSeries collection (…/<series>/Segments/<segment>); "
				+"TimeSeries submodels are not supported yet");
		List<Key> seriesKeys=keys.subList(0,keys.size()-2);
		var series=references.resolve(new DefaultReference.Builder().type(ReferenceTypes.MODEL_REFERENCE)
				.keys(List.copyOf(seriesKeys)).build());
		if(!series.found())return propagate(series);
		if(!(series.value() instanceof AasReferenceResolver.ResolvedElement container))
			return invalid("the LinkedSegment's container is not an AAS element");
		SubmodelElement segments=Aas.children(container.element()).stream()
				.filter(child -> keys.get(keys.size()-2).getValue().equals(child.getIdShort())).findFirst().orElse(null);
		TimeSeriesProjection.Series projection;
		try { projection=TimeSeriesProjection.project(container.element(),segments,segment.element(),this::recordReference); }
		catch(UnresolvedReference unresolved) { return propagate(unresolved.result); }
		catch(IllegalArgumentException invalid) { return invalid("TimeSeries "+AasReferenceResolver.idShortPath(keys)+": "+invalid.getMessage()); }
		return found(new EndpointPlan(endpoint.id(),endpoint.reference(),
				new SeriesTarget(segment,container.element(),AasReferenceResolver.idShortPath(seriesKeys),projection),
				endpoint.pollingInterval(),endpoint.evaluation()));
	}

	/** A Record's reference resolved like a mapping endpoint; a repository failure keeps its status for retries. */
	private SubmodelElement recordReference(Reference reference) {
		var resolved=references.resolve(reference);
		if(!resolved.found()) {
			if(resolved.status()==AasRepositoryClient.Status.INVALID_RESPONSE) throw new IllegalArgumentException(resolved.detail());
			throw new UnresolvedReference(resolved);
		}
		if(!(resolved.value() instanceof AasReferenceResolver.ResolvedElement element))
			throw new IllegalArgumentException("a Record reference must address an AAS element, not an AID affordance");
		return element.element();
	}

	private static final class UnresolvedReference extends RuntimeException {
		private final transient AasRepositoryClient.Result<?> result;
		UnresolvedReference(AasRepositoryClient.Result<?> result) { super(result.detail(),null,false,false); this.result=result; }
	}

	private static <T> AasRepositoryClient.Result<T> found(T value) {
		return new AasRepositoryClient.Result<>(AasRepositoryClient.Status.FOUND,value,200,"");
	}
	private static <T> AasRepositoryClient.Result<T> invalid(String detail) {
		return new AasRepositoryClient.Result<>(AasRepositoryClient.Status.INVALID_RESPONSE,null,0,detail);
	}
	private static <T> AasRepositoryClient.Result<T> propagate(AasRepositoryClient.Result<?> result) {
		return new AasRepositoryClient.Result<>(result.status(),null,result.httpStatus(),result.detail());
	}
}
