package dk.aau.smartproductionlab.dmp.management.compiler;

import dk.aau.smartproductionlab.dmp.core.plan.Evaluation;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultFile;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementList;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;

import dk.aau.smartproductionlab.dmp.core.plan.BundleResources;
import dk.aau.smartproductionlab.dmp.core.plan.Endpoint;
import dk.aau.smartproductionlab.dmp.core.plan.MappingRoute;
import dk.aau.smartproductionlab.dmp.core.plan.Response;
import dk.aau.smartproductionlab.dmp.core.plan.Sink;
import dk.aau.smartproductionlab.dmp.core.plan.Source;
import dk.aau.smartproductionlab.dmp.core.routebuilder.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.aas.configuration.AasElementSink;
import dk.aau.smartproductionlab.dmp.aas.configuration.AasElementSource;
import dk.aau.smartproductionlab.dmp.aas.configuration.AasEventSource;
import dk.aau.smartproductionlab.dmp.aas.event.AasElementEventProcessor;
import dk.aau.smartproductionlab.dmp.aas.operation.OperationRoute;
import dk.aau.smartproductionlab.dmp.http.configuration.HttpObservation;
import dk.aau.smartproductionlab.dmp.http.configuration.HttpPoll;
import dk.aau.smartproductionlab.dmp.http.configuration.HttpWrite;
import dk.aau.smartproductionlab.dmp.snmp.configuration.SnmpEndpoint;
import dk.aau.smartproductionlab.dmp.lua.configuration.LuaScript;
import dk.aau.smartproductionlab.dmp.paho.configuration.MqttPublication;
import dk.aau.smartproductionlab.dmp.paho.configuration.MqttSubscription;
import dk.aau.smartproductionlab.dmp.opcua.configuration.OpcUaEndpoint;
import dk.aau.smartproductionlab.dmp.plc4x.configuration.Plc4xEndpoint;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.AidTarget;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.DirectTarget;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.EndpointPlan;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.MappingPlan;
import dk.aau.smartproductionlab.dmp.management.compiler.MappingPlanCompiler.Plan;
import dk.aau.smartproductionlab.dmp.management.model.WireModel;
import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import dk.aau.smartproductionlab.dmp.management.model.AasFixtures;
import dk.aau.smartproductionlab.dmp.management.model.WireModel.Presence;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;
import org.junit.jupiter.api.Test;

class ConnectorConfigurationFactoryTest {
	/** The endpoints here only name what they bind, so a URI is resolved without a bundle. */
	private static final BundleResources RESOURCES = new BundleResources() {
		@Override public <T extends ManagedRouteResource> T own(T resource) { return resource; }
	};

	private final ConnectorConfigurationFactory factory = new ConnectorConfigurationFactory();

	@Test
	void projectsHttpEndpointsAndManagedLuaWithoutPuttingScriptInUri() {
		MappingPlan plan = mapping(WireModel.ProtocolFamily.HTTP, true);
		var result = factory.create(new Plan(List.of(plan)), "owner/mapping-1");

		assertTrue(result.issues().isEmpty());
		var route = assertInstanceOf(MappingRoute.class, result.routes().get(0));
		assertEquals(1, route.sources().size());
		var source = route.sources().get(0);
		var sink = route.sinks().get(0);
		assertInstanceOf(HttpPoll.class, source.endpoint());
		assertInstanceOf(HttpWrite.class, sink.endpoint());
		assertEquals("Source.authored", source.sourceId());
		assertEquals("Sink.authored", sink.sinkId());
		assertEquals("owner/mapping-1:3", route.routeId());
		String transformer = uri(assertInstanceOf(LuaScript.class, route.transformation()));
		assertTrue(transformer.contains("managed:"));
		assertFalse(transformer.contains("aimc_main"));
	}

	@Test
	void emitsOneStableOwnedConfigurationPerMapping() {
		var first=mappingPlan("map-one",0,
				new EndpointPlan("source-one",reference(),target(WireModel.ProtocolFamily.HTTP,"GET",Presence.absent()),Presence.of("1")),
				new EndpointPlan("sink-one",reference(),target(WireModel.ProtocolFamily.HTTP,"POST",Presence.absent())));
		var second=mappingPlan("map-two",1,
				new EndpointPlan("source-two",reference(),target(WireModel.ProtocolFamily.HTTP,"GET",Presence.absent()),Presence.of("1")),
				new EndpointPlan("sink-two",reference(),target(WireModel.ProtocolFamily.HTTP,"POST",Presence.absent())));
		var plan=new Plan("urn:owner/submodels/AIMC",List.of(first,second));
		var bundles=factory.createBundles(plan,null);
		assertTrue(bundles.issues().isEmpty());
		assertEquals(2,bundles.bundles().size());
		assertEquals("map-one",bundles.bundles().get(0).mappingIdentity());
		assertEquals("map-two",bundles.bundles().get(1).mappingIdentity());
		assertFalse(bundles.bundles().get(0).bundleId().equals(bundles.bundles().get(1).bundleId()));
		for(var bundle:bundles.bundles()) {
			var route=assertInstanceOf(MappingRoute.class,bundle.route());
			assertEquals(1,route.sources().size());
			assertEquals(1,route.sinks().size());
		}
	}

	@Test
	void reportsUnsupportedFamilyWithPathAndMappingIdentity() {
		var result = factory.create(new Plan(List.of(mapping(WireModel.ProtocolFamily.BACNET, false))), "route");
		assertEquals(2, result.issues().size());
		assertTrue(result.issues().stream().allMatch(issue -> issue.mappingId().equals("logical-map")));
		assertTrue(result.issues().stream().allMatch(issue -> issue.path().contains("Sources") || issue.path().contains("Sinks")));
		assertTrue(result.routes().isEmpty());
	}

	@Test
	void projectsMqttBaseRelativeTopicsAndDeclaredQualityTerms() {
		// AID 1.2 Annex B: an href resolves against the base (RFC 3986), so the base path ends with "/".
		var endpointMetadata = AasFixtures.collection("EndpointMetadata", property("base", "mqtt://broker.example:1883/base/"));
		var sourceForm = mqttForm("state", "0", null);
		var sinkForm = mqttForm("command", "1", "true");
		var sourceTarget = mqttTarget(endpointMetadata, sourceForm, "State");
		var sinkTarget = mqttTarget(endpointMetadata, sinkForm, "Command");
		var source = new EndpointPlan("State", reference(), sourceTarget);
		var sink = new EndpointPlan("Command", reference(), sinkTarget);
		var mapping = new MappingPlan("Fan", 0, List.of(source), List.of(sink), Presence.absent(),
				Presence.absent(), Presence.absent(), Presence.absent(), Presence.absent(), List.of());
		var result = factory.create(new Plan("urn:owner/submodels/AIMC", List.of(mapping)), "unit");

		assertTrue(result.issues().isEmpty());
		var dataSource = assertInstanceOf(MqttSubscription.class, sources(result).get(0).endpoint());
		var dataSink = assertInstanceOf(MqttPublication.class, sinks(result).get(0).endpoint());
		assertEquals("broker.example", dataSource.host());
		assertEquals(1883, dataSource.port());
		assertEquals("base/state", dataSource.topic());
		assertEquals(0, dataSource.qos());
		assertEquals("base/command", dataSink.topic());
		assertEquals(1, dataSink.qos());
		assertEquals(true, dataSink.retained());
		assertTrue(uri(dataSource).contains("name=dmp-"));
	}

	@Test
	void resolvesRelativeHttpFormsAgainstTheirAuthoredEndpointBase() {
		var source=polledEndpointPlan("read",protocolTarget(WireModel.ProtocolFamily.HTTP,
				"http://api.example:8080/api/v1/","temperature",Map.of(),"GET"));
		var sink=endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,
				"http://api.example:8080/api/v1/","alarm",Map.of(),"POST"));
		var result=factory.create(new Plan(List.of(mappingWithEndpoints(source,sink))),"route");
		assertTrue(result.issues().isEmpty());
		var httpSource=assertInstanceOf(HttpPoll.class,sources(result).get(0).endpoint());
		assertEquals("http://api.example:8080/api/v1/temperature",httpSource.url());
		var httpSink=assertInstanceOf(HttpWrite.class,sinks(result).get(0).endpoint());
		assertEquals("api.example",httpSink.host());
		assertEquals("/api/v1/alarm",httpSink.path());
		assertEquals("POST",httpSink.method());
		assertTrue(uri(httpSink).contains("httpMethod=POST"));

		// RFC 3986: a path-absolute href replaces the base path; a write without htv_methodName is a PUT (WoT default).
		var absolute=polledEndpointPlan("read",protocolTarget(WireModel.ProtocolFamily.HTTP,
				"http://api.example:8080/api/v1/","/temperature",Map.of(),null));
		var put=endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,
				"http://api.example:8080/api/v1/","alarm",Map.of(),null));
		var resolved=factory.create(new Plan(List.of(mappingWithEndpoints(absolute,put))),"route");
		assertTrue(resolved.issues().isEmpty(),resolved.issues().toString());
		assertEquals("http://api.example:8080/temperature",
				assertInstanceOf(HttpPoll.class,sources(resolved).get(0).endpoint()).url());
		assertEquals("PUT",assertInstanceOf(HttpWrite.class,sinks(resolved).get(0).endpoint()).method());

		// A GET cannot write.
		var getWrite=endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080/","alarm",Map.of(),"GET"));
		var rejected=factory.create(new Plan(List.of(mappingWithEndpoints(absolute,getWrite))),"route");
		assertTrue(rejected.issues().stream().anyMatch(issue -> issue.path().endsWith("/forms/htv_methodName")),rejected.issues().toString());
	}

	@Test
	void projectsOpcUaNodeAndModbusTagDetailsFromTypedBindingTerms() {
		var uaSource = polledEndpointPlan("ua-read", protocolTarget(WireModel.ProtocolFamily.OPCUA,
				"opc.tcp://ua.example:4840/milo", "/?id=ns=2;s=Temperature", Map.of(), null));
		var uaSink = endpointPlan("ua-write", protocolTarget(WireModel.ProtocolFamily.OPCUA,
				"opc.tcp://ua.example:4840/milo", "/?id=ns=2;s=Temperature", Map.of(), null));
		var modbusSource = polledEndpointPlan("mb-read", protocolTarget(WireModel.ProtocolFamily.MODBUS,
				"modbus+tcp://plc.example:502", "/2/0", Map.of("modv_entity", "Coil"), null));
		var modbusSink = endpointPlan("mb-write", protocolTarget(WireModel.ProtocolFamily.MODBUS,
				"modbus+tcp://plc.example:502", "/2/0", Map.of("modv_entity", "Coil"), null));
		var plan = new Plan(List.of(mappingPlan("ua-map",0,uaSource,uaSink),
				mappingPlan("mb-map",1,modbusSource,modbusSink)));
		var result = factory.create(plan, "route");
		assertTrue(result.issues().isEmpty());
		var uaDataSource = endpoint(sources(result), OpcUaEndpoint.class);
		assertEquals("ua.example", uaDataSource.host());
		assertEquals("milo", uaDataSource.servicePath());
		assertEquals("ns=2;s=Temperature", uaDataSource.node());
		var uaDataSink = endpoint(sinks(result), OpcUaEndpoint.class);
		assertEquals("ns=2;s=Temperature", uaDataSink.node());
		var plcSource = endpoint(sources(result), Plc4xEndpoint.class);
		assertEquals("modbus-tcp", plcSource.driver());
		assertEquals("", plcSource.servicePath());
		assertEquals("default-unit-identifier", plcSource.options().get(0).getName());
		assertEquals("2", plcSource.options().get(0).getValue());
		assertEquals("coil:1", plcSource.tags().get(0).getValue(), "protocol address 0 is PLC4X coil 1");
		var plcSink = endpoint(sinks(result), Plc4xEndpoint.class);
		assertEquals("coil:1", plcSink.tags().get(0).getValue());
	}

	@Test
	void translatesWotModbusFormsIntoPlc4xTags() {
		var source=polledEndpointPlan("level",protocolTarget(WireModel.ProtocolFamily.MODBUS,"modbus+tcp://plc.example:502",
				"/7/10?quantity=4",Map.of("modv_function","readHoldingRegisters","modv_type","xsd:int",
						"modv_mostSignificantWord","false","modv_timeout","500","modv_pollingTime","250"),null));
		var sink=endpointPlan("setpoint",protocolTarget(WireModel.ProtocolFamily.MODBUS,"modbus+tcp://plc.example:502",
				"/7/20",Map.of("modv_entity","modv:HoldingRegister","modv_type","xsd:unsignedShort"),null));
		var result=factory.create(new Plan(List.of(mappingPlan("mb-map",0,source,sink))),"route");
		assertTrue(result.issues().isEmpty(),String.valueOf(result.issues()));
		var polled=sources(result).get(0);
		var read=assertInstanceOf(Plc4xEndpoint.class,polled.endpoint());
		assertEquals("holding-register:11:DINT[2]",read.tags().get(0).getValue(),"four registers are two 32-bit values");
		assertEquals(Map.of("default-unit-identifier","7","default-payload-byte-order","LITTLE_ENDIAN_BYTE_SWAP","request-timeout","500"),
				read.options().stream().collect(java.util.stream.Collectors.toMap(o -> o.getName(),o -> o.getValue())));
		assertEquals(1000,polled.pollingIntervalMs(),"the AIMC interval, above modv_pollingTime");
		var write=assertInstanceOf(Plc4xEndpoint.class,sinks(result).get(0).endpoint());
		assertEquals("holding-register:21:UINT",write.tags().get(0).getValue());
	}

	@Test
	void anOpcUaFormNamesItsNodeByHrefNodeIdElseByBrowsePath() {
		var byPath=polledEndpointPlan("level",protocolTarget(WireModel.ProtocolFamily.OPCUA,"opc.tcp://ua.example:4840",
				"/",Map.of("uav_browsePath","/Root/Objects/2:Tank/2:Level"),null));
		var byId=endpointPlan("out",protocolTarget(WireModel.ProtocolFamily.OPCUA,"opc.tcp://ua.example:4840",
				"/?id=ns=2;s=Out",Map.of("uav_browsePath","/Root/Objects/2:Tank/2:Out"),null));
		var result=factory.create(new Plan(List.of(mappingPlan("ua",0,byPath,byId))),"route");
		assertTrue(result.issues().isEmpty(),String.valueOf(result.issues()));
		String source=uri(sources(result).get(0).endpoint());
		assertTrue(source.contains("browsePath=RAW(/Root/Objects/2:Tank/2:Level)") && !source.contains("node="),source);
		String sink=uri(sinks(result).get(0).endpoint());
		assertTrue(sink.contains("node=RAW(ns=2;s=Out)"),"the href's NodeId wins: "+sink);

		var relative=endpointPlan("level",protocolTarget(WireModel.ProtocolFamily.OPCUA,"opc.tcp://ua.example:4840",
				"/",Map.of("uav_browsePath","Objects/2:Level"),null));
		var rejected=factory.create(new Plan(List.of(mappingPlan("ua",0,relative,byId))),"route");
		assertTrue(rejected.issues().stream().anyMatch(issue -> issue.path().endsWith("/forms/uav_browsePath")),String.valueOf(rejected.issues()));
	}

	@Test
	void resolvesAidModbusFormsAgainstTheUnitIdInTheBase() {
		// AID 1.2 Annex B: modbus+tcp://host:port/{unitID}/ and {address}?quantity={n}; one-based addressing
		// counts from 1, and modv_pollingTime is a lower limit for the AIMC interval.
		var source=new EndpointPlan("level",reference(),protocolTarget(WireModel.ProtocolFamily.MODBUS,
				"modbus+tcp://plc.example:502/7/","10?quantity=2",Map.of("modv_entity","HoldingRegister","modv_type","xsd:short",
						"modv_zeroBasedAddressing","false","modv_pollingTime","250"),null),Presence.of("0.1"));
		var sink=endpointPlan("setpoint",protocolTarget(WireModel.ProtocolFamily.MODBUS,"modbus+tcp://plc.example:502/7/",
				"20",Map.of("modv_entity","HoldingRegister","modv_type","xsd:unsignedShort"),null));
		var result=factory.create(new Plan(List.of(mappingPlan("mb-map",0,source,sink))),"route");
		assertTrue(result.issues().isEmpty(),String.valueOf(result.issues()));
		var polled=sources(result).get(0);
		var read=assertInstanceOf(Plc4xEndpoint.class,polled.endpoint());
		assertEquals("holding-register:10:INT[2]",read.tags().get(0).getValue(),"one-based 10 is protocol address 9, PLC4X 10");
		assertEquals("7",read.options().get(0).getValue());
		assertEquals(250,polled.pollingIntervalMs(),"the device's polling rate bounds the AIMC interval");
		var write=assertInstanceOf(Plc4xEndpoint.class,sinks(result).get(0).endpoint());
		assertEquals("holding-register:21:UINT",write.tags().get(0).getValue());
	}

	@Test
	void mqttTopicsFollowAidHrefRulesAndControlPacketsAreTheConsumers() {
		var endpointMetadata=AasFixtures.collection("EndpointMetadata",property("base","mqtt://broker.example:1883/site/"));
		// ".//" writes a topic with a leading "/", "%23" the "#" wildcard (AID 1.2 Annex B.2).
		var events=new EndpointPlan("Events",reference(),mqttTarget(endpointMetadata,mqttForm(".//alarms/%23","1",null),"Events"));
		var command=new EndpointPlan("Command",reference(),mqttTarget(endpointMetadata,mqttForm("cmd","1",null),"Command"));
		var result=factory.create(new Plan(List.of(mappingPlan("topics",0,events,command))),"route");
		assertTrue(result.issues().isEmpty(),String.valueOf(result.issues()));
		assertEquals("site//alarms/#",assertInstanceOf(MqttSubscription.class,sources(result).get(0).endpoint()).topic());
		assertEquals("site/cmd",assertInstanceOf(MqttPublication.class,sinks(result).get(0).endpoint()).topic());

		// mqv_controlPacket describes the consumer: a form whose packet is publish cannot be subscribed to.
		Map<String,String> terms=Map.of("mqv_qos","1","mqv_controlPacket","publish");
		var publishOnly=new WireModel.Form(Presence.of("state"),Presence.absent(),List.of(),Presence.absent(),terms,
				Presence.absent(),terms,emptyElement());
		var wrong=new EndpointPlan("State",reference(),mqttTarget(endpointMetadata,publishOnly,"State"));
		var rejected=factory.create(new Plan(List.of(mappingPlan("packet",0,wrong,command))),"route");
		assertTrue(rejected.issues().stream().anyMatch(issue -> issue.path().endsWith("/forms/mqv_controlPacket")),
				String.valueOf(rejected.issues()));
	}

	@Test
	void httpSubprotocolsObservePropertiesAndSubscribeEvents() {
		var sink=endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080/","alarm",Map.of(),"POST"));
		var sse=factory.create(new Plan(List.of(mappingWithEndpoints(endpointPlan("read",httpObserved("property","sse",true)),sink))),"route");
		assertTrue(sse.issues().isEmpty(),sse.issues().toString());
		var observed=sources(sse).get(0);
		assertInstanceOf(HttpObservation.class,observed.endpoint());
		assertEquals(Source.Delivery.OBSERVED,observed.delivery());
		// Headers (and Basic credentials) are a registry binding the URI names.
		String observedUri=uri(observed.endpoint());
		assertTrue(observedUri.startsWith("dmp-http-observe:sse?url=RAW(http://api.example:8080/temperature)"
				+ "&contentType=RAW(application/json)&headers=#bean:dmp-http-"),observedUri);

		var event=factory.create(new Plan(List.of(mappingWithEndpoints(endpointPlan("read",httpObserved("event","longpoll",false)),sink))),"route");
		assertTrue(event.issues().isEmpty(),event.issues().toString());
		assertTrue(uri(assertInstanceOf(HttpObservation.class,sources(event).get(0).endpoint()))
				.startsWith("dmp-http-observe:longpoll?"));

		for(var rejected:List.of(
				Map.entry(httpObserved("event",null,false),"subscribed through subprotocol"),
				Map.entry(httpObserved("property","websub",true),"websub is not supported"),
				Map.entry(httpObserved("property","sse",false),"does not serve observeproperty"))) {
			var result=factory.create(new Plan(List.of(mappingWithEndpoints(endpointPlan("read",rejected.getKey()),sink))),"route");
			assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains(rejected.getValue())),result.issues().toString());
		}
		var writeObserved=factory.create(new Plan(List.of(mappingWithEndpoints(endpointPlan("read",httpObserved("property","sse",true)),
				endpointPlan("write",httpObserved("property","sse",true))))),"route");
		assertTrue(writeObserved.issues().stream().anyMatch(issue -> issue.message().contains("cannot be a writable destination")),
				writeObserved.issues().toString());
	}

	/** An HTTP property or event on {@code http://api.example:8080/temperature} with the given form subprotocol. */
	private static AidTarget httpObserved(String kind,String subprotocol,boolean observable) {
		SubmodelElement formElement=subprotocol==null?emptyElement():AasFixtures.collection(null,property("subprotocol",subprotocol));
		var form=new WireModel.Form(Presence.of("temperature"),Presence.absent(),List.of(),Presence.of("GET"),Map.of(),Presence.absent(),
				Map.of(),formElement);
		SubmodelElement element=observable?AasFixtures.collection("value",property("observable","true")):emptyElement();
		var affordance=new WireModel.Affordance(kind,Presence.of("value"),Presence.absent(),List.of(form),
				Presence.absent(),Presence.absent(),Presence.absent(),element);
		SubmodelElement endpointMetadata=AasFixtures.collection("EndpointMetadata",property("base","http://api.example:8080/"));
		var iface=new WireModel.Interface("interface-semantic","interface_http",List.of("http"),List.of(WireModel.ProtocolFamily.HTTP),
				emptyElement(),List.of(form),List.of(),"event".equals(kind)?List.of():List.of(affordance),List.of(),
				"event".equals(kind)?List.of(affordance):List.of(),Presence.of(AasFixtures.nosec(endpointMetadata,"interface_http")),
				Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent());
		return new AidTarget(new AasReferenceResolver.AidTarget(iface,affordance,Presence.absent(),List.of()));
	}

	@Test
	void snmpFormsCompileToCheckedGetsAndTypedSets() {
		var read=polledEndpointPlan("read",protocolTarget(WireModel.ProtocolFamily.SNMP,"snmp://10.0.0.5:161/","1.3.6.1.2.1.1.3.0",Map.of(),null));
		var write=endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.SNMP,"snmp://private@10.0.0.5",
				"/1.3.6.1.4.1.9999.1.0",Map.of("snmpv_pduType","snmpv:SetRequestPDU","snmpv_version","SNMPv1"),null));
		var result=factory.create(new Plan(List.of(mappingWithEndpoints(read,write))),"route");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var source=sources(result).get(0);
		assertEquals("dmp-snmp:10.0.0.5:161?oid=1.3.6.1.2.1.1.3.0&version=SNMPv2c",uri(assertInstanceOf(SnmpEndpoint.class,source.endpoint())));
		assertEquals(Source.Delivery.READ,source.delivery());
		String sink=uri(assertInstanceOf(SnmpEndpoint.class,sinks(result).get(0).endpoint()));
		// The community is a credential: the URI names its registry binding, never the value.
		assertTrue(sink.startsWith("dmp-snmp:10.0.0.5:161?oid=1.3.6.1.4.1.9999.1.0&version=SNMPv1&community=#bean:dmp-credential-"),
				sink);
		assertFalse(sink.contains("private"),sink);

		for(var rejected:List.of(
				Map.entry(protocolTarget(WireModel.ProtocolFamily.SNMP,"snmp://10.0.0.5/","sysDescr",Map.of(),null),"snmp://{address}:{port}/{oid}"),
				Map.entry(protocolTarget(WireModel.ProtocolFamily.SNMP,"snmp://10.0.0.5/","1.3.6.1.2.1.1.3.0",
						Map.of("snmpv_pduType","SetRequestPDU"),null),"does not serve this mapping role"),
				Map.entry(protocolTarget(WireModel.ProtocolFamily.SNMP,"snmp://10.0.0.5/","1.3.6.1.2.1.1.3.0",
						Map.of("snmpv_version","SNMPv3"),null),"not supported"))) {
			var failed=factory.create(new Plan(List.of(mappingWithEndpoints(endpointPlan("read",rejected.getKey()),write))),"route");
			assertTrue(failed.issues().stream().anyMatch(issue -> issue.message().contains(rejected.getValue())),failed.issues().toString());
		}
	}

	@Test
	void staticHttpHeadersAreBoundAndInvalidOnesRejected() {
		var poll=polledEndpointPlan("read",httpWithHeaders("GET",null,header("X-Api-Key","s3cret")));
		var write=endpointPlan("write",httpWithHeaders("PUT",null,header("Authorization","Bearer token-1")));
		var result=factory.create(new Plan(List.of(mappingWithEndpoints(poll,write))),"route");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		String source=uri(assertInstanceOf(HttpPoll.class,sources(result).get(0).endpoint()));
		assertTrue(source.contains("httpClientConfigurer=#bean:dmp-http-"),source);
		assertFalse(source.contains("s3cret"));
		String sink=uri(assertInstanceOf(HttpWrite.class,sinks(result).get(0).endpoint()));
		assertFalse(sink.contains("token-1"));

		for(var rejected:List.of(
				Map.entry(httpWithHeaders("GET",null,header("Content-Type","text/plain")),"set by the connector"),
				Map.entry(httpWithHeaders("GET",null,header("Bad Name","x")),"valid htv_fieldName"),
				Map.entry(httpWithHeaders("GET",null,header("X-A","1"),header("x-a","2")),"declared twice"),
				Map.entry(httpWithHeaders("GET","sse",header("Accept","text/plain")),"set by its subprotocol"))) {
			var failed=factory.create(new Plan(List.of(mappingWithEndpoints(endpointPlan("read",rejected.getKey()),write))),"route");
			assertTrue(failed.issues().stream().anyMatch(issue -> issue.message().contains(rejected.getValue())),failed.issues().toString());
		}
	}

	private static SubmodelElement header(String name,String value) {
		return AasFixtures.collection(null,property("htv_fieldName",name),property("htv_fieldValue",value));
	}

	/** An observable HTTP property on http://api.example:8080/value whose form carries htv_headers (and a subprotocol). */
	private static AidTarget httpWithHeaders(String method,String subprotocol,SubmodelElement... headers) {
		var list=new org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementList.Builder().idShort("htv_headers")
				.typeValueListElement(org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION)
				.value(List.of(headers)).build();
		SubmodelElement formElement=subprotocol==null?AasFixtures.collection(null,list)
				:AasFixtures.collection(null,list,property("subprotocol",subprotocol));
		var form=new WireModel.Form(Presence.of("value"),Presence.absent(),List.of(),Presence.of(method),Map.of(),Presence.absent(),
				Map.of(),formElement);
		var affordance=new WireModel.Affordance("property",Presence.of("value"),Presence.absent(),List.of(form),
				Presence.absent(),Presence.absent(),Presence.absent(),AasFixtures.collection("value",property("observable","true")));
		SubmodelElement endpointMetadata=AasFixtures.collection("EndpointMetadata",property("base","http://api.example:8080/"));
		var iface=new WireModel.Interface("interface-semantic","interface_http",List.of("http"),List.of(WireModel.ProtocolFamily.HTTP),
				emptyElement(),List.of(form),List.of(),List.of(affordance),List.of(),List.of(),Presence.of(AasFixtures.nosec(endpointMetadata,"interface_http")),
				Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent());
		return new AidTarget(new AasReferenceResolver.AidTarget(iface,affordance,Presence.absent(),List.of()));
	}

	@Test
	void theFormMustServeTheMappingRole() {
		var endpointMetadata=AasFixtures.collection("EndpointMetadata",property("base","mqtt://broker.example:1883/"));
		var readOnly=new WireModel.Affordance("property",Presence.of("State"),Presence.absent(),List.of(mqttForm("state","0",null)),
				Presence.absent(),Presence.absent(),Presence.absent(),
				AasFixtures.collection("State",property("observable","true"),property("readOnly","true")));
		var iface=new WireModel.Interface("interface-semantic","interface_mqtt",List.of("http://www.w3.org/2011/mqtt"),
				List.of(WireModel.ProtocolFamily.MQTT),emptyElement(),readOnly.forms(),List.of(),List.of(readOnly),List.of(),List.of(),
				Presence.of(AasFixtures.nosec(endpointMetadata,"interface_mqtt")),Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent());
		var target=new AidTarget(new AasReferenceResolver.AidTarget(iface,readOnly,Presence.absent(),List.of()));
		var source=new EndpointPlan("in",reference(),mqttTarget(endpointMetadata,mqttForm("in","0",null),"In"));
		var result=factory.create(new Plan(List.of(mappingPlan("write-read-only",0,source,new EndpointPlan("State",reference(),target)))),"route");
		assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains("not writable")),String.valueOf(result.issues()));
	}

	@Test
	void rejectsModbusFormsTheWotBindingDoesNotDescribe() {
		for(var form:List.of(Map.entry("/coil:1",Map.<String,String>of()),
				Map.entry("/1/0",Map.of("modv_entity","InputRegister")),
				Map.entry("/1/0",Map.of("modv_zeroBasedAddressing","false")),
				Map.entry("/1/0?quantity=3",Map.of("modv_entity","HoldingRegister","modv_type","xsd:int")))) {
			var read=endpointPlan("in",protocolTarget(WireModel.ProtocolFamily.MODBUS,"modbus+tcp://plc.example:502","/1/1",Map.of(),null));
			var write=endpointPlan("out",protocolTarget(WireModel.ProtocolFamily.MODBUS,"modbus+tcp://plc.example:502",form.getKey(),form.getValue(),null));
			var result=factory.create(new Plan(List.of(mappingPlan("mb-map",0,read,write))),"route");
			assertFalse(result.issues().isEmpty(),form+" must be rejected");
			assertTrue(result.routes().isEmpty());
		}
		var legacy=endpointPlan("in",protocolTarget(WireModel.ProtocolFamily.MODBUS,"tcp://plc.example:502","/1/1",Map.of(),null));
		assertTrue(factory.create(new Plan(List.of(mappingWithEndpoints(legacy,legacy))),"route").issues().stream()
				.anyMatch(issue -> issue.path().contains("EndpointMetadata/base")),"a tcp:// base is not the WoT modbus+tcp scheme");
	}

	@Test
	void rejectsAnAuthoredModbusFunctionNotRepresentedByTheConnectorConfiguration() {
		var modbus=endpointPlan("mb",protocolTarget(WireModel.ProtocolFamily.MODBUS,
				"modbus+tcp://plc.example:502", "/1/0", Map.of("modv_function", "read-holding"), null));
		var result=factory.create(new Plan(List.of(mappingWithEndpoints(modbus,modbus))),"route");
		assertFalse(result.issues().isEmpty());
		assertTrue(result.issues().stream().anyMatch(issue -> issue.path().contains("modv_function")));
		assertTrue(result.routes().isEmpty());
	}

	@Test
	void bacnetMappingsAreRejectedBecauseThePlc4xDriverNeitherReadsNorWrites() {
		var source=endpointPlan("present-value",protocolTarget(WireModel.ProtocolFamily.BACNET,
				"bacnet://5", "/0,1/85", Map.of(), null));
		var sink=endpointPlan("present-value",protocolTarget(WireModel.ProtocolFamily.BACNET,
				"bacnet://5", "/1,2/85", Map.of(), null));
		var result=factory.create(new Plan(List.of(mappingPlan("bacnet-map",0,source,sink))),"route");
		assertTrue(result.routes().isEmpty());
		assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains("BACnet is not supported")),
				String.valueOf(result.issues()));
	}

	@Test
	void mapsIolinkRestBindingsThroughTheHttpConnector() {
		var source=polledEndpointPlan("input",protocolTarget(WireModel.ProtocolFamily.IOLINK,
				"http://iolink.example:8080/api", "/input",Map.of(),"GET"));
		var sink=endpointPlan("output",protocolTarget(WireModel.ProtocolFamily.IOLINK,
				"http://iolink.example:8080/api", "/output",Map.of(),"POST"));
		var result=factory.create(new Plan(List.of(mappingPlan("iolink-map",0,source,sink))),"route");
		assertTrue(result.issues().isEmpty());
		assertInstanceOf(HttpPoll.class,sources(result).get(0).endpoint());
		assertInstanceOf(HttpWrite.class,sinks(result).get(0).endpoint());
	}

	@Test
	void directAasTargetsRequireAndUseExplicitRepositoryRuntimeSettings() {
		var target=new DirectTarget(new AasReferenceResolver.ResolvedElement(
				AasFixtures.submodel("urn:owner/submodels/Variables","Variables"),
				property("State","RUNNING")));
		var reference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","Property","State");
		var source=new EndpointPlan("PackMLState",reference,target,Presence.of("2"));
		var sink=new EndpointPlan("OccupationState",reference,target);
		var plan=new Plan(List.of(mappingPlan("aas-map",0,source,sink)));
		var noServer=factory.create(plan,"route");
		assertTrue(noServer.routes().isEmpty());
		assertTrue(noServer.issues().stream().anyMatch(issue -> issue.message().contains("explicit repository runtime settings")));

		var result=factory.create(plan,"route",new ConnectorConfigurationFactory.RuntimeSettings(
				URI.create("http://aas.example:8081")));
		assertTrue(result.issues().isEmpty());
		var polled=sources(result).get(0);
		var ds=assertInstanceOf(AasElementSource.class,polled.endpoint());
		String encoded=java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
				"urn:owner/submodels/Variables".getBytes(StandardCharsets.UTF_8));
		assertEquals("http://aas.example:8081/submodels/"+encoded,ds.submodelEndpoint());
		assertEquals("State",ds.idShortPath());
		assertEquals(2000,polled.pollingIntervalMs(),"an authored PollingInterval keeps polling");
		var sinkConfig=sinks(result).get(0);
		assertInstanceOf(AasElementSink.class,sinkConfig.endpoint());
		assertEquals("OccupationState",sinkConfig.sinkId());
		assertEquals("OccupationState",sinkConfig.valuePath());
	}

	@Test
	void aasPropertySourcesWithoutPollingIntervalFollowRepositoryEvents() {
		var collection=AasFixtures.collection("Status",property("State","RUNNING"));
		var target=new DirectTarget(new AasReferenceResolver.ResolvedElement(
				AasFixtures.submodel("urn:owner/submodels/Variables","Variables"),collection.getValue().get(0)));
		var reference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","SubmodelElementCollection","Status","Property","State");
		var plan=new Plan(List.of(mappingPlan("aas-event",0,new EndpointPlan("PackMLState",reference,target),
				new EndpointPlan("OccupationState",reference,target))));
		var result=factory.create(plan,"route",new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081")));
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var source=sources(result).get(0);
		var ds=assertInstanceOf(AasEventSource.class,source.endpoint());
		assertEquals("urn:owner/submodels/Variables",ds.submodelId());
		assertEquals("Status.State",ds.idShortPath());
		assertEquals(Source.Delivery.OBSERVED,source.delivery());
		assertInstanceOf(AasElementEventProcessor.class,ds.normalizer(source),"repository events are read as element events");
		assertTrue(uri(ds).startsWith("aas-event:"));
		String encoded=java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
				"urn:owner/submodels/Variables".getBytes(StandardCharsets.UTF_8));
		assertEquals("http://aas.example:8081/submodels/"+encoded+"/submodel-elements/Status.State",ds.readUrl());
		assertEquals(Map.of("type","string"),source.schema());
	}

	@Test
	void anOrdinaryMappingProjectsWhatItsSinkWasSentIntoItsResponse() {
		var status=AasFixtures.collection("Status",property("Setpoint","0"),property("LastWrite",""));
		var submodel=AasFixtures.submodel("urn:owner/submodels/Variables","Variables");
		java.util.function.Function<String,EndpointPlan> element=name -> new EndpointPlan(name,
				AasFixtures.reference("Submodel","urn:owner/submodels/Variables","SubmodelElementCollection","Status","Property",name),
				new DirectTarget(new AasReferenceResolver.ResolvedElement(submodel,
						status.getValue().stream().filter(member -> member.getIdShort().equals(name)).findFirst().orElseThrow())));
		var state=endpointPlan("state",protocolTarget(WireModel.ProtocolFamily.MQTT,"mqtt://broker.example:1883","/state",Map.of("mqv_retain","true"),null));
		var setpoint=element.apply("Setpoint");
		var written=new EndpointPlan("Written",setpoint.reference(),setpoint.target());
		var lastWrite=element.apply("LastWrite");
		var lua=Presence.of(new WireModel.Blob("function aimc_main(sources) return { LastWrite = sources.Written } end"
				.getBytes(StandardCharsets.UTF_8),"text/x-lua"));
		var runtime=new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081"));
		var mapping=new MappingPlan("logged",0,List.of(state),List.of(setpoint),Presence.absent(),Presence.absent(),
				Presence.of(List.of(written)),Presence.of(List.of(lastWrite)),lua,List.of());
		var result=factory.create(new Plan(List.of(mapping)),"route",runtime);
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var route=assertInstanceOf(MappingRoute.class,result.routes().get(0));
		var binding=route.response().sources().get(0);
		assertEquals("Written",binding.sourceId());
		assertEquals("Setpoint",binding.sinkId(),"the ResponseSource projects the value Setpoint was sent");
		assertEquals(null,binding.valuePath());
		assertEquals("LastWrite",route.response().sinks().get(0).sinkId());
		assertTrue(route.response().transformation()!=null && route.response().transformation()!=route.transformation(),
				"the response Lua is the response half's, not a request transformer");
		assertEquals(1,route.sinks().size(),"ResponseSinks are not written by the request half");

		// a member of a collection the mapping writes: the member of what was sent
		var whole=new EndpointPlan("Status",AasFixtures.reference("Submodel","urn:owner/submodels/Variables","SubmodelElementCollection","Status"),
				new DirectTarget(new AasReferenceResolver.ResolvedElement(submodel,status)));
		var nested=new MappingPlan("nested",0,List.of(state),List.of(whole),Presence.absent(),Presence.absent(),
				Presence.of(List.of(written)),Presence.of(List.of(lastWrite)),lua,List.of());
		var member=factory.create(new Plan(List.of(nested)),"route",runtime);
		assertTrue(member.issues().isEmpty(),member.issues().toString());
		var memberBinding=assertInstanceOf(MappingRoute.class,member.routes().get(0)).response().sources().get(0);
		assertEquals("Status",memberBinding.sinkId());
		assertEquals("$.Setpoint",memberBinding.valuePath());

		var reads=new EndpointPlan("Reads",state.reference(),state.target());
		var notASink=new MappingPlan("logged",0,List.of(state),List.of(setpoint),Presence.absent(),Presence.absent(),
				Presence.of(List.of(reads)),Presence.of(List.of(lastWrite)),lua,List.of());
		var rejected=factory.create(new Plan(List.of(notASink)),"route",runtime);
		assertTrue(rejected.issues().stream().anyMatch(issue -> issue.message().contains("must select one of the mapping's Sinks")),
				rejected.issues().toString());
		var withoutLua=new MappingPlan("logged",0,List.of(state),List.of(setpoint),Presence.absent(),Presence.absent(),
				Presence.of(List.of(written)),Presence.of(List.of(lastWrite)),Presence.absent(),List.of());
		var incomplete=factory.create(new Plan(List.of(withoutLua)),"route",runtime);
		assertTrue(incomplete.issues().stream().anyMatch(issue -> issue.message().contains("requires a ResponseTransformation")),
				incomplete.issues().toString());
	}

	@Test
	void aSourcesEvaluationDecidesWhetherItTriggersAndHowItIsRead() {
		var get=protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"GET");
		var sink=endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"POST"));
		var mqtt=protocolTarget(WireModel.ProtocolFamily.MQTT,"mqtt://broker.example:1883","/state",Map.of("mqv_retain","true"),null);
		java.util.function.BiFunction<String,String,EndpointPlan> source=(id,mode) -> new EndpointPlan(id,reference(),
				id.startsWith("mqtt")?mqtt:get,Presence.absent(),mode==null?Presence.absent():Presence.of(mode));
		java.util.function.Function<List<EndpointPlan>,MappingPlan> mapping=sources -> new MappingPlan("modes",0,sources,List.of(sink),
				Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent(),List.of());

		// an observed trigger and a GET read at each evaluation: no interval needed
		var result=factory.create(new Plan(List.of(mapping.apply(List.of(source.apply("mqtt","OnChange"),source.apply("read","ReadAtEvaluation"))))),"route");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var configured=sources(result);
		assertEquals(Evaluation.ON_CHANGE,configured.get(0).evaluation());
		assertEquals(Evaluation.READ_AT_EVALUATION,configured.get(1).evaluation());
		assertEquals(Source.Delivery.READ,configured.get(1).delivery());
		assertEquals(0,configured.get(1).pollingIntervalMs());

		for(var rejected:List.of(
				Map.entry(List.of(source.apply("mqtt","Cached"),source.apply("read","ReadAtEvaluation")),"no Source triggers the mapping"),
				Map.entry(List.of(source.apply("mqtt",null),source.apply("mqttRead","ReadAtEvaluation")),"received by subscription"),
				Map.entry(List.of(source.apply("mqtt",null),new EndpointPlan("read",reference(),get,Presence.of("2"),Presence.of("ReadAtEvaluation"))),
						"is not polled"))) {
			var failed=factory.create(new Plan(List.of(mapping.apply(rejected.getKey()))),"route");
			assertTrue(failed.issues().stream().anyMatch(issue -> issue.message().contains(rejected.getValue())),failed.issues().toString());
		}
	}

	@Test
	void anOperationIsItsMappingsOnlyTriggerAndItsFurtherSourcesAreReadAtTheCall() {
		var plan=explicitResponsePlan("aid-command");
		var bridge=plan.mappings().get(0);
		var operation=bridge.sources().get(0);
		java.util.function.Function<EndpointPlan,MappingPlan> withSources=first -> new MappingPlan(bridge.identity(),bridge.listIndex(),
				List.of(first),bridge.sinks(),bridge.defaultPollingInterval(),bridge.transformation(),bridge.responseSources(),
				bridge.responseSinks(),bridge.responseTransformation(),bridge.qualifiers());
		var evaluated=new EndpointPlan(operation.id(),operation.reference(),operation.target(),Presence.absent(),Presence.of("OnUpdate"));
		var result=factory.create(new Plan(List.of(withSources.apply(evaluated))),"bridge",
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081")));
		assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains("only trigger; it takes no Evaluation")),
				result.issues().toString());
	}

	/** An AAS Property that follows repository events (no PollingInterval), with the authored {@code mode}. */
	private static EndpointPlan eventProperty(String id,String idShort,String mode) {
		var target=new DirectTarget(new AasReferenceResolver.ResolvedElement(
				AasFixtures.submodel("urn:owner/submodels/Variables","Variables"),property(idShort,"IDLE")));
		return new EndpointPlan(id,AasFixtures.reference("Submodel","urn:owner/submodels/Variables","Property",idShort),
				target,Presence.absent(),mode==null?Presence.absent():Presence.of(mode));
	}

	@Test
	void aCachedSourceThatCanBeReadIsReadOnceThenFollowsItsUpdates() {
		var sink=endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"POST"));
		var mqtt=protocolTarget(WireModel.ProtocolFamily.MQTT,"mqtt://broker.example:1883","/state",Map.of("mqv_retain","true"),null);
		var mapping=new MappingPlan("cached",0,List.of(eventProperty("trigger","Mode",null),eventProperty("state","State","Cached"),
				new EndpointPlan("topic",reference(),mqtt,Presence.absent(),Presence.of("Cached"))),List.of(sink),
				Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent(),List.of());
		var result=factory.create(new Plan(List.of(mapping)),"route",
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081")));
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var configured=sources(result);
		assertNull(configured.get(0).initialRead(),"a trigger is not read: its first update evaluates the mapping");
		assertInstanceOf(AasEventSource.class,configured.get(1).endpoint());
		var read=assertInstanceOf(AasElementSource.class,configured.get(1).initialRead());
		assertEquals("State",read.idShortPath());
		assertNull(configured.get(2).initialRead(),"an MQTT topic is only received by subscription");
	}

	@Test
	void anOperationReadsItsFurtherAasSourcesAtTheCallAndAcceptsNoOtherEvaluation() {
		var bridge=explicitResponsePlan("aid-command").mappings().get(0);
		var mqtt=protocolTarget(WireModel.ProtocolFamily.MQTT,"mqtt://broker.example:1883","/state",Map.of("mqv_retain","true"),null);
		java.util.function.Function<List<EndpointPlan>,MappingPlan> withFurther=further -> {
			List<EndpointPlan> sources=new ArrayList<>(bridge.sources());
			sources.addAll(further);
			return new MappingPlan(bridge.identity(),bridge.listIndex(),sources,bridge.sinks(),bridge.defaultPollingInterval(),
					bridge.transformation(),bridge.responseSources(),bridge.responseSinks(),bridge.responseTransformation(),
					bridge.qualifiers());
		};
		var runtime=new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081"));
		var result=factory.create(new Plan(List.of(withFurther.apply(List.of(eventProperty("mode","Mode",null),
				eventProperty("state","State","ReadAtEvaluation"))))),"bridge",runtime);
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var fanIn=assertInstanceOf(OperationRoute.class,result.routes().get(0)).fanIn();
		assertEquals(List.of(Evaluation.READ_AT_EVALUATION,Evaluation.READ_AT_EVALUATION),fanIn.stream().map(Source::evaluation).toList());
		fanIn.forEach(source -> assertInstanceOf(AasElementSource.class,source.endpoint(),"read at the call"));

		for(var rejected:List.of(
				Map.entry(eventProperty("state","State","Cached"),"reads its further sources at the call"),
				Map.entry(eventProperty("state","State","OnUpdate"),"reads its further sources at the call"),
				Map.entry(new EndpointPlan("topic",reference(),mqtt),"fans in AAS elements only"))) {
			var failed=factory.create(new Plan(List.of(withFurther.apply(List.of(rejected.getKey())))),"bridge",runtime);
			assertTrue(failed.routes().isEmpty());
			assertTrue(failed.issues().stream().anyMatch(issue -> issue.message().contains(rejected.getValue())),
					failed.issues().toString());
		}
	}

	@Test
	void anAasPropertySourceWithAPollingIntervalOfZeroFollowsRepositoryEvents() {
		var collection=AasFixtures.collection("Status",property("State","RUNNING"));
		var target=new DirectTarget(new AasReferenceResolver.ResolvedElement(
				AasFixtures.submodel("urn:owner/submodels/Variables","Variables"),collection.getValue().get(0)));
		var reference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","SubmodelElementCollection","Status","Property","State");
		var plan=new Plan(List.of(withDefaultPollingInterval(mappingPlan("aas-event",0,new EndpointPlan("PackMLState",reference,target,Presence.of("0")),
				new EndpointPlan("OccupationState",reference,target)),"2")));
		var result=factory.create(plan,"route",new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081")));
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		assertInstanceOf(AasEventSource.class,sources(result).get(0).endpoint());
	}

	@Test
	void aCollectionOrListSourceWithoutAPollingIntervalFollowsRepositoryEventsAndOneWithAnIntervalIsRead() {
		var collection=AasFixtures.collection("Status",property("State","RUNNING"));
		var list=new org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementList.Builder().idShort("Readings")
				.typeValueListElement(org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements.PROPERTY)
				.valueTypeListElement(dk.aau.smartproductionlab.dmp.aas.model.Aas.xsd("xs:double")).build();
		var submodel=AasFixtures.submodel("urn:owner/submodels/Variables","Variables");
		var collectionReference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","SubmodelElementCollection","Status");
		var listReference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","SubmodelElementList","Readings");
		var out=new DirectTarget(new AasReferenceResolver.ResolvedElement(submodel,property("Out","")));
		var outReference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","Property","Out");
		var runtime=new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081"));
		java.util.function.BiFunction<EndpointPlan,EndpointPlan,Source> compiled=(source,sink) -> {
			var result=factory.create(new Plan(List.of(mappingPlan("aas-element",0,source,sink))),"route",runtime);
			assertTrue(result.issues().isEmpty(),result.issues().toString());
			return sources(result).get(0);
		};
		var followed=compiled.apply(new EndpointPlan("Status",collectionReference,
				new DirectTarget(new AasReferenceResolver.ResolvedElement(submodel,collection))),new EndpointPlan("Out",outReference,out));
		var events=assertInstanceOf(AasEventSource.class,followed.endpoint());
		assertEquals("SubmodelElementCollection",events.modelType());
		assertEquals("Status",events.idShortPath());
		assertEquals(Source.Delivery.OBSERVED,followed.delivery());
		var listEvents=assertInstanceOf(AasEventSource.class,compiled.apply(new EndpointPlan("Readings",listReference,
				new DirectTarget(new AasReferenceResolver.ResolvedElement(submodel,list)),Presence.of("0")),
				new EndpointPlan("Out",outReference,out)).endpoint());
		assertEquals("SubmodelElementList",listEvents.modelType());
		var polled=compiled.apply(new EndpointPlan("Status",collectionReference,
				new DirectTarget(new AasReferenceResolver.ResolvedElement(submodel,collection)),Presence.of("2")),
				new EndpointPlan("Out",outReference,out));
		assertInstanceOf(AasElementSource.class,polled.endpoint(),"a positive PollingInterval reads the collection");
		assertEquals(2000,polled.pollingIntervalMs());
	}

	@Test
	void withoutRepositoryEventsAnAasSourceIsPolledAndNeedsAnInterval() {
		var collection=AasFixtures.collection("Status",property("State","RUNNING"));
		var target=new DirectTarget(new AasReferenceResolver.ResolvedElement(
				AasFixtures.submodel("urn:owner/submodels/Variables","Variables"),collection.getValue().get(0)));
		var reference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","SubmodelElementCollection","Status","Property","State");
		var out=new EndpointPlan("Out",reference,target);
		var runtime=new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081"),null,null,false);
		var blocked=factory.create(new Plan(List.of(mappingPlan("aas-poll",0,new EndpointPlan("State",reference,target),out))),"route",runtime);
		assertTrue(blocked.routes().isEmpty());
		assertTrue(blocked.issues().stream().anyMatch(issue -> issue.message().contains("change events do not reach the DMP")),
				blocked.issues().toString());
		var polled=factory.create(new Plan(List.of(withDefaultPollingInterval(mappingPlan("aas-poll",0,
				new EndpointPlan("State",reference,target),out),"2"))),"route",runtime);
		assertTrue(polled.issues().isEmpty(),polled.issues().toString());
		assertInstanceOf(AasElementSource.class,sources(polled).get(0).endpoint(),"the mapping's DefaultPollingInterval reads it");
	}

	@Test
	void nestedAasSinksAreAddressedByTheirIdShortPath() {
		var collection=AasFixtures.collection("Location",property("x","0.0"));
		var target=new DirectTarget(new AasReferenceResolver.ResolvedElement(
				AasFixtures.submodel("urn:owner/submodels/Variables","Variables"),collection.getValue().get(0)));
		var reference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","SubmodelElementCollection","Location","Property","x");
		var plan=new Plan(List.of(mappingPlan("aas-nested",0,new EndpointPlan("Location",reference,target,Presence.of("2")),
				new EndpointPlan("Location_x",reference,target))));
		var result=factory.create(plan,"route",new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081")));
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var ds=assertInstanceOf(AasElementSource.class,sources(result).get(0).endpoint());
		assertEquals("Location.x",ds.idShortPath());
		var sink=assertInstanceOf(AasElementSink.class,sinks(result).get(0).endpoint());
		assertEquals("Location.x",sink.idShortPath());
	}

	@Test
	void aCollectionSinkIsWrittenAsACollectionAndAListSinkIsBlocked() {
		var collection=AasFixtures.collection("Location",property("x","0.0"));
		var submodel=AasFixtures.submodel("urn:owner/submodels/Variables","Variables");
		var runtime=new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081"));
		var reference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","SubmodelElementCollection","Location");
		var whole=new DirectTarget(new AasReferenceResolver.ResolvedElement(submodel,collection));
		var result=factory.create(new Plan(List.of(mappingPlan("aas-collection",0,new EndpointPlan("In",reference,whole,Presence.of("2")),
				new EndpointPlan("Location",reference,whole)))),"route",runtime);
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		String sink=uri(assertInstanceOf(AasElementSink.class,sinks(result).get(0).endpoint()));
		assertTrue(sink.contains("&elementType=SubmodelElementCollection"),sink);

		var list=new org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementList.Builder().idShort("Items")
				.typeValueListElement(org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements.PROPERTY)
				.value(List.of(property("ignored","1"))).build();
		var listReference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","SubmodelElementList","Items");
		var listTarget=new DirectTarget(new AasReferenceResolver.ResolvedElement(submodel,list));
		var blocked=factory.create(new Plan(List.of(mappingPlan("aas-list",0,new EndpointPlan("In",reference,whole,Presence.of("2")),
				new EndpointPlan("Items",listReference,listTarget)))),"route",runtime);
		assertTrue(blocked.issues().stream().anyMatch(issue -> issue.message().contains("not a SubmodelElementList")),
				blocked.issues().toString());
	}

	@Test
	void bridgeRestPathIsTheDelegationQualifierPathAndAMissingQualifierBlocksTheMapping() {
		var result=factory.create(explicitResponsePlan("aid-command"),"bridge",new ConnectorConfigurationFactory.RuntimeSettings(
				URI.create("http://aas.example:8081")));
		var bridge=assertInstanceOf(OperationRoute.class,result.routes().get(0));
		assertEquals(OperationPaths.invokePath("cci","Skills.Move.operation"),bridge.identity().restPath());
		assertEquals("Skills.Move.operation",bridge.identity().path());

		var plan=explicitResponsePlan("aid-command");
		var operation=(org.eclipse.digitaltwin.aas4j.v3.model.Operation)((DirectTarget)plan.mappings().get(0).sources().get(0).target())
				.resolved().element();
		// Missing, relative, templated, empty, or any path but the Operation's own AAS API invoke path.
		for (String invalid : new String[] { null, "", "/operations/asset/Move", "http://dmp:8080/operations/{aas_id_short}/Move",
				"http://dmp:8080/", "http://dmp:8080/operations/asset/Move",
				"http://dmp:8080"+OperationPaths.invokePath("other","Skills.Move.operation"),
				"http://dmp:8080"+OperationPaths.invokePath("cci","Skills.Halt.operation") }) {
			operation.setQualifiers(invalid==null?List.of():List.of(AasFixtures.delegation(invalid)));
			var blocked=factory.create(plan,"bridge",new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081")));
			assertTrue(blocked.routes().isEmpty(),String.valueOf(invalid));
			assertTrue(blocked.issues().stream().anyMatch(issue -> issue.message().contains("invocationDelegation")),String.valueOf(invalid));
		}
	}

	/** {@code plan}'s one mapping with {@code timeout} as its InvocationTimeout. */
	private static Plan withInvocationTimeout(Plan plan,String timeout) {
		MappingPlan m=plan.mappings().get(0);
		return new Plan(plan.aimcSubmodelId(),List.of(new MappingPlan(m.identity(),m.listIndex(),m.sources(),m.sinks(),
				m.defaultPollingInterval(),m.transformation(),m.responseSources(),m.responseSinks(),m.responseTransformation(),
				m.qualifiers(),Presence.of(timeout))));
	}

	@Test
	void anOperationWaitsForItsActionAsLongAsItsInvocationTimeoutSays() {
		var runtime=new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081"));
		var unauthored=assertInstanceOf(OperationRoute.class,factory.create(explicitResponsePlan("aid-command"),"bridge",runtime).routes().get(0));
		assertEquals(30L,unauthored.command().connectorOptions().get("timeoutSeconds"),"30 s unless authored");
		var authored=factory.create(withInvocationTimeout(explicitResponsePlan("aid-command"),"PT2M"),"bridge",runtime);
		assertTrue(authored.issues().isEmpty(),authored.issues().toString());
		assertEquals(120L,assertInstanceOf(OperationRoute.class,authored.routes().get(0)).command().connectorOptions().get("timeoutSeconds"));
		for (String invalid : new String[] { "PT0S", "PT16M", "PT1.5S", "120", "P1D", "" }) {
			var blocked=factory.create(withInvocationTimeout(explicitResponsePlan("aid-command"),invalid),"bridge",runtime);
			assertTrue(blocked.routes().isEmpty(),invalid);
			assertTrue(blocked.issues().stream().anyMatch(issue -> issue.message().contains("InvocationTimeout")),invalid);
		}
	}

	@Test
	void anInvocationTimeoutOnAMappingNoOperationTriggersBlocksIt() {
		var device=protocolTarget(WireModel.ProtocolFamily.HTTP,"http://device.example:8080","/state",Map.of(),"GET");
		var plan=new Plan("aimc",List.of(new MappingPlan("plain",0,List.of(new EndpointPlan("In",reference(),device,Presence.of("1"))),
				List.of(new EndpointPlan("Out",reference(),device)),Presence.absent(),Presence.absent(),
				Presence.absent(),Presence.absent(),Presence.absent(),List.of(),Presence.of("PT2M"))));
		var blocked=factory.create(plan,"plain",new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081")));
		assertTrue(blocked.routes().isEmpty());
		assertTrue(blocked.issues().stream().anyMatch(issue -> issue.message().contains("not triggered by an Operation")),
				blocked.issues().toString());
	}

	@Test
	void anOperationInsideAListCannotBeDelegated() {
		var plan=explicitResponsePlan("aid-command");
		var source=plan.mappings().get(0).sources().get(0);
		// Skills[0].Move.operation: the skill keeps its name, but the path holds a list index.
		var listed=AasFixtures.reference("Submodel","cci","SubmodelElementList","Skills","SubmodelElementCollection","0",
				"SubmodelElementCollection","Move","Operation","operation");
		MappingPlan m=plan.mappings().get(0);
		var blocked=factory.create(new Plan("aimc",List.of(new MappingPlan(m.identity(),m.listIndex(),
				List.of(new EndpointPlan(source.id(),listed,source.target())),m.sinks(),m.defaultPollingInterval(),m.transformation(),
				m.responseSources(),m.responseSinks(),m.responseTransformation(),m.qualifiers()))),"bridge",
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081")));
		assertTrue(blocked.routes().isEmpty());
		assertTrue(blocked.issues().stream().anyMatch(issue -> issue.message().contains("inside a SubmodelElementList")),
				blocked.issues().toString());
	}

	@Test
	void typedPropertyListsAreTypedEvenWhenEmpty() {
		var list=new org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementList.Builder().idShort("Readings")
				.typeValueListElement(org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements.PROPERTY)
				.valueTypeListElement(dk.aau.smartproductionlab.dmp.aas.model.Aas.xsd("xs:double")).build();
		var target=new DirectTarget(new AasReferenceResolver.ResolvedElement(
				AasFixtures.submodel("urn:owner/submodels/Variables","Variables"),list));
		var reference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","SubmodelElementList","Readings");
		var source=new EndpointPlan("Readings",reference,target,Presence.of("1"));
		var out=new DirectTarget(new AasReferenceResolver.ResolvedElement(
				AasFixtures.submodel("urn:owner/submodels/Variables","Variables"),property("Out","")));
		var plan=new Plan(List.of(mappingPlan("list",0,source,new EndpointPlan("Out",
				AasFixtures.reference("Submodel","urn:owner/submodels/Variables","Property","Out"),out))));
		var result=factory.create(plan,"route",new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081")));
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		assertEquals(Map.of("type","array","items",Map.of("type","number")),sources(result).get(0).schema());
	}

	@Test void mqttBasicTakesTheBrokersCredentialsAsBoundReferences() {
		SubmodelElement metadata=AasFixtures.collection("EndpointMetadata",property("base","mqtt://broker.example:1883/site/"),
				AasFixtures.securityList(AasFixtures.schemeReference("interface_mqtt","basic_sc")),
				AasFixtures.collection("securityDefinitions",scheme("basic_sc","basic")));
		var source=new EndpointPlan("State",reference(),mqttTarget(metadata,mqttForm("state","0",null),"State"));
		var sink=new EndpointPlan("Command",reference(),mqttTarget(metadata,mqttForm("command","1",null),"Command"));
		var mapping=new MappingPlan("Broker",0,List.of(source),List.of(sink),Presence.absent(),
				Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent(),List.of());
		var credentials=DmpCredentials.parse("""
				{"endpoints":{"mqtt://broker.example:1883":{"username":"dmp","password":"s3cret"}}}""");
		var result=factory.create(new Plan(List.of(mapping)),"route",
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081"),null,credentials));
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		String uri=uri(sources(result).get(0).endpoint());
		assertTrue(uri.contains("#bean:dmp-credential-") && !uri.contains("s3cret"),uri);
	}

	@Test void credentialsInTheAidAreRefused() {
		var result=factory.createBundles(credentialsPlan("one-password"),null);
		assertTrue(result.bundles().isEmpty());
		assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains("credentials do not belong in the AID")),
				result.issues().toString());
		assertFalse(result.issues().toString().contains("one-password"));
	}

	/** An HTTP interface on device:8080 selecting {@code selected} among {@code schemes}. */
	private static AidTarget securedHttp(List<String> selected,SubmodelElement... schemes) {
		SubmodelElement metadata=AasFixtures.collection("EndpointMetadata",property("base","http://device:8080"),
				AasFixtures.securityList(selected.stream().map(name -> AasFixtures.schemeReference("interface_device",name))
						.toArray(org.eclipse.digitaltwin.aas4j.v3.model.Reference[]::new)),
				AasFixtures.collection("securityDefinitions",schemes));
		return protocolTarget(WireModel.ProtocolFamily.HTTP,"/state",Map.of(),"GET",metadata);
	}

	private static SubmodelElement scheme(String idShort,String kind,SubmodelElement... terms) {
		List<SubmodelElement> children=new ArrayList<>(List.of(property("scheme",kind)));
		children.addAll(List.of(terms));
		return AasFixtures.collection(idShort,children.toArray(SubmodelElement[]::new));
	}

	private ConnectorConfigurationFactory.Result secured(AidTarget source,DmpCredentials credentials) {
		var sink=endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"POST"));
		return factory.create(new Plan(List.of(mappingPlan("secured",0,polledEndpointPlan("read",source),sink))),"route",
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081"),null,credentials));
	}

	@Test void aidSecuritySchemesAreAnsweredFromTheDmpsCredentialsNeverTheAas() {
		var basic=securedHttp(List.of("basic_sc"),scheme("basic_sc","basic"));
		var missing=secured(basic,DmpCredentials.none());
		assertTrue(missing.issues().stream().anyMatch(issue -> issue.message().contains("no credentials for basic_sc of http://device:8080")),
				missing.issues().toString());

		var credentials=DmpCredentials.parse("""
				{"endpoints":{"http://device:8080":{"username":"dmp","password":"p@ss","bearerToken":"t0ken","apiKey":"k3y"}}}""");
		var withBasic=secured(basic,credentials);
		assertTrue(withBasic.issues().isEmpty(),withBasic.issues().toString());
		String source=uri(sources(withBasic).get(0).endpoint());
		assertFalse(source.contains("p@ss") || source.contains("dmp:"),source);

		var apikey=secured(securedHttp(List.of("apikey_sc"),scheme("apikey_sc","apikey",property("name","key"))),credentials);
		assertTrue(apikey.issues().isEmpty(),apikey.issues().toString());
		String uri=uri(sources(apikey).get(0).endpoint());
		assertFalse(uri.contains("k3y"),"an API key in the query is bound, not written into the URI: "+uri);

		var bearer=secured(securedHttp(List.of("bearer_sc"),scheme("bearer_sc","bearer")),credentials);
		assertTrue(bearer.issues().isEmpty(),bearer.issues().toString());

		// combo_sc oneOf takes the first alternative the DMP applies: digest is not, basic is.
		var combo=secured(securedHttp(List.of("combo_sc"),scheme("digest_sc","digest"),scheme("basic_sc","basic"),
				scheme("combo_sc","combo",new org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementList.Builder()
						.idShort("oneOf").value(List.of(
								new org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultReferenceElement.Builder()
										.value(AasFixtures.schemeReference("interface_device","digest_sc")).build(),
								new org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultReferenceElement.Builder()
										.value(AasFixtures.schemeReference("interface_device","basic_sc")).build())).build())),credentials);
		assertTrue(combo.issues().isEmpty(),combo.issues().toString());

		for(var blocked:List.of(
				Map.entry(securedHttp(List.of("digest_sc"),scheme("digest_sc","digest")),"digest (digest_sc) is not applied"),
				Map.entry(securedHttp(List.of(),scheme("nosec_sc","nosec")),"at least one security scheme"),
				Map.entry(securedHttp(List.of("missing_sc"),scheme("nosec_sc","nosec")),"securityDefinitions does not define"))) {
			var result=secured(blocked.getKey(),credentials);
			assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains(blocked.getValue())),result.issues().toString());
		}
		// OPC UA applies nosec only until the DMP opens its own sessions (PLAN H4).
		SubmodelElement uaMetadata=AasFixtures.collection("EndpointMetadata",property("base","opc.tcp://ua:4840"),
				AasFixtures.securityList(AasFixtures.schemeReference("interface_device","auth_sc")),
				AasFixtures.collection("securityDefinitions",scheme("auth_sc","uav_authentication_")));
		var uaSource=polledEndpointPlan("read",protocolTarget(WireModel.ProtocolFamily.OPCUA,"/?id=ns=1;s=x",Map.of(),null,uaMetadata));
		var uaSink=endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"POST"));
		var opcua=factory.create(new Plan(List.of(mappingPlan("ua",0,uaSource,uaSink))),"route",
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081"),null,credentials));
		assertTrue(opcua.issues().stream().anyMatch(issue -> issue.message().contains("not applied to OPCUA")),opcua.issues().toString());
	}

	@Test void authoredMappingIdentitySurvivesListReorderingAndEffectiveOptionsChangeFingerprint() {
		var first=mapping(WireModel.ProtocolFamily.HTTP,true);
		var relocated=new MappingPlan(first.identity(),17,first.sources(),first.sinks(),first.defaultPollingInterval(),
				first.transformation(),first.responseSources(),first.responseSinks(),first.responseTransformation(),first.qualifiers());
		var a=factory.createBundles(new Plan(List.of(first)),null).bundles().get(0);
		var b=factory.createBundles(new Plan(List.of(relocated)),null).bundles().get(0);
		assertEquals(a.bundleId(),b.bundleId());assertEquals(a.routeId(),b.routeId());
		assertTrue(a.fingerprint().sameEffectiveConfiguration(b.fingerprint()));
		var interval=new MappingPlan(first.identity(),first.listIndex(),first.sources(),first.sinks(),
				Presence.of("7"),first.transformation(),first.responseSources(),first.responseSinks(),
				first.responseTransformation(),first.qualifiers());
		var changed=factory.createBundles(new Plan(List.of(interval)),null).bundles().get(0);
		assertFalse(a.fingerprint().sameEffectiveConfiguration(changed.fingerprint()));
		assertFalse(a.fingerprint().exported().equals(changed.fingerprint().exported()));
	}

	@Test void defaultPollingDoesNotConvertMqttSubscriptionToQueueSampling() {
		var source=endpointPlan("mqtt",protocolTarget(WireModel.ProtocolFamily.MQTT,"mqtt://broker.example:1883","/state",Map.of("mqv_retain","true"),null));
		var sink=endpointPlan("http",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"POST"));
		var map=mappingPlan("fan",0,source,sink);
		map=new MappingPlan(map.identity(),0,map.sources(),map.sinks(),Presence.of("2"),
				map.transformation(),map.responseSources(),map.responseSinks(),map.responseTransformation(),map.qualifiers());
		var result=factory.create(new Plan(List.of(map)),"route");
		assertTrue(result.issues().isEmpty());
		var configured=sources(result).get(0);
		assertEquals(Source.Delivery.OBSERVED,configured.delivery());assertEquals(0,configured.pollingIntervalMs());
	}

	@Test void aSourceWithoutAPositiveIntervalIsEventBasedAndOneThatCanOnlyBeReadIsBlocked() {
		var get=protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"GET");
		var sink=endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"POST"));
		var modbus=protocolTarget(WireModel.ProtocolFamily.MODBUS,"modbus+tcp://plc.example:502","/7/10",
				Map.of("modv_entity","HoldingRegister","modv_pollingTime","250"),null);
		for(var unpaced:List.of(mappingPlan("none",0,endpointPlan("read",get),sink),
				withDefaultPollingInterval(mappingPlan("zero-default",0,endpointPlan("read",get),sink),"0.0"),
				withDefaultPollingInterval(mappingPlan("zero-own",0,new EndpointPlan("read",reference(),get,Presence.of("0")),sink),"2"),
				mappingPlan("modbus",0,endpointPlan("read",modbus),sink))) {
			var result=factory.createBundles(new Plan(List.of(unpaced)),null);
			assertTrue(result.bundles().isEmpty(),unpaced.identity()+" assumes no interval");
			assertTrue(result.issues().stream().anyMatch(issue -> issue.path().endsWith("/Sources/value/0")
					&& issue.message().contains("can only be read")),String.valueOf(result.issues()));
		}
		var paced=factory.create(new Plan(List.of(withDefaultPollingInterval(mappingPlan("default",0,endpointPlan("read",get),sink),"2"))),"route");
		assertTrue(paced.issues().isEmpty(),String.valueOf(paced.issues()));
		assertEquals(2000,sources(paced).get(0).pollingIntervalMs());

		var mqtt=new EndpointPlan("state",reference(),protocolTarget(WireModel.ProtocolFamily.MQTT,"mqtt://broker.example:1883","/state",
				Map.of("mqv_retain","true"),null),Presence.of("0"));
		var observed=factory.create(new Plan(List.of(mappingPlan("mqtt",0,mqtt,sink))),"route");
		assertTrue(observed.issues().isEmpty(),"a PollingInterval of 0 observes: "+observed.issues());
		assertEquals(Source.Delivery.OBSERVED,sources(observed).get(0).delivery());
	}

	private static MappingPlan withDefaultPollingInterval(MappingPlan map,String seconds) {
		return new MappingPlan(map.identity(),map.listIndex(),map.sources(),map.sinks(),Presence.of(seconds),
				map.transformation(),map.responseSources(),map.responseSinks(),map.responseTransformation(),map.qualifiers());
	}

	@Test void authoredUnknownProtocolOptionsBlockOnlyTheirMapping() {
		var bad=endpointPlan("bad",protocolTarget(WireModel.ProtocolFamily.OPCUA,"opc.tcp://ua.example:4840/service", "ns=2;s=Value",
				Map.of("uav_securityPolicy","unsupported"),null));
		var invalid=mappingPlan("blocked",0,bad,bad);
		var valid=mappingPlan("valid",1,polledEndpointPlan("read",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"GET")),
				endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"POST")));
		var result=factory.createBundles(new Plan(List.of(invalid,valid)),null);
		assertEquals(1,result.bundles().size());assertEquals("valid",result.bundles().get(0).mappingIdentity());
		assertTrue(result.issues().stream().allMatch(issue -> issue.mappingId().equals("blocked")));
		assertTrue(result.issues().stream().anyMatch(issue -> issue.path().endsWith("uav_securityPolicy")));
	}

	private static Plan credentialsPlan(String password) {
		var source=polledEndpointPlan("read",protocolTarget(WireModel.ProtocolFamily.HTTP,
				"http://alice:"+password+"@api.example:8080", "/state",Map.of(),"GET"));
		var sink=endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"POST"));
		return new Plan(List.of(mappingPlan("credentials",0,source,sink)));
	}

	@Test void compilesExactActionOutputProjectionCallerReplyAndGroupedExtraWrites() {
		var plan=explicitResponsePlan("aid-command");
		var result=factory.create(plan,"bridge",new ConnectorConfigurationFactory.RuntimeSettings(
				URI.create("http://aas.example:8081")));
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var bridge=assertInstanceOf(OperationRoute.class,result.routes().get(0));
		assertTrue(bridge.response()!=null);
		assertEquals("Reply",bridge.caller().replySinkId());
		assertEquals(List.of(new Response.ResponseSource("Body,&#",null,"$.Count")),bridge.response().sources());
		assertEquals(2,bridge.endpoints().stream().filter(LuaScript.class::isInstance).count());
		assertTrue(bridge.requestTransformation()!=bridge.response().transformation(),"response Lua must not execute in the request chain");
		assertInstanceOf(LuaScript.class,bridge.requestTransformation());
		assertInstanceOf(LuaScript.class,bridge.response().transformation());
		assertEquals(Map.of("Count","integer"),bridge.command().responseTypes());
		assertEquals(Map.of("Count","xs:string"),bridge.caller().responseTypes());
		var responseSinks=bridge.response().sinks();
		assertEquals(2,responseSinks.size(),"caller Operation is a reply, not another device write");
		assertEquals(responseSinks.get(0).payloadGroup(),responseSinks.get(1).payloadGroup());
		assertEquals("$.x",responseSinks.get(0).valuePath());
		assertEquals("$.y",responseSinks.get(1).valuePath());
		assertEquals(3,1+responseSinks.size(),"one action target plus grouped response members");
	}

	@Test void scalarCciInoutputsAreCallerInputsAndOutputs() {
		var limit=AasFixtures.property("Limit","xs:int",null);
		var inoutputs=List.of(AasFixtures.variable(property("Uuid","id")),AasFixtures.variable(property("Trace","trace")),
				AasFixtures.variable(limit));
		var result=factory.create(explicitResponsePlan("aid-command",inoutputs),"bridge");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var bridge=(OperationRoute)result.routes().get(0);
		assertEquals(Map.of("Uuid","xs:string","Trace","xs:string","Limit","xs:int","Count","xs:string"),
				bridge.caller().responseTypes());
		assertEquals(List.of("Uuid","Trace","Limit","Count"),bridge.caller().outputOrder());
	}

	@Test void structuredCciVariablesAreDeclaredTemplates() {
		var position=AasFixtures.collection("Target",AasFixtures.property("x","xs:double",null),
				AasFixtures.property("y","xs:double",null));
		var waypoints=new DefaultSubmodelElementList.Builder().idShort("Waypoints")
				.typeValueListElement(AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION)
				.value(List.of(AasFixtures.collection(null,AasFixtures.property("x","xs:double",null)))).build();
		var codes=new DefaultSubmodelElementList.Builder().idShort("Codes").typeValueListElement(AasSubmodelElements.PROPERTY)
				.valueTypeListElement(Aas.xsd("xs:int")).build();
		var result=factory.create(explicitResponsePlan("aid-command",
				List.of(AasFixtures.variable(position),AasFixtures.variable(waypoints),AasFixtures.variable(codes))),"bridge");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var bridge=(OperationRoute)result.routes().get(0);
		assertEquals(List.of("Target","Waypoints","Codes","Count"),bridge.caller().outputOrder());
		assertEquals(Map.of("Count","xs:string"),bridge.caller().responseTypes(),
				"only scalar Properties have a scalar caller type");
		assertEquals(Set.of("Target","Waypoints","Codes"),bridge.caller().inputTemplates().keySet());
		assertEquals(Set.of("Target","Waypoints","Codes","Count"),bridge.caller().outputTemplates().keySet());
		assertEquals(position,Aas.read(bridge.caller().outputTemplates().get("Target"),SubmodelElement.class));
		assertEquals(waypoints,Aas.read(bridge.caller().inputTemplates().get("Waypoints"),SubmodelElement.class));
	}

	@Test void rejectsUnsupportedStructuredAndDuplicateCciInoutputs() {
		var customType=AasFixtures.property("Uuid","xs:dateTime",null);
		var nestedCustomType=AasFixtures.collection("Uuid",AasFixtures.property("at","xs:dateTime",null));
		var duplicateMembers=AasFixtures.collection("Uuid",property("a","1"),property("a","2"));
		var mixedList=new DefaultSubmodelElementList.Builder().idShort("Uuid").typeValueListElement(AasSubmodelElements.PROPERTY)
				.valueTypeListElement(Aas.xsd("xs:int")).value(List.of(property("a","x"))).build();
		var untypedEmptyList=new DefaultSubmodelElementList.Builder().idShort("Uuid")
				.typeValueListElement(AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION).build();
		var file=new DefaultFile.Builder().idShort("Uuid").contentType("text/plain").build();
		var trace=AasFixtures.variable(property("Trace","trace"));
		for (var inoutputs : List.of(List.of(AasFixtures.variable(customType)),List.of(AasFixtures.variable(nestedCustomType)),
				List.of(AasFixtures.variable(duplicateMembers)),List.of(AasFixtures.variable(mixedList)),
				List.of(AasFixtures.variable(untypedEmptyList)),List.of(AasFixtures.variable(file)),List.of(trace,trace))) {
			var result=factory.create(explicitResponsePlan("aid-command",inoutputs),"bridge");
			assertTrue(result.routes().isEmpty(),inoutputs.toString());
			assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains("CCI inoutput")),
					"InOut variables must be supported Properties, collections or lists with unique names");
		}
	}

	@Test void aCommandSinkAddressesTheActionsInput() {
		var base=explicitResponsePlan("aid-command");
		var original=base.mappings().get(0);
		var command=(AidTarget)original.sinks().get(0).target();
		for(var part:List.of(AasReferenceResolver.SchemaPart.NONE,AasReferenceResolver.SchemaPart.OUTPUT)) {
			var target=command.resolved();
			var retargeted=new AidTarget(new AasReferenceResolver.AidTarget(target.owner(),target.affordance(),target.schema(),
					target.schemaPath(),target.submodelId(),part));
			var mapping=new MappingPlan(original.identity(),original.listIndex(),original.sources(),
					List.of(new EndpointPlan("Move",original.sinks().get(0).reference(),retargeted)),original.defaultPollingInterval(),
					original.transformation(),original.responseSources(),original.responseSinks(),original.responseTransformation(),original.qualifiers());
			var result=factory.create(new Plan(base.aimcSubmodelId(),List.of(mapping)),"bridge");
			assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains("addresses the action's input")),
					part+": "+result.issues());
		}
	}

	@Test void identicalRemoteActionShapeCannotSubstituteForExactResponseAnchor() {
		var result=factory.create(explicitResponsePlan("aid-remote"),"bridge");
		assertTrue(result.routes().isEmpty());
		assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains("exact command ACTION anchor")));
	}

	/** The explicit response plan with its response parts replaced. */
	private static Plan withResponse(Plan base,Presence<List<EndpointPlan>> sources,Presence<List<EndpointPlan>> sinks,
			Presence<WireModel.Blob> transformation) {
		var original=base.mappings().get(0);
		return new Plan(base.aimcSubmodelId(),List.of(new MappingPlan(original.identity(),original.listIndex(),original.sources(),
				original.sinks(),original.defaultPollingInterval(),original.transformation(),sources,sinks,transformation,original.qualifiers())));
	}

	@Test void operationOutputsAreNeverMatchedByNameWithoutAResponseMapping() {
		// The device output Count has the same name as the Operation's output Count; that is no mapping.
		var result=factory.create(withResponse(explicitResponsePlan("aid-command"),Presence.absent(),Presence.absent(),
				Presence.absent()),"bridge");
		assertTrue(result.routes().isEmpty());
		assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains(
				"need ResponseSources, a ResponseTransformation and a ResponseSink selecting the Operation")),
				result.issues().toString());
	}

	/** The explicit response plan whose Operation output Count carries the Optional qualifier {@code value}. */
	private static Plan withOptionalCount(String value) {
		var base=explicitResponsePlan("aid-command");
		var operation=(org.eclipse.digitaltwin.aas4j.v3.model.Operation)((DirectTarget)base.mappings().get(0).sources().get(0)
				.target()).resolved().element();
		operation.getOutputVariables().get(0).getValue().setQualifiers(List.of(
				new org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultQualifier.Builder().type("Optional")
						.valueType(org.eclipse.digitaltwin.aas4j.v3.model.DataTypeDefXsd.BOOLEAN).value(value)
						.semanticId(new org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultReference.Builder()
								.type(org.eclipse.digitaltwin.aas4j.v3.model.ReferenceTypes.EXTERNAL_REFERENCE)
								.keys(new org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultKey.Builder()
										.type(org.eclipse.digitaltwin.aas4j.v3.model.KeyTypes.GLOBAL_REFERENCE)
										.value(dk.aau.smartproductionlab.dmp.aas.operation.OptionalQualifier.SEMANTIC_ID).build())
								.build()).build()));
		return base;
	}

	@Test void anOperationWhoseOutputsAreAllOptionalNeedsNoResponseMapping() {
		var result=factory.create(withResponse(withOptionalCount("true"),Presence.absent(),Presence.absent(),Presence.absent()),"bridge");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var bridge=(OperationRoute)result.routes().get(0);
		assertEquals(null,bridge.response(),"no explicit response mapping");
		// A response mapping that only fans out may leave out the caller's ResponseSink too.
		var base=withOptionalCount("true");
		var original=base.mappings().get(0);
		var fanOutOnly=factory.create(withResponse(base,original.responseSources(),
				Presence.of(original.responseSinks().value().subList(1,3)),original.responseTransformation()),"bridge");
		assertTrue(fanOutOnly.issues().isEmpty(),fanOutOnly.issues().toString());
	}

	@Test void anUnreadableOptionalQualifierBlocksTheMapping() {
		var result=factory.create(withOptionalCount("maybe"),"bridge");
		assertTrue(result.routes().isEmpty());
		assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains(
				"the Optional qualifier needs an xs:boolean value")),result.issues().toString());
	}

	@Test void aResponseMappingIsAuthoredAsAWhole() {
		var base=explicitResponsePlan("aid-command");
		var original=base.mappings().get(0);
		var withoutLua=factory.create(withResponse(base,original.responseSources(),original.responseSinks(),Presence.absent()),"bridge");
		assertTrue(withoutLua.issues().stream().anyMatch(issue -> issue.message().contains("requires a ResponseTransformation")),
				withoutLua.issues().toString());
		var withoutSources=factory.create(withResponse(base,Presence.absent(),original.responseSinks(),
				original.responseTransformation()),"bridge");
		assertTrue(withoutSources.issues().stream().anyMatch(issue -> issue.message().contains("requires ResponseSources")),
				withoutSources.issues().toString());
		var withoutSinks=factory.create(withResponse(base,original.responseSources(),Presence.absent(),
				original.responseTransformation()),"bridge");
		assertTrue(withoutSinks.issues().stream().anyMatch(issue -> issue.message().contains("requires ResponseSinks")),
				withoutSinks.issues().toString());
		for(var result:List.of(withoutLua,withoutSources,withoutSinks))assertTrue(result.routes().isEmpty());
	}

	@Test void operationOutputsNeedTheResponseSinkSelectingTheOperation() {
		var base=explicitResponsePlan("aid-command");
		var original=base.mappings().get(0);
		// Only the AAS fan-out sinks X and Y, not the caller Operation.
		var result=factory.create(withResponse(base,original.responseSources(),
				Presence.of(original.responseSinks().value().subList(1,3)),original.responseTransformation()),"bridge");
		assertTrue(result.routes().isEmpty());
		assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains("need a ResponseSink selecting the Operation")),
				result.issues().toString());
	}

	@Test void responseSourcesMayFanOutTheSameMemberToDistinctSourceIds() {
		var base=explicitResponsePlan("aid-command");
		var original=base.mappings().get(0);
		var response=original.responseSources().value().get(0);
		var duplicate=new EndpointPlan("reply-count-copy",response.reference(),response.target());
		var mapping=new MappingPlan(original.identity(),original.listIndex(),original.sources(),original.sinks(),
				original.defaultPollingInterval(),original.transformation(),Presence.of(List.of(response,duplicate)),
				original.responseSinks(),original.responseTransformation(),original.qualifiers());
		var result=factory.create(new Plan(base.aimcSubmodelId(),List.of(mapping)),"bridge");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var bridge=(OperationRoute)result.routes().get(0);
		assertEquals(List.of(new Response.ResponseSource("Body,&#",null,"$.Count"),
				new Response.ResponseSource("reply-count-copy",null,"$.Count")),bridge.response().sources());
	}

	@Test void methodArgumentTypesPreserveArrayRankAndTypedPropertySchemas() {
		var base=explicitResponsePlan("aid-command");
		var original=base.mappings().get(0);
		var sink=original.sinks().get(0);
		var target=((AidTarget)sink.target()).resolved();
		var integer=dataSchema("integer",Map.of());
		var array=new WireModel.DataSchema(Presence.absent(),Presence.absent(),Presence.absent(),Presence.of("array"),
				Map.of(),Presence.of(integer),Presence.absent(),Map.of(),emptyElement());
		var matrix=new WireModel.DataSchema(Presence.absent(),Presence.absent(),Presence.absent(),Presence.of("array"),
				Map.of(),Presence.of(array),Presence.absent(),Map.of(),emptyElement());
		var property=new WireModel.DataSchema(Presence.of("xs:string"),Presence.absent(),Presence.absent(),Presence.absent(),
				Map.of(),Presence.absent(),Presence.absent(),Map.of(),emptyElement());
		var output=dataSchema("object",Map.of("Count",integer,"Samples",array,"Matrix",matrix,"Label",property));
		var action=target.affordance();
		var typedAction=new WireModel.Affordance(action.kind(),action.idShort(),action.semanticId(),action.forms(),
				action.schema(),action.input(),Presence.of(output),action.element());
		var typedTarget=new AidTarget(new AasReferenceResolver.AidTarget(target.owner(),typedAction,target.schema(),
				target.schemaPath(),target.submodelId(),target.part()));
		// The Operation's output Count still comes from the response mapping, now on the typed action.
		var responseSource=original.responseSources().value().get(0);
		var typedCount=new AidTarget(new AasReferenceResolver.AidTarget(target.owner(),typedAction,Presence.of(integer),
				List.of("Count"),target.submodelId(),AasReferenceResolver.SchemaPart.OUTPUT));
		var mapping=new MappingPlan(original.identity(),original.listIndex(),original.sources(),
				List.of(new EndpointPlan(sink.id(),sink.reference(),typedTarget)),original.defaultPollingInterval(),
				original.transformation(),Presence.of(List.of(new EndpointPlan(responseSource.id(),responseSource.reference(),typedCount))),
				Presence.of(List.of(original.responseSinks().value().get(0))),original.responseTransformation(),original.qualifiers());
		var result=factory.create(new Plan(base.aimcSubmodelId(),List.of(mapping)),"bridge");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var bridge=(OperationRoute)result.routes().get(0);
		assertEquals(Map.of("Count","integer","Samples","array<integer>","Matrix","array<array<integer>>",
				"Label","string"),bridge.command().responseTypes());
	}

	@Test void compiledResponseLuaAndRealFanOutBoundaryEmitOneCompletePayload() throws Exception {
		var result=factory.create(explicitResponsePlan("aid-command"),"bridge");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var bridge=(OperationRoute)result.routes().get(0);
		var writes=new java.util.concurrent.atomic.AtomicInteger();
		var payload=new java.util.concurrent.atomic.AtomicReference<Object>();
		List<ManagedRouteResource> owned=new ArrayList<>();
		try(var context=new org.apache.camel.impl.DefaultCamelContext()) {
			context.addRoutes(new org.apache.camel.builder.RouteBuilder() {
				@Override public void configure() {
					from("direct:response-write").process(exchange -> { writes.incrementAndGet();payload.set(exchange.getMessage().getBody()); });
				}
			});
			// The response Lua binds its compiled source in the context, as a bundle would.
			String lua=bridge.response().transformation().uri(new BundleResources() {
				@Override public <T extends ManagedRouteResource> T own(T resource) {
					try { resource.bind(context); } catch (Exception e) { throw new IllegalStateException(e); }
					owned.add(resource);
					return resource;
				}
			});
			context.start();
			List<dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.ResponseSinkFanOutProcessor.Target> targets=
					bridge.response().sinks().stream().map(sink -> new dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline
							.ResponseSinkFanOutProcessor.Target(sink.sinkId(),"direct:response-write",sink.payloadGroup(),sink.valuePath(),
							sink.payloadSchema(),sink.schema(),sink.endpoint().jsonWire())).toList();
			try(var template=context.createProducerTemplate();var fanout=new dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.ResponseSinkFanOutProcessor(context,"compiled-response",targets)) {
				Object projected=dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.ResponseProjectionProcessor.project(
						Map.of("Count",42),bridge.response().sources());
				Map<?,?> transformed=template.requestBody(lua,projected,Map.class);
				assertEquals(Map.of("Count","42"),transformed.get("Reply"));
				var exchange=new org.apache.camel.support.DefaultExchange(context);exchange.getMessage().setBody(transformed);
				var jobs=fanout.prepareJobs(exchange);assertEquals(1,jobs.size());
				String writeRoute="direct:compiled-response-write";
				context.addRoutes(new org.apache.camel.builder.RouteBuilder() {
					@Override public void configure() { fanout.configureWrite(from(writeRoute)); }
				});
				template.sendBody(writeRoute,jobs.get(0));
				assertEquals(1,writes.get());assertEquals(Map.of("x",1L,"y",2L),payload.get());
				exchange.getMessage().setBody(Map.of("X",1));assertTrue(fanout.prepareJobs(exchange).isEmpty());
				assertEquals(1,writes.get(),"missing group members must not emit a partial payload");
			} finally { context.stop();for(var resource:owned)resource.close(); }
		}
	}

	/** {@code endpoint}'s Camel URI. */
	private static String uri(Endpoint endpoint) {
		return endpoint.uri(RESOURCES);
	}

	/** The sources of the ordinary mappings {@code result} compiled, in order. */
	private static List<Source> sources(ConnectorConfigurationFactory.Result result) {
		return result.routes().stream().filter(MappingRoute.class::isInstance).map(MappingRoute.class::cast)
				.flatMap(route -> route.sources().stream()).toList();
	}

	/** The sinks of the ordinary mappings {@code result} compiled, in order. */
	private static List<Sink> sinks(ConnectorConfigurationFactory.Result result) {
		return result.routes().stream().filter(MappingRoute.class::isInstance).map(MappingRoute.class::cast)
				.flatMap(route -> route.sinks().stream()).toList();
	}

	/** The first endpoint of {@code type} among those of {@code sourcesOrSinks}. */
	private static <T extends Endpoint> T endpoint(List<?> sourcesOrSinks,Class<T> type) {
		return sourcesOrSinks.stream().map(each -> each instanceof Source source?source.endpoint():((Sink)each).endpoint())
				.filter(type::isInstance).map(type::cast).findFirst().orElseThrow();
	}

	/** The delegation URL of the test's Operation, Skills.Move.operation in submodel {@code cci}. */
	private static final String MOVE_URL="http://dmp:8080"+OperationPaths.invokePath("cci","Skills.Move.operation");

	private static Plan explicitResponsePlan(String responseAnchor) {
		return explicitResponsePlan(responseAnchor,List.of());
	}
	private static Plan explicitResponsePlan(String responseAnchor,List<OperationVariable> inoutputs) {
		var callerProperty=property("Count","0");
		var operation=AasFixtures.operation("operation",List.of(),List.of(AasFixtures.variable(callerProperty)),inoutputs);
		operation.setQualifiers(List.of(AasFixtures.delegation(MOVE_URL)));
		var direct=new DirectTarget(new AasReferenceResolver.ResolvedElement(AasFixtures.submodel("cci","CCI",operation),operation));
		var operationRef=AasFixtures.reference("Submodel","cci","SubmodelElementCollection","Skills",
				"SubmodelElementCollection","Move","Operation","operation");
		var source=new EndpointPlan("invoke",operationRef,direct);
		var base=protocolTarget(WireModel.ProtocolFamily.HTTP,"http://device.example:8080","/move",Map.of(),"POST");
		var input=dataSchema("object",Map.of());
		var count=dataSchema("integer",Map.of());var output=dataSchema("object",Map.of("Count",count));
		var action=new WireModel.Affordance("action",Presence.of("Move"),Presence.absent(),base.resolved().affordance().forms(),
				Presence.absent(),Presence.of(input),Presence.of(output),emptyElement());
		var iface=base.resolved().owner();
		var command=new AidTarget(new AasReferenceResolver.AidTarget(iface,action,Presence.of(input),List.of(),"aid-command",AasReferenceResolver.SchemaPart.INPUT));
		var response=new AidTarget(new AasReferenceResolver.AidTarget(iface,action,Presence.of(count),List.of("Count"),responseAnchor,AasReferenceResolver.SchemaPart.OUTPUT));
		var commandEndpoint=new EndpointPlan("Move",actionReference("aid-command","input"),command);
		var responseSource=new EndpointPlan("Body,&#",actionReference(responseAnchor,"output"),response);
		var extraBase=protocolTarget(WireModel.ProtocolFamily.HTTP,"http://extra.example:8080","/state",Map.of(),"POST");
		var payload=dataSchema("object",Map.of("x",count,"y",count));
		var extraAffordance=new WireModel.Affordance("property",Presence.of("State"),Presence.absent(),extraBase.resolved().affordance().forms(),
				Presence.of(payload),Presence.absent(),Presence.absent(),emptyElement());
		var x=new AidTarget(new AasReferenceResolver.AidTarget(extraBase.resolved().owner(),extraAffordance,Presence.of(count),List.of("x"),"aid-extra"));
		var y=new AidTarget(new AasReferenceResolver.AidTarget(extraBase.resolved().owner(),extraAffordance,Presence.of(count),List.of("y"),"aid-extra"));
		var mapping=new MappingPlan("responses",0,List.of(source),List.of(commandEndpoint),Presence.absent(),
				Presence.of(new WireModel.Blob("function aimc_main(sources) return {Move=sources.invoke} end".getBytes(StandardCharsets.UTF_8),"text/x-lua")),
				Presence.of(List.of(responseSource)),Presence.of(List.of(new EndpointPlan("Reply",operationRef,direct),
						new EndpointPlan("X",reference(),x),new EndpointPlan("Y",reference(),y))),
				Presence.of(new WireModel.Blob("function aimc_main(sources) return {Reply={Count=tostring(sources['Body,&#'])},X=1,Y=2} end".getBytes(StandardCharsets.UTF_8),"text/x-lua")),List.of());
		return new Plan("aimc",List.of(mapping));
	}

	@Test
	void compilesMultipleCallerVariableSourcesAndResponseSinksAroundOneOperation() {
		var mapping=variableOperationMapping(false);
		var result=factory.create(new Plan("aimc",List.of(mapping)),"bridge");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var bridge=assertInstanceOf(OperationRoute.class,result.routes().get(0));
		assertEquals(Map.of("x-source","$.Position.x","y-source","$.Position.y"),bridge.caller().inputSourcePaths());
		assertEquals(Map.of("count-sink","$.Count"),bridge.caller().replyBindings());
		assertEquals(List.of(),bridge.response().sinks(),"caller variable sinks are assembled into the one operation reply");
	}

	@Test
	void twoResponseSinksMayNotAnswerTheSameCallerVariable() {
		var result=factory.create(new Plan("aimc",List.of(variableOperationMapping(true))),"bridge");
		assertTrue(result.issues().stream().anyMatch(issue -> issue.toString().contains("only one ResponseSink may select the caller variable Count")),
				result.issues().toString());
	}

	/** A Move Operation whose Position members are separate Sources and whose Count output is a ResponseSink. */
	private MappingPlan variableOperationMapping(boolean duplicateCountSink) {
		var x=property("x","0");
		var y=property("y","0");
		var position=AasFixtures.collection("Position",x,y);
		var count=property("Count","0");
		var operation=AasFixtures.operation("operation",List.of(AasFixtures.variable(position)),List.of(),
				List.of(AasFixtures.variable(count)));
		operation.setQualifiers(List.of(AasFixtures.delegation(MOVE_URL)));
		var submodel=AasFixtures.submodel("cci","CCI",operation);
		String operationPath="Skills.Move.operation";
		var sourceX=new EndpointPlan("x-source",AasFixtures.reference("Submodel","cci",
				"SubmodelElementCollection","Skills","SubmodelElementCollection","Move","Operation","operation",
				"SubmodelElementCollection","Position","Property","x"),
				new DirectTarget(new AasReferenceResolver.ResolvedElement(submodel,x,operation,operationPath)));
		var sourceY=new EndpointPlan("y-source",AasFixtures.reference("Submodel","cci",
				"SubmodelElementCollection","Skills","SubmodelElementCollection","Move","Operation","operation",
				"SubmodelElementCollection","Position","Property","y"),
				new DirectTarget(new AasReferenceResolver.ResolvedElement(submodel,y,operation,operationPath)));
		var output=new EndpointPlan("count-sink",AasFixtures.reference("Submodel","cci",
				"SubmodelElementCollection","Skills","SubmodelElementCollection","Move","Operation","operation",
				"Property","Count"),new DirectTarget(new AasReferenceResolver.ResolvedElement(submodel,count,operation,operationPath)));
		var base=protocolTarget(WireModel.ProtocolFamily.HTTP,"http://device.example:8080","/move",Map.of(),"POST");
		var input=dataSchema("object",Map.of());
		var replySchema=dataSchema("integer",Map.of());
		var action=new WireModel.Affordance("action",Presence.of("Move"),Presence.absent(),base.resolved().affordance().forms(),
				Presence.absent(),Presence.of(input),Presence.of(dataSchema("object",Map.of("Count",replySchema))),emptyElement());
		var command=new AidTarget(new AasReferenceResolver.AidTarget(base.resolved().owner(),action,Presence.of(input),List.of(),"aid-command",AasReferenceResolver.SchemaPart.INPUT));
		var commandEndpoint=new EndpointPlan("Move",actionReference("aid-command","input"),command);
		var response=new AidTarget(new AasReferenceResolver.AidTarget(base.resolved().owner(),action,
				Presence.of(replySchema),List.of("Count"),"aid-command",AasReferenceResolver.SchemaPart.OUTPUT));
		var responseSource=new EndpointPlan("reply-count",actionReference("aid-command","output"),response);
		return new MappingPlan("variable-operation",0,List.of(sourceX,sourceY),List.of(commandEndpoint),Presence.absent(),
				Presence.of(new WireModel.Blob("function aimc_main(sources) return {Move={x=sources['x-source'],y=sources['y-source']}} end"
						.getBytes(StandardCharsets.UTF_8),"text/x-lua")),
				Presence.of(List.of(responseSource)),Presence.of(duplicateCountSink
						? List.of(output,new EndpointPlan("count-sink-copy",output.reference(),output.target())) : List.of(output)),
				Presence.of(new WireModel.Blob("function aimc_main(sources) return {['count-sink']=tostring(sources['reply-count'])} end"
						.getBytes(StandardCharsets.UTF_8),"text/x-lua")),List.of());
	}

	private static Reference actionReference(String anchor,String direction) {
		return AasFixtures.reference("Submodel",anchor,"SubmodelElementCollection","interface_device",
				"SubmodelElementCollection","InteractionMetadata","SubmodelElementCollection","actions",
				"SubmodelElementCollection","Move","SubmodelElementCollection",direction);
	}
	private static WireModel.DataSchema dataSchema(String type,Map<String,WireModel.DataSchema> properties) {
		return new WireModel.DataSchema(Presence.absent(),Presence.absent(),Presence.absent(),Presence.of(type),properties,
				Presence.absent(),Presence.absent(),Map.of(),emptyElement());
	}

	private static MappingPlan mapping(WireModel.ProtocolFamily family, boolean withLua) {
		var schema = Presence.<WireModel.DataSchema>absent();
		var source = new EndpointPlan("Source.authored", reference(), target(family, "GET", schema));
		var sink = new EndpointPlan("Sink.authored", reference(), target(family, "POST", schema));
		Presence<WireModel.Blob> transformation = withLua
				? Presence.of(new WireModel.Blob("function aimc_main(sources) return sources end".getBytes(StandardCharsets.UTF_8), "text/x-lua"))
				: Presence.absent();
		return new MappingPlan("logical-map", 3, List.of(source), List.of(sink), Presence.of("1"),
				transformation, Presence.absent(), Presence.absent(), Presence.absent(), List.of());
	}

	private static AidTarget target(WireModel.ProtocolFamily family, String method,
			Presence<WireModel.DataSchema> schema) {
		var form = new WireModel.Form(Presence.of("http://device.example/api/value"), Presence.absent(),
				List.of(), family == WireModel.ProtocolFamily.HTTP ? Presence.of(method) : Presence.absent(),
				Map.of(), Presence.absent(), Map.of(), emptyElement());
		var affordance = new WireModel.Affordance("property", Presence.of("value"), Presence.absent(),
				List.of(form), schema, schema, schema, emptyElement());
		var iface = new WireModel.Interface("interface-semantic", "interface-device", List.of(), List.of(family),
				emptyElement(), List.of(form), List.of(), List.of(affordance), List.of(), List.of(),
				Presence.of(AasFixtures.nosec(AasFixtures.collection("EndpointMetadata"),"interface-device")), Presence.absent(), schema, schema, Presence.absent(), Presence.absent());
		return new AidTarget(new AasReferenceResolver.AidTarget(iface, affordance, schema, List.of()));
	}

	private static AidTarget protocolTarget(WireModel.ProtocolFamily family,String base,String href,
			Map<String,String> bindingTerms, String httpMethod) {
		return protocolTarget(family,href,bindingTerms,httpMethod,
				AasFixtures.nosec(AasFixtures.collection("EndpointMetadata",property("base",base)),"interface_device"));
	}

	/** A one-form property of interface {@code interface_device} with the given endpoint metadata. */
	private static AidTarget protocolTarget(WireModel.ProtocolFamily family,String href,
			Map<String,String> bindingTerms, String httpMethod, SubmodelElement endpointMetadata) {
		Map<String,String> terms=new java.util.LinkedHashMap<>(bindingTerms);
		Map<String,String> mqtt=new java.util.LinkedHashMap<>();
		terms.forEach((name,value) -> { if(name.startsWith("mqv_"))mqtt.put(name,value); });
		var form=new WireModel.Form(Presence.of(href),Presence.absent(),List.of(),
				httpMethod==null?Presence.absent():Presence.of(httpMethod),mqtt,Presence.absent(),terms,emptyElement());
		var affordance=new WireModel.Affordance("property",Presence.of("value"),Presence.absent(),
				List.of(form),Presence.absent(),Presence.absent(),Presence.absent(),emptyElement());
		var iface=new WireModel.Interface("interface-semantic","interface_device",List.of("binding"),List.of(family),
				emptyElement(),List.of(form),List.of(),List.of(affordance),List.of(),List.of(),Presence.of(endpointMetadata),
				Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent());
		return new AidTarget(new AasReferenceResolver.AidTarget(iface,affordance,Presence.absent(),List.of()));
	}

	private static EndpointPlan endpointPlan(String id,AidTarget target) {
		return new EndpointPlan(id,reference(),target);
	}

	/** A source read every second. */
	private static EndpointPlan polledEndpointPlan(String id,AidTarget target) {
		return new EndpointPlan(id,reference(),target,Presence.of("1"));
	}

	private static MappingPlan mappingWithEndpoints(EndpointPlan source,EndpointPlan sink) {
		return mappingPlan("logical-map",0,source,sink);
	}

	private static MappingPlan mappingPlan(String identity,int index,EndpointPlan source,EndpointPlan sink) {
		return new MappingPlan(identity,index,List.of(source),List.of(sink),Presence.absent(),Presence.absent(),
				Presence.absent(),Presence.absent(),Presence.absent(),List.of());
	}

	private static Reference reference() {
		return AasFixtures.reference("Submodel", "aid");
	}
	/** An observable MQTT property (AID recommends observable for every MQTT property). */
	private static AidTarget mqttTarget(SubmodelElement endpoint, WireModel.Form form, String id) {
		var affordance = new WireModel.Affordance("property", Presence.of(id), Presence.absent(), List.of(form),
				Presence.absent(), Presence.absent(), Presence.absent(), AasFixtures.collection(id, property("observable", "true")));
		var iface = new WireModel.Interface("interface-semantic", "interface_mqtt", List.of("http://www.w3.org/2011/mqtt"),
				List.of(WireModel.ProtocolFamily.MQTT), emptyElement(), List.of(form), List.of(), List.of(affordance),
					List.of(), List.of(), Presence.of(AasFixtures.nosec(endpoint,"interface_mqtt")), Presence.absent(), Presence.absent(), Presence.absent(),
					Presence.absent(), Presence.absent());
		return new AidTarget(new AasReferenceResolver.AidTarget(iface, affordance, Presence.absent(), List.of()));
	}
	private static WireModel.Form mqttForm(String href, String qos, String retain) {
		Map<String, String> terms = new java.util.LinkedHashMap<>();
		terms.put("mqv_qos", qos);
		if (retain != null) terms.put("mqv_retain", retain);
		return new WireModel.Form(Presence.of(href), Presence.absent(), List.of(), Presence.absent(),
				terms, Presence.absent(), terms, emptyElement());
	}
	private static SubmodelElement property(String id, String value) {
		return AasFixtures.property(id, value);
	}
	private static SubmodelElement emptyElement() {
		return AasFixtures.collection(null);
	}
}
