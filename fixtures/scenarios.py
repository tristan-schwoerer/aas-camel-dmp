"""The evaluation's scenarios (evaluation/test_aas/), authored with aas-model and written as AAS JSON.

Every scenario is authored once, here; the evaluation's adapters turn it into each system's own configuration.
The JSON is generated, never edited by hand: the runner checks it against the SHA-256 in ``manifest.json``.
Regenerate from fixtures/ after changing a scenario:

    uv run python -m scenarios

Each ``<ID>.json`` is an AAS environment: one shell with an AID, an AIMC and the
``Target`` submodel. ``manifest.json`` gives the harness what it needs without
parsing the AIMC: the topic, the AIMC's id, the target element, the sink.

Northbound (experiments/northbound.yaml): the device publishes ``{"seq":…,"tSend":"…","value":"<seq>|<tSend>|xx…"}``
on an observable MQTT property; the AIMC Source references the ``value`` member of its data schema (AID 1.2 nested
schema), so a system must parse the JSON and pick the member. The value carries ``seq`` and ``tSend`` itself, so a
stored value still names its message.

- ``aas-property``      the value into the AAS string Property ``Target/Value``. Only what IDTA AIMC 2.0 and
  AID 1.2 define, so every system can be measured on it.
- ``influxdb-segment``  the value as one point of a TimeSeries segment ``Target/Series/Segments/Live`` (IDTA
  02008 LinkedSegment with the DMP's ``InfluxDb2Target``, one string field ``Value``). The DMP only.

Southbound (experiments/southbound.yaml): the AAS drives the device (tools/device_sim.py), one affordance per
mapping. The value is the string ``<seq>|<tSend>|xx…``, so the device sees which write or call it received.

- ``write-<protocol>``   the AAS Property ``Target/Setpoint`` (xs:string, an event-sourced Source) → the device's
  property ``Setpoint``: an MQTT publish, an HTTP PUT, an OPC UA Write. No Transformation.
- ``invoke-<protocol>``  the AAS Operation ``Target/Echo/operation`` (input ``Request``, output ``Reply``),
  delegated by BaSyx to the DMP → the device's action ``Echo``: MQTT 5 request/reply, an HTTP POST, an OPC UA
  Method Call; the device answers with the request. The request and response Lua copy the one value.

MQTT, HTTP and OPC UA are the protocols the DMP both writes and invokes on (SPEC §9). OPC UA forms declare
``application/octet-stream``, the binding's default UA serialization.
"""

from __future__ import annotations

import hashlib
import json
from importlib import metadata
from pathlib import Path

from basyx.aas import model

from aas_model.json_schema_aid import datapoint_from_schema
from aas_model.submodel_templates.aid import DmpActionInput, DmpActionOutput
from aas_model.testing import aimc_ref, build_aid, http_interface, mqtt_interface, opcua_interface
from aas_model.constants import INFLUX_ORG, INFLUX_ORIGIN
from aas_model.invocation_authoring import operation_path
from aas_model.submodel_templates.time_series import (
    DmpLinkedSegment, InfluxDb2Target, RecordField, Tag, Tags, TimeSeriesCollection, TimeSeriesMetadata,
    TimeSeriesRecord, TimeSeriesSegments,
)
from aas_pydantic.submodel_templates.time_series import Name
from aas_pydantic import Key, ModelReference, Submodel
from aas_pydantic.submodel_templates.asset_interfaces_description import ContentType, MqvQos, Observable, Title
from aas_model.submodel_templates import Aimc, AimcMappingConfiguration, DmpResponseSink, DmpResponseSource
from aas_pydantic.submodel_templates.asset_interfaces_mapping_configuration import SinksItem, SourcesItem

from serialize import authored, store_json

OUTPUT = Path(__file__).resolve().parent.parent / "evaluation/test_aas"

# The testbed's shared broker (testbed/docker-compose.yml): every system reaches it by this name.
BROKER = "mqtt://mosquitto:1883/"
QOS = 1

# The device message (tools/loadgen.py --format extract). tSend is a string: nanoseconds since the epoch
# exceed 2^53, so as a JSON number they would lose precision in every double-based runtime (Lua, Node-RED).
# Only what IDTA AID 1.2 defines: `required` is left out, it is aas-model's extension (AID 1.2
# leaves it EXTERNAL), and every system under test must be able to read the scenario.
EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "seq": {"type": "integer"},
        "tSend": {"type": "string"},
        "value": {"type": "string"},
    },
}

# The northbound scenarios by their sink: the AAS repository, or an InfluxDB 2 TimeSeries segment.
NORTHBOUND = {"aas-property": "aas", "influxdb-segment": "influxdb2"}
TRANSFORMATIONS = {"aas": 'function aimc_main(sources) return { Value = sources.Value } end\n',
                   "influxdb2": 'function aimc_main(sources) return { Series = { Value = sources.Value } } end\n'}

# The southbound scenarios: (kind, protocol). The device simulator (tools/device_sim.py) is reached by this name
# on the testbed network; the DMP serves delegated Operations on its port 8080.
DEVICE_HTTP = "http://device-sim:8090/"
DEVICE_OPCUA = "opc.tcp://device-sim:4840/"
OPCUA_DEVICE = "ns=2;s=Device"  # the Object owning the Method
DMP = "http://dmp:8080"
SOUTHBOUND = {
    f"{kind}-{protocol}": (kind, protocol) for kind in ("write", "invoke") for protocol in ("mqtt", "http", "opcua")
}
STRING_SCHEMA = {"type": "string"}
REQUEST_SCHEMA = {"type": "object", "properties": {"Request": {"type": "string"}}}
REPLY_SCHEMA = {"type": "object", "properties": {"Reply": {"type": "string"}}}
REQUEST_LUA = 'function aimc_main(sources) return { Request = sources.Request } end\n'
RESPONSE_LUA = 'function aimc_main(sources) return { Reply = sources.Reply } end\n'

# influxdb-segment's series: one string field per point, the scenario as its tag.
MEASUREMENT = "dmp_eval"
SERIES_PATH = ("Series", "Segments", "Live")


class SeriesTarget(Submodel):
    """The Target submodel of influxdb-segment: the series as an SMC, as aas-model's Variables holds one."""
    Series: TimeSeriesCollection


def ids(scenario: str) -> dict:
    base = f"urn:dmp-eval:{scenario}"
    return {
        "aas": f"{base}:aas",
        "asset": f"{base}:asset",
        "aid": f"{base}:submodel:AID",
        "aimc": f"{base}:submodel:AIMC",
        "target": f"{base}:submodel:Target",
    }


# --------------------------------------------------------------------------- #
# The scenario
# --------------------------------------------------------------------------- #
def topic(scenario: str) -> str:
    return f"dmp-eval/{scenario}/device"


def _aid(scenario: str):
    """One observable MQTT property ``Device`` on the scenario's topic, its message's schema declared."""
    interface = mqtt_interface(BROKER, properties={"Device": topic(scenario)})
    properties = interface.InteractionMetadata.properties.property_name
    form = properties["Device"].forms
    form.mqv_qos = MqvQos(value=str(QOS))
    form.mqv_retain = None
    device = datapoint_from_schema(EXTRACT_SCHEMA, key="Device", cls=type(properties["Device"]), forms=form)
    device.title = Title(value="Device")
    device.observable = Observable(value="true")
    properties["Device"] = device
    return build_aid(mqtt=interface)


def _aimc(scenario: str, i: dict):
    # The value member is reached through the property's own ``properties``, as its idShorts nest.
    path = ("interface_mqtt", "InteractionMetadata", "properties", "Device", "properties", "value")
    source = SourcesItem(SourceId="Value", Source=aimc_ref(i["aid"], *path))
    if NORTHBOUND[scenario] == "influxdb2":
        segment = tuple(Key(type_="SubmodelElementCollection", value=name) for name in SERIES_PATH)
        sink = SinksItem(SinkId="Series", Sink=ModelReference(key=(Key(type_="Submodel", value=i["target"]), *segment)))
    else:
        sink = SinksItem(SinkId="Value", Sink=ModelReference(key=(Key(type_="Submodel", value=i["target"]),
                                                      Key(type_="Property", value="Value"))))
    return Aimc(
        id_short="AIMC",
        MappingConfigurations=[
            AimcMappingConfiguration(
                id_short="Mapping", Sources=[source], Sinks=[sink], Transformation=TRANSFORMATIONS[NORTHBOUND[scenario]]
            )
        ],
    )


def _target(scenario: str, i: dict) -> model.Submodel:
    if NORTHBOUND[scenario] == "influxdb2":
        series = TimeSeriesCollection(
            Metadata=TimeSeriesMetadata(Name=Name(value={"en": "Device values"}),
                                        Record=TimeSeriesRecord(field={"Value": RecordField(value_type="xs:string")})),
            Segments=TimeSeriesSegments(LinkedSegment={"Live": DmpLinkedSegment(InfluxDb2Target=InfluxDb2Target(
                Measurement=MEASUREMENT, Tags=Tags(tag={"scenario": Tag(value=scenario)})))}),
        )
        return authored(SeriesTarget(id_short="Target", Series=series), i["target"])
    return model.Submodel(i["target"], id_short="Target",
                          submodel_element=[model.Property("Value", model.datatypes.String)])


# --------------------------------------------------------------------------- #
# Southbound scenarios
# --------------------------------------------------------------------------- #
def device(scenario: str) -> dict:
    """Where the device takes the scenario's write or command (tools/device_sim.py, loadgen.py, invoke.py)."""
    kind, protocol = SOUTHBOUND[scenario]
    name = "setpoint" if kind == "write" else "echo"
    if protocol == "mqtt":
        entry = {"protocol": "mqtt", "topic": f"dmp-eval/{scenario}/{name}"}
        if kind == "invoke":
            entry["responseTopic"] = f"dmp-eval/{scenario}/{name}/reply"
        return entry
    if protocol == "opcua":
        if kind == "write":
            return {"protocol": "opcua", "url": DEVICE_OPCUA, "node": "ns=2;s=Setpoint"}
        return {"protocol": "opcua", "url": DEVICE_OPCUA, "object": OPCUA_DEVICE, "method": "ns=2;s=Echo"}
    return {"protocol": "http", "url": f"{DEVICE_HTTP}{scenario}/{name}",
            "method": "PUT" if kind == "write" else "POST"}


def delegation_path(scenario: str) -> str:
    """The Operation's AAS API invoke path, which the DMP serves (aas-model ``operation_path``)."""
    return operation_path(ids(scenario)["target"], "Echo.operation")


def _southbound_aid(scenario: str):
    """One writable property ``Setpoint`` (a string) or one action ``Echo``, by MQTT, HTTP or OPC UA."""
    (kind, protocol), where = SOUTHBOUND[scenario], device(scenario)
    if protocol == "mqtt":
        build, base, href = mqtt_interface, BROKER, where["topic"]
    elif protocol == "http":
        build, base, href = http_interface, DEVICE_HTTP, where["url"].removeprefix(DEVICE_HTTP)
    else:
        build, base, href = opcua_interface, DEVICE_OPCUA, f"/?id={where.get('node') or where.get('method')}"
    if kind == "write":
        interface = build(base, properties={"Setpoint": href})
        properties = interface.InteractionMetadata.properties.property_name
        form = properties["Setpoint"].forms
        if protocol == "mqtt":
            form.mqv_qos = MqvQos(value=str(QOS))
            form.mqv_retain = None
        if protocol == "opcua":
            form.contentType = ContentType(value="application/octet-stream")
        setpoint = datapoint_from_schema(STRING_SCHEMA, key="Setpoint", cls=type(properties["Setpoint"]), forms=form)
        setpoint.title = Title(value="Setpoint")
        properties["Setpoint"] = setpoint
    else:
        if protocol == "mqtt":
            interface = build(base, actions={"Echo": (href, where["responseTopic"])})
        elif protocol == "http":
            interface = build(base, actions={"Echo": (href, "POST")})
        else:
            interface = build(base, actions={"Echo": (href, "POST", OPCUA_DEVICE)})
        action = interface.InteractionMetadata.actions.property_name["Echo"]
        if protocol == "mqtt":
            action.forms.mqv_qos = MqvQos(value=str(QOS))
        if protocol == "opcua":
            action.forms.contentType = ContentType(value="application/octet-stream")
        action.input = datapoint_from_schema(REQUEST_SCHEMA, cls=DmpActionInput)
        action.output = datapoint_from_schema(REPLY_SCHEMA, cls=DmpActionOutput)
    if protocol == "opcua":
        interface.EndpointMetadata.contentType = ContentType(value="application/octet-stream")
    return build_aid(**{protocol: interface})


def _southbound_aimc(scenario: str, i: dict):
    kind, protocol = SOUTHBOUND[scenario]
    interface = (i["aid"], f"interface_{protocol}", "InteractionMetadata")
    if kind == "write":
        setpoint = ModelReference(
            key=(Key(type_="Submodel", value=i["target"]), Key(type_="Property", value="Setpoint"))
        )
        return Aimc(
            id_short="AIMC",
            MappingConfigurations=[
                AimcMappingConfiguration(
                    id_short="Mapping",
                    Sources=[SourcesItem(SourceId="Setpoint", Source=setpoint)],
                    Sinks=[SinksItem(SinkId="Setpoint", Sink=aimc_ref(*interface, "properties", "Setpoint"))],
                )
            ],
        )
    operation = (Key(type_="Submodel", value=i["target"]), Key(type_="SubmodelElementCollection", value="Echo"),
                 Key(type_="Operation", value="operation"))
    action = (*interface, "actions", "Echo")
    return Aimc(
        id_short="AIMC",
        MappingConfigurations=[
            AimcMappingConfiguration(
                id_short="Mapping",
                Sources=[
                    SourcesItem(
                        SourceId="Request",
                        Source=ModelReference(key=(*operation, Key(type_="Property", value="Request"))),
                    )
                ],
                Sinks=[SinksItem(SinkId="Request", Sink=aimc_ref(*action, "input", "properties", "Request"))],
                Transformation=REQUEST_LUA,
                ResponseSources=[
                    DmpResponseSource(SourceId="Reply", Source=aimc_ref(*action, "output", "properties", "Reply"))
                ],
                ResponseSinks=[
                    DmpResponseSink(
                        SinkId="Reply", Sink=ModelReference(key=(*operation, Key(type_="Property", value="Reply")))
                    )
                ],
                ResponseTransformation=RESPONSE_LUA,
            )
        ],
    )


def _southbound_target(scenario: str, i: dict) -> model.Submodel:
    """The Property a client writes, or the Operation it invokes (delegated to the DMP)."""
    if SOUTHBOUND[scenario][0] == "write":
        return model.Submodel(i["target"], id_short="Target",
                              submodel_element=[model.Property("Setpoint", model.datatypes.String)])
    operation = model.Operation(
        "operation", input_variable=[model.Property("Request", model.datatypes.String)],
        output_variable=[model.Property("Reply", model.datatypes.String)],
        qualifier=[model.Qualifier("invocationDelegation", model.datatypes.String,
                                   value=DMP + delegation_path(scenario))])
    return model.Submodel(i["target"], id_short="Target",
                          submodel_element=[model.SubmodelElementCollection("Echo", value=[operation])])


def environment(scenario: str) -> dict:
    i = ids(scenario)
    if scenario in SOUTHBOUND:
        submodels = [
            authored(_southbound_aid(scenario), i["aid"]),
            authored(_southbound_aimc(scenario, i), i["aimc"]),
            _southbound_target(scenario, i),
        ]
    else:
        submodels = [authored(_aid(scenario), i["aid"]), authored(_aimc(scenario, i), i["aimc"]),
                     _target(scenario, i)]
    shell = model.AssetAdministrationShell(
        model.AssetInformation(model.AssetKind.INSTANCE, global_asset_id=i["asset"]),
        i["aas"], id_short=f"eval_{scenario.replace('-', '_')}",
        submodel={model.ModelReference.from_referable(s) for s in submodels})
    # Nothing reads these AASs back as aas-model classes: they hold only the AAS content.
    environment = store_json(shell, *submodels)
    # The shell holds its submodel references as a set; fix their order.
    for shell_json in environment["assetAdministrationShells"]:
        shell_json["submodels"].sort(key=lambda reference: reference["keys"][0]["value"])
    return environment


def _aas_model_commit() -> str:
    url = json.loads(metadata.distribution("aas-model").read_text("direct_url.json") or "{}")
    return url.get("vcs_info", {}).get("commit_id", "unknown")


def main() -> None:
    manifest = {"aasModelCommit": _aas_model_commit(), "broker": BROKER, "qos": QOS, "scenarios": {}}
    for scenario in [*NORTHBOUND, *SOUTHBOUND]:
        text = json.dumps(environment(scenario), indent=2, sort_keys=True) + "\n"
        (OUTPUT / f"{scenario}.json").write_text(text)
        i = ids(scenario)
        if scenario in NORTHBOUND:
            sink = NORTHBOUND[scenario]
            entry = {
                "topic": topic(scenario),
                "aimc": i["aimc"],
                "sink": sink,
                "target": {"submodel": i["target"], "path": ".".join(SERIES_PATH) if sink == "influxdb2" else "Value"},
            }
            if sink == "influxdb2":
                entry["influxdb"] = {"origin": INFLUX_ORIGIN, "org": INFLUX_ORG}
        else:
            kind, where = SOUTHBOUND[scenario][0], device(scenario)
            entry = {"kind": kind, "topic": where.get("topic"), "aimc": i["aimc"], "sink": "device", "device": where,
                     "target": {"submodel": i["target"], "path": "Setpoint" if kind == "write" else "Echo.operation"}}
            if kind == "invoke":
                entry["delegationPath"] = delegation_path(scenario)
        manifest["scenarios"][scenario] = {**entry, "sha256": hashlib.sha256(text.encode()).hexdigest()}
    (OUTPUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"wrote {', '.join(manifest['scenarios'])} (aas-model {manifest['aasModelCommit'][:10]})")


if __name__ == "__main__":
    main()
