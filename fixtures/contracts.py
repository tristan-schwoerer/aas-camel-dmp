"""The DMP's model contract fixtures, authored with aas-model.

The output is test input for the DMP's model contract tests (``RawModelContractFixtureTest``,
``WireFixtureCompatibilityTest``); nothing at runtime reads it. Every fixture is authored here with aas-model and
serialized the way registration publishes it; the JSON files are never edited by hand (the contract test checks
their hashes). Regenerate from fixtures/ after changing aas-model or a case, and review every changed compile
outcome in the DMP:

    uv run python -m contracts

Each case directory holds ``aimc.json``, ``aid.json``, ``dependencies/N.json``
(the further submodels the AIMC references) and ``expected.json`` (the authored
mappings and the identities the DMP resolves ``{aas_id}`` macros with).
``manifest.json`` lists the cases, whether each is expected to compile in the
DMP, and the SHA-256 of every file.

Synthetic cases cover one protocol or mapping form each; ``stoppering`` and
``station-*`` are the resources of ``RESOURCE_FACTORIES`` as registration
builds them.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

from basyx.aas import model

from aas_model.builder import build_from_model
from aas_model.constants import INFLUX_ORG, INFLUX_ORIGIN
from aas_model.json_schema_aid import datapoint_from_schema
from aas_model.resource_instances import RESOURCE_FACTORIES
from aas_model.resource_template.references import operation_ref
from aas_model.submodel_templates.aid import (
    AdditionalResponse, AdditionalResponseSchema, AdditionalResponseSuccess, AdditionalResponses, DmpActionInput,
    DmpActionOutput,
)
from aas_pydantic.submodel_templates.asset_interfaces_description import ContentType
from aas_model.testing import (
    aimc_ref, build_aid, http_interface, modbus_interface, mqtt_interface, opcua_interface, snmp_interface,
)
from aas_pydantic import Key, ModelReference
from aas_model.submodel_templates import Aimc, AimcMappingConfiguration
from aas_pydantic.submodel_templates.asset_interfaces_mapping_configuration import SinksItem, SourcesItem

from serialize import authored, store_json

OUTPUT = Path(__file__).resolve().parent.parent / "dmp.component/src/test/resources/runtime-model-contracts"

AID_ID = "a/submodels/AID"
AIMC_ID = "contractAAS/submodels/AIMC"
CONTEXT = {"aas_id": "contractAAS", "aas_id_short": "contract"}
AIMC_SEMANTIC = "https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel"
AID_SEMANTIC_PREFIX = "https://admin-shell.io/idta/AssetInterfacesDescription/"
CCI_SEMANTIC = "https://admin-shell.io/idta/ControlComponent/Instance/2/0"


def _submodel_json(submodel: model.Submodel) -> dict:
    return store_json(submodel)["submodels"][0]


# --------------------------------------------------------------------------- #
# Plain submodels the synthetic mappings read from or write to
# --------------------------------------------------------------------------- #
def _submodel(identifier: str, id_short: str, *elements, semantic_id: str | None = None) -> dict:
    return _submodel_json(model.Submodel(
        identifier, id_short=id_short, submodel_element=list(elements),
        semantic_id=model.ExternalReference((model.Key(model.KeyTypes.GLOBAL_REFERENCE, semantic_id),))
        if semantic_id else None))


def _string(id_short: str) -> model.Property:
    return model.Property(id_short, model.datatypes.String)


def _collection(id_short: str, *children) -> model.SubmodelElementCollection:
    return model.SubmodelElementCollection(id_short, value=list(children))


def _kpi(identifier: str, *names: str, collections: bool = True) -> dict:
    return _submodel(identifier, "KPI", *[_collection(n) if collections else _string(n) for n in names])


def _cci(identifier: str, *skills: str) -> dict:
    return _submodel(identifier, "ControlComponentInstance",
                     _collection("Skills", *[_collection(s, model.Operation("operation")) for s in skills]),
                     semantic_id=CCI_SEMANTIC)


def _operation(cci_id: str, skill: str) -> ModelReference:
    return ModelReference(key=(
        Key(type_="Submodel", value=cci_id),
        Key(type_="SubmodelElementCollection", value="Skills"),
        Key(type_="SubmodelElementCollection", value=skill),
        Key(type_="Operation", value="operation"),
    ))


def _element(submodel_id: str, element_type: str, path: str) -> ModelReference:
    return ModelReference(key=(Key(type_="Submodel", value=submodel_id), Key(type_=element_type, value=path)))


def _mapping(source_id, source_ref, sinks, *, interval=None, name="Mapping"):
    return AimcMappingConfiguration(
        id_short=name,
        Sources=[SourcesItem(SourceId=source_id, Source=source_ref)],
        Sinks=sinks,
        Transformation='function aimc_main(sources) return { Out = sources["%s"] } end' % source_id,
        DefaultPollingInterval=None if interval is None else str(interval),
    )


def _kpi_sink(name: str = "Out"):
    return SinksItem(SinkId=name, Sink=aimc_ref("{aas_id}/submodels/KPI", name))


def _with_schema(interface, schemas: dict):
    """Give the named properties of an interface a declared data schema, keeping their forms."""
    properties = interface.InteractionMetadata.properties.property_name
    for name, schema in schemas.items():
        current = properties[name]
        properties[name] = datapoint_from_schema(schema, key=name, cls=type(current), forms=current.forms)
    return interface


NESTED_SCHEMA = {"type": "object", "properties": {"Reading": {"type": "number"}}}
SETPOINT_SCHEMA = {"type": "object", "properties": {"mode": {"type": "string"}}}


TD_INPUT = {
    "type": "object",
    "properties": {
        "Uuid": {"type": "string", "pattern": "^[0-9a-f-]{36}$"},
        "Speed": {"type": "number", "exclusiveMinimum": 0, "multipleOf": 0.5},
        "Note": {"oneOf": [{"type": "string", "maxLength": 8}, {"type": "null"}]},
        "Points": {"type": "array", "items": {
            "type": "object", "properties": {"X": {"type": "number"}, "Label": {"type": "string"}},
            "required": ["X"]}},
    },
    "required": ["Uuid", "Speed"],
}
TD_OUTPUT = {
    "type": "object",
    "properties": {
        "State": {"type": "string", "enum": ["SUCCESS", "FAILURE"]},
        "Image": {"type": "string", "contentEncoding": "base64", "contentMediaType": "image/png"},
    },
    "required": ["State"],
}
TD_ERROR = {"type": "object", "properties": {"Error": {"type": "string"}}, "required": ["Error"]}


def _aid_ref(interface: str, kind: str, *path: str):
    return aimc_ref(AID_ID, interface, "InteractionMetadata", kind, *path)


# --------------------------------------------------------------------------- #
# Synthetic cases: (aimc, aid, dependencies, compiles, description)
# --------------------------------------------------------------------------- #
def synthetic_cases():
    empty_mqtt = mqtt_interface("mqtt://192.168.0.104:1883/base/")

    for name, element_type, dependency in (
            ("aas-property-event", "Property", _string("State")),
            ("aas-property-poll", "Property", _string("State")),
            ("aas-collection-poll", "SubmodelElementCollection", _collection("State"))):
        interval = None if name.endswith("event") else 2.5
        aimc = Aimc(
            id_short="AIMC",
            MappingConfigurations=[
                _mapping(
                    "State", _element("a/submodels/Runtime", element_type, "State"), [_kpi_sink()], interval=interval
                )
            ],
        )
        yield name, aimc, build_aid(mqtt=empty_mqtt), [
            _submodel("a/submodels/Runtime", "Runtime", dependency), _kpi("contractAAS/submodels/KPI", "Out")], True, \
            f"an AAS {element_type} source, {'observed through change events' if interval is None else 'polled'}"

    yield (
        "opcua-read",
        Aimc(
            id_short="AIMC",
            MappingConfigurations=[
                _mapping("Pressure", _aid_ref("interface_opcua", "properties", "Pressure"), [_kpi_sink()], interval=2.5)
            ],
        ),
        build_aid(
            opcua=opcua_interface(
                "opc.tcp://192.168.0.10:4840/opcua/server/", properties={"Pressure": "/?id=ns=2;s=Pressure"}
            )
        ),
        [_kpi("contractAAS/submodels/KPI", "Out")],
        True,
        "a polled OPC UA variable",
    )

    yield (
        "http-poll",
        Aimc(
            id_short="AIMC",
            MappingConfigurations=[
                _mapping("Pressure", _aid_ref("interface_http", "properties", "Pressure"), [_kpi_sink()], interval=2.5)
            ],
        ),
        build_aid(http=http_interface("http://192.168.0.30:8080/", properties={"Pressure": "pressure"})),
        [_kpi("contractAAS/submodels/KPI", "Out")],
        True,
        "a polled HTTP resource",
    )

    coil = _aid_ref("interface_modbus", "properties", "Coil1")
    yield (
        "plc4x",
        Aimc(
            id_short="AIMC",
            MappingConfigurations=[_mapping("Coil1", coil, [SinksItem(SinkId="Out", Sink=coil)], interval=5)],
        ),
        build_aid(modbus=modbus_interface("modbus+tcp://192.168.0.20:502/2/", properties={"Coil1": "0"})),
        [],
        True,
        "a Modbus coil read and written through PLC4X",
    )

    setpoint = _aid_ref("interface_mqtt", "properties", "SetPoint", "mode")
    yield (
        "fine-path",
        Aimc(
            id_short="AIMC",
            MappingConfigurations=[_mapping("SetPoint", setpoint, [SinksItem(SinkId="Out", Sink=setpoint)])],
        ),
        build_aid(
            mqtt=_with_schema(
                mqtt_interface(
                    "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering/", properties={"SetPoint": "SET/SetPoint"}
                ),
                {"SetPoint": SETPOINT_SCHEMA},
            )
        ),
        [],
        True,
        "a reference into an MQTT payload's schema",
    )

    http_nested = _aid_ref("interface_http", "properties", "Pressure", "Reading")
    opcua_nested = _aid_ref("interface_opcua", "properties", "Level", "Reading")
    yield (
        "fine-protocols",
        Aimc(
            id_short="AIMC",
            MappingConfigurations=[
                AimcMappingConfiguration(
                    id_short="Nested",
                    Sources=[
                        SourcesItem(SourceId="Pressure", Source=http_nested),
                        SourcesItem(SourceId="Level", Source=opcua_nested),
                    ],
                    Sinks=[
                        SinksItem(SinkId="PressureOut", Sink=http_nested),
                        SinksItem(SinkId="LevelOut", Sink=opcua_nested),
                    ],
                    Transformation="function aimc_main(sources) return "
                    "{ PressureOut = sources.Pressure, LevelOut = sources.Level } end",
                    DefaultPollingInterval="2.5",
                )
            ],
        ),
        build_aid(
            http=_with_schema(
                http_interface("http://192.168.0.30:8080/api/", properties={"Pressure": "pressure"}),
                {"Pressure": NESTED_SCHEMA},
            ),
            opcua=_with_schema(
                opcua_interface("opc.tcp://192.168.0.40:4840/server/", properties={"Level": "?id=ns=2;s=Level"}),
                {"Level": NESTED_SCHEMA},
            ),
        ),
        [],
        True,
        "references into HTTP and OPC UA schemas",
    )

    yield (
        "snmp",
        Aimc(
            id_short="AIMC",
            MappingConfigurations=[
                AimcMappingConfiguration(
                    id_short="Snmp",
                    Sources=[SourcesItem(SourceId="Uptime", Source=_aid_ref("interface_snmp", "properties", "Uptime"))],
                    Sinks=[
                        _kpi_sink("Out"),
                        SinksItem(SinkId="Contact", Sink=_aid_ref("interface_snmp", "properties", "Contact")),
                    ],
                    Transformation="function aimc_main(sources) return { Out = sources.Uptime, Contact = 'ops@example.com' } end",
                    DefaultPollingInterval="2.5",
                )
            ],
        ),
        build_aid(
            snmp=snmp_interface(
                "snmp://10.0.0.5:161/",
                properties={
                    "Uptime": "1.3.6.1.2.1.1.3.0",
                    "Contact": ("1.3.6.1.2.1.1.4.0", {"pduType": "snmpv:SetRequestPDU"}),
                },
            )
        ),
        [_kpi("contractAAS/submodels/KPI", "Out")],
        True,
        "an SNMP object read with GET, another written with SET",
    )

    states = [
        SourcesItem(SourceId=key, Source=_aid_ref("interface_mqtt", "properties", key)) for key in ("State", "Mode")
    ]
    yield (
        "fan-in-out",
        Aimc(
            id_short="AIMC",
            MappingConfigurations=[
                AimcMappingConfiguration(
                    id_short="Fan",
                    Sources=states,
                    Sinks=[_kpi_sink("Out"), _kpi_sink("Mirror")],
                    Transformation="function aimc_main(sources) return { Out = sources.State, Mirror = sources.Mode } end",
                )
            ],
        ),
        build_aid(
            mqtt=mqtt_interface("mqtt://192.168.0.104:1883/base/", properties={"State": "state", "Mode": "mode"})
        ),
        [_kpi("contractAAS/submodels/KPI", "Out", "Mirror")],
        True,
        "two MQTT sources into two AAS sinks",
    )

    cci = "a/submodels/ControlComponentInstance"
    yield (
        "opcua-method",
        Aimc(
            id_short="AIMC",
            MappingConfigurations=[
                AimcMappingConfiguration(
                    id_short="Calibrate",
                    Sources=[SourcesItem(SourceId="Calibrate", Source=_operation(cci, "Calibrate"))],
                    Sinks=[
                        SinksItem(SinkId="Calibrate", Sink=_aid_ref("interface_opcua", "actions", "Calibrate", "input"))
                    ],
                    Transformation="function aimc_main(sources) return { Calibrate = sources.Calibrate } end",
                )
            ],
        ),
        build_aid(
            opcua=opcua_interface(
                "opc.tcp://192.168.0.90:4840/",
                actions={"Calibrate": ("/?id=ns=2;s=RunCalibration", "POST", "ns=1;s=CalibrationObject")},
            )
        ),
        [_cci(cci, "Calibrate")],
        True,
        "an Operation bound to an OPC UA Method",
    )

    yield (
        "http-operation",
        Aimc(
            id_short="AIMC",
            MappingConfigurations=[
                AimcMappingConfiguration(
                    id_short="Restart",
                    Sources=[SourcesItem(SourceId="Restart", Source=_operation(cci, "Restart"))],
                    Sinks=[SinksItem(SinkId="Restart", Sink=_aid_ref("interface_http", "actions", "Restart", "input"))],
                    Transformation="function aimc_main(sources) return { Restart = sources.Restart } end",
                )
            ],
        ),
        build_aid(
            http=http_interface(
                "http://192.168.0.60:8080/plc/api/", actions={"Restart": ("actions/Restart?mode=quick", "POST")}
            )
        ),
        [_cci(cci, "Restart")],
        True,
        "an Operation bound to an HTTP action",
    )

    grouped = mqtt_interface("mqtt://192.168.0.104:1883/base/", properties={"State": "state"},
                             actions={"Move": ("move", "move/reply")})
    grouped.InteractionMetadata.actions.property_name["Move"].input = datapoint_from_schema(
        {"type": "object", "properties": {"X": {"type": "number"}, "Y": {"type": "number"}},
         "required": ["X", "Y"]}, cls=DmpActionInput)
    move_input = ("interface_mqtt", "actions", "Move", "input")
    yield (
        "grouped-action",
        Aimc(
            id_short="AIMC",
            MappingConfigurations=[
                AimcMappingConfiguration(
                    id_short="Grouped",
                    Sources=[SourcesItem(SourceId="Invoke", Source=operation_ref("Move"))],
                    Sinks=[
                        SinksItem(SinkId="X", Sink=_aid_ref(*move_input, "X")),
                        SinksItem(SinkId="Y", Sink=_aid_ref(*move_input, "Y")),
                    ],
                    Transformation="function aimc_main(sources) return { X=1, Y=2 } end",
                )
            ],
        ),
        build_aid(mqtt=grouped),
        [_cci("contractAAS/submodels/ControlComponentInstance", "Move")],
        True,
        "an Operation whose sinks fill members of one MQTT 5 request/reply action",
    )

    # Every TD DataSchema term AID marks EXTERNAL, nested, and an additional (error) response.
    inspect = mqtt_interface("mqtt://192.168.0.104:1883/base/", actions={"Inspect": ("inspect", "inspect/reply")})
    action = inspect.InteractionMetadata.actions.property_name["Inspect"]
    action.input = datapoint_from_schema(TD_INPUT, cls=DmpActionInput)
    action.output = datapoint_from_schema(TD_OUTPUT, cls=DmpActionOutput)
    action.forms.additionalResponses = AdditionalResponses(value=[AdditionalResponse(
        id_short="failure", success=AdditionalResponseSuccess(value="false"),
        contentType=ContentType(value="application/json"),
        schema=datapoint_from_schema(TD_ERROR, cls=AdditionalResponseSchema))])
    yield (
        "td-data-schemas",
        Aimc(
            id_short="AIMC",
            MappingConfigurations=[
                AimcMappingConfiguration(
                    id_short="Inspect",
                    Sources=[SourcesItem(SourceId="Inspect", Source=operation_ref("Inspect"))],
                    Sinks=[SinksItem(SinkId="Inspect", Sink=_aid_ref("interface_mqtt", "actions", "Inspect", "input"))],
                    Transformation="function aimc_main(sources) return { Inspect = sources.Inspect } end",
                )
            ],
        ),
        build_aid(mqtt=inspect),
        [_cci("contractAAS/submodels/ControlComponentInstance", "Inspect")],
        True,
        "an Operation whose action schemas use the TD DataSchema terms AID marks EXTERNAL",
    )

    yield (
        "projection-extensions",
        Aimc(
            id_short="AIMC",
            MappingConfigurations=[
                AimcMappingConfiguration(
                    id_short="Extensions",
                    Sources=[SourcesItem(SourceId="invoke", Source=_operation(cci, "Move"))],
                    Sinks=[SinksItem(SinkId="command", Sink=_element("contractAAS/submodels/KPI", "Property", "Out"))],
                    ResponseTransformation="function aimc_main(sources) return sources end",
                    DefaultPollingInterval="2.5",
                )
            ],
        ),
        build_aid(mqtt=mqtt_interface("mqtt://fixture.invalid/move/", actions={"Move": "move"})),
        [_cci(cci, "Move"), _kpi("contractAAS/submodels/KPI", "Out", collections=False)],
        False,
        "the AIMC extensions project; an Operation sinking into an AAS Property is not a bridge the DMP compiles",
    )


# --------------------------------------------------------------------------- #
# Station cases: the resources as registration builds them
# --------------------------------------------------------------------------- #
def _semantic(submodel: dict) -> str:
    keys = (submodel.get("semanticId") or {}).get("keys") or [{}]
    return keys[0].get("value", "")


def _referenced_submodels(aimc: dict) -> list[str]:
    ids = []

    def walk(value):
        if isinstance(value, dict):
            if value.get("modelType") == "ReferenceElement" and isinstance(value.get("value"), dict):
                keys = value["value"].get("keys") or []
                if keys and keys[0].get("type") == "Submodel" and keys[0]["value"] not in ids:
                    ids.append(keys[0]["value"])
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
    walk(aimc)
    return ids


def station_cases():
    for factory_name, factory in RESOURCE_FACTORIES.items():
        if factory_name in ("planarShuttle2", "planarShuttle3"):
            continue  # the same model as planarShuttle1
        name = "stoppering" if factory_name == "syntegonStoppering" else f"station-{factory_name}"
        resource = factory()
        submodels = store_json(*build_from_model(resource))["submodels"]
        aimc = next((s for s in submodels if _semantic(s) == AIMC_SEMANTIC), None)
        aid = next((s for s in submodels if _semantic(s).startswith(AID_SEMANTIC_PREFIX)), None)
        if aimc is None or aid is None or not _authored_mappings(aimc):
            continue
        by_id = {s["id"]: s for s in submodels}
        dependencies = [
            by_id[i] for i in _referenced_submodels(aimc) if i in by_id and i not in (aimc["id"], aid["id"])
        ]
        yield name, aimc, aid, dependencies, {"aas_id": resource.id, "aas_id_short": resource.id_short}, \
            f"the {factory_name} resource of aas-model"


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
def _authored_mappings(aimc: dict) -> list[dict]:
    mappings = next(e for e in aimc["submodelElements"] if e["idShort"] == "MappingConfigurations").get("value", [])
    result = []
    for index, mapping in enumerate(mappings):
        members = {m["idShort"]: m for m in mapping["value"]}

        def ids(container, key):
            return [next(x["value"] for x in item["value"] if x["idShort"] == key)
                    for item in members.get(container, {}).get("value", [])]
        result.append({"index": index, "idShort": mapping.get("idShort"),
                       "sourceIds": ids("Sources", "SourceId"), "sinkIds": ids("Sinks", "SinkId")})
    return result


def _write(path: Path, document: dict, resources: dict, root: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = (json.dumps(document, indent=1, ensure_ascii=False) + "\n").encode("utf-8")
    path.write_bytes(data)
    resources[path.relative_to(root).as_posix()] = {"byteLength": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def generate(root: Path) -> None:
    for child in root.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
    resources, cases = {}, []

    def emit(name, aimc, aid, dependencies, context, compiles, description):
        dependency_index = []
        _write(root / name / "aimc.json", aimc, resources, root)
        _write(root / name / "aid.json", aid, resources, root)
        for i, dependency in enumerate(dependencies):
            _write(root / name / "dependencies" / f"{i}.json", dependency, resources, root)
            dependency_index.append({"id": dependency["id"], "path": f"dependencies/{i}.json"})
        # The workspace's InfluxDB, which every TimeSeries segment names (constants.INFLUX_*).
        _write(
            root / name / "expected.json",
            {
                "compilerContext": {
                    **context,
                    "aas_base_url": "http://aas-server:8081",
                    "influx_url": INFLUX_ORIGIN,
                    "influx_org": INFLUX_ORG,
                },
                "authoredMappings": _authored_mappings(aimc),
            },
            resources,
            root,
        )
        cases.append({"name": name, "description": description, "aimc": f"{name}/aimc.json", "aid": f"{name}/aid.json",
                      "expected": f"{name}/expected.json", "dependencies": dependency_index, "compiles": compiles})

    for name, aimc, aid, dependencies, compiles, description in synthetic_cases():
        emit(name, _submodel_json(authored(aimc, AIMC_ID)), _submodel_json(authored(aid, AID_ID)), dependencies,
             CONTEXT, compiles, description)
    for name, aimc, aid, dependencies, context, description in station_cases():
        emit(name, aimc, aid, dependencies, context, True, description)

    manifest = {"formatVersion": 2,
                "generator": "aas-camel-dmp/fixtures/contracts.py",
                "note": "generated; do not edit. Regenerate from aas-model and review every changed compile outcome.",
                "cases": cases, "resources": dict(sorted(resources.items()))}
    (root / "manifest.json").write_text(json.dumps(manifest, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    OUTPUT.mkdir(parents=True, exist_ok=True)
    generate(OUTPUT)
    print(f"wrote {OUTPUT}")
