"""nodered-dmp (RWTH IAT): a generator that reads an AIMC from the repository and emits a
Node-RED flow, which then runs in Node-RED.

Two adaptations, both forced by the generator and disclosed with the results:

- It parses AIMC **1.0** only (InterfaceReference + MappingSourceSinkRelations), so the
  adapter renders the scenario's AIMC 2.0 mapping as AIMC 1.0: the interface is the first
  two keys of the Source, one relation from the Source to each Sink. The AID and the
  target submodel are the scenario's own.
- It reads no Transformation and no schema: every mapping gets a JavaScript function node
  ``return msg;`` for the user to edit; the adapter writes aas-property's member extraction into it.

The flow writes each value with a GET of the target element and a PUT of it with the new
value (Part 2 ``PutSubmodelElementByPath``); the capture (O5) matches the PUT, not the GET.
Readiness is a canary message, since Node-RED reports no subscription state.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import requests

from adapters.base import Adapter, Repository, Scenario, canary
from harness.testbed import EVALUATION, TOOLS_CPUSET

GENERATOR = EVALUATION / "testbed" / "nodered-dmp-generator"  # its Dockerfile and generate.py
GENERATOR_IMAGE = "dmp-eval-nodered-gen:local"
REPOSITORY_INTERNAL = "http://aas-env:8081"
AIMC10 = "https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/1/0/"

# aas-property in the function node: the generator references whole affordances only, so the member is picked here.
# The flow hands the function the device payload, as text, in msg.updateValue.
FUNCTIONS = {"aas-property": """msg.updateValue = JSON.parse(msg.updateValue).value;
return msg;"""}


def _children(element: dict) -> dict[str, dict]:
    return {child.get("idShort"): child for child in element.get("value", []) if isinstance(child, dict)}


def _affordance(reference: dict) -> dict:
    keys = reference["keys"]
    depth = next((i for i in range(len(keys) - 1) if keys[i]["value"] == "properties" and i >= 2), None)
    return reference if depth is None else {**reference, "keys": keys[:depth + 2]}


def aimc_1_0(scenario: Scenario) -> dict:
    """The scenario's AIMC 2.0 mappings as an AIMC 1.0 submodel."""
    aimc = next(s for s in scenario.environment["submodels"] if s["id"] == scenario.aimc)
    configurations = next(e for e in aimc["submodelElements"] if e["idShort"] == "MappingConfigurations")
    semantic = lambda name: {"type": "ExternalReference", "keys": [{"type": "GlobalReference", "value": AIMC10 + name}]}
    rendered = []
    for configuration in configurations["value"]:
        members = _children(configuration)
        # A reference into an affordance's data schema becomes the affordance: AIMC 1.0's relation and
        # the generator take whole properties (the member is picked in the function node).
        sources = [_affordance(_children(item)["Source"]["value"]) for item in members["Sources"]["value"]]
        sinks = [_children(item)["Sink"]["value"] for item in members["Sinks"]["value"]]
        if len(sources) != 1:
            raise ValueError("AIMC 1.0 relates one source to its sinks; this mapping has several sources")
        rendered.append({"modelType": "SubmodelElementCollection", "semanticId": semantic("MappingConfiguration"), "value": [
            {"modelType": "ReferenceElement", "idShort": "InterfaceReference", "semanticId": semantic("InterfaceReference"),
             "value": {"type": "ModelReference", "keys": sources[0]["keys"][:2]}},
            {"modelType": "SubmodelElementList", "idShort": "MappingSourceSinkRelations",
             "semanticId": semantic("MappingSourceSinkRelations"), "semanticIdListElement": semantic("MappingSourceSinkRelation"),
             "typeValueListElement": "RelationshipElement", "orderRelevant": True,
             "value": [{"modelType": "RelationshipElement", "semanticId": semantic("MappingSourceSinkRelation"),
                        "first": sources[0], "second": sink} for sink in sinks]}]})
    return {"modelType": "Submodel", "id": scenario.aimc + "10", "idShort": "AIMC10", "semanticId": semantic("Submodel"),
            "submodelElements": [{"modelType": "SubmodelElementList", "idShort": "MappingConfigurations",
                                  "semanticId": semantic("MappingConfigurations"),
                                  "semanticIdListElement": semantic("MappingConfiguration"),
                                  "typeValueListElement": "SubmodelElementCollection", "orderRelevant": True,
                                  "value": rendered}]}


class NoderedDmpAdapter(Adapter):
    name = "nodered-dmp"
    profile = "nodered-dmp"
    service = "nodered"

    def supports(self, scenario: Scenario) -> bool:
        return scenario.id in FUNCTIONS

    def prepare(self, run) -> dict:
        self.run = run
        self.url = f"http://127.0.0.1:{run.ports['nodered']}"
        print(f"[nodered-dmp] building {GENERATOR_IMAGE}", flush=True)
        subprocess.run(["docker", "build", "-q", "-t", GENERATOR_IMAGE, str(GENERATOR)], check=True, stdout=subprocess.DEVNULL)
        dockerfile = (GENERATOR / "Dockerfile").read_text()
        pin = lambda arg: next(line.split("=", 1)[1] for line in dockerfile.splitlines() if line.startswith(f"ARG {arg}="))
        image_id = subprocess.run(["docker", "image", "inspect", "-f", "{{.Id}}", GENERATOR_IMAGE], check=True,
                                  capture_output=True, text=True).stdout.strip()
        return {"noderedDmpCommit": pin("NODERED_DMP_COMMIT"), "noderedFlowgenCommit": pin("NODERED_FLOWGEN_COMMIT"),
                "generatorImageId": image_id,
                "adaptations": ["AIMC 2.0 rendered as AIMC 1.0 (the generator parses 1.0 only)",
                                "the member extraction written in JavaScript into the generated function node "
                                "(the generator reads no Transformation and no schema)"]}

    def compose_env(self, scenario: Scenario, point_dir: Path, variant: str | None = None) -> dict[str, str]:
        self.config_dir = point_dir / "config"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        return {}

    def deploy(self, scenario: Scenario, repository: Repository) -> None:
        repository.post_environment(scenario.environment, last=scenario.aimc)
        aimc10 = aimc_1_0(scenario)
        repository.post_submodel(aimc10)
        (self.config_dir / "aimc10.json").write_text(json.dumps(aimc10, indent=1) + "\n")
        generated = subprocess.run(["docker", "run", "--rm", "--network", "dmp-eval", "--cpuset-cpus", TOOLS_CPUSET,
                                    GENERATOR_IMAGE,
                                    REPOSITORY_INTERNAL, aimc10["id"]], check=True, capture_output=True, text=True)
        flow = json.loads(generated.stdout)
        functions = [node for node in flow if node.get("type") == "function"]
        if len(functions) != 1:
            raise RuntimeError(f"expected one generated function node, found {len(functions)}")
        functions[0]["func"] = FUNCTIONS[scenario.id]
        (self.config_dir / "flow.json").write_text(json.dumps(flow, indent=1) + "\n")
        response = requests.post(f"{self.url}/flows", json=flow, timeout=30,
                                 headers={"Node-RED-Deployment-Type": "full"})
        if response.status_code not in (200, 204):
            raise RuntimeError(f"Node-RED refused the flow: {response.status_code} {response.text[:300]}")

    def ready(self, scenario: Scenario) -> bool:
        return canary(self.run, scenario)
