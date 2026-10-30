"""Eclipse BaSyx DataBridge, the system the DMP replaces (its last image, 1.0.0-milestone-08;
the project is archived).

DataBridge reads no AIMC: it is configured with its own files, which the adapter writes
from the scenario (broker, topic, target submodel and element), as its documentation
describes: an MQTT consumer, a JSONata transformer and an AAS server sink with the
``DotAAS-V3`` API (``PATCH …/submodel-elements/<path>/$value``).

Its AAS sink sends the body between two quotes without escaping it, so a value must reach it
already encoded as a JSON string, or BaSyx stores a broken value. The JSONata transformer picks
aas-property's ``value`` member, which JSONata serializes as a JSON string. Disclosed with the results.

The subscription QoS is camel-paho's default (2); the broker delivers at the publish's QoS 1.
Readiness is a canary message: DataBridge's health endpoint does not cover its routes.
"""

from __future__ import annotations

import json
from pathlib import Path

from adapters.base import Adapter, Repository, Scenario, b64url, canary, REPOSITORY_INTERNAL

IMAGE = "eclipsebasyx/databridge:1.0.0-milestone-08@sha256:206bc8a68ea73e06dac7d048c0bd9c4133697b7872790bf4b7bee44a10f1c16b"
SOURCE_COMMIT = "f9ea5d8e4b0cce97d492656d0ff3c4a992856b07"

# The member, a string: JSONata serializes it as a JSON string, the form the sink needs.
TRANSFORMATIONS = {"aas-property": "value"}


def configuration(scenario: Scenario) -> dict[str, str]:
    """DataBridge's configuration files for the scenario, by file name."""
    host, _, port = scenario.broker.removeprefix("mqtt://").rstrip("/").partition(":")
    return {
        "routes.json": [{"datasource": "device", "transformers": ["transformation"], "datasinks": ["target"],
                         "trigger": "event"}],
        "mqttconsumer.json": [{"uniqueId": "device", "serverUrl": host, "serverPort": int(port or 1883),
                               "topic": scenario.topic, "clientId": "dmp-eval-databridge"}],
        "jsonatatransformer.json": [{"uniqueId": "transformation", "queryPath": "transformation.jsonata",
                                     "inputType": "JsonString", "outputType": "JsonString"}],
        "aasserver.json": [{"uniqueId": "target", "api": "DotAAS-V3", "idShortPath": scenario.target_path,
                            "submodelEndpoint": f"{REPOSITORY_INTERNAL}/submodels/{b64url(scenario.target_submodel)}"}],
        "transformation.jsonata": TRANSFORMATIONS[scenario.id],
    }


class DataBridgeAdapter(Adapter):
    name = "databridge"
    profile = "databridge"
    service = "databridge"

    def supports(self, scenario: Scenario) -> bool:
        return scenario.id in TRANSFORMATIONS

    def prepare(self, run) -> dict:
        self.run = run
        return {"image": IMAGE, "sourceCommit": SOURCE_COMMIT, "heap": run.options["jvm_heap"],
                "adaptations": ["configured with its own files, written from the scenario (it reads no AIMC)",
                                "the value member picked by a JSONata transformer, which also encodes it as a JSON "
                                "string (its AAS sink quotes without escaping)",
                                "per-message INFO logging set to WARN"]}

    def compose_env(self, scenario: Scenario, point_dir: Path, variant: str | None = None) -> dict[str, str]:
        directory = point_dir / "config" / "databridge"
        directory.mkdir(parents=True, exist_ok=True)
        for name, content in configuration(scenario).items():
            (directory / name).write_text(content if isinstance(content, str) else json.dumps(content, indent=2) + "\n")
        return {"EVAL_DATABRIDGE_CONFIG": str(directory), "JVM_HEAP": self.run.options["jvm_heap"]}

    def deploy(self, scenario: Scenario, repository: Repository) -> None:
        # The same repository content as every other system; DataBridge needs only the target.
        repository.post_environment(scenario.environment, last=scenario.aimc)

    def ready(self, scenario: Scenario) -> bool:
        return canary(self.run, scenario)
