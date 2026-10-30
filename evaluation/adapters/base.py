"""The adapter contract and the shared scenario and repository helpers.

The runner calls, per measurement point, on a freshly started testbed:

    prepare(run)               once per run: build or resolve the system's image, return manifest entries
    supports(s)                whether the system takes the scenario at all
    variants(s, config)        the ways the system takes a scenario, one point each (the DMP's event
                               transport, or where an Operation is invoked); [None] for one way
    compose_env(s, point, v)   environment for the testbed's Compose services
    deploy(s, repository)      turn the shared scenario into the system's configuration and apply it
    ready(s)                   true once the system reports the mapping running
    load_args(s, v)            how the load reaches the system: further arguments for tools/loadgen.py (or
                               tools/invoke.py for an Operation)

The scenario reaches an adapter as :class:`Scenario`, read from test_aas/ (an AAS environment and its manifest entry).
"""

from __future__ import annotations

import base64
import hashlib
import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

TEST_AAS = Path(__file__).resolve().parent.parent / "test_aas"
# Inside the testbed network (tools, systems under test).
REPOSITORY_INTERNAL = "http://aas-env:8081"


@dataclass(frozen=True)
class Scenario:
    id: str
    broker: str
    qos: int
    topic: str
    aimc: str
    target_submodel: str
    target_path: str
    environment: dict[str, Any]
    sink: str = "aas"               # where the value goes: the AAS repository, "influxdb2" (a TimeSeries segment), "device"
    influxdb: dict | None = None    # for an influxdb2 sink: origin, org, bucket, measurement, field, tags
    kind: str = "observe"           # observe: device → AAS; write, invoke: AAS → device
    device: dict | None = None      # write, invoke: where the device takes it (protocol, topic or url)
    delegation_path: str | None = None  # invoke: the path of the Operation's invocationDelegation URL

    @property
    def southbound(self) -> bool:
        return self.kind != "observe"

    @property
    def message(self) -> str:
        """The client's message form (tools/loadgen.py --format): a JSON device message, or the value itself."""
        return "value" if self.southbound else "extract"

    @property
    def services(self) -> tuple[str, ...]:
        """Testbed services beyond the shared infrastructure that the scenario needs."""
        return ("influxdb",) if self.sink == "influxdb2" else ()

    @staticmethod
    def load(scenario_id: str) -> "Scenario":
        manifest = json.loads((TEST_AAS / "manifest.json").read_text())
        entry = manifest["scenarios"].get(scenario_id)
        if entry is None:
            raise SystemExit(f"unknown scenario {scenario_id!r}; known: {', '.join(manifest['scenarios'])}")
        text = (TEST_AAS / f"{scenario_id}.json").read_bytes()
        if hashlib.sha256(text).hexdigest() != entry["sha256"]:
            raise SystemExit(f"test_aas/{scenario_id}.json does not match manifest.json; "
                             "regenerate it with `uv run python -m scenarios` in fixtures/")
        return Scenario(scenario_id, manifest["broker"], manifest["qos"], entry["topic"], entry["aimc"],
                        entry["target"]["submodel"], entry["target"]["path"], json.loads(text), entry["sink"],
                        entry.get("influxdb"), entry.get("kind", "observe"),
                        entry.get("device"), entry.get("delegationPath"))


def java_runtime(image: str) -> str | None:
    """The JVM the image runs, as ``java -version`` names it (vendor and exact build); None if it cannot tell."""
    try:
        version = subprocess.run(["docker", "run", "--rm", "--entrypoint", "java", image, "-version"], check=True,
                                 capture_output=True, text=True, timeout=60).stderr
    except (OSError, subprocess.SubprocessError):
        return None
    return next((line.strip() for line in version.splitlines() if "Runtime Environment" in line), None)


def b64url(identifier: str) -> str:
    return base64.urlsafe_b64encode(identifier.encode()).decode().rstrip("=")


class Repository:
    """The testbed's AAS repository, from the host."""

    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self.session = requests.Session()

    def wait(self, timeout: float = 180) -> None:
        deadline = time.monotonic() + timeout
        while True:
            try:
                if self.session.get(f"{self.url}/shells", timeout=5).status_code == 200:
                    return
            except requests.RequestException:
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(f"AAS repository {self.url} did not answer within {timeout:.0f} s")
            time.sleep(1)

    def post_environment(self, environment: dict, *, last: str | None = None) -> None:
        """Every submodel, then the shells. ``last`` names a submodel posted after the others (an
        AIMC, so that what it references exists when a system reads it)."""
        submodels = sorted(environment["submodels"], key=lambda s: s["id"] == last)
        for submodel in submodels:
            self._post("submodels", submodel)
        for shell in environment.get("assetAdministrationShells", []):
            self._post("shells", shell)

    def post_submodel(self, submodel: dict) -> None:
        self._post("submodels", submodel)

    def _post(self, collection: str, body: dict) -> None:
        response = self.session.post(f"{self.url}/{collection}", json=body, timeout=30)
        if response.status_code not in (200, 201):
            raise RuntimeError(f"POST /{collection} {body.get('id')}: {response.status_code} {response.text[:500]}")


def canary(run, scenario: Scenario) -> bool:
    """Publish one marked device message (seq -1) and tell whether the repository holds it: the
    end-to-end readiness of a system that reports none of its own. Called until it holds; the
    load starts after it, so the canary is never measured."""
    t_send = str(time.time_ns())
    run.testbed.publish(scenario.topic, json.dumps({"seq": -1, "tSend": t_send, "value": f"-1|{t_send}|"}), scenario.qos)
    time.sleep(0.5)
    url = (f"http://127.0.0.1:{run.ports['aas']}/submodels/{b64url(scenario.target_submodel)}"
           f"/submodel-elements/{scenario.target_path}/$value")
    try:
        response = requests.get(url, timeout=5)
    except requests.RequestException:
        return False
    from tools.common import device_value
    value = device_value(response.content) if response.status_code == 200 else None
    return value is not None and value[0] == -1


class Adapter:
    name: str = ""
    profile: str | None = ""     # the Compose profile that starts the system; None: it has no container
    service: str | None = ""     # its Compose service (logs, capture)

    def supports(self, scenario: Scenario) -> bool:
        return True

    def prepare(self, run) -> dict:
        return {}

    def variants(self, scenario: Scenario, config: dict) -> list[str | None]:
        return [None]

    def compose_env(self, scenario: Scenario, point_dir: Path, variant: str | None = None) -> dict[str, str]:
        return {}

    def deploy(self, scenario: Scenario, repository: Repository) -> None:
        raise NotImplementedError

    def ready(self, scenario: Scenario) -> bool:
        raise NotImplementedError

    def load_args(self, scenario: Scenario, variant: str | None = None) -> list[str]:
        """Further arguments for the load generator (the PATCH reference writes instead of publishing); for a
        southbound scenario, how its writes or calls reach the device (tools/loadgen.py, tools/invoke.py)."""
        return []
