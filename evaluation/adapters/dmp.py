"""AAS-Camel-DMP, built from this repository (or a given image).

The DMP finds the scenario's AIMC through the repository's Kafka events, so deploying is posting the
scenario into the repository; it is ready when ``/management/mappings`` reports the AIMC's mapping installed.

Southbound, a point's variant says how the DMP is reached: for a Property write the event transport it
takes the repository's changes from (``kafka`` or ``mqtt``; BaSyx then publishes its events on that
transport only); for an Operation whether the client invokes it through the repository (``aas``, BaSyx
delegates it) or calls the DMP's delegation endpoint itself (``dmp``).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import requests

from adapters.base import REPOSITORY_INTERNAL, Adapter, Repository, Scenario, java_runtime

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
INFLUX_TOKEN = "dmp-eval-token"  # testbed/docker-compose.yml, influxdb
DMP_INTERNAL = "http://dmp:8080"  # the DMP inside the testbed network (delegated Operations)


def _git(*args: str) -> str:
    return subprocess.run(["git", "-C", str(REPOSITORY_ROOT), *args], check=True,
                          capture_output=True, text=True).stdout.strip()


class DmpAdapter(Adapter):
    name = "dmp"
    profile = "dmp"
    service = "dmp"

    def prepare(self, run) -> dict:
        self.run = run
        self.status_url = f"http://127.0.0.1:{run.ports['dmp']}"
        image = run.options.get("dmp_image")
        if image:
            commit, dirty = None, None
        else:
            commit = _git("rev-parse", "HEAD")
            # The image is built from everything but evaluation/ (.dockerignore).
            dirty = bool(_git("status", "--porcelain", "--", ".", ":!evaluation"))
            image = f"dmp:eval-{commit[:12]}" + ("-dirty" if dirty else "")
            print(f"[dmp] building {image} from {REPOSITORY_ROOT}", flush=True)
            subprocess.run(["docker", "build", "-q", "-t", image, str(REPOSITORY_ROOT)], check=True,
                           stdout=subprocess.DEVNULL)
        self.image = image
        image_id = subprocess.run(["docker", "image", "inspect", "-f", "{{.Id}}", image], check=True,
                                  capture_output=True, text=True).stdout.strip()
        return {"image": image, "imageId": image_id, "runtime": java_runtime(image), "commit": commit, "dirty": dirty,
                "heap": run.options["jvm_heap"]}

    def variants(self, scenario: Scenario, config: dict) -> list[str | None]:
        if scenario.kind == "write":
            return list(config.get("event_transports", ["kafka"]))
        if scenario.kind == "invoke":
            return list(config.get("invoke_via", ["aas"]))
        return [None]

    def compose_env(self, scenario: Scenario, point_dir: Path, variant: str | None = None) -> dict[str, str]:
        env = {"DMP_IMAGE": self.image, "JVM_HEAP": self.run.options["jvm_heap"]}
        if scenario.influxdb:
            env.update({"DMP_INFLUX_URL": scenario.influxdb["origin"], "DMP_INFLUX_ORG": scenario.influxdb["org"],
                        "DMP_INFLUX_TOKEN": INFLUX_TOKEN})
        if variant == "mqtt":
            env.update({"DMP_EVENT_TRANSPORT": "mqtt", "BASYX_KAFKA_ENABLED": "false", "BASYX_MQTT_ENABLED": "true"})
        return env

    def load_args(self, scenario: Scenario, variant: str | None = None) -> list[str]:
        repository = ["--repository", REPOSITORY_INTERNAL.removeprefix("http://"),
                      "--submodel", scenario.target_submodel, "--path", scenario.target_path]
        if scenario.kind == "write":
            # The client writes the Property; the DMP takes its change event to the device.
            return ["--target", "patch", *repository]
        if scenario.kind == "invoke":
            if variant == "dmp":
                return ["--target", "dmp", "--url", DMP_INTERNAL + scenario.delegation_path]
            return ["--target", "aas", *repository]
        return []

    def deploy(self, scenario: Scenario, repository: Repository) -> None:
        repository.post_environment(scenario.environment, last=scenario.aimc)

    def ready(self, scenario: Scenario) -> bool:
        try:
            report = requests.get(f"{self.status_url}/management/mappings",
                                  params={"aimc": scenario.aimc}, timeout=5).json()
        except (requests.RequestException, ValueError):
            return False
        mappings = report.get("mappings", [])
        broken = [m for m in mappings if m.get("state") in ("blocked", "failed")]
        if broken:
            raise RuntimeError(f"DMP mapping not installed: {broken}")
        return bool(mappings) and all(m.get("state") == "installed" for m in mappings)
