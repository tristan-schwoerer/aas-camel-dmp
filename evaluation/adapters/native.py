"""The native interface: the device's own protocol, called by the client itself (the southbound baseline).

No AAS and no system in between: tools/loadgen.py publishes or PUTs each write straight to the device,
tools/invoke.py calls the device's action itself (MQTT 5 request/reply, or a POST), with the same value,
the same QoS and the same device (tools/device_sim.py) as every system's path. What a system adds is its
path's time minus this one's.
"""

from __future__ import annotations

from adapters.base import Adapter, Repository, Scenario


class NativeAdapter(Adapter):
    name = "native"
    profile = None  # nothing to start: the client calls the device
    service = None

    def supports(self, scenario: Scenario) -> bool:
        return scenario.southbound

    def prepare(self, run) -> dict:
        self.run = run
        return {"image": run.harness_image, "implementation": "tools/loadgen.py, tools/invoke.py to the device"}

    def deploy(self, scenario: Scenario, repository: Repository) -> None:
        # The same repository content as every other system, though nothing reads it.
        repository.post_environment(scenario.environment, last=scenario.aimc)

    def ready(self, scenario: Scenario) -> bool:
        return True

    def load_args(self, scenario: Scenario, variant: str | None = None) -> list[str]:
        device = scenario.device
        if device["protocol"] == "opcua":
            if scenario.kind == "invoke":
                return ["--target", "opcua", "--url", device["url"], "--object", device["object"],
                        "--method", device["method"]]
            return ["--target", "opcua", "--url", device["url"], "--node", device["node"]]
        if device["protocol"] == "http":
            return ["--target", "http", "--url", device["url"]]
        if scenario.kind == "invoke":
            return ["--target", "mqtt", "--topic", device["topic"], "--response-topic", device["responseTopic"]]
        return ["--target", "mqtt"]  # loadgen publishes on the scenario's topic, the device's
