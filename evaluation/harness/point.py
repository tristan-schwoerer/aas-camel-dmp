"""One measurement point: one system, one scenario, one payload size (and the system's variant), on a
freshly started testbed.

Northbound (device → sink):

    testbed down → infrastructure up → system up → deploy → ready
    → capture (O5) → open-loop load (warm-up, measurement) → drain (until the capture is quiet)
    → capture stops → system stops → logs → testbed down → residence.csv → summary.json

Southbound (the AAS writes to or invokes the device): the device (tools/device_sim.py) starts before the
system; once the system is ready, a canary goes through the path until the device has it; a write the
system takes from the repository's change events has those events observed (O7); the client writes
(tools/loadgen.py) or calls (tools/invoke.py) on the schedule; the drain waits until the device is quiet.
The path in between is the adapter's (``load_args``): through the repository and a system, or the native
interface straight to the device.
"""

from __future__ import annotations

import csv
import json
import time
import traceback
from dataclasses import asdict, dataclass
from pathlib import Path

from adapters.base import Repository, Scenario
from analysis.summarize import summarize_point
from harness.testbed import Tool, wait_for

READY_TIMEOUT_S = 180
QUIET_S = 5
DEVICE_ALIAS = "device-sim"      # the device's name on the testbed network (fixtures/scenarios.py)
CANARY = [{"phase": "canary", "rate": 1, "seconds": 1}]


@dataclass
class Point:
    name: str
    system: str
    scenario: str
    payload_bytes: int
    schedule: list[dict]
    cooldown_s: float
    capture: bool = False       # record the system's traffic for O5 (tools/residence.py)
    variant: str | None = None  # how the system takes the scenario (adapter.variants)

    def load_seconds(self) -> float:
        return sum(segment["seconds"] for segment in self.schedule)


def run_point(run, adapter, point: Point, point_dir: Path) -> dict:
    samples = point_dir / "samples"
    samples.mkdir(parents=True, exist_ok=True)
    (point_dir / "point.json").write_text(json.dumps(asdict(point), indent=2) + "\n")
    scenario = Scenario.load(point.scenario)
    testbed = run.testbed
    env = adapter.compose_env(scenario, point_dir, point.variant)
    capture = point.capture and adapter.service is not None
    timeline: dict[str, int] = {}
    tools: list[Tool] = []
    status = {"status": "ok"}

    def mark(event: str) -> None:
        timeline[event] = time.time_ns()
        print(f"  [{point.name}] {event}", flush=True)

    try:
        mark("down")
        testbed.down()
        mark("infrastructure")
        testbed.up_infrastructure(scenario.services, env)
        repository = Repository(f"http://127.0.0.1:{run.ports['aas']}")
        repository.wait()
        if scenario.southbound:
            mark("device")
            tools.append(_start_device(samples))
        mark("system")
        if adapter.service:
            testbed.up_system(adapter.profile, adapter.service, env)
        mark("deploy")
        adapter.deploy(scenario, repository)
        wait_for(lambda: adapter.ready(scenario), READY_TIMEOUT_S, f"{adapter.name} was not ready")
        if scenario.southbound:
            _canary(adapter, scenario, point, samples, READY_TIMEOUT_S)
        mark("ready")

        if scenario.southbound:
            tools.extend(_event_observers(scenario, point, samples))
        if capture:
            tools.append(Tool.capture(testbed.container(adapter.service), samples).start())
            wait_for(lambda: (samples / "sut.pcap").exists(), 30, "the capture did not start")
        mark("load")
        out = "/out/calls.csv" if scenario.kind == "invoke" else "/out/sent.csv"
        _client(adapter, scenario, point, samples, point.schedule, out).run(timeout=point.load_seconds() + 300)
        mark("drain")
        if scenario.southbound:
            _drain(samples / "device.csv", point.cooldown_s)
        elif capture:  # the PATCH reference writes synchronously: nothing to drain
            _drain(samples / "sut.pcap", point.cooldown_s)
        mark("drained")
    except Exception as error:  # noqa: BLE001 (a failed point is recorded, the run goes on)
        status = {"status": "failed", "error": f"{type(error).__name__}: {error}",
                  "traceback": traceback.format_exc()}
        print(f"  [{point.name}] FAILED: {status['error']}", flush=True)
    finally:
        for tool in tools:
            tool.stop()
        try:
            if adapter.service:
                testbed.stop(adapter.profile, adapter.service, env)
            testbed.logs(point_dir / "log" / "compose.log", adapter.profile, *scenario.services)
        except Exception as error:  # noqa: BLE001
            status.setdefault("collectError", f"{type(error).__name__}: {error}")
        testbed.down()
        mark("teardown")
        if capture and (samples / "sut.pcap").exists():
            Tool.own(samples, "sut.pcap")
            try:
                from tools.residence import residence, write
                write(residence(samples / "sut.pcap"), samples / "residence.csv")
            except Exception as error:  # noqa: BLE001 (raw capture kept; summarize again after a fix)
                print(f"  [{point.name}] residence failed: {type(error).__name__}: {error}", flush=True)

    (point_dir / "timeline.json").write_text(json.dumps(timeline, indent=2) + "\n")
    summary = {**status, "point": asdict(point), "system": adapter.name, "timeline": timeline}
    if status["status"] == "ok":
        summary.update(summarize_point(point_dir))
    (point_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def _client(adapter, scenario, point, samples: Path, schedule: list[dict], out: str, seq_start: int = 0) -> Tool:
    """The client of the point's path: publishes or writes (tools/loadgen.py), or calls (tools/invoke.py)."""
    common = ["--size", str(point.payload_bytes), "--seq-start", str(seq_start), "--schedule", json.dumps(schedule),
              "--out", out]
    if scenario.kind == "invoke":
        return Tool("invoke", "invoke", [*adapter.load_args(scenario, point.variant), *common], samples)
    topic = ["--topic", scenario.topic, "--qos", str(scenario.qos)] if scenario.topic else []
    return Tool("loadgen", "loadgen", [*topic, "--format", scenario.message,
                                       *adapter.load_args(scenario, point.variant), *common], samples)


def _start_device(samples: Path) -> Tool:
    tool = Tool("device", "device_sim", ["--out", "/out/device.csv", "--ready", "/out/device.ready"], samples,
                options=("--network-alias", DEVICE_ALIAS)).start()
    wait_for((samples / "device.ready").exists, 60, "the device simulator did not start")
    return tool


def _rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def _canary(adapter, scenario, point, samples: Path, timeout: float) -> None:
    """One marked write or call (seq -1) at a time until the device took a write, or a call's reply carried
    its request back: the path works end to end. The measurement starts after it, so it is never measured."""
    deadline = time.monotonic() + timeout
    while True:
        _client(adapter, scenario, point, samples, CANARY, "/out/canary.csv", seq_start=-1).run(timeout=120)
        if scenario.kind == "invoke":
            if any(row["ok"] == "1" for row in _rows(samples / "canary.csv")):
                return
        elif any(row["seq"] == "-1" for row in _rows(samples / "device.csv")):
            return
        if time.monotonic() > deadline:
            raise TimeoutError(f"no {scenario.kind} reached the device within {timeout:.0f} s")
        time.sleep(1)


def _event_observers(scenario, point, samples: Path) -> list[Tool]:
    """A write the system takes from the repository's change events: those events, on the point's
    transport, so that its latency splits at the event (repository | system and device)."""
    if scenario.kind != "write" or point.variant not in ("kafka", "mqtt"):
        return []
    tool = Tool("events", "observe_events", ["--transport", point.variant, "--submodel", scenario.target_submodel,
                                            "--path", scenario.target_path, "--out", "/out/events.csv",
                                            "--ready", "/out/events.ready"], samples).start()
    wait_for((samples / "events.ready").exists, 60, "the event observer was not positioned")
    return [tool]


def _drain(signal: Path, cooldown_s: float) -> None:
    """Wait until ``signal`` (the capture, or the device's log) has not grown for a while: the system worked off
    its backlog. At most ``cooldown_s``."""
    deadline = time.monotonic() + cooldown_s
    size, quiet_since = -1, time.monotonic()
    while time.monotonic() < deadline:
        current = signal.stat().st_size if signal.exists() else 0
        if current != size:
            size, quiet_since = current, time.monotonic()
        elif time.monotonic() - quiet_since >= QUIET_S:
            return
        time.sleep(0.5)
