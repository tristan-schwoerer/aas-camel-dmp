"""Run one experiment: every system it names, one figure and its data.

    uv run python -m harness.run experiments/northbound.yaml
    uv run python -m harness.run experiments/southbound.yaml --dry-run
    uv run python -m harness.run experiments/southbound.yaml --resume results/southbound/<run>

A run is the experiment's points (system × scenario × payload size × the system's variants of a southbound
scenario), each on a freshly started testbed, in a seeded random order. Everything goes to
``results/<experiment>/<UTC time>/``: ``manifest.json`` (versions, images, host, parameters), ``points/<point>/``
(raw samples, logs, the point's summary), ``summary.json``, and the experiment's figure (``.pdf``, ``.png``)
with the data it plots (``.csv``). A failed point is recorded and the run goes on; ``--resume`` repeats only
the points without a successful summary.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

from adapters import ADAPTERS
from adapters.base import Scenario
from analysis.figures import draw
from analysis.summarize import summarize_run
from harness.point import Point, run_point
from harness.testbed import (EVALUATION, HARNESS_IMAGE, INFRA_CPUSET, PROJECT, SUT_CPUSET, TOOLS_CPUSET, Testbed,
                             build_harness)

# Per point beyond its load: testbed up, deploy, ready, drain, teardown (measured: 30–65 s; for --dry-run).
OVERHEAD_S = 45


class Run:
    def __init__(self, directory: Path, testbed: Testbed, harness_image: str, options: dict):
        self.dir = directory
        self.testbed = testbed
        self.ports = testbed.ports
        self.harness_image = harness_image
        self.options = options


def plan(config: dict, adapters: dict) -> list[Point]:
    limits = config.get("max_payload_bytes", {})
    points = [Point(name="-".join([system, scenario, *([variant] if variant else []), f"{size}B"]), system=system,
                    scenario=scenario, payload_bytes=size, schedule=config["schedule"], cooldown_s=config["cooldown_s"],
                    capture=bool(config.get("capture", False)), variant=variant)
              for system, adapter in adapters.items()
              for scenario in config["scenarios"] if adapter.supports(Scenario.load(scenario))
              for size in config["payload_bytes"] if size <= limits.get(scenario, size)
              for variant in adapter.variants(Scenario.load(scenario), config)]
    for system in adapters:
        if not any(point.system == system for point in points):
            raise SystemExit(f"{system} takes none of {config['experiment']}'s scenarios")
    random.Random(config["seed"]).shuffle(points)
    return points


def _out(*command: str) -> str:
    try:
        return subprocess.run(command, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def host() -> dict:
    cpu = next((line.split(":", 1)[1].strip() for line in _out("lscpu").splitlines()
                if line.startswith("Model name")), platform.processor())
    memory = next((int(line.split()[1]) * 1024 for line in Path("/proc/meminfo").read_text().splitlines()
                   if line.startswith("MemTotal")), None)
    governor = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    return {"cpu": cpu, "cores": os.cpu_count(), "memoryBytes": memory, "kernel": platform.release(),
            "os": _out("sh", "-c", ". /etc/os-release && echo $PRETTY_NAME"),
            "docker": _out("docker", "version", "--format", "{{.Server.Version}}"),
            "compose": _out("docker", "compose", "version", "--short"),
            "governor": governor.read_text().strip() if governor.exists() else None}


def other_containers() -> list[str]:
    """Running containers outside the testbed: the host is not quiet while there are any."""
    rows = _out("docker", "ps", "--format", '{{.Names}}\t{{.Label "com.docker.compose.project"}}').splitlines()
    return sorted(name for name, _, project in (row.partition("\t") for row in rows)
                  if project != PROJECT and not name.startswith("dmp-eval-"))


def harness_digest() -> str:
    """SHA-256 over the harness's own files, so a run names the exact scripts that made it."""
    digest = hashlib.sha256()
    for path in sorted(EVALUATION.rglob("*")):
        relative = path.relative_to(EVALUATION)
        if path.is_file() and not ({".venv", "results", "__pycache__"} & set(relative.parts)):
            digest.update(str(relative).encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def git(*args: str) -> str:
    return _out("git", "-C", str(EVALUATION), *args)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config", type=Path, help="experiments/<id>.yaml")
    parser.add_argument("--dmp-image", help="measure this DMP image instead of building one from the checkout")
    parser.add_argument("--jvm-heap", default="1g", help="-Xmx of every JVM system under test (default 1g)")
    parser.add_argument("--resume", type=Path, help="a run directory to complete")
    parser.add_argument("--results", type=Path, default=EVALUATION / "results")
    parser.add_argument("--aas-port", type=int, default=28081)
    parser.add_argument("--dmp-port", type=int, default=28086)
    parser.add_argument("--nodered-port", type=int, default=28880)
    parser.add_argument("--require-quiet-host", action="store_true",
                        help="refuse to run while other containers are running")
    parser.add_argument("--dry-run", action="store_true", help="print the plan and its duration only")
    args = parser.parse_args()

    config = yaml.safe_load(args.config.read_text())
    adapters = {system: ADAPTERS[system]() for system in config["systems"]}
    points = plan(config, adapters)
    hours = sum(point.load_seconds() + OVERHEAD_S for point in points) / 3600
    print(f"{config['experiment']}: {len(points)} points on {', '.join(adapters)}, about {hours:.1f} h", flush=True)
    if args.dry_run:
        for point in points:
            print(f"  {point.name}  ({point.load_seconds():.0f} s of load)")
        return 0

    others = other_containers()
    if others:
        message = f"the host is not quiet: {len(others)} other containers running ({', '.join(others[:8])}…)"
        if args.require_quiet_host:
            raise SystemExit(message)
        print(f"WARNING: {message}; recorded in the manifest", flush=True)

    if args.resume:
        run_dir = args.resume.resolve()
        manifest = json.loads((run_dir / "manifest.json").read_text())
        if manifest["config"] != config:
            raise SystemExit(f"{run_dir} was made with another configuration")
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_dir = (args.results / config["experiment"] / stamp).resolve()
        run_dir.mkdir(parents=True)

    testbed = Testbed({"aas": args.aas_port, "dmp": args.dmp_port, "nodered": args.nodered_port})
    print("building the harness image", flush=True)
    harness_id = build_harness()
    run = Run(run_dir, testbed, HARNESS_IMAGE, {"dmp_image": args.dmp_image, "jvm_heap": args.jvm_heap})
    systems = {system: adapter.prepare(run) for system, adapter in adapters.items()}

    if not args.resume:
        scenarios = json.loads((EVALUATION / "test_aas" / "manifest.json").read_text())
        (run_dir / "testbed.resolved.yml").write_text(testbed.resolved_config())
        manifest = {
            "experiment": config["experiment"], "config": config, "configFile": str(args.config),
            "started": datetime.now(timezone.utc).isoformat(), "command": sys.argv,
            "repository": {"commit": git("rev-parse", "HEAD"),
                           "evaluationDirty": bool(git("status", "--porcelain", "--", "."))},
            "harness": {"digest": harness_digest(), "image": HARNESS_IMAGE, "imageId": harness_id},
            "systemsUnderTest": systems,
            "scenarios": {"aasModelCommit": scenarios["aasModelCommit"],
                          "sha256": {k: v["sha256"] for k, v in scenarios["scenarios"].items()}},
            "testbedImages": testbed.images(),
            "limits": {"cpus": os.environ.get("SUT_CPUS", "2"), "memory": os.environ.get("SUT_MEMORY", "2g"),
                       "cpusets": {"systemUnderTest": SUT_CPUSET, "infrastructure": INFRA_CPUSET, "tools": TOOLS_CPUSET}},
            "host": host(), "otherContainers": others,
            "points": [point.name for point in points],
        }
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    started = time.monotonic()
    for index, point in enumerate(points, 1):
        point_dir = run_dir / "points" / point.name
        previous = point_dir / "summary.json"
        if previous.exists() and json.loads(previous.read_text()).get("status") == "ok":
            continue
        print(f"[{index}/{len(points)}] {point.name}  ({(time.monotonic() - started) / 60:.0f} min elapsed)",
              flush=True)
        run_point(run, adapters[point.system], point, point_dir)

    summary = summarize_run(run_dir)
    failed = [name for name, entry in summary["points"].items() if entry.get("status") != "ok"]
    if failed:
        print(f"{len(failed)} failed points: {', '.join(failed)}; complete them with --resume {run_dir}")
    try:
        print(f"wrote {draw(run_dir)}")
    except SystemExit as error:
        print(f"no figure: {error}")
        return 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
