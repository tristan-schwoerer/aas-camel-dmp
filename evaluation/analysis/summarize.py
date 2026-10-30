"""Raw samples → summary.json, per point and per run.

The runner calls this after every point; it can be run again at any time, since it reads only a point's
raw samples and point.json:

    uv run python -m analysis.summarize results/<experiment>/<run> [more runs…]

Only the ``measure`` phase counts (the warm-up does not). A northbound point (device → sink) has:

- ``acked_ms``  from tSend to the sink's answer to the write reaching the system (O5, ``residence.csv``
  from the capture): end to end until the write is acknowledged, alike for the AAS repository and InfluxDB,
  which emits no BaSyx event. The PATCH reference has no capture: its own request round trip.

A southbound point (``device.csv`` from tools/device_sim.py) has:

- ``write``   a Property write: ``latency_ms`` from the client starting the write (the PATCH, or the native
  publish, PUT or Write) to the device having it; with the repository's events observed (O7), ``event_ms``
  from the write to the event reaching a consumer; ``lost``
- ``invoke``  an Operation call: ``rtt_ms`` from the client sending to having the reply, of the calls whose
  reply carried their request back; ``failed`` and the statuses
"""

from __future__ import annotations

import csv
import json
import math
import sys
from collections import Counter
from pathlib import Path

NS_PER_MS = 1e6
MEASURED = "measure"


def percentile(ordered: list[float], q: float) -> float:
    """Linear interpolation between closest ranks (NumPy's default), of sorted data."""
    position = (len(ordered) - 1) * q / 100
    low, high = math.floor(position), math.ceil(position)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def describe(values) -> dict:
    ordered = sorted(values)
    if not ordered:
        return {"n": 0}
    return {"n": len(ordered), "min": ordered[0], "p25": percentile(ordered, 25), "p50": percentile(ordered, 50),
            "p75": percentile(ordered, 75), "p99": percentile(ordered, 99), "max": ordered[-1]}


def _rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def _first(rows: list[dict]) -> dict[int, int]:
    """The first ``t_recv_ns`` of every seq."""
    first: dict[int, int] = {}
    for row in rows:
        first.setdefault(int(row["seq"]), int(row["t_recv_ns"]))
    return first


def _ms(later: int, earlier: int) -> float:
    return (later - earlier) / NS_PER_MS


def _northbound(samples: Path, rows: list[dict]) -> dict:
    stays = {int(r["seq"]): r for r in _rows(samples / "residence.csv")}
    if not (samples / "residence.csv").exists():
        done = [r for r in rows if r.get("t_done_ns")]
        return {"acknowledged": len(done), "acked_ms": describe(_ms(int(r["t_done_ns"]), int(r["t_send_ns"])) for r in done)}
    acked = [(r, stays[int(r["seq"])]) for r in rows if stays.get(int(r["seq"]), {}).get("t_ack_ns")]
    return {"acknowledged": len(acked),
            "matched_by_order": sum(1 for _, stay in acked if stay["matched"] in ("order", "batch")),
            "acked_ms": describe(_ms(int(stay["t_ack_ns"]), int(r["t_send_ns"])) for r, stay in acked)}


def _southbound(samples: Path, rows: list[dict], invoke: bool) -> dict:
    device = _first(_rows(samples / "device.csv"))
    if invoke:
        ok = [r for r in rows if r["ok"] == "1"]
        return {"invoke": {"failed": len(rows) - len(ok), "statuses": dict(Counter(r["status"] for r in rows)),
                           "rtt_ms": describe(_ms(int(r["t_done_ns"]), int(r["t_send_ns"])) for r in ok)}}
    events = _first(_rows(samples / "events.csv"))
    arrived = [r for r in rows if int(r["seq"]) in device]
    seen = [r for r in arrived if int(r["seq"]) in events]
    return {"write": {"lost": len(rows) - len(arrived),
                      "latency_ms": describe(_ms(device[int(r["seq"])], int(r["t_send_ns"])) for r in arrived),
                      "event_ms": describe(_ms(events[int(r["seq"])], int(r["t_send_ns"])) for r in seen)}}


def summarize_point(point_dir: Path) -> dict:
    samples = point_dir / "samples"
    invoke = (samples / "calls.csv").exists()
    rows = [r for r in _rows(samples / ("calls.csv" if invoke else "sent.csv")) if r["phase"] == MEASURED]
    if not rows:
        return {"phases": {}}
    duration = _ms(int(rows[-1]["t_send_ns"]), int(rows[0]["t_send_ns"])) / 1000
    phase = {"sent": len(rows), "achieved_rate_hz": (len(rows) - 1) / duration if duration > 0 else None}
    if (samples / "device.csv").exists():
        phase.update(_southbound(samples, rows, invoke))
    else:
        phase.update(_northbound(samples, rows))
    return {"phases": {MEASURED: phase}}


def summarize_run(run_dir: Path) -> dict:
    manifest = json.loads((run_dir / "manifest.json").read_text())
    points = {}
    for name in manifest["points"]:
        path = run_dir / "points" / name / "summary.json"
        points[name] = json.loads(path.read_text()) if path.exists() else {"status": "not run"}
    summary = {"experiment": manifest["experiment"], "run": run_dir.name, "points": points}
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    for run_dir in map(Path, sys.argv[1:]):
        for point_dir in sorted((run_dir / "points").iterdir()):
            path = point_dir / "summary.json"
            if not path.exists():
                continue
            summary = json.loads(path.read_text())
            if summary.get("status") == "ok":
                summary.update(summarize_point(point_dir))
                path.write_text(json.dumps(summary, indent=2) + "\n")
        summarize_run(run_dir)
        print(f"summarized {run_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
