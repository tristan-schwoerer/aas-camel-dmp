"""An experiment's figure and the data it plots, from one run's summary.json.

    uv run python -m analysis.figures results/<experiment>/<run>

harness/run.py draws it at the end of a run; this redraws it. Writes into the run folder:

- northbound: ``acked-vs-payload-influxdb-all.pdf/png``, the median latency until the sink acknowledged the
  write against payload size, one line per system and sink, the interquartile range shaded, the PATCH
  reference (the repository write alone) dashed; ``.csv`` one row per line and size
- southbound: ``southbound-components.pdf/png``, each path's median split into the device interface, the DMP and
  BaSyx; ``.csv`` one row per bar
"""

from __future__ import annotations

import csv
import json
import math
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FixedLocator, FuncFormatter, NullFormatter  # noqa: E402

INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"


def measured(run: Path) -> list[tuple[str, dict, dict]]:
    """(system, point, measured phase) of every successful point of the run."""
    summary = json.loads((run / "summary.json").read_text())
    return [(entry["system"], entry["point"], entry["phases"]["measure"]) for entry in summary["points"].values()
            if entry.get("status") == "ok" and entry.get("phases", {}).get("measure")]


def _save(figure, run: Path, name: str, rows: list[dict], dpi: int) -> Path:
    for suffix in ("pdf", "png"):
        figure.savefig(run / f"{name}.{suffix}", dpi=dpi)
    plt.close(figure)
    with open(run / f"{name}.csv", "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return run / f"{name}.pdf"


# --------------------------------------------------------------------------- #
# Northbound: latency until acknowledged against payload size
# --------------------------------------------------------------------------- #
# (system, scenario): label, colour, marker, line style; the reference categorical palette in fixed order,
# the PATCH reference a neutral grey, dashed.
SERIES = [
    (("dmp", "aas-property"), "AAS-Camel-DMP to the AAS repository", "#2a78d6", "o", "-"),
    (("databridge", "aas-property"), "BaSyx DataBridge", "#eb6834", "s", "-"),
    (("nodered-dmp", "aas-property"), "nodered-dmp", "#1baf7a", "^", "-"),
    (("dmp", "influxdb-segment"), "AAS-Camel-DMP to InfluxDB 2", "#eda100", "v", "-"),
    (("patch", "aas-property"), "BaSyx PATCH alone (reference)", "#6f6e69", "D", "--"),
]
COLUMN_WIDTH_IN = 3.54  # 90 mm, one column of a two-column page
PAPER_STYLE = {
    "font.family": "serif", "font.serif": ["STIXGeneral", "Times New Roman", "DejaVu Serif"],
    "mathtext.fontset": "stix", "font.size": 8, "axes.labelsize": 8, "xtick.labelsize": 7, "ytick.labelsize": 7,
    "legend.fontsize": 7, "axes.edgecolor": MUTED, "axes.labelcolor": INK, "xtick.color": MUTED,
    "ytick.color": MUTED, "axes.linewidth": 0.6, "xtick.major.width": 0.6, "ytick.major.width": 0.6,
    "pdf.fonttype": 42, "ps.fonttype": 42,
}


def _size_label(size: int) -> str:
    for unit, factor in (("MiB", 2 ** 20), ("KiB", 2 ** 10)):
        if size >= factor:
            return f"{size // factor} {unit}"
    return f"{size} B"


def payload_axis(ax, sizes: list[int], label_every: int = 4) -> None:
    """A log2 payload axis that shows its scale: gridlines at every power of 4, light ones at every power of 2
    between them, whatever sizes were measured; labels every ``label_every``-fold."""
    low, high = sizes[0] / 1.4, sizes[-1] * 1.4
    powers = [2 ** k for k in range(int(math.floor(math.log2(low))), int(math.ceil(math.log2(high))) + 1)]
    ax.xaxis.set_major_locator(FixedLocator([p for p in powers if math.log2(p) % 2 == 0]))
    ax.xaxis.set_minor_locator(FixedLocator([p for p in powers if math.log2(p) % 2 == 1]))
    step = int(round(math.log2(label_every)))
    ax.xaxis.set_major_formatter(FuncFormatter(
        lambda v, _: _size_label(int(round(v))) if round(math.log2(v)) % step == 0 else ""))
    ax.xaxis.set_minor_formatter(NullFormatter())
    ax.set_xlim(low, high)
    ax.grid(True, which="major", axis="x", color=GRID, linewidth=0.6)
    ax.grid(True, which="minor", axis="x", color=GRID, linewidth=0.35, linestyle=(0, (2, 2)))
    ax.tick_params(axis="x", which="minor", length=2)


def latency_axis(ax, values: list[float], max_labels: int = 8) -> None:
    """A log2 latency axis evenly stepped like the payload axis: a labelled gridline at every power of 2, or, when
    that would be more than ``max_labels``, at every power of 4 with light ones at the powers of 2 between."""
    low, high = min(values) / 1.25, max(values) * 1.25
    powers = [2.0 ** k for k in range(int(math.floor(math.log2(low))), int(math.ceil(math.log2(high))) + 1)]
    powers = [p for p in powers if low <= p <= high]
    step = 1 if len(powers) <= max_labels else 2
    ax.set_yscale("log", base=2)
    ax.yaxis.set_major_locator(FixedLocator([p for p in powers if round(math.log2(p)) % step == 0]))
    ax.yaxis.set_minor_locator(FixedLocator([p for p in powers if round(math.log2(p)) % step != 0]))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.set_ylim(low, high)
    ax.grid(True, which="major", axis="y", color=GRID, linewidth=0.6)
    ax.grid(True, which="minor", axis="y", color=GRID, linewidth=0.35, linestyle=(0, (2, 2)))
    ax.tick_params(axis="y", which="minor", length=2)


def northbound(run: Path) -> Path:
    series: dict[tuple[str, str], list[dict]] = {}
    for system, point, phase in measured(run):
        stats = phase.get("acked_ms") or {}
        if stats.get("n"):
            series.setdefault((system, point["scenario"]), []).append({"size": point["payload_bytes"], **stats})
    if not series:
        raise SystemExit("no point acknowledged a write")
    data = [{"system": key[0], "scenario": key[1], "label": label, "payload_bytes": r["size"], "acknowledged": r["n"],
             "p25_ms": r["p25"], "p50_ms": r["p50"], "p75_ms": r["p75"]}
            for key, label, *_ in SERIES for r in sorted(series.get(key, []), key=lambda r: r["size"])]
    with plt.rc_context(PAPER_STYLE):
        figure, ax = plt.subplots(figsize=(COLUMN_WIDTH_IN, 2.35), constrained_layout=True)
        sizes = sorted({r["size"] for rows in series.values() for r in rows})
        for key, label, color, marker, linestyle in SERIES:
            rows = sorted(series.get(key, []), key=lambda r: r["size"])
            if not rows:
                continue
            x = [r["size"] for r in rows]
            reference = key[0] == "patch"
            ax.fill_between(x, [r["p25"] for r in rows], [r["p75"] for r in rows], color=color,
                            alpha=0.10 if reference else 0.16, linewidth=0, zorder=1)
            ax.plot(x, [r["p50"] for r in rows], color=color, linestyle=linestyle, linewidth=1.3, marker=marker,
                    markersize=3.6, markerfacecolor="white" if reference else color, markeredgewidth=0.9, label=label,
                    zorder=3)
        ax.set_xscale("log", base=2)
        payload_axis(ax, sizes)
        latency_axis(ax, [r[q] for rows in series.values() for r in rows for q in ("p25", "p50", "p75")])
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.set_xlabel("Payload per message (log scale)")
        ax.set_ylabel("Latency until acknowledged (ms, log scale)")
        ax.legend(loc="upper left", frameon=False, handlelength=2.4, borderaxespad=0.2)
        return _save(figure, run, "acked-vs-payload-influxdb-all", data, dpi=300)


# --------------------------------------------------------------------------- #
# Southbound: where the time of a write or a call goes
# --------------------------------------------------------------------------- #
# Who spends the time: the device's own interface (the reference), the DMP, the AAS repository (BaSyx).
COMPONENTS = (("Native interface", "#a3a29e"), ("DMP", "#2a78d6"), ("BaSyx", "#eb6834"))
REFERENCE = COMPONENTS[0][1]


def southbound_medians(run: Path) -> tuple[dict, dict]:
    """Per (path, scenario), the median of a write's latency to the device or a call's round trip, and of a
    write's latency to its change event. A path is the system and its variant."""
    whole, event = {}, {}
    for system, point, phase in measured(run):
        key = (system + (f"-{point['variant']}" if point.get("variant") else ""), point["scenario"])
        write, invoke = phase.get("write"), phase.get("invoke")
        main = write["latency_ms"] if write else invoke["rtt_ms"] if invoke else {}
        if main.get("n"):
            whole[key] = main["p50"]
        if write and write["event_ms"].get("n"):
            event[key] = write["event_ms"]["p50"]
    return whole, event


def southbound_components(median: dict, event: dict) -> list[tuple[str, str | None, str, list[float], float]]:
    """(group, subgroup, label, [device, DMP, BaSyx], total) per realistic path, from the medians. Each bar sums
    to the path's measured median: a write's BaSyx share is the PATCH until its change event reached a consumer
    and the DMP's the rest minus the native write; a call's DMP share is the DMP endpoint's median minus the
    native call (the ablation without BaSyx) and BaSyx's the rest. Writes are subgrouped by the event transport
    the DMP follows; calls have no subgroup."""
    # Short labels: the group and subgroup headings say whether it is a write or a call, and over which events.
    protocols = {"mqtt": "MQTT", "http": "HTTP", "opcua": "OPC UA"}
    rows = []
    for transport, name in (("kafka", "Kafka"), ("mqtt", "MQTT")):
        for protocol, label in protocols.items():
            scenario = f"write-{protocol}"
            native = median.get(("native", scenario))
            total, repository = median.get((f"dmp-{transport}", scenario)), event.get((f"dmp-{transport}", scenario))
            if None not in (native, total, repository):
                rows.append(("Property\nwrite → device", f"{name} eventing", label,
                             [native, total - repository - native, repository], total))
    for protocol, label in protocols.items():
        scenario = f"invoke-{protocol}"
        native, direct, total = (median.get((path, scenario)) for path in ("native", "dmp-dmp", "dmp-aas"))
        if None not in (native, direct, total):
            rows.append(("Operation\ncall → reply", None, label, [native, direct - native, total - direct], total))
    return rows


def southbound(run: Path) -> Path:
    rows = southbound_components(*southbound_medians(run))
    if not rows:
        raise SystemExit("no path has every measurement the figure needs (native, dmp and their variants)")
    data = [{"group": group.replace("\n", " "), "events": sub or "", "protocol": label,
             **{f"{name.split()[0].lower()}_ms": value for (name, _), value in zip(COMPONENTS, parts)}, "total_ms": total}
            for group, sub, label, parts, total in rows]
    groups = list(dict.fromkeys(group for group, *_ in rows))
    subgroups = list(dict.fromkeys((group, sub) for group, sub, *_ in rows if sub is not None))
    gap, heading = 0.5, 0.8  # between groups, and a subgroup's heading row, in rows
    height = len(rows) + gap * (len(groups) - 1) + heading * len(subgroups)
    figure, ax = plt.subplots(figsize=(6.5, 0.3 * height + 0.8), constrained_layout=True)
    # Rows are stacked downwards from edge 0: a bar takes one row, a subgroup heading `heading` rows.
    edge, ticks, labels, spans = 0.0, [], [], []
    for group in groups:
        top, current = edge, None
        for _, sub, label, parts, total in (row for row in rows if row[0] == group):
            if sub is not None and sub != current:
                # The subgroup's name on a thin rule across the plot, on a row of its own above its bars.
                y = edge - heading / 2
                ax.axhline(y, color=GRID, linewidth=0.8, zorder=0)
                ax.annotate(sub, xy=(0, y), xycoords=("axes fraction", "data"), xytext=(4, 0),
                            textcoords="offset points", ha="left", va="center", fontsize=7, color=MUTED,
                            fontstyle="italic", bbox=dict(boxstyle="square,pad=0.2", facecolor="white", edgecolor="none"))
                edge -= heading
                current = sub
            y = edge - 0.5
            left = 0.0
            for value, (_, color) in zip(parts, COMPONENTS):
                ax.barh(y, value, left=left, height=0.62, color=color, edgecolor="white", linewidth=1)
                if value >= 0.25:  # every share labelled: the DMP's is the smallest and the one asked about
                    ax.annotate(f"{value:.1f}", xy=(left + value / 2, y), ha="center", va="center", fontsize=7,
                                color="white" if color != REFERENCE else INK)
                left += value
            ax.annotate(f"{total:.1f} ms", xy=(total, y), xytext=(4, 0), textcoords="offset points", va="center",
                        fontsize=7, color=INK)
            ticks.append(y)
            labels.append(label)
            edge -= 1
        spans.append((group, top, edge))
        edge -= gap
    ax.set_yticks(ticks, labels, fontsize=7, color=INK)
    ax.set_ylim(edge + gap, 0)
    ax.set_xlim(0, max(total for *_, total in rows) * 1.15)
    ax.set_xlabel("ms, median", fontsize=8, color=MUTED)
    ax.grid(True, which="major", color=GRID, linewidth=0.6)
    ax.grid(False, axis="y")
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(MUTED)
    ax.tick_params(colors=MUTED, labelsize=7)
    ax.tick_params(axis="y", length=0, pad=3)
    # Each group's name runs vertically left of its protocol labels, centred on its rows and subgroup headings: no
    # heading rows for the groups, so the figure stays low. The offset clears the widest protocol label.
    renderer = figure.canvas.get_renderer()
    widest = max(t.get_window_extent(renderer).width for t in ax.get_yticklabels()) * 72 / figure.dpi
    names = [ax.annotate(group, xy=(0, (top + bottom) / 2), xycoords=("axes fraction", "data"),
                         xytext=(-widest - 3 - 6, 0), textcoords="offset points", rotation=90, ha="right",
                         va="center", multialignment="center", fontsize=7, color=INK, fontweight="bold")
             for group, top, bottom in spans]
    handles = [plt.Rectangle((0, 0), 1, 1, color=color) for _, color in COMPONENTS]
    # Top right, one entry per line, the swatch right of its name: beside the first group, right of its totals. An
    # opaque frame without an edge, so that the first subgroup's rule stops at the legend.
    ax.legend(handles, [name for name, _ in COMPONENTS], loc="upper right", ncol=1, fontsize=7, frameon=True,
              facecolor="white", edgecolor="none", framealpha=1, handlelength=1.2, borderaxespad=0.2,
              markerfirst=False)
    # Between groups a rule darker than the subgroups', reaching left under the group names, so that a group's
    # last subgroup does not seem to run on into the next group. Its left end is known only after the layout.
    figure.canvas.draw()
    left = min(name.get_window_extent().x0 for name in names) - 4
    left = ax.transAxes.inverted().transform((left, 0))[0]
    for _, _, bottom in spans[:-1]:
        rule, = ax.plot([left, 1], [bottom - gap / 2] * 2, transform=ax.get_yaxis_transform(), color=MUTED,
                        linewidth=0.8, clip_on=False)
        rule.set_in_layout(False)
    return _save(figure, run, "southbound-components", data, dpi=200)


FIGURES = {"northbound": northbound, "southbound": southbound}


def draw(run: Path) -> Path:
    """The figure of the run's experiment, and its data, into the run folder."""
    return FIGURES[json.loads((run / "manifest.json").read_text())["experiment"]](run)


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    print(f"wrote {draw(Path(sys.argv[1]).resolve())}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
