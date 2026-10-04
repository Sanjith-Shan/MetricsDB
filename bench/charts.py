#!/usr/bin/env python3
"""README charts from results/*.jsonl. Usage: python3 bench/charts.py [results_dir] [out_dir]

Each chart answers one question with one axis. Colours follow a fixed categorical order:
MetricsDB, VictoriaMetrics, InfluxDB always get the same slot, whatever is plotted.
"""
import json
import os
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RES = sys.argv[1] if len(sys.argv) > 1 else "results"
OUT = sys.argv[2] if len(sys.argv) > 2 else "docs"
os.makedirs(OUT, exist_ok=True)

# validated categorical slots 1-3 (blue, orange, aqua), then neutral ink
COLOR = {"metricsdb_nofsync": "#2a78d6", "metricsdb": "#2a78d6", "victoriametrics": "#eb6834", "influxdb": "#1baf7a", "raw": "#8a8985"}
NAME = {"metricsdb_nofsync": "MetricsDB, fsync off", "metricsdb": "MetricsDB", "victoriametrics": "VictoriaMetrics", "influxdb": "InfluxDB", "raw": "Uncompressed"}
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"
ORDER = ["metricsdb", "victoriametrics", "influxdb"]
DBKEY = {"metricsdb-fsync-off": "metricsdb_nofsync", "metricsdb": "metricsdb", "vm": "victoriametrics", "victoriametrics": "victoriametrics",
         "influx": "influxdb", "influxdb": "influxdb"}

plt.rcParams.update({
    "font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED,
    "ytick.color": MUTED, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "text.color": INK, "figure.dpi": 100, "savefig.dpi": 100, "savefig.bbox": "tight",
})


def rows(name):
    p = os.path.join(RES, name)
    if not os.path.exists(p):
        return []
    return [json.loads(l) for l in open(p) if l.strip()]


def latest_by(rs, key):
    out = {}
    for r in rs:
        out[key(r)] = r
    return out


def save(fig, name):
    path = os.path.join(OUT, name)
    fig.savefig(path)
    plt.close(fig)
    print(path, os.path.getsize(path), "bytes")


def bar_chart(values, title, xlabel, fname, fmt):
    """Horizontal bars, one per database, value labelled at the bar end."""
    keys = [k for k in ["metricsdb", "metricsdb_nofsync", "victoriametrics", "influxdb", "raw"] if k in values]
    fig, ax = plt.subplots(figsize=(7, 0.55 * len(keys) + 1.1))
    ys = range(len(keys))[::-1]
    bars = ax.barh(list(ys), [values[k] for k in keys], color=[COLOR[k] for k in keys], height=0.55)
    for b, k in zip(bars, keys):
        if k == "metricsdb_nofsync":
            b.set_hatch("///")  # same entity, variant: texture, not a new colour
            b.set_edgecolor("white")
    ax.set_yticks(list(ys), [NAME[k] for k in keys])
    for y, k in zip(ys, keys):
        ax.text(values[k], y, "  " + fmt(values[k]), va="center", color=INK)
    ax.set_xlabel(xlabel)
    ax.set_title(title, loc="left", fontsize=11, color=INK)
    ax.grid(axis="y", visible=False)
    ax.set_xlim(0, max(values.values()) * 1.25)
    save(fig, fname)


def exp1():
    rs = [r for r in rows("exp1.jsonl") if not r.get("label", "").startswith("prelim")]
    if not rs:
        return
    last = latest_by(rs, lambda r: DBKEY.get(r.get("label", r["db"]), DBKEY.get(r["db"], r["db"])))
    last.pop("metricsdb_nofsync", None)
    vals = {k: v["bytes_per_sample"] for k, v in last.items()}
    vals["raw"] = 16
    bar_chart(vals, "Bytes per sample on disk, standard benchmark (TSBS DevOps, 87.3M samples)",
              "bytes per sample (lower is better)", "bytes_per_sample.png", lambda v: f"{v:.2f}")


def exp2():
    rs = rows("exp2.jsonl")
    if not rs:
        return
    rs = [r for r in rs if r["db"] != "metricsdb-cluster"]
    last = latest_by(rs, lambda r: DBKEY.get(r.get("label", r["db"]), DBKEY.get(r["db"], r["db"])))
    vals = {k: v["samples_per_s"] / 1000 for k, v in last.items()}
    bar_chart(vals, "Ingest throughput, same loader and data, one database at a time",
              "thousand samples per second (higher is better)", "ingest_throughput.png", lambda v: f"{v:,.0f}k")


TYPE_ORDER = ["single-groupby-1-1-1", "single-groupby-1-1-12", "single-groupby-1-8-1", "single-groupby-5-1-1",
              "single-groupby-5-1-12", "single-groupby-5-8-1", "cpu-max-all-1", "cpu-max-all-8",
              "double-groupby-1", "double-groupby-5", "double-groupby-all"]


def exp3():
    rs = rows("exp3.jsonl")
    if not rs:
        return
    last = latest_by(rs, lambda r: (DBKEY.get(r["db"], r["db"]), r["query_type"]))
    dbs = [d for d in ORDER if any(k[0] == d for k in last)]
    types = [t for t in TYPE_ORDER if any(k[1] == t for k in last)]
    fig, ax = plt.subplots(figsize=(8, 0.42 * len(types) * len(dbs) / 2 + 1.6))
    h = 0.8 / len(dbs)
    for i, d in enumerate(dbs):
        ys = [len(types) - 1 - j + (len(dbs) / 2 - i - 0.5) * h for j in range(len(types))]
        xs = [last.get((d, t), {}).get("latency_ms", {}).get("p50", 0) for t in types]
        ax.barh(ys, xs, height=h * 0.9, color=COLOR[d], label=NAME[d])
    ax.set_yticks(range(len(types))[::-1], types)
    ax.set_xscale("log")
    ax.set_xlabel("median latency, ms (log scale, lower is better)")
    ax.set_title("Latency per benchmark query type", loc="left", fontsize=11)
    ax.grid(axis="y", visible=False)
    ax.legend(frameon=False, loc="upper center", bbox_to_anchor=(0.45, -0.12), ncol=3)
    save(fig, "query_latency.png")


def exp5():
    rs = rows("exp5_timeline.jsonl")
    if not rs:
        return
    last = latest_by(rs, lambda r: r["label"])
    labels = [l for l in ["rf1", "rf2"] if l in last]
    if not labels:
        return
    fig, axes = plt.subplots(len(labels), 1, figsize=(8, 2.3 * len(labels) + 0.6), sharex=True)
    if len(labels) == 1:
        axes = [axes]
    for ax, lab in zip(axes, labels):
        r = last[lab]
        ax.axvspan(r["kill_at_s"], r["restart_at_s"], color="#f0efec", zorder=0)
        ax.text((r["kill_at_s"] + r["restart_at_s"]) / 2, 1.04, "node killed (SIGKILL) until restart",
                ha="center", va="bottom", color=MUTED, fontsize=8, transform=ax.get_xaxis_transform())
        secs = r["writes_per_second"]
        xs = [s[0] for s in secs]
        ok = [s[1] / (s[1] + s[2]) if s[1] + s[2] else None for s in secs]
        ax.plot(xs, ok, color=COLOR["metricsdb"], lw=2, label="writes acknowledged on first try")
        probes = r["probes"]
        expected = max(p["visible_series"] for p in probes) or 1
        ax.plot([p["t"] for p in probes], [max(p["visible_series"], 0) / expected for p in probes],
                color=COLOR["victoriametrics"], lw=2, label="series visible to queries")
        ax.set_ylim(-0.05, 1.12)
        ax.set_ylabel("fraction")
        ax.set_title("RF=%s" % lab[2:], loc="left", fontsize=10)
        ax.grid(axis="x", visible=False)
    axes[0].legend(frameon=False, loc="lower left", fontsize=8)
    axes[-1].set_xlabel("seconds into the run")
    fig.suptitle("One of three storage nodes killed mid-write", x=0.01, ha="left", fontsize=11)
    save(fig, "exp5_timeline.png")


if __name__ == "__main__":
    exp1()
    exp2()
    exp3()
    exp5()
