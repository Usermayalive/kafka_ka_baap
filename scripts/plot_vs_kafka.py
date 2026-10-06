#!/usr/bin/env python3
"""Renders docs/img/stormlog-vs-kafka.png from the CSV written by bench-vs-kafka.sh.

    python3 scripts/plot_vs_kafka.py target-cli/bench/vs-kafka.csv docs/img/stormlog-vs-kafka.png

Small multiples, one quantity and one y-axis per panel. Series colors are the first three
categorical slots of the reference palette (validated all-pairs for CVD); each series also has
its own marker shape, and lines are labeled directly at their right end.
"""
import csv
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, FuncFormatter, NullLocator

SURFACE, INK, INK_2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
SERIES = {  # key -> (label, color, marker)
    "stormlog": ("StormLog (1 record/request)", "#2a78d6", "o"),
    "kafka-unbatched": ("Kafka 4.3.1, batching off", "#eb6834", "s"),
    "kafka-batched": ("Kafka 4.3.1, default batching", "#1baf7a", "^"),
}


def load(path):
    rows = defaultdict(dict)  # (system, mode) -> size -> row
    with open(path) as f:
        for r in csv.DictReader(f):
            rows[(r["system"], r["mode"])][int(r["record_size"])] = r
    return rows


def size_label(v, _):
    return f"{int(v) // 1024} KB" if v >= 1024 else f"{int(v)} B"


def style(ax, title, ylabel):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=INK, fontsize=11, fontweight="bold", pad=10)
    ax.set_ylabel(ylabel, color=INK_2, fontsize=9)
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(FuncFormatter(size_label))
    ax.set_xlabel("Record size", color=INK_2, fontsize=9)
    ax.tick_params(colors=MUTED, labelsize=8, length=0)
    ax.grid(True, axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)


SHORT = {"stormlog": "StormLog", "kafka-unbatched": "Kafka, no batching", "kafka-batched": "Kafka, batching"}


def plot_series(ax, sizes, values, key):
    _, color, marker = SERIES[key]
    ax.plot(sizes, values, color=color, linewidth=1.8, marker=marker, markersize=6.5,
            markeredgecolor=SURFACE, markeredgewidth=1.2, solid_capstyle="round", solid_joinstyle="round",
            zorder=3)
    return key, sizes[-1], values[-1]


def label_ends(ax, ends, min_gap_px=13):
    """Direct labels in text ink at each line's right end, nudged apart so they never overlap."""
    ax.figure.canvas.draw()  # finalize limits so data -> pixel transforms are exact
    to_px = ax.transData
    placed = sorted(((to_px.transform((x, y))[1], key, x, y) for key, x, y in ends))
    ys = [p[0] for p in placed]
    for i in range(1, len(ys)):  # push upward until separated
        ys[i] = max(ys[i], ys[i - 1] + min_gap_px)
    shift = (sum(p[0] for p in placed) - sum(ys)) / len(ys)  # re-center the group on its anchors
    for (py, key, x, y), new_py in zip(placed, ys):
        dy_points = (new_py + shift - py) * 72 / ax.figure.dpi
        ax.annotate(SHORT[key], (x, y), xytext=(9, dy_points), textcoords="offset points",
                    color=INK_2, fontsize=8, va="center")


def main(csv_path, out_path):
    rows = load(csv_path)
    throughput = {
        "stormlog": rows[("stormlog", "unbatched")],
        "kafka-unbatched": rows[("kafka", "unbatched")],
        "kafka-batched": rows[("kafka", "batched")],
    }
    fixed = {k: v for (system, mode), v in rows.items() if mode.startswith("fixed-rate")
             for k in [("stormlog" if system == "stormlog" else "kafka-unbatched")]}
    rate = next(mode for (_, mode) in rows if mode.startswith("fixed-rate")).split("-")[-1]

    fig, axes = plt.subplots(2, 2, figsize=(12, 8.5), facecolor=SURFACE)
    fig.subplots_adjust(left=0.07, right=0.9, top=0.82, bottom=0.08, hspace=0.45, wspace=0.38)

    ax = axes[0][0]
    style(ax, "Throughput", "records / s (log scale)")
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(FixedLocator([10_000, 20_000, 50_000, 100_000, 200_000, 500_000]))
    ax.yaxis.set_minor_locator(NullLocator())
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1000:.0f}K"))
    ax.set_ylim(8_000, 700_000)
    label_ends(ax, [plot_series(ax, sorted(series), [float(series[s]["records_per_sec"]) for s in sorted(series)], key)
                    for key, series in throughput.items()])

    ax = axes[0][1]
    style(ax, f"p99 latency at a fixed {int(rate):,} records/s", "ms (log scale)")
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(FixedLocator([0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50]))
    ax.yaxis.set_minor_locator(NullLocator())
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax.set_ylim(0.1, 50)
    label_ends(ax, [plot_series(ax, sorted(series), [max(float(series[s]["p99_ms"]), 0.01) for s in sorted(series)], key)
                    for key, series in fixed.items()])

    ax = axes[1][0]
    style(ax, "Broker CPU per million records", "CPU-seconds")
    ax.set_ylim(0, 55)
    label_ends(ax, [plot_series(ax, sorted(series), [float(series[s]["broker_cpu_cores"]) * float(series[s]["seconds"])
                                                     / float(series[s]["records"]) * 1e6 for s in sorted(series)], key)
                    for key, series in throughput.items()])

    ax = axes[1][1]
    style(ax, "Broker disk writes per byte of payload", "bytes written / payload byte")
    ax.set_ylim(0, 1.6)
    label_ends(ax, [plot_series(ax, sorted(series), [float(series[s]["broker_disk_mb_written"]) * 1024 * 1024
                                                     / (float(series[s]["records"]) * s) for s in sorted(series)], key)
                    for key, series in throughput.items()])

    fig.text(0.07, 0.965, "StormLog vs Apache Kafka: one broker, one partition, acks=1, page cache",
             color=INK, fontsize=14, fontweight="bold")
    fig.text(0.07, 0.938, "Same Apple M1 laptop, separate broker processes with 1 GB heaps; 5 requests in flight. "
             "Latency pass: Kafka batching off; Kafka reports whole milliseconds.",
             color=INK_2, fontsize=9)
    handles = [plt.Line2D([], [], color=c, marker=m, linewidth=1.8, markersize=6.5, markeredgecolor=SURFACE)
               for (_, c, m) in SERIES.values()]
    fig.legend(handles, [l for (l, _, _) in SERIES.values()], loc="upper left", bbox_to_anchor=(0.065, 0.915),
               ncol=3, frameon=False, fontsize=9, labelcolor=INK_2, handlelength=2.2, columnspacing=2.0)
    fig.savefig(out_path, dpi=160, facecolor=SURFACE)
    print("wrote", out_path)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
