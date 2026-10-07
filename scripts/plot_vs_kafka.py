#!/usr/bin/env python3
"""Renders docs/img/stormlog-vs-kafka.png from the CSV written by bench-vs-kafka.sh.

    python3 scripts/plot_vs_kafka.py target-cli/bench/vs-kafka.csv docs/img/stormlog-vs-kafka.png

Small multiples: every panel compares the same two systems in the same two colors (the first two
categorical slots of the reference palette, validated for color-vision deficiency), one quantity
and one y-axis per panel. Batched and unbatched producers are separate panels rather than extra
colors. Lines are labeled directly at their right end; marker shapes are a second encoding.
"""
import csv
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, FuncFormatter, NullLocator

SURFACE, INK, INK_2, GRID, AXIS, MUTED = "#fcfcfb", "#0b0b0b", "#52514e", "#e1e0d9", "#c3c2b7", "#898781"
SYSTEMS = {"stormlog": ("StormLog", "#2a78d6", "o"), "kafka": ("Kafka 4.3.1", "#eb6834", "s")}


def load(path):
    rows = {}
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.setdefault((r["system"], r["mode"]), {})[int(r["record_size"])] = r
    return rows


def size_label(v, _):
    return f"{int(v) // 1024} KB" if v >= 1024 else f"{int(v)} B"


def style(ax, title, ylabel, log_y=False):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=INK, fontsize=10.5, fontweight="bold", pad=9)
    ax.set_ylabel(ylabel, color=INK_2, fontsize=8.5)
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(FuncFormatter(size_label))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.set_xlabel("Record size", color=INK_2, fontsize=8.5)
    ax.tick_params(colors=MUTED, labelsize=7.5, length=0)
    ax.grid(True, axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)
    if log_y:
        ax.set_yscale("log")
        ax.yaxis.set_minor_locator(NullLocator())


def series(ax, rows, mode, value, systems=("stormlog", "kafka")):
    ends = []
    for system in systems:
        data = rows.get((system, mode))
        if not data:
            continue
        label, color, marker = SYSTEMS[system]
        sizes = sorted(data)
        ys = [value(data[s], s) for s in sizes]
        ax.plot(sizes, ys, color=color, linewidth=1.8, marker=marker, markersize=6, markeredgecolor=SURFACE,
                markeredgewidth=1.2, solid_capstyle="round", zorder=3)
        ends.append((label, sizes[-1], ys[-1]))
    label_ends(ax, ends)


def label_ends(ax, ends, min_gap_px=13):
    ax.figure.canvas.draw()
    placed = sorted((ax.transData.transform((x, y))[1], label, x, y) for label, x, y in ends)
    ys = [p[0] for p in placed]
    for i in range(1, len(ys)):
        ys[i] = max(ys[i], ys[i - 1] + min_gap_px)
    shift = (sum(p[0] for p in placed) - sum(ys)) / max(1, len(ys))
    for (py, label, x, y), new_py in zip(placed, ys):
        ax.annotate(label, (x, y), xytext=(9, (new_py + shift - py) * 72 / ax.figure.dpi),
                    textcoords="offset points", color=INK_2, fontsize=8, va="center")


def rps_axis(ax, ticks):
    ax.yaxis.set_major_locator(FixedLocator(ticks))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1000:.0f}K"))
    ax.set_ylim(ticks[0] * 0.8, ticks[-1] * 1.3)


def cpu_per_million(r, _):
    return float(r["broker_cpu_cores"]) * float(r["seconds"]) / float(r["records"]) * 1e6


def main(csv_path, out_path):
    rows = load(csv_path)
    fixed = next(mode for (_, mode) in rows if mode.startswith("fixed-rate"))
    rate = int(fixed.split("-")[-1])

    fig, axes = plt.subplots(2, 3, figsize=(15, 8.6), facecolor=SURFACE)
    fig.subplots_adjust(left=0.06, right=0.93, top=0.83, bottom=0.08, hspace=0.5, wspace=0.42)

    ax = axes[0][0]
    style(ax, "Throughput: batched producers", "records / s (log)", log_y=True)
    rps_axis(ax, [10_000, 20_000, 50_000, 100_000, 200_000, 500_000])
    series(ax, rows, "batched", lambda r, s: float(r["records_per_sec"]))

    ax = axes[0][1]
    style(ax, "Throughput: one record per request", "records / s (log)", log_y=True)
    rps_axis(ax, [10_000, 20_000, 50_000, 100_000, 200_000, 500_000])
    series(ax, rows, "unbatched", lambda r, s: float(r["records_per_sec"]))

    ax = axes[0][2]
    style(ax, f"p99 latency at {rate:,} records/s (no batching)", "ms (log)", log_y=True)
    p99_max = max(float(r["p99_ms"]) for (_, mode), data in rows.items() if mode == fixed for r in data.values())
    ax.yaxis.set_major_locator(FixedLocator([v for v in (0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000)
                                             if v <= p99_max * 2]))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax.set_ylim(0.1, p99_max * 1.6)  # fit the data: tails vary run to run
    series(ax, rows, fixed, lambda r, s: max(float(r["p99_ms"]), 0.01))

    ax = axes[1][0]
    style(ax, "Broker CPU per million records: batched", "CPU-seconds")
    series(ax, rows, "batched", cpu_per_million)
    ax.set_ylim(bottom=0)

    ax = axes[1][1]
    style(ax, "Broker CPU per million records: unbatched", "CPU-seconds")
    series(ax, rows, "unbatched", cpu_per_million)
    ax.set_ylim(bottom=0)

    ax = axes[1][2]
    style(ax, "Broker disk writes per payload byte: batched", "bytes written / payload byte")
    series(ax, rows, "batched",
           lambda r, s: float(r["broker_disk_mb_written"]) * 1024 * 1024 / (float(r["records"]) * s))
    ax.set_ylim(0, 1.6)

    fig.text(0.06, 0.965, "StormLog vs Apache Kafka: one broker, one partition, acks=1, page cache",
             color=INK, fontsize=14, fontweight="bold")
    fig.text(0.06, 0.94, "Same Apple M1 laptop, separate broker processes with 1 GB heaps, 5 requests in flight. "
             "Batched: 16 KB batches, linger 5 ms on both sides. Kafka reports whole milliseconds.",
             color=INK_2, fontsize=9)
    handles = [plt.Line2D([], [], color=c, marker=m, linewidth=1.8, markersize=6, markeredgecolor=SURFACE)
               for (_, c, m) in SYSTEMS.values()]
    fig.legend(handles, [l for (l, _, _) in SYSTEMS.values()], loc="upper left", bbox_to_anchor=(0.055, 0.925),
               ncol=2, frameon=False, fontsize=9, labelcolor=INK_2, handlelength=2.2, columnspacing=2.0)
    fig.savefig(out_path, dpi=150, facecolor=SURFACE)
    print("wrote", out_path)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
