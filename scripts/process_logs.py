#!/usr/bin/env python3
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt


LOG_DIR = Path("./logs")
PREFERRED_ORDER = [
    "1DataNode",
    "1DataNode_opt",
    "3DataNode",
    "3DataNode_opt",
    "3DataNode_2Workers",
    "3DataNode_2Workers_opt",
]

TIME_COLORS = {
    "1DataNode": "#A9D6F5",
    "1DataNode_opt": "#F7B7B2",
    "3DataNode": "#5FA8D3",
    "3DataNode_opt": "#E76F6A",
    "3DataNode_2Workers": "#1F77B4",
    "3DataNode_2Workers_opt": "#C73E3A",
}

RAM_COLOR = "#54A24B"
OPT_HATCH = "///"


def load_metrics():
    items = []
    for path in sorted(LOG_DIR.glob("*_metrics.json")):
        if path.name == "comparison_summary.json":
            continue
        items.append(json.loads(path.read_text(encoding="utf-8")))
    order_map = {name: index for index, name in enumerate(PREFERRED_ORDER)}
    items.sort(key=lambda item: order_map.get(item.get("run_label", ""), 999))
    return items


def value_for_action(metrics, label):
    for action in metrics.get("actions", []):
        if action.get("label") == label:
            return round(action.get("duration_sec", 0), 3)
    return 0


def value_for_ram(metrics):
    actions = metrics.get("actions", [])
    if not actions:
        return 0
    executors = actions[-1].get("memory_snapshot", {}).get("executors", [])
    total = 0
    for executor in executors:
        total += executor.get("used_mb", 0)
    return round(total, 2)


def value_for_actual_work(metrics):
    total = 0
    for action in metrics.get("actions", []):
        label = action.get("label", "")
        if label in ("count input rows", "materialize cached dataframe"):
            continue
        total += action.get("duration_sec", 0)
    return round(total, 3)


def draw_bar_chart(title, labels, values, unit, output_path, use_time_palette):
    figure, axis = plt.subplots(figsize=(14, 8))
    x_positions = list(range(len(labels)))

    colors = []
    hatches = []
    for label in labels:
        if use_time_palette:
            colors.append(TIME_COLORS.get(label, "#888888"))
            hatches.append(OPT_HATCH if label.endswith("_opt") else "")
        else:
            colors.append(RAM_COLOR)
            hatches.append("")

    bars = axis.bar(
        x_positions,
        values,
        width=0.62,
        color=colors,
        edgecolor="black",
        linewidth=1.2,
    )

    for bar, hatch in zip(bars, hatches):
        if hatch:
            bar.set_hatch(hatch)

    axis.set_title(title, fontsize=20, pad=18)
    axis.set_xticks(x_positions, labels)
    plt.setp(axis.get_xticklabels(), rotation=-10, ha="right")
    axis.grid(axis="y", linestyle="--", alpha=0.35)
    axis.set_axisbelow(True)
    axis.spines["top"].set_visible(False)
    axis.spines["right"].set_visible(False)
    axis.margins(x=0.04)

    max_value = max(values) if values else 0
    offset = max(max_value * 0.015, 0.05)
    axis.set_ylim(0, max(max_value * 1.15, 1))

    for bar, value in zip(bars, values):
        axis.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + offset,
            f"{value} {unit}".strip(),
            ha="center",
            va="bottom",
            fontsize=10,
        )

    figure.tight_layout()
    figure.savefig(output_path, format="jpg", dpi=160, bbox_inches="tight")
    plt.close(figure)


def main():
    metrics_list = load_metrics()
    if not metrics_list:
        print("No metrics files found in ./logs")
        return

    labels = [item["run_label"] for item in metrics_list]
    total_times = [round(item.get("total_runtime_sec", 0), 3) for item in metrics_list]
    preprocess_times = []
    for item in metrics_list:
        cache_time = value_for_action(item, "materialize cached dataframe")
        count_time = value_for_action(item, "count input rows")
        preprocess_times.append(cache_time if cache_time > 0 else count_time)
    work_times = [value_for_actual_work(item) for item in metrics_list]
    ram_values = [value_for_ram(item) for item in metrics_list]

    draw_bar_chart("Total Time", labels, total_times, "sec", LOG_DIR / "time_total.jpg", True)
    draw_bar_chart("Preprocess Or Cache Time", labels, preprocess_times, "sec", LOG_DIR / "time_preprocess.jpg", True)
    draw_bar_chart("Actual Work Time", labels, work_times, "sec", LOG_DIR / "time_work.jpg", True)
    draw_bar_chart("RAM Consumption", labels, ram_values, "MB", LOG_DIR / "ram_consumption.jpg", False)

    summary = {}
    for index, item in enumerate(metrics_list):
        summary[item["run_label"]] = {
            "time_total_sec": total_times[index],
            "time_preprocess_sec": preprocess_times[index],
            "time_work_sec": work_times[index],
            "ram_consumption_mb": ram_values[index],
        }

    (LOG_DIR / "comparison_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    print("Graphs written to ./logs/time_total.jpg, ./logs/time_preprocess.jpg, ./logs/time_work.jpg, ./logs/ram_consumption.jpg")
    print("Summary written to ./logs/comparison_summary.json")


if __name__ == "__main__":
    main()
