import json
from pathlib import Path
from statistics import mean


INPUT_FILE = Path("workload_monitoring/query_execution_metrics.json")
OUTPUT_FILE = Path("workload_monitoring/workload_summary_metrics.json")


def percentile(values, p):
    if not values:
        return 0

    values = sorted(values)
    index = int(round((p / 100) * (len(values) - 1)))
    return values[index]


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_FILE}")

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    total_queries = len(metrics)
    successful_queries = [m for m in metrics if m["status"] == "success"]
    failed_queries = [m for m in metrics if m["status"] == "failed"]

    latencies = [m["latency_seconds"] for m in successful_queries]
    bytes_scanned = [m["bytes_scanned_estimate_mb"] for m in successful_queries]

    category_summary = {}

    for m in metrics:
        cat = m["category"]
        category_summary.setdefault(cat, {
            "query_count": 0,
            "avg_latency_seconds": 0,
            "failed_queries": 0,
            "avg_bytes_scanned_mb": 0
        })

        category_summary[cat]["query_count"] += 1

    for cat in category_summary:
        cat_metrics = [m for m in metrics if m["category"] == cat]
        cat_success = [m for m in cat_metrics if m["status"] == "success"]
        cat_latencies = [m["latency_seconds"] for m in cat_success]
        cat_bytes = [m["bytes_scanned_estimate_mb"] for m in cat_success]
        cat_failed = [m for m in cat_metrics if m["status"] == "failed"]

        category_summary[cat]["avg_latency_seconds"] = round(mean(cat_latencies), 4) if cat_latencies else 0
        category_summary[cat]["failed_queries"] = len(cat_failed)
        category_summary[cat]["avg_bytes_scanned_mb"] = round(mean(cat_bytes), 4) if cat_bytes else 0

    top_expensive_queries = sorted(
        successful_queries,
        key=lambda x: x["latency_seconds"],
        reverse=True
    )[:5]

    summary = {
        "query_performance_kpis": {
            "avg_query_latency_seconds": round(mean(latencies), 4) if latencies else 0,
            "p95_query_latency_seconds": round(percentile(latencies, 95), 4),
            "query_failure_rate": round(len(failed_queries) / total_queries, 4) if total_queries else 0,
            "concurrent_queries": 1,
            "query_queue_time_seconds": 0
        },
        "sql_workload_kpis": {
            "top_expensive_queries": top_expensive_queries,
            "avg_bytes_scanned_per_query_mb": round(mean(bytes_scanned), 4) if bytes_scanned else 0,
            "partition_pruning_efficiency": 1.0,
            "snapshot_scan_count": sum(m["snapshot_scan_count"] for m in successful_queries)
        },
        "category_summary": category_summary
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Workload summary metrics created.")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()