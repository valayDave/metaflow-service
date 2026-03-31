import argparse
from pathlib import Path

from benchmarks.agentic_queries.config import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REPORT_PATH,
    DEFAULT_RESULTS_PATH,
    HISTORY_SCALES,
    LOG_SIZE_SCALES,
    TASK_PARALLEL_SCALES,
)
from benchmarks.agentic_queries.report import write_markdown_report, write_results_json
from benchmarks.agentic_queries.scenarios import (
    run_cross_run_artifact_search,
    run_failed_task_discovery,
    run_log_retrieval,
    run_status_filtered_run_listing,
    run_time_horizon_queries,
)
from benchmarks.agentic_queries.seed import seed_history, seed_log_retrieval, seed_task_failure, write_manifest


def parse_args():
    parser = argparse.ArgumentParser(description="Run the full agentic query benchmark suite.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []

    for scale in TASK_PARALLEL_SCALES:
        manifest = seed_task_failure(scale)
        write_manifest(manifest, output_dir / f"seed_task_failure_{scale}.json")
        results.append(run_failed_task_discovery(manifest))

    for scale in LOG_SIZE_SCALES:
        manifest = seed_log_retrieval(scale)
        write_manifest(manifest, output_dir / f"seed_log_retrieval_{scale}.json")
        results.append(run_log_retrieval(manifest))

    for scale in HISTORY_SCALES:
        manifest = seed_history(scale)
        write_manifest(manifest, output_dir / f"seed_history_{scale}.json")
        results.append(run_status_filtered_run_listing(manifest))
        results.append(run_time_horizon_queries(manifest))
        results.append(run_cross_run_artifact_search(manifest))

    write_results_json(results, output_dir / DEFAULT_RESULTS_PATH.name)
    write_markdown_report(results, output_dir / DEFAULT_REPORT_PATH.name)
    print(f"Wrote benchmark results to {output_dir}")


if __name__ == "__main__":
    main()
