import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from benchmarks.agentic_queries.config import (
    BENCHMARK_FLOW_NAME,
    DEFAULT_MANIFEST_PATH,
    DEFAULT_OUTPUT_DIR,
    FIXED_HISTORY_TASKS,
    TARGET_ARTIFACT_NAME,
)
from benchmarks.agentic_queries.models import SeedManifest, SeededRun
from benchmarks.agentic_queries.prereqs import ensure_metaflow_on_path, load_metaflow_symbols


def _python_executable():
    return sys.executable or "python"


def _load_db_helpers():
    try:
        from benchmarks.agentic_queries.db import backdate_run, cleanup_flow_data
    except ImportError as error:
        raise RuntimeError(
            "psycopg2 is required to seed benchmark data. Install the service repo "
            "test/runtime dependencies before running the seeder."
        ) from error
    return backdate_run, cleanup_flow_data


def _flow_script_path():
    return Path(__file__).resolve().parent / "benchmark_flow.py"


def _build_failed_indexes(num_parallel_tasks):
    if num_parallel_tasks <= 5:
        return [0]
    step = max(1, num_parallel_tasks // 5)
    indexes = list(range(0, num_parallel_tasks, step))
    return indexes[: max(1, num_parallel_tasks // step)]


def _subprocess_env():
    env = os.environ.copy()
    metaflow_root = ensure_metaflow_on_path()
    pythonpath_entries = [str(Path(__file__).resolve().parents[2]), str(metaflow_root)]
    if env.get("PYTHONPATH"):
        pythonpath_entries.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)
    return env


def _run_flow_and_capture_latest(allow_failure=False, **parameters):
    Flow, _, namespace = load_metaflow_symbols()
    namespace(None)
    before_latest = None
    try:
        before_latest = Flow(BENCHMARK_FLOW_NAME).latest_run
        before_latest = before_latest.id if before_latest else None
    except Exception:
        before_latest = None

    command = [_python_executable(), str(_flow_script_path()), "run"]
    for key, value in parameters.items():
        command.extend([f"--{key.replace('_', '-')}", str(value)])

    completed = subprocess.run(command, check=False, env=_subprocess_env())
    if completed.returncode != 0 and not allow_failure:
        raise subprocess.CalledProcessError(completed.returncode, command)

    deadline = time.time() + 10
    latest_run = None
    while time.time() < deadline:
        latest_run = Flow(BENCHMARK_FLOW_NAME).latest_run
        if latest_run and latest_run.id != before_latest:
            return latest_run.id
        time.sleep(0.25)

    raise RuntimeError("Unable to resolve the newly created benchmark run.")


def seed_task_failure(scale: int):
    _, cleanup_flow_data = _load_db_helpers()
    cleanup_flow_data(BENCHMARK_FLOW_NAME)
    failed_indexes = _build_failed_indexes(scale)
    run_id = _run_flow_and_capture_latest(
        allow_failure=True,
        num_parallel_tasks=scale,
        failed_task_indexes=",".join(str(index) for index in failed_indexes),
        stderr_payload_bytes=4096,
        emit_target_artifact=0,
        benchmark_label=f"task-failure-{scale}",
    )
    return SeedManifest(
        dataset_type="task_failure",
        scale=str(scale),
        flow_name=BENCHMARK_FLOW_NAME,
        runs=[
            SeededRun(
                run_id=str(run_id),
                num_parallel_tasks=scale,
                expected_failed=True,
                failed_task_indexes=failed_indexes,
                stderr_payload_bytes=4096,
            )
        ],
    )


def seed_log_retrieval(scale: int):
    _, cleanup_flow_data = _load_db_helpers()
    cleanup_flow_data(BENCHMARK_FLOW_NAME)
    run_id = _run_flow_and_capture_latest(
        allow_failure=True,
        num_parallel_tasks=5,
        failed_task_indexes="0",
        stderr_payload_bytes=scale,
        emit_target_artifact=0,
        benchmark_label=f"log-retrieval-{scale}",
    )
    return SeedManifest(
        dataset_type="log_retrieval",
        scale=str(scale),
        flow_name=BENCHMARK_FLOW_NAME,
        runs=[
            SeededRun(
                run_id=str(run_id),
                num_parallel_tasks=5,
                expected_failed=True,
                failed_task_indexes=[0],
                stderr_payload_bytes=scale,
            )
        ],
        metadata={"target_failed_task_index": 0},
    )


def seed_history(scale: int):
    backdate_run, cleanup_flow_data = _load_db_helpers()
    cleanup_flow_data(BENCHMARK_FLOW_NAME)
    seeded_runs = []
    now = datetime.utcnow()
    expected_failed_run_ids = []
    expected_matching_run_ids = []

    for index in range(scale):
        expected_failed = index % 5 == 0
        emits_target_artifact = not expected_failed and index % 4 == 0
        failed_task_indexes = [0] if expected_failed else []
        run_id = _run_flow_and_capture_latest(
            allow_failure=expected_failed,
            num_parallel_tasks=FIXED_HISTORY_TASKS,
            failed_task_indexes=",".join(str(item) for item in failed_task_indexes),
            stderr_payload_bytes=4096,
            emit_target_artifact=1 if emits_target_artifact else 0,
            target_artifact_name=TARGET_ARTIFACT_NAME,
            benchmark_label=f"history-{scale}-{index}",
        )
        synthetic_time = now - timedelta(hours=index)
        backdate_run(BENCHMARK_FLOW_NAME, str(run_id), synthetic_time)
        seeded_runs.append(
            SeededRun(
                run_id=str(run_id),
                num_parallel_tasks=FIXED_HISTORY_TASKS,
                expected_failed=expected_failed,
                failed_task_indexes=failed_task_indexes,
                stderr_payload_bytes=4096,
                emits_target_artifact=emits_target_artifact,
                synthetic_timestamp_iso=synthetic_time.isoformat(),
            )
        )
        if expected_failed:
            expected_failed_run_ids.append(str(run_id))
        if emits_target_artifact:
            expected_matching_run_ids.append(str(run_id))

    return SeedManifest(
        dataset_type="history",
        scale=str(scale),
        flow_name=BENCHMARK_FLOW_NAME,
        runs=seeded_runs,
        metadata={
            "cutoff_time_iso": (now - timedelta(hours=24)).isoformat(),
            "target_artifact_name": TARGET_ARTIFACT_NAME,
            "expected_failed_run_ids": expected_failed_run_ids,
            "expected_matching_run_ids": expected_matching_run_ids,
        },
    )


SEEDERS = {
    "task_failure": seed_task_failure,
    "log_retrieval": seed_log_retrieval,
    "history": seed_history,
}


def write_manifest(manifest: SeedManifest, output_path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest.to_dict(), indent=2), encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser(description="Seed benchmark data for agentic query baselines.")
    parser.add_argument("--dataset-type", choices=sorted(SEEDERS), required=True)
    parser.add_argument("--scale", required=True, type=int)
    parser.add_argument("--output", default=str(DEFAULT_MANIFEST_PATH))
    return parser.parse_args()


def main():
    args = parse_args()
    DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest = SEEDERS[args.dataset_type](args.scale)
    write_manifest(manifest, args.output)
    print(json.dumps(manifest.to_dict(), indent=2))


if __name__ == "__main__":
    main()
