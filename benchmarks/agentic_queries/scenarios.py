import time
from datetime import datetime, timezone

from benchmarks.agentic_queries.config import STDERR_ERROR_MARKER
from benchmarks.agentic_queries.http_capture import ServiceRequestCapture
from benchmarks.agentic_queries.models import ScenarioResult, SeedManifest
from benchmarks.agentic_queries.prereqs import load_metadata_tracer, load_metaflow_symbols
from benchmarks.agentic_queries.service_logs import fetch_db_trace_summary


def _metadata_summary(tracer):
    summary = tracer.summary()
    records = getattr(tracer, "records", [])
    max_depth = None
    if records:
        depths = [getattr(record, "depth", None) for record in records]
        depths = [depth for depth in depths if depth is not None]
        max_depth = max(depths) if depths else None
    return summary.get("by_obj_type", {}), max_depth


def _build_result(scenario_name, scale, tracer, http_summary, db_summary, wall_time_ms, extras=None):
    metadata_by_obj_type, max_depth = _metadata_summary(tracer)
    return ScenarioResult(
        scenario=scenario_name,
        scale=str(scale),
        logical_metadata_requests=getattr(tracer, "request_count", len(getattr(tracer, "records", []))),
        http_requests=http_summary.request_count,
        http_response_bytes=http_summary.response_bytes,
        db_queries=db_summary.db_queries,
        db_rows=db_summary.db_rows,
        db_time_ms=db_summary.db_time_ms,
        wall_time_ms=wall_time_ms,
        metadata_by_obj_type=metadata_by_obj_type,
        max_depth=max_depth,
        extras=extras or {},
    )


def _require_db_traces(http_summary, db_summary):
    if http_summary.request_count > 0 and db_summary.traced_requests == 0:
        raise RuntimeError(
            "No metadata-service DB trace summaries were found. Issue #5 must be deployed "
            "with request tracing enabled before running this benchmark suite."
        )


def run_failed_task_discovery(manifest: SeedManifest):
    Flow, Run, namespace = load_metaflow_symbols()
    del Flow
    MetadataTracer = load_metadata_tracer()
    namespace(None)

    seeded_run = manifest.runs[0]
    pathspec = f"{manifest.flow_name}/{seeded_run.run_id}"
    scenario_start = datetime.now(timezone.utc)
    with MetadataTracer() as tracer, ServiceRequestCapture() as http_summary:
        wall_start = time.perf_counter()
        run = Run(pathspec)
        failed_tasks = [
            task.pathspec
            for step in run
            for task in step
            if not task.successful
        ]
        wall_time_ms = (time.perf_counter() - wall_start) * 1000

    db_summary = fetch_db_trace_summary(scenario_start)
    _require_db_traces(http_summary, db_summary)
    return _build_result(
        "failed_task_discovery",
        manifest.scale,
        tracer,
        http_summary,
        db_summary,
        wall_time_ms,
        extras={"failed_tasks_found": len(failed_tasks)},
    )


def run_log_retrieval(manifest: SeedManifest):
    Flow, Run, namespace = load_metaflow_symbols()
    del Flow
    MetadataTracer = load_metadata_tracer()
    namespace(None)

    seeded_run = manifest.runs[0]
    scenario_start = datetime.now(timezone.utc)
    with MetadataTracer() as tracer, ServiceRequestCapture() as http_summary:
        wall_start = time.perf_counter()
        run = Run(f"{manifest.flow_name}/{seeded_run.run_id}")
        task = next(task for task in run["worker"] if not task.successful)
        stderr_output = task.stderr
        wall_time_ms = (time.perf_counter() - wall_start) * 1000

    db_summary = fetch_db_trace_summary(scenario_start)
    _require_db_traces(http_summary, db_summary)
    return _build_result(
        "log_retrieval",
        manifest.scale,
        tracer,
        http_summary,
        db_summary,
        wall_time_ms,
        extras={
            "target_task_pathspec": task.pathspec,
            "stderr_transferred_bytes": len(stderr_output.encode("utf-8")),
            "actual_error_bytes": len((STDERR_ERROR_MARKER + "\n").encode("utf-8")),
        },
    )


def run_status_filtered_run_listing(manifest: SeedManifest):
    Flow, Run, namespace = load_metaflow_symbols()
    del Run
    MetadataTracer = load_metadata_tracer()
    namespace(None)

    scenario_start = datetime.now(timezone.utc)
    with MetadataTracer() as tracer, ServiceRequestCapture() as http_summary:
        wall_start = time.perf_counter()
        flow = Flow(manifest.flow_name)
        runs_traversed = 0
        failed_runs = []
        for run in flow.runs():
            runs_traversed += 1
            if not run.successful:
                failed_runs.append(run.id)
        wall_time_ms = (time.perf_counter() - wall_start) * 1000

    db_summary = fetch_db_trace_summary(scenario_start)
    _require_db_traces(http_summary, db_summary)
    return _build_result(
        "status_filtered_run_listing",
        manifest.scale,
        tracer,
        http_summary,
        db_summary,
        wall_time_ms,
        extras={
            "runs_traversed": runs_traversed,
            "failed_runs_found": len(failed_runs),
        },
    )


def run_time_horizon_queries(manifest: SeedManifest):
    Flow, Run, namespace = load_metaflow_symbols()
    del Run
    MetadataTracer = load_metadata_tracer()
    namespace(None)

    cutoff = datetime.fromisoformat(manifest.metadata["cutoff_time_iso"])
    scenario_start = datetime.now(timezone.utc)
    with MetadataTracer() as tracer, ServiceRequestCapture() as http_summary:
        wall_start = time.perf_counter()
        flow = Flow(manifest.flow_name)
        runs_traversed = 0
        runs_in_window = []
        for run in flow.runs():
            runs_traversed += 1
            if run.created_at and run.created_at >= cutoff:
                runs_in_window.append(run)

        tasks_traversed = 0
        tasks_in_window = []
        for run in runs_in_window:
            for step in run:
                for task in step:
                    tasks_traversed += 1
                    if task.created_at and task.created_at >= cutoff:
                        tasks_in_window.append(task.pathspec)
        wall_time_ms = (time.perf_counter() - wall_start) * 1000

    db_summary = fetch_db_trace_summary(scenario_start)
    _require_db_traces(http_summary, db_summary)
    return _build_result(
        "time_horizon_queries",
        manifest.scale,
        tracer,
        http_summary,
        db_summary,
        wall_time_ms,
        extras={
            "runs_traversed": runs_traversed,
            "runs_in_window": len(runs_in_window),
            "tasks_traversed": tasks_traversed,
            "tasks_in_window": len(tasks_in_window),
        },
    )


def run_cross_run_artifact_search(manifest: SeedManifest):
    Flow, Run, namespace = load_metaflow_symbols()
    del Run
    MetadataTracer = load_metadata_tracer()
    namespace(None)

    artifact_name = manifest.metadata["target_artifact_name"]
    scenario_start = datetime.now(timezone.utc)
    with MetadataTracer() as tracer, ServiceRequestCapture() as http_summary:
        wall_start = time.perf_counter()
        flow = Flow(manifest.flow_name)
        matching_runs = []
        for run in flow.runs():
            found = False
            for step in run:
                for task in step:
                    try:
                        task[artifact_name]
                    except KeyError:
                        continue
                    matching_runs.append(run.id)
                    found = True
                    break
                if found:
                    break
        wall_time_ms = (time.perf_counter() - wall_start) * 1000

    db_summary = fetch_db_trace_summary(scenario_start)
    _require_db_traces(http_summary, db_summary)
    return _build_result(
        "cross_run_artifact_search",
        manifest.scale,
        tracer,
        http_summary,
        db_summary,
        wall_time_ms,
        extras={"matching_runs_found": len(matching_runs), "target_artifact_name": artifact_name},
    )


SCENARIO_RUNNERS = {
    "failed_task_discovery": run_failed_task_discovery,
    "log_retrieval": run_log_retrieval,
    "status_filtered_run_listing": run_status_filtered_run_listing,
    "time_horizon_queries": run_time_horizon_queries,
    "cross_run_artifact_search": run_cross_run_artifact_search,
}
