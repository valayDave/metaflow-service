import os
import re
import subprocess
from datetime import datetime, timezone

from benchmarks.agentic_queries.config import DEFAULT_METADATA_POD_LABEL, METADATA_POD_ENV_VAR
from benchmarks.agentic_queries.models import DbTraceSummary


SUMMARY_KEYS = ("query_count", "total_rows", "db_time_ms", "request_time_ms")


def parse_trace_summary_line(line: str):
    if not all(f"{key}=" in line for key in SUMMARY_KEYS):
        return None

    values = {}
    for key in SUMMARY_KEYS:
        match = re.search(rf"{key}=([0-9.]+)", line)
        if not match:
            return None
        values[key] = match.group(1)

    return DbTraceSummary(
        traced_requests=1,
        db_queries=int(values["query_count"]),
        db_rows=int(values["total_rows"]),
        db_time_ms=float(values["db_time_ms"]),
        request_time_ms=float(values["request_time_ms"]),
    )


def combine_trace_summaries(summaries):
    aggregate = DbTraceSummary()
    for summary in summaries:
        aggregate.add(summary)
    return aggregate


def _resolve_metadata_pod_name():
    pod_name = os.environ.get(METADATA_POD_ENV_VAR)
    if pod_name:
        return pod_name

    result = subprocess.run(
        [
            "kubectl",
            "get",
            "pods",
            "-l",
            DEFAULT_METADATA_POD_LABEL,
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    pod_name = result.stdout.strip()
    if not pod_name:
        raise RuntimeError(
            "Unable to resolve the metadata-service pod. Make sure `metaflow-dev up` "
            "is running with the metadata-service component enabled."
        )
    return pod_name


def _since_time_arg(start_time: datetime):
    utc_time = start_time.astimezone(timezone.utc)
    return utc_time.isoformat().replace("+00:00", "Z")


def fetch_db_trace_summary(start_time: datetime):
    pod_name = _resolve_metadata_pod_name()
    result = subprocess.run(
        ["kubectl", "logs", pod_name, "--since-time", _since_time_arg(start_time)],
        capture_output=True,
        text=True,
        check=True,
    )
    summaries = []
    for line in result.stdout.splitlines():
        summary = parse_trace_summary_line(line)
        if summary:
            summaries.append(summary)
    return combine_trace_summaries(summaries)
