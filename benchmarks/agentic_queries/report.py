import json
from pathlib import Path

from benchmarks.agentic_queries.models import ScenarioResult


def write_results_json(results, output_path):
    output = [result.to_dict() for result in results]
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2), encoding="utf-8")


def _notes_for_result(result: ScenarioResult):
    if not result.extras:
        return ""
    return ", ".join(f"{key}={value}" for key, value in sorted(result.extras.items()))


def _build_analysis(results):
    by_scenario = {}
    for result in results:
        by_scenario.setdefault(result.scenario, []).append(result)

    lines = ["## Analysis", ""]
    worst_wall = max(results, key=lambda item: item.wall_time_ms)
    worst_queries = max(results, key=lambda item: item.db_queries)
    lines.append(
        f"- Highest wall time: `{worst_wall.scenario}` at scale `{worst_wall.scale}` "
        f"with `{round(worst_wall.wall_time_ms, 2)}` ms."
    )
    lines.append(
        f"- Highest DB query count: `{worst_queries.scenario}` at scale `{worst_queries.scale}` "
        f"with `{worst_queries.db_queries}` queries."
    )
    for scenario_name, scenario_results in sorted(by_scenario.items()):
        ordered = scenario_results
        if len(ordered) < 2:
            continue
        first = ordered[0]
        last = ordered[-1]
        growth = 0.0
        if first.wall_time_ms:
            growth = last.wall_time_ms / first.wall_time_ms
        lines.append(
            f"- `{scenario_name}` grows from `{round(first.wall_time_ms, 2)}` ms to "
            f"`{round(last.wall_time_ms, 2)}` ms across the recorded scales "
            f"({round(growth, 2)}x wall-time growth)."
        )
    return "\n".join(lines)


def render_markdown_report(results):
    lines = [
        "# Agentic Query Benchmark Results",
        "",
        "| Scenario | Scale | Logical Requests | HTTP Requests | HTTP Bytes | DB Queries | DB Rows | DB Time (ms) | Wall Time (ms) | Notes |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for result in results:
        lines.append(
            "| {scenario} | {scale} | {logical} | {http_requests} | {http_bytes} | "
            "{db_queries} | {db_rows} | {db_time} | {wall_time} | {notes} |".format(
                scenario=result.scenario,
                scale=result.scale,
                logical=result.logical_metadata_requests,
                http_requests=result.http_requests,
                http_bytes=result.http_response_bytes,
                db_queries=result.db_queries,
                db_rows=result.db_rows,
                db_time=round(result.db_time_ms, 2),
                wall_time=round(result.wall_time_ms, 2),
                notes=_notes_for_result(result),
            )
        )
    lines.append("")
    lines.append(_build_analysis(results))
    return "\n".join(lines) + "\n"


def write_markdown_report(results, output_path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_markdown_report(results), encoding="utf-8")
