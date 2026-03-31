import pytest

from benchmarks.agentic_queries.models import ScenarioResult
from benchmarks.agentic_queries.report import render_markdown_report


pytestmark = [pytest.mark.unit_tests]


def test_render_markdown_report_includes_table_and_analysis():
    results = [
        ScenarioResult(
            scenario="failed_task_discovery",
            scale="5",
            logical_metadata_requests=10,
            http_requests=10,
            http_response_bytes=100,
            db_queries=7,
            db_rows=20,
            db_time_ms=5.5,
            wall_time_ms=10.0,
            metadata_by_obj_type={"run": 1},
            extras={"failed_tasks_found": 1},
        ),
        ScenarioResult(
            scenario="failed_task_discovery",
            scale="50",
            logical_metadata_requests=50,
            http_requests=50,
            http_response_bytes=500,
            db_queries=35,
            db_rows=200,
            db_time_ms=15.0,
            wall_time_ms=25.0,
            metadata_by_obj_type={"run": 1},
            extras={"failed_tasks_found": 10},
        ),
    ]

    rendered = render_markdown_report(results)

    assert "# Agentic Query Benchmark Results" in rendered
    assert "| failed_task_discovery | 5 |" in rendered
    assert "## Analysis" in rendered
    assert "failed_tasks_found=1" in rendered
