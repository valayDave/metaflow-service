import pytest

from benchmarks.agentic_queries.models import DbTraceSummary
from benchmarks.agentic_queries.service_logs import combine_trace_summaries, parse_trace_summary_line


pytestmark = [pytest.mark.unit_tests]


def test_parse_trace_summary_line_extracts_metrics():
    line = (
        "INFO QueryTracing method=GET path=/flows/demo query_count=5 total_rows=42 "
        "db_time_ms=7.5 request_time_ms=9.2"
    )

    summary = parse_trace_summary_line(line)

    assert isinstance(summary, DbTraceSummary)
    assert summary.traced_requests == 1
    assert summary.db_queries == 5
    assert summary.db_rows == 42
    assert summary.db_time_ms == 7.5
    assert summary.request_time_ms == 9.2


def test_combine_trace_summaries_sums_multiple_lines():
    combined = combine_trace_summaries(
        [
            DbTraceSummary(traced_requests=1, db_queries=2, db_rows=10, db_time_ms=1.5, request_time_ms=2.0),
            DbTraceSummary(traced_requests=1, db_queries=3, db_rows=5, db_time_ms=2.5, request_time_ms=3.0),
        ]
    )

    assert combined.traced_requests == 2
    assert combined.db_queries == 5
    assert combined.db_rows == 15
    assert combined.db_time_ms == 4.0
    assert combined.request_time_ms == 5.0


def test_parse_trace_summary_line_ignores_unrelated_logs():
    assert parse_trace_summary_line("INFO something else entirely") is None
