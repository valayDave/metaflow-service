import pytest

from benchmarks.agentic_queries.db import build_backdate_statements, build_cleanup_statements


pytestmark = [pytest.mark.unit_tests]


def test_build_cleanup_statements_target_only_the_benchmark_flow():
    statements = build_cleanup_statements("AgenticBenchmarkFlow")

    assert statements
    for sql, params in statements:
        assert "WHERE flow_id = %s" in sql
        assert params == ("AgenticBenchmarkFlow",)


def test_build_backdate_statements_scope_updates_to_one_flow_and_run():
    statements = build_backdate_statements("AgenticBenchmarkFlow", "123", 456)

    assert statements
    for sql, params in statements:
        assert "SET ts_epoch = %s" in sql
        assert "WHERE flow_id = %s" in sql
        assert "run_id = %s OR run_number::text = %s" in sql
        assert params == (456, "AgenticBenchmarkFlow", "123", "123")
