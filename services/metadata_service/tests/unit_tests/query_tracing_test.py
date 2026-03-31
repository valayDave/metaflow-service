import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

import services.data.query_tracing as query_tracing
from services.data.postgres_async_db import AsyncFlowTablePostgres

pytestmark = [pytest.mark.unit_tests]


class _SuccessfulCursor(object):
    def __init__(self, records):
        self._records = records

    async def execute(self, *_args, **_kwargs):
        return None

    async def fetchall(self):
        return self._records


class _FailingCursor(object):
    async def execute(self, *_args, **_kwargs):
        raise RuntimeError("boom")

    async def fetchall(self):
        return []


def _query_trace_records(caplog):
    return [
        record for record in caplog.records
        if record.name == "QueryTracing" and "[RequestTrace]" in record.message
    ]


def _query_detail_records(caplog):
    return [
        record for record in caplog.records
        if record.name == "QueryTracing" and "[QueryTrace " in record.message
    ]


def test_db_query_tracer_context_cleanup():
    with patch.object(query_tracing, "QUERY_TRACING_ENABLED", True):
        assert query_tracing.get_active_tracer() is None
        with query_tracing.DBQueryTracer() as tracer:
            assert query_tracing.get_active_tracer() is tracer
        assert query_tracing.get_active_tracer() is None


def test_request_tracing_requested_requires_enabled_header():
    request = make_mocked_request(
        "GET",
        "/flows",
        headers={query_tracing.QUERY_TRACING_HEADER: "1"},
    )

    with patch.object(query_tracing, "QUERY_TRACING_ENABLED", False):
        assert query_tracing.request_tracing_requested(request) is False

    with patch.object(query_tracing, "QUERY_TRACING_ENABLED", True):
        assert query_tracing.request_tracing_requested(request) is True
        assert query_tracing.request_tracing_requested(
            make_mocked_request("GET", "/flows")
        ) is False
        assert query_tracing.request_tracing_requested(
            make_mocked_request(
                "GET",
                "/flows",
                headers={query_tracing.QUERY_TRACING_HEADER: "false"},
            )
        ) is False


async def test_query_tracing_middleware_logs_summary_only_at_info(caplog):
    async def handler(request):
        tracer = query_tracing.get_active_tracer()
        tracer.record_query(
            table_name="flows_v3",
            row_count=2,
            db_time_ms=1.5,
            success=True,
            sql_template="SELECT * FROM flows_v3 WHERE flow_id = %s",
        )
        return web.Response(status=200)

    request = make_mocked_request(
        "GET",
        "/flows",
        headers={query_tracing.QUERY_TRACING_HEADER: "1"},
    )

    with patch.object(query_tracing, "QUERY_TRACING_ENABLED", True):
        with caplog.at_level(logging.INFO, logger="QueryTracing"):
            response = await query_tracing.query_tracing_middleware(request, handler)

    assert response.status == 200
    assert len(_query_trace_records(caplog)) == 1
    assert len(_query_detail_records(caplog)) == 0
    assert "query_count=1" in caplog.text
    assert "total_rows=2" in caplog.text


async def test_query_tracing_middleware_logs_query_details_at_debug(caplog):
    long_sql = "SELECT " + "x" * 400

    async def handler(request):
        tracer = query_tracing.get_active_tracer()
        tracer.record_query(
            table_name="flows_v3",
            row_count=1,
            db_time_ms=2.0,
            success=True,
            sql_template=long_sql,
        )
        return web.Response(status=200)

    request = make_mocked_request(
        "GET",
        "/flows",
        headers={query_tracing.QUERY_TRACING_HEADER: "1"},
    )

    with patch.object(query_tracing, "QUERY_TRACING_ENABLED", True):
        with caplog.at_level(logging.DEBUG, logger="QueryTracing"):
            await query_tracing.query_tracing_middleware(request, handler)

    assert len(_query_trace_records(caplog)) == 1
    assert len(_query_detail_records(caplog)) == 1
    assert long_sql[:query_tracing.QUERY_TRACING_SQL_PREVIEW_LIMIT] in caplog.text
    assert long_sql[:query_tracing.QUERY_TRACING_SQL_PREVIEW_LIMIT + 1] not in caplog.text


async def test_execute_sql_records_failed_queries():
    fake_db = SimpleNamespace(logger=logging.getLogger("AsyncPostgresDB:test"))
    table = AsyncFlowTablePostgres(fake_db)

    with patch.object(query_tracing, "QUERY_TRACING_ENABLED", True):
        with query_tracing.DBQueryTracer(capture_query_details=True) as tracer:
            response, _ = await table.execute_sql(
                select_sql="SELECT * FROM flows_v3 WHERE flow_id = %s",
                values=["secret-flow-id"],
                cur=_FailingCursor(),
            )

    assert response.response_code == 500
    assert tracer.query_count == 1
    assert tracer.total_rows == 0
    assert tracer.queries[0]["success"] is False
    assert tracer.queries[0]["error_type"] == "RuntimeError"
    assert "secret-flow-id" not in tracer.queries[0]["sql"]
    assert "%s" in tracer.queries[0]["sql"]


async def test_execute_sql_uses_sql_template_not_values():
    fake_db = SimpleNamespace(logger=logging.getLogger("AsyncPostgresDB:test"))
    table = AsyncFlowTablePostgres(fake_db)
    records = [{
        "flow_id": "HelloFlow",
        "user_name": "tester",
        "ts_epoch": 1,
        "tags": "[]",
        "system_tags": "[]",
    }]

    with patch.object(query_tracing, "QUERY_TRACING_ENABLED", True):
        with query_tracing.DBQueryTracer(capture_query_details=True) as tracer:
            response, _ = await table.execute_sql(
                select_sql="SELECT * FROM flows_v3 WHERE flow_id = %s",
                values=["secret-flow-id"],
                cur=_SuccessfulCursor(records),
            )

    assert response.response_code == 200
    assert tracer.query_count == 1
    assert tracer.total_rows == 1
    assert "secret-flow-id" not in tracer.queries[0]["sql"]
    assert "%s" in tracer.queries[0]["sql"]


async def test_db_query_tracer_isolated_across_concurrent_tasks():
    with patch.object(query_tracing, "QUERY_TRACING_ENABLED", True):
        async def run_trace(table_name, rows):
            with query_tracing.DBQueryTracer() as tracer:
                await asyncio.sleep(0)
                tracer.record_query(
                    table_name=table_name,
                    row_count=rows,
                    db_time_ms=1.0,
                    success=True,
                )
                await asyncio.sleep(0)
                return tracer.summary()

        first, second = await asyncio.gather(
            run_trace("flows_v3", 1),
            run_trace("runs_v3", 3),
        )

    assert first["query_count"] == 1
    assert first["total_rows"] == 1
    assert second["query_count"] == 1
    assert second["total_rows"] == 3
    assert query_tracing.get_active_tracer() is None
