import logging
from unittest.mock import patch

import pytest

from .utils import (
    init_app,
    init_db,
    clean_db,
    cli,
    db,
    add_flow,
    add_run,
    add_step,
    add_task,
)
import services.data.query_tracing as query_tracing

pytestmark = [pytest.mark.integration_tests]


def _request_trace_records(caplog):
    return [
        record for record in caplog.records
        if record.name == "QueryTracing" and "[RequestTrace]" in record.message
    ]


@pytest.fixture
async def traced_cli(aiohttp_client):
    with patch.object(query_tracing, "QUERY_TRACING_ENABLED", True):
        cli = await init_app(aiohttp_client)
        yield cli


@pytest.fixture
async def traced_db(traced_cli):
    async_db = await init_db(traced_cli)
    yield async_db
    await clean_db(async_db)


async def test_query_tracing_runs_endpoint_with_header(traced_cli, traced_db, caplog):
    await add_flow(traced_db, flow_id="HelloFlow")
    await add_run(traced_db, flow_id="HelloFlow", run_id="run-1")
    await add_run(traced_db, flow_id="HelloFlow", run_id="run-2")

    with caplog.at_level(logging.INFO, logger="QueryTracing"):
        response = await traced_cli.get(
            "/flows/HelloFlow/runs",
            headers={query_tracing.QUERY_TRACING_HEADER: "1"},
        )
        await response.text()

    trace_records = _request_trace_records(caplog)
    assert response.status == 200
    assert len(trace_records) == 1
    assert "status=200" in trace_records[0].message
    assert "query_count=1" in trace_records[0].message
    assert "total_rows=2" in trace_records[0].message


async def test_query_tracing_tasks_endpoint_with_header(traced_cli, traced_db, caplog):
    _flow = (await add_flow(traced_db, flow_id="HelloFlow")).body
    _run = (await add_run(traced_db, flow_id=_flow.get("flow_id"), run_id="run-1")).body
    _step = (await add_step(
        traced_db,
        flow_id=_run.get("flow_id"),
        step_name="start",
        run_number=_run.get("run_number"),
        run_id=_run.get("run_id"),
    )).body
    await add_task(
        traced_db,
        flow_id=_step.get("flow_id"),
        step_name=_step.get("step_name"),
        run_number=_step.get("run_number"),
        run_id=_step.get("run_id"),
        task_name="task-1",
    )
    await add_task(
        traced_db,
        flow_id=_step.get("flow_id"),
        step_name=_step.get("step_name"),
        run_number=_step.get("run_number"),
        run_id=_step.get("run_id"),
        task_name="task-2",
    )

    with caplog.at_level(logging.INFO, logger="QueryTracing"):
        response = await traced_cli.get(
            "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(**_step),
            headers={query_tracing.QUERY_TRACING_HEADER: "1"},
        )
        await response.text()

    trace_records = _request_trace_records(caplog)
    assert response.status == 200
    assert len(trace_records) == 1
    assert "query_count=2" in trace_records[0].message
    assert "total_rows=3" in trace_records[0].message


async def test_query_tracing_without_header_emits_no_trace(traced_cli, traced_db, caplog):
    await add_flow(traced_db, flow_id="HelloFlow")
    await add_run(traced_db, flow_id="HelloFlow", run_id="run-1")

    with caplog.at_level(logging.INFO, logger="QueryTracing"):
        response = await traced_cli.get("/flows/HelloFlow/runs")
        await response.text()

    assert response.status == 200
    assert _request_trace_records(caplog) == []


async def test_query_tracing_disabled_even_with_header(cli, db, caplog):
    await add_flow(db, flow_id="HelloFlow")
    await add_run(db, flow_id="HelloFlow", run_id="run-1")

    with caplog.at_level(logging.INFO, logger="QueryTracing"):
        response = await cli.get(
            "/flows/HelloFlow/runs",
            headers={query_tracing.QUERY_TRACING_HEADER: "1"},
        )
        await response.text()

    assert response.status == 200
    assert _request_trace_records(caplog) == []
