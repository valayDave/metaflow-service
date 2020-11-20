from aiohttp import web
from pyee import ExecutorEventEmitter
import json
import datetime

from services.data.postgres_async_db import AsyncPostgresDB
from services.utils import DBConfiguration

from services.ui_backend_service.api.flow import FlowApi
from services.ui_backend_service.api.run import RunApi
from services.ui_backend_service.api.step import StepApi
from services.ui_backend_service.api.task import TaskApi
from services.ui_backend_service.api.metadata import MetadataApi
from services.ui_backend_service.api.artifact import ArtificatsApi
from services.ui_backend_service.api.tag import TagApi
from services.ui_backend_service.api.ws import Websocket

from services.ui_backend_service.api.admin import AdminApi

from services.data.models import FlowRow, RunRow, StepRow, TaskRow, MetadataRow, ArtifactRow

# Migration imports

from services.migration_service.api.admin import AdminApi as MigrationAdminApi
from services.migration_service.data.postgres_async_db import AsyncPostgresDB as MigrationAsyncPostgresDB

# Constants

TIMEOUT_FUTURE = 0.1

# Test fixture helpers begin


def init_app(loop, aiohttp_client, queue_ttl=30):
    app = web.Application()
    app.event_emitter = ExecutorEventEmitter()

    # Migration routes as a subapp
    migration_app = web.Application()
    MigrationAdminApi(migration_app)
    app.add_subapp("/migration/", migration_app)

    FlowApi(app)
    RunApi(app)
    StepApi(app)
    TaskApi(app)
    MetadataApi(app)
    ArtificatsApi(app)
    TagApi(app)

    Websocket(app, app.event_emitter, queue_ttl)

    AdminApi(app)

    return loop.run_until_complete(aiohttp_client(app))


async def init_db(cli):
    db_conf = DBConfiguration()

    # Make sure migration scripts are applied
    migration_db = MigrationAsyncPostgresDB.get_instance()
    await migration_db._init(db_conf)

    # Apply migrations and make sure "is_up_to_date" == True
    await cli.patch("/migration/upgrade")
    status = await (await cli.get("/migration/db_schema_status")).json()
    assert status["is_up_to_date"] is True

    db = AsyncPostgresDB.get_instance()
    await db._init(db_conf)
    return db


async def clean_db(db: AsyncPostgresDB):
    # Tables to clean (order is important due to foreign keys)
    tables = [
        db.metadata_table_postgres,
        db.artifact_table_postgres,
        db.task_table_postgres,
        db.step_table_postgres,
        db.run_table_postgres,
        db.flow_table_postgres
    ]
    for table in tables:
        await table.execute_sql(select_sql="DELETE FROM {}".format(table.table_name))

# Test fixture helpers end

# Row helpers begin


async def add_flow(db: AsyncPostgresDB, flow_id="HelloFlow",
                   user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"]):
    flow = FlowRow(
        flow_id=flow_id,
        user_name=user_name,
        tags=tags,
        system_tags=system_tags
    )
    return await db.flow_table_postgres.add_flow(flow)


async def add_run(db: AsyncPostgresDB, flow_id="HelloFlow",
                  run_number: int = None, run_id: str = None,
                  user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"],
                  last_heartbeat_ts=None):
    run = {
        "flow_id": flow_id,
        "run_id": run_id,
        "user_name": user_name,
        "tags": json.dumps(tags),
        "system_tags": json.dumps(system_tags),
        "last_heartbeat_ts": last_heartbeat_ts
    }
    return await db.run_table_postgres.create_record(run)


async def add_step(db: AsyncPostgresDB, flow_id="HelloFlow",
                   run_number: int = None, run_id: str = None, step_name="step",
                   user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"]):
    step = StepRow(
        flow_id=flow_id,
        run_number=run_number,
        run_id=run_id,
        step_name=step_name,
        user_name=user_name,
        tags=tags,
        system_tags=system_tags
    )
    return await db.step_table_postgres.add_step(step)


async def add_task(db: AsyncPostgresDB, flow_id="HelloFlow",
                   run_number: int = None, run_id: str = None, step_name="step", task_id=None, task_name=None,
                   user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"],
                   last_heartbeat_ts=None):
    task = {
        "flow_id": flow_id,
        "run_number": run_number,
        "step_name": step_name,
        "task_name": task_name,
        "user_name": user_name,
        "tags": json.dumps(tags),
        "system_tags": json.dumps(system_tags),
        "last_heartbeat_ts": last_heartbeat_ts
    }
    return await db.task_table_postgres.create_record(task)


async def add_metadata(db: AsyncPostgresDB, flow_id="HelloFlow",
                       run_number: int = None, run_id: str = None, step_name="step", task_id=None, task_name=None,
                       metadata={},
                       user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"]):
    values = {
        "flow_id": flow_id,
        "run_number": run_number,
        "run_id": run_id,
        "step_name": step_name,
        "task_id": task_id,
        "task_name": task_name,
        "field_name": metadata.get("field_name", " "),
        "value": metadata.get("value", " "),
        "type": metadata.get("type", " "),
        "user_name": user_name,
        "tags": tags,
        "system_tags": system_tags
    }
    return await db.metadata_table_postgres.add_metadata(**values)


async def add_artifact(db: AsyncPostgresDB, flow_id="HelloFlow",
                       run_number: int = None, run_id: str = None, step_name="step", task_id=None, task_name=None,
                       artifact={},
                       user_name="dipper", tags=["foo:bar"], system_tags=["runtime:dev"]):
    values = {
        "flow_id": flow_id,
        "run_number": run_number,
        "run_id": run_id,
        "step_name": step_name,
        "task_id": task_id,
        "task_name": task_name,
        "name": artifact.get("name", " "),
        "location": artifact.get("location", " "),
        "ds_type": artifact.get("ds_type", " "),
        "sha": artifact.get("sha", " "),
        "type": artifact.get("type", " "),
        "content_type": artifact.get("content_type", " "),
        "attempt_id": artifact.get("attempt_id", 0),
        "user_name": user_name,
        "tags": tags,
        "system_tags": system_tags
    }
    return await db.artifact_table_postgres.add_artifact(**values)

# Row helpers end

# Resource helpers begin


async def _test_list_resources(cli, db: AsyncPostgresDB, path: str, expected_status=200, expected_data=[]):
    resp = await cli.get(path)
    body = await resp.json()
    data = body.get("data")

    try:
        assert resp.status == expected_status
    except AssertionError as ex:
        print("expected status: {0} but got status: {1}".format(resp.status, expected_status))
        raise ex from None

    try:
        assert data == expected_data
    except AssertionError as ex:
        print("Expected data to be:\n", expected_data)
        print("Instead got data:\n", data)
        raise ex from None


async def _test_single_resource(cli, db: AsyncPostgresDB, path: str, expected_status=200, expected_data={}):
    resp = await cli.get(path)
    body = await resp.json()
    data = body.get("data")

    try:
        assert resp.status == expected_status
    except AssertionError as ex:
        print("expected status: {0} but got status: {1}".format(resp.status, expected_status))
        raise ex from None

    try:
        assert data == expected_data
    except AssertionError as ex:
        print("Expected data to be:\n", expected_data)
        print("Instead got data:\n", data)
        raise ex from None


def get_heartbeat_ts(offset=5):
    "Return a heartbeat timestamp with the given offset in seconds. Default offset is 5 seconds"
    return int(datetime.datetime.utcnow().timestamp()) + offset

# Resource helpers end