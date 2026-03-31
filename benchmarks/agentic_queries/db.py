import os
from contextlib import contextmanager
from datetime import datetime

import psycopg2

from benchmarks.agentic_queries.config import (
    BENCHMARK_FLOW_NAME,
    DB_HOST_ENV_VAR,
    DB_NAME_ENV_VAR,
    DB_PASSWORD_ENV_VAR,
    DB_PORT_ENV_VAR,
    DB_USER_ENV_VAR,
    DEFAULT_DB_CONFIG,
)


TABLES_IN_DELETE_ORDER = (
    "artifact_v3",
    "metadata_v3",
    "task_v3",
    "step_v3",
    "run_v3",
    "flow_v3",
)

TABLES_IN_BACKDATE_ORDER = (
    "artifact_v3",
    "metadata_v3",
    "task_v3",
    "step_v3",
    "run_v3",
)


def db_connect_kwargs():
    return {
        "host": os.environ.get(DB_HOST_ENV_VAR, DEFAULT_DB_CONFIG["host"]),
        "port": int(os.environ.get(DB_PORT_ENV_VAR, DEFAULT_DB_CONFIG["port"])),
        "user": os.environ.get(DB_USER_ENV_VAR, DEFAULT_DB_CONFIG["user"]),
        "password": os.environ.get(DB_PASSWORD_ENV_VAR, DEFAULT_DB_CONFIG["password"]),
        "dbname": os.environ.get(DB_NAME_ENV_VAR, DEFAULT_DB_CONFIG["dbname"]),
    }


@contextmanager
def db_connection():
    connection = psycopg2.connect(**db_connect_kwargs())
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def build_cleanup_statements(flow_name=BENCHMARK_FLOW_NAME):
    return [
        (f"DELETE FROM {table_name} WHERE flow_id = %s", (flow_name,))
        for table_name in TABLES_IN_DELETE_ORDER
    ]


def build_backdate_statements(flow_name: str, run_id: str, ts_epoch: int):
    statements = []
    for table_name in TABLES_IN_BACKDATE_ORDER:
        statements.append(
            (
                f"UPDATE {table_name} "
                "SET ts_epoch = %s "
                "WHERE flow_id = %s AND (run_id = %s OR run_number::text = %s)",
                (ts_epoch, flow_name, str(run_id), str(run_id)),
            )
        )
    return statements


def cleanup_flow_data(flow_name=BENCHMARK_FLOW_NAME):
    with db_connection() as connection:
        with connection.cursor() as cursor:
            for sql, params in build_cleanup_statements(flow_name):
                cursor.execute(sql, params)


def backdate_run(flow_name: str, run_id: str, when: datetime):
    ts_epoch = int(round(when.timestamp() * 1000))
    with db_connection() as connection:
        with connection.cursor() as cursor:
            for sql, params in build_backdate_statements(flow_name, run_id, ts_epoch):
                cursor.execute(sql, params)
