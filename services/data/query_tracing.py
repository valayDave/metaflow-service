import os
import time
from contextvars import ContextVar

from aiohttp import web
from services.utils import logging

QUERY_TRACING_ENABLED = os.environ.get("QUERY_TRACING_ENABLED", "0") == "1"
QUERY_TRACING_HEADER = "X-Metaflow-Trace-DB"
QUERY_TRACING_SQL_PREVIEW_LIMIT = 200

_TRUTHY_VALUES = {"1", "true", "yes", "on"}
_active_db_tracer = ContextVar("active_db_tracer", default=None)

logger = logging.getLogger("QueryTracing")


class DBQueryTracer(object):
    def __init__(self, capture_query_details: bool = False):
        self.capture_query_details = capture_query_details
        self.query_count = 0
        self.total_rows = 0
        self.total_db_time_ms = 0.0
        self.queries = [] if capture_query_details else None
        self._started_at = time.perf_counter()
        self._token = None

    def __enter__(self):
        self._token = _active_db_tracer.set(self)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._token is not None:
            _active_db_tracer.reset(self._token)
        return False

    def record_query(self, table_name: str, row_count: int, db_time_ms: float,
                     success: bool, error_type: str = None, sql_template: str = None):
        self.query_count += 1
        self.total_rows += row_count
        self.total_db_time_ms += db_time_ms

        if self.capture_query_details:
            self.queries.append({
                "table": table_name,
                "rows": row_count,
                "time_ms": db_time_ms,
                "success": success,
                "error_type": error_type,
                "sql": _truncate_sql(sql_template),
            })

    def request_time_ms(self):
        return (time.perf_counter() - self._started_at) * 1000

    def summary(self):
        return {
            "query_count": self.query_count,
            "total_rows": self.total_rows,
            "db_time_ms": self.total_db_time_ms,
            "request_time_ms": self.request_time_ms(),
        }


def _truncate_sql(sql_template: str):
    if sql_template is None:
        return None
    return sql_template[:QUERY_TRACING_SQL_PREVIEW_LIMIT]


def get_active_tracer():
    if not QUERY_TRACING_ENABLED:
        return None
    return _active_db_tracer.get()


def request_tracing_requested(request: web.Request):
    if not QUERY_TRACING_ENABLED:
        return False

    header_value = request.headers.get(QUERY_TRACING_HEADER)
    if header_value is None:
        return False
    return header_value.strip().lower() in _TRUTHY_VALUES


def _log_request_trace(request: web.Request, tracer: DBQueryTracer,
                       response: web.StreamResponse = None, error: Exception = None):
    status = response.status if response is not None else getattr(error, "status", 500 if error else "unknown")
    summary = tracer.summary()

    logger.info(
        "[RequestTrace] %s %s status=%s query_count=%d total_rows=%d db_time_ms=%.2f request_time_ms=%.2f",
        request.method,
        request.path,
        status,
        summary["query_count"],
        summary["total_rows"],
        summary["db_time_ms"],
        summary["request_time_ms"],
    )

    if tracer.capture_query_details:
        for index, query in enumerate(tracer.queries, start=1):
            logger.debug(
                "[QueryTrace %d/%d] table=%s success=%s row_count=%d db_time_ms=%.2f error_type=%s sql=%s",
                index,
                tracer.query_count,
                query["table"],
                query["success"],
                query["rows"],
                query["time_ms"],
                query["error_type"],
                query["sql"],
            )


@web.middleware
async def query_tracing_middleware(request, handler):
    if not request_tracing_requested(request):
        return await handler(request)

    response = None
    error = None
    capture_query_details = logger.isEnabledFor(logging.DEBUG)

    with DBQueryTracer(capture_query_details=capture_query_details) as tracer:
        try:
            response = await handler(request)
            return response
        except Exception as ex:
            error = ex
            raise
        finally:
            _log_request_trace(request, tracer, response=response, error=error)
