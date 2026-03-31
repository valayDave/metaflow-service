from contextlib import AbstractContextManager
from functools import wraps
from urllib.parse import urlparse

from benchmarks.agentic_queries.config import TRACE_HEADER_NAME, TRACE_HEADER_VALUE
from benchmarks.agentic_queries.models import HttpRequestRecord, HttpTraceSummary
from benchmarks.agentic_queries.prereqs import load_service_metadata_provider


class ServiceRequestCapture(AbstractContextManager):
    """Capture metadata-service HTTP requests during a benchmark scenario."""

    def __init__(self):
        self.summary = HttpTraceSummary()
        self._session = None
        self._originals = {}

    def _wrap_method(self, method_name, original):
        @wraps(original)
        def _wrapped(url, *args, **kwargs):
            headers = dict(kwargs.get("headers") or {})
            headers[TRACE_HEADER_NAME] = TRACE_HEADER_VALUE
            kwargs["headers"] = headers
            response = original(url, *args, **kwargs)
            self.summary.add_record(
                HttpRequestRecord(
                    method=method_name.upper(),
                    url=url,
                    path=urlparse(url).path,
                    status_code=response.status_code,
                    response_bytes=len(response.content or b""),
                )
            )
            return response

        return _wrapped

    def __enter__(self):
        provider = load_service_metadata_provider()
        self._session = provider._session
        for method_name in ("get", "post", "patch"):
            original = getattr(self._session, method_name)
            self._originals[method_name] = original
            setattr(self._session, method_name, self._wrap_method(method_name, original))
        return self.summary

    def __exit__(self, exc_type, exc, exc_tb):
        for method_name, original in self._originals.items():
            setattr(self._session, method_name, original)
        self._originals.clear()
        self._session = None
        return False
