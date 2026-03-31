import pytest

from benchmarks.agentic_queries.http_capture import ServiceRequestCapture


pytestmark = [pytest.mark.unit_tests]


class _FakeResponse(object):
    def __init__(self, status_code=200, content=b"ok"):
        self.status_code = status_code
        self.content = content


class _FakeSession(object):
    def get(self, url, **kwargs):
        return _FakeResponse(content=b"get-response")

    def post(self, url, **kwargs):
        return _FakeResponse(content=b"post-response")

    def patch(self, url, **kwargs):
        return _FakeResponse(content=b"patch-response")


class _FakeProvider(object):
    _session = _FakeSession()


def test_service_request_capture_injects_trace_header_and_counts_bytes(monkeypatch):
    monkeypatch.setattr(
        "benchmarks.agentic_queries.http_capture.load_service_metadata_provider",
        lambda: _FakeProvider,
    )

    with ServiceRequestCapture() as summary:
        response = _FakeProvider._session.get("http://localhost:8080/flows/demo")

    assert response.status_code == 200
    assert summary.request_count == 1
    assert summary.response_bytes == len(b"get-response")
    assert summary.requests[0].path == "/flows/demo"


def test_service_request_capture_restores_original_session_methods(monkeypatch):
    monkeypatch.setattr(
        "benchmarks.agentic_queries.http_capture.load_service_metadata_provider",
        lambda: _FakeProvider,
    )

    original_get = _FakeProvider._session.get
    with ServiceRequestCapture():
        pass

    assert _FakeProvider._session.get == original_get
