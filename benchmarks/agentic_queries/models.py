from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class HttpRequestRecord:
    method: str
    url: str
    path: str
    status_code: int
    response_bytes: int


@dataclass
class HttpTraceSummary:
    request_count: int = 0
    response_bytes: int = 0
    requests: List[HttpRequestRecord] = field(default_factory=list)

    def add_record(self, record: HttpRequestRecord):
        self.request_count += 1
        self.response_bytes += record.response_bytes
        self.requests.append(record)

    def to_dict(self):
        return {
            "request_count": self.request_count,
            "response_bytes": self.response_bytes,
            "requests": [asdict(record) for record in self.requests],
        }


@dataclass
class DbTraceSummary:
    traced_requests: int = 0
    db_queries: int = 0
    db_rows: int = 0
    db_time_ms: float = 0.0
    request_time_ms: float = 0.0

    def add(self, other: "DbTraceSummary"):
        self.traced_requests += other.traced_requests
        self.db_queries += other.db_queries
        self.db_rows += other.db_rows
        self.db_time_ms += other.db_time_ms
        self.request_time_ms += other.request_time_ms

    def to_dict(self):
        return asdict(self)


@dataclass
class SeededRun:
    run_id: str
    num_parallel_tasks: int
    expected_failed: bool
    failed_task_indexes: List[int] = field(default_factory=list)
    stderr_payload_bytes: int = 0
    emits_target_artifact: bool = False
    synthetic_timestamp_iso: Optional[str] = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]):
        return cls(**payload)


@dataclass
class SeedManifest:
    dataset_type: str
    scale: str
    flow_name: str
    runs: List[SeededRun] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self):
        return {
            "dataset_type": self.dataset_type,
            "scale": self.scale,
            "flow_name": self.flow_name,
            "runs": [run.to_dict() for run in self.runs],
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]):
        return cls(
            dataset_type=payload["dataset_type"],
            scale=str(payload["scale"]),
            flow_name=payload["flow_name"],
            runs=[SeededRun.from_dict(item) for item in payload.get("runs", [])],
            metadata=payload.get("metadata", {}),
        )


@dataclass
class ScenarioResult:
    scenario: str
    scale: str
    logical_metadata_requests: int
    http_requests: int
    http_response_bytes: int
    db_queries: int
    db_rows: int
    db_time_ms: float
    wall_time_ms: float
    metadata_by_obj_type: Dict[str, int]
    max_depth: Optional[int] = None
    extras: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self):
        payload = asdict(self)
        payload["db_time_ms"] = round(self.db_time_ms, 3)
        payload["wall_time_ms"] = round(self.wall_time_ms, 3)
        return payload
