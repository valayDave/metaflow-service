import sys

from metaflow import FlowSpec, Parameter, step

from benchmarks.agentic_queries.config import STDERR_ERROR_MARKER, TARGET_ARTIFACT_NAME


def _parse_failed_indexes(raw_value):
    if not raw_value:
        return set()
    return {int(item.strip()) for item in raw_value.split(",") if item.strip()}


class AgenticBenchmarkFlow(FlowSpec):
    num_parallel_tasks = Parameter("num_parallel_tasks", type=int, default=5)
    fail_every = Parameter("fail_every", type=int, default=0)
    failed_task_indexes = Parameter("failed_task_indexes", type=str, default="")
    stderr_payload_bytes = Parameter("stderr_payload_bytes", type=int, default=4096)
    emit_target_artifact = Parameter("emit_target_artifact", type=int, default=0)
    target_artifact_name = Parameter("target_artifact_name", type=str, default=TARGET_ARTIFACT_NAME)
    benchmark_label = Parameter("benchmark_label", type=str, default="default")

    @step
    def start(self):
        self.task_indexes = list(range(self.num_parallel_tasks))
        self.next(self.worker, foreach="task_indexes")

    @step
    def worker(self):
        self.task_index = self.input
        self.worker_label = f"{self.benchmark_label}:{self.task_index}"
        self.always_present_artifact = self.worker_label

        if self.emit_target_artifact and self.task_index == 0:
            setattr(
                self,
                self.target_artifact_name,
                f"{self.benchmark_label}:artifact:{self.task_index}",
            )

        failed_indexes = _parse_failed_indexes(self.failed_task_indexes)
        should_fail = (
            self.task_index in failed_indexes
            or (
                self.fail_every > 0
                and self.task_index > 0
                and self.task_index % self.fail_every == 0
            )
        )

        if should_fail:
            filler_size = max(self.stderr_payload_bytes - len(STDERR_ERROR_MARKER) - 1, 0)
            if filler_size:
                sys.stderr.write("X" * filler_size)
            sys.stderr.write(STDERR_ERROR_MARKER + "\n")
            sys.stderr.flush()
            raise RuntimeError(STDERR_ERROR_MARKER)

        self.next(self.join)

    @step
    def join(self, inputs):
        self.completed_tasks = len(inputs)
        self.next(self.end)

    @step
    def end(self):
        self.summary = {"completed_tasks": getattr(self, "completed_tasks", 0)}


if __name__ == "__main__":
    AgenticBenchmarkFlow()
