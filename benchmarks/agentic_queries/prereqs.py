import importlib
import os
import sys
from pathlib import Path

from benchmarks.agentic_queries.config import METAFLOW_ROOT_ENV_VAR, REPO_ROOT


def _default_metaflow_root() -> Path:
    return REPO_ROOT.parent / "metaflow"


def ensure_metaflow_on_path():
    metaflow_root = Path(os.environ.get(METAFLOW_ROOT_ENV_VAR, _default_metaflow_root()))
    if metaflow_root.exists():
        metaflow_root_str = str(metaflow_root)
        if metaflow_root_str not in sys.path:
            sys.path.insert(0, metaflow_root_str)
    return metaflow_root


def load_metadata_tracer():
    ensure_metaflow_on_path()
    try:
        module = importlib.import_module("metaflow.metadata_provider")
    except ImportError as error:
        raise RuntimeError(
            "Unable to import Metaflow. Run this benchmark from `metaflow-dev shell` "
            "or set AGENTIC_BENCHMARK_METAFLOW_ROOT to a local Metaflow checkout."
        ) from error

    try:
        return getattr(module, "MetadataTracer")
    except AttributeError as error:
        raise RuntimeError(
            "MetadataTracer is not available. Issue #4 must be present in the local "
            "Metaflow checkout before running this benchmark suite."
        ) from error


def load_metaflow_symbols():
    ensure_metaflow_on_path()
    try:
        module = importlib.import_module("metaflow")
    except ImportError as error:
        raise RuntimeError(
            "Unable to import Metaflow. Run this benchmark from `metaflow-dev shell` "
            "or set AGENTIC_BENCHMARK_METAFLOW_ROOT to a local Metaflow checkout."
        ) from error

    return module.Flow, module.Run, module.namespace


def load_service_metadata_provider():
    ensure_metaflow_on_path()
    try:
        module = importlib.import_module("metaflow.plugins.metadata_providers.service")
    except ImportError as error:
        raise RuntimeError(
            "Unable to import ServiceMetadataProvider from the local Metaflow checkout."
        ) from error
    return module.ServiceMetadataProvider
