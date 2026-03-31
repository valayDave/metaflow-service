import argparse
import json
from pathlib import Path

from benchmarks.agentic_queries.models import SeedManifest
from benchmarks.agentic_queries.report import write_results_json
from benchmarks.agentic_queries.scenarios import SCENARIO_RUNNERS


def _load_manifest(path):
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return SeedManifest.from_dict(payload)


def parse_args():
    parser = argparse.ArgumentParser(description="Run a single benchmark scenario.")
    parser.add_argument("--scenario", choices=sorted(SCENARIO_RUNNERS), required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output")
    return parser.parse_args()


def main():
    args = parse_args()
    manifest = _load_manifest(args.manifest)
    result = SCENARIO_RUNNERS[args.scenario](manifest)
    print(json.dumps(result.to_dict(), indent=2))
    if args.output:
        write_results_json([result], args.output)


if __name__ == "__main__":
    main()
