# Agentic Query Benchmark Suite

This benchmark harness lives in `metaflow-service`, but it depends on the tracing work from:
- issue `#4` in the sibling `metaflow` repo for `MetadataTracer`
- issue `#5` in the metadata service for per-request DB trace summaries

## Prerequisites

1. Start the local dev stack from the sibling `metaflow` checkout:

```bash
cd /Users/vagarth/Documents/GitHub/metaflow
metaflow-dev up
```

2. Open a configured shell:

```bash
metaflow-dev shell
```

3. Make sure the local Metaflow checkout includes issue `#4`.

4. Make sure the running metadata service includes issue `#5` and has request tracing enabled.

5. From the dev shell, run the benchmark harness from this repo:

```bash
cd /Users/vagarth/Documents/GitHub/metaflow-service
python -m benchmarks.agentic_queries.suite --output-dir benchmarks/agentic_queries/output
```

## Individual Commands

Seed a history dataset:

```bash
python -m benchmarks.agentic_queries.seed --dataset-type history --scale 10 --output benchmarks/agentic_queries/output/history_10.json
```

Run a single scenario from a manifest:

```bash
python -m benchmarks.agentic_queries.run \
  --scenario status_filtered_run_listing \
  --manifest benchmarks/agentic_queries/output/history_10.json
```

## Outputs

The suite writes:
- seed manifests for reproducibility
- `results.json` with one record per scenario and scale
- `report.md` with a Markdown table and short analysis

The output directory is intentionally not committed because the numbers depend on the local dev stack, the local Metaflow checkout, and whether issues `#4` and `#5` are present in that runtime.
