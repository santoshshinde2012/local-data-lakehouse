# Graph on gold: operations runbook

How to build, check, serve, refresh and clean up the graph on a laptop. Prerequisites: macOS 15+ (the
ladybug 0.21.2 wheels are `macosx_15_0`) or Linux, [uv](https://docs.astral.sh/uv/), Python 3.12 (uv
fetches it), and for the parity check only a JDK 17 or 21 plus `.venv-graph-spark`. The system `python3` is
never used by the graph targets.

## Quick path (no Docker)

```bash
make graph-venv                                   # .venv-graph from requirements-graph.txt (hash-checked)
make graph-sample PROFILE=s42                      # generator + gold into $GRAPH_ROOT/s42 (data/sample untouched)
make graph-sample PROFILE=tiny                     # exports for the committed tiny fixture
make graph-local PROFILE=tiny                      # build + strict contract + repo contracts
make graph-local PROFILE=s42
make graph-local                                   # PROFILE=default: data/sample/churn (run make churn-gold-local first)
make graph-promote                                 # $GRAPH_ROOT/current -> the default build
.venv-graph/bin/python scripts/build_lineage_local.py --graph-profile default       # lineage
.venv-graph/bin/python scripts/build_graph_cohorts.py --profile default             # cohorts
.venv-graph/bin/python scripts/build_lineage_local.py --graph-profile s42
.venv-graph/bin/python scripts/build_graph_cohorts.py --profile s42
.venv-graph/bin/python scripts/graph_evidence.py                                    # docs/graph/results + charts
```

`GRAPH_ROOT` (default `data/graph`, gitignored) can point anywhere: `make graph-local PROFILE=s42
GRAPH_ROOT=/path/to/scratch`. Then start Claude Code in the repo root and approve `.mcp.json`, or use
`scripts/graph_ask.sh` ([agent.md](agent.md#the-claude-path)).

## Make targets

The Makefile has the Phase 1 graph targets plus `graph-test`, `graph-e2e`, `lineage-local`, `graph-cohorts` and `graph-evidence`. For the rest, run the
command in the last column directly (from the repo root, with `GRAPH_PY=.venv-graph/bin/python`).

| Target | In the Makefile | What it runs |
|---|---|---|
| `graph-venv` | yes | `uv venv --python 3.12 .venv-graph` + `uv pip sync --require-hashes requirements-graph.txt` |
| `graph-sample PROFILE=s<seed>\|tiny\|inject [N_USERS=]` | yes | `scripts/build_graph_local.py sample` (refuses `default`) |
| `graph-build [PROFILE=] [GRAPH_BUILD_FLAGS=--rebuild]` | yes | `scripts/build_graph_local.py build` (`--verify-seed` for default) |
| `graph-check [GRAPH_CHECK_FLAGS=--strict]` | yes | `scripts/check_graph_contract.py` + `scripts/check_repo_contracts.py` |
| `graph-local` | yes | `graph-build` then `graph-check` |
| `graph-promote [BUILD=<id>]` | yes | `scripts/build_graph_local.py promote` |
| `graph-clean` | yes | `scripts/build_graph_local.py gc --keep 3` |
| `graph-golden [CONFIRM=1] [ONLY=tiny\|s42]` | yes | `scripts/build_graph_local.py golden` |
| `graph-test` | yes | `.venv-graph/bin/python -m pytest -q tests/graph` (about 11 to 13 min on an M1 Pro) |
| `graph-e2e` | yes | `make up-full`, then `docker-compose.graph.yml` up `--build --wait`, then `pipelines/run_graph_e2e.sh` ([lakehouse-twin.md](lakehouse-twin.md#docker-overlay)) |
| `graph-evidence` | yes | `$GRAPH_PY scripts/graph_evidence.py --graph-root $GRAPH_ROOT` |
| `lineage-local [PROFILE=]` | yes | `$GRAPH_PY scripts/build_lineage_local.py --graph-profile $PROFILE && $GRAPH_PY scripts/check_lineage_contract.py --graph-profile $PROFILE --strict` |
| `graph-cohorts [PROFILE=]` | yes | `$GRAPH_PY scripts/build_graph_cohorts.py build --profile $PROFILE` |
| `graph-viz RENEWAL=<id>` | no | `$GRAPH_PY scripts/graph_viz.py --renewal sub_santosh:2026-10-07` (writes `$GRAPH_ROOT/viz/<build>/...html`) |
| `graph-tools-check` | no | `$GRAPH_PY scripts/check_graph_tools.py --profile s42 [--bench 20]` |
| `graph-serve` | no | `scripts/graph_mcp.sh --toolset graph` (debug; stdio) |
| `graph-ask` | no | `scripts/graph_ask.sh` |
| `graph-sandbox-check` | no | `$GRAPH_PY scripts/graph_sandbox_check.py --control --log-check` (macOS) |
| `graph-parity` | no | `.venv-graph-spark/bin/python scripts/check_graph_parity.py parity --profile tiny --strict` |
| `graph-up` / `graph-down` / `airflow-trigger-graph` | no | see [lakehouse-twin.md](lakehouse-twin.md#docker-overlay) |
| `graph-chat` | no (the script's docstring mentions it) | `.venv-graph-eval/bin/python scripts/graph_chat.py "question"` (experimental) |
| `graph-eval`, `graph-bench` | no | `scripts/graph_eval.py` (eval venv), `scripts/graph_bench.py`; the leakage demo is `scripts/graph_leakage_demo.py` ([evaluation.md](evaluation.md#agent-eval)) |
| `graph-ui`, `graph-demo` | no | there is no UI and no demo script |

The spark venv: `uv venv --python 3.12 .venv-graph-spark && uv pip sync --python
.venv-graph-spark/bin/python --require-hashes requirements-graph-spark.txt`. The parity check finds a JDK
17 or 21 through `GRAPH_JAVA_HOME`, `JAVA_HOME`, the Zulu 17 path or `java_home`; the local Iceberg
lakehouse also needs `iceberg-spark-runtime-4.1_2.13-1.12.0.jar` and `sqlite-jdbc-3.46.1.3.jar` in `~/.ivy2`, `~/.m2` or
`GRAPH_SPARK_JARS_DIR` (never downloaded at run time).

## RAM and modes

Measured on a 16 GB M1 Pro that was already swapping (6 to 7 GB of 7 GB swap in use). Treat the numbers
as indicative. Unmeasured rows say "estimate".

| Mode | Runs | RAM |
|---|---|---|
| BUILD (local) | pandas gold twin, builder, Ladybug loader, lineage, cohorts | builder 309 MiB max RSS at seed 42 (`ru_maxrss`), loader 208 to 226 MiB (256 MB pool); measured in each contract run |
| BUILD (parity) | Spark local mode (2 GB driver) + numpy | about 2.5 GB, estimate; 61 s at seed 42 |
| SERVE (Claude) | up to 4 stdio servers (128 MB pool each) | graph 283 MiB, lineage 292 MiB, metrics 171 MiB, cohorts 181 MiB after warm calls (latest bench, [tools-bench-s42.json](results/tools-bench-s42.json)) |
| SERVE (open-source agent, experimental) | + Ollama `qwen3:4b` at `num_ctx` 8192 | about 3.9 GB for the model + 0.2 GB harness; Docker must be stopped |
| LAKEHOUSE | Postgres + Lakekeeper + RustFS (or SILO) + Spark (+ `ldl-graph`, + Airflow) | measured 2026-10-02: light stack idle about 190 MiB; Spark about 1.2 GiB while a job runs; Airflow overlay about 1.16 GiB; `ldl-graph` builder 576 to 582 MiB (2026-10-01), `mem_limit` 1,536 MiB ([README](../../README.md#prerequisites)) |

Rule: never run the lakehouse stack, Spark parity and a local LLM at the same time. A preflight that
refuses `graph-parity` / `graph-eval` when Docker is up, memory pressure is critical or free swap is under
1 GB (`FORCE=1` to override) is planned, not implemented.

## Housekeeping

- **One build at a time.** Build, promote, gc and contract writes take `$GRAPH_ROOT/.lock` (`fcntl`). A
  second build waits up to 600 s, then fails with "another graph build holds ... (waited N s)".
- **Unchanged builds are kept.** `make graph-build` on the same inputs and code keeps the build and
  refreshes its pins (exports, seed status). `GRAPH_BUILD_FLAGS=--rebuild` replaces it atomically and
  carries over the contract, lineage and cohorts when the Parquet is byte-identical.
- **Promote and roll back.** `make graph-promote` points `current` at the newest fresh default build with
  a strict contract pass; `make graph-promote BUILD=<id>` rolls back. Running servers keep the build
  they resolved at start: restart them (or Claude Code) after a promote.
- **GC.** `make graph-clean` keeps the newest 3 builds per profile and never removes `current`, `latest`
  or a build a live server holds (`<build>/.pids/<pid>.pid`, checked with `os.kill(pid, 0)`). The
  sandboxed server cannot delete its own pidfile; gc ignores pidfiles of dead pids but does not delete
  them (they are a few bytes each; a build that gc removes takes its pidfiles with it).
- **Audit logs** grow by one line per tool call in `$GRAPH_ROOT/logs/`; nothing rotates them.

## Refreshing goldens

Goldens move when the renewal model, the generator or the gold SQL change.

```bash
make graph-golden                       # fresh tiny + s42 in a scratch GRAPH_ROOT, diff against the committed files
make graph-golden CONFIRM=1             # write the files that differ (review the diff first)
.venv-graph/bin/python scripts/check_lineage_contract.py --print-golden > /tmp/core.json   # lineage golden; review, then copy over
```

The contract warns "inputs differ from ... and N golden value(s) differ" when a build's bronze has no
matching golden. A few plan numbers are also asserted directly in the tests as a second witness; update
them only with a written reason.

## Upgrading

- **ladybug** (pre-1.0, weekly releases, a reused PyPI name): bump the exact pin in
  `requirements-graph.in`, recompile with `uv pip compile --universal --generate-hashes --python-version
  3.12 requirements-graph.in -o requirements-graph.txt`, recompile the spark and client locks against it
  (`-c requirements-graph.txt`), `make graph-venv`, then rebuild every profile. Never migrate a `.lbdb`
  file: the version is in the build id and the server refuses a database loaded by another version.
- **pandas / numpy / pyarrow**: also in the build id. Parquet bytes are asserted within one platform and
  lock only; expect new build ids and identical contract answers.
- **The renewal model** (`3efe31a` today): rebuild, run the contracts, refresh the goldens with a reason.
- **Linux**: the CI `graph` job (ubuntu-latest) builds tiny and runs the strict contract; the
  `d2_q` goldens stay compared tie-aware across platforms.
- **Airflow**: the stack runs 3.3.2 (2.10.4 reached end of life on 2026-04-22); see [lakehouse-twin.md](lakehouse-twin.md#airflow).

## Troubleshooting

| Symptom | Fix |
|---|---|
| `GRAPH_PY=.venv-graph/bin/python not found: run 'make graph-venv'` | `make graph-venv` (or `GRAPH_PY=python` in CI) |
| `.venv-graph is not Python 3.12` | `rm -rf .venv-graph && make graph-venv` (the system `python3` may be 3.14) |
| no ladybug wheel for this platform | the wheels are macOS 15+ (arm64) and manylinux 2.28 (x86_64, aarch64), CPython 3.12 |
| `bronze CSVs missing in ...` | `make churn-gold-local` (default) or `make graph-sample PROFILE=s42` |
| `another graph build holds .../.lock` | wait for the other build; check for a stuck process |
| `inputs differ from ... golden value(s) differ` | the renewal model changed: `make graph-golden`, review, `CONFIRM=1` |
| `stale lineage build`, or a lineage `unresolved name` | rebuild the lineage after code changes; an unresolved name is a real extractor gap ([lineage.md](lineage.md#known-issue-the-ci-clone-line)) |
| server exits 3, `provenance unavailable: ...` | the build moved, changed, was loaded by another ladybug, or has no passing contract: rebuild / re-check; `--allow-unchecked` only for scratch builds |
| `sandbox-exec not available ... refusing to start unsandboxed (fail closed)` | macOS without `/usr/bin/sandbox-exec`; `GRAPH_SANDBOX=0` opts out with a banner (not recommended) |
| `graph_ask.sh: the Claude Code CLI ('claude') is not on PATH` | install Claude Code, sign in, rerun; older than 2.1.248 is refused |
| Claude Code does not show the lakehouse tools | start it in the repo root (relative command in `.mcp.json`), approve the project servers, restart after a promote |
| sandbox check fails on "reported sandbox denials" while other sandboxed Python processes run | the unified-log check is not filtered by pid: rerun it alone |
| Docker: port 8181 taken | Postgres publishes no host port any more; the catalog is Lakekeeper on `LAKEKEEPER_PORT` (default 8181): change it in `.env` |
| Docker: port 8080 taken | `AIRFLOW_API_PORT=8081` in `.env` |
| `run_graph_e2e.sh` stops at `check_lineage_contract` | an unresolved name in the lineage extractor, e.g. a clone URL moved into a shell variable ([lineage.md](lineage.md#known-issue-the-ci-clone-line)) |
| `make churn-e2e` crashed on `Decimal is not JSON serializable` | the export fix in this branch ([lakehouse-twin.md](lakehouse-twin.md#the-export-fix-04_export_featurespy)) |
| Ollama answers ignore the system prompt | context overflow is silent in Ollama; use the derived tag with `num_ctx 8192` ([agent.md](agent.md#the-open-source-path)) |

## Docker cleanup

```bash
docker compose -f docker-compose.yml -f docker-compose.graph.yml stop graph
docker compose -f docker-compose.yml -f docker-compose.graph.yml rm -f graph
make airflow-down && make down
docker compose -f docker-compose.yml -f docker-compose.airflow.yml down -v   # also deletes the volumes (catalog, objects, tags)
docker image rm ldl-graph:local                                               # the 629 MB graph image
```

The verification runs kept the volumes and images on purpose; the last command pair removes them.
`make churn-e2e` rewrites `data/export`; back it up first if you want to keep the no-Docker exports.

## CI

No CI job runs the graph yet. The plan is an independent `graph` job (Python 3.12, `pip install
--require-hashes -r requirements-graph.txt`, `GRAPH_PY=python`): build tiny and s42 with `graph-sample`,
strict contracts, `check_graph_tools.py` with the MCP smoke, the lineage contract, `pytest tests/graph`,
and the Spark parity gate with JDK 17. It would also be the first Linux run.
