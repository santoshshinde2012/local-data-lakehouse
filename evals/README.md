# Agent eval: cases, recorded traces and the report schema

Everything here is read by `scripts/graph_eval.py` (local only; it calls a model). CI never calls a model: it runs
the deterministic replay test `tests/graph/test_eval_replay.py` over `replay/` (the merge gate, PLAN 9.2).

## `graph_cases.yaml`

40 cases in the PLAN 9.2 categories: evidence 5, similarity 5, exposure 3, entity resolution 3, lapse rates 6,
feature cards 3, lineage 6, multi-step 2, honesty / refusal / injection 7. Shapes: `graph`, `metric`, `lineage`,
`honesty` (the model gate reads the first three).

No case stores an answer. A case names oracle references, evaluated on the build of every seed:

| field | meaning |
|---|---|
| `bind` | placeholders for the question and the arguments (`$name`): e.g. the hero's rank-1 neighbour, the September pricing change, a historical renewal with hidden neighbour outcomes |
| `oracle` | `{fn, args}`: the reference answer, a dict of named values computed from the build's Parquet by `scripts/graph_eval.py`'s oracle registry (`lakehouse_graph.oracle`, `lakehouse_graph.lineage.oracle`, `spec.FEATURE_CARDS`), never by the tools under test |
| `checks` | code graders over the final answer: `exact`, `numeric` (`tol`, default 0.001; rates may be written as %), `set_f1` (`min`, default 1.0; ids, names or column refs), `affirm` (yes / no), `date`, `caveat` (preset or regex), `forbidden` (preset or regex; claims are checked per sentence, a negated sentence or a question passes) |
| `toolsets` | the toolsets an answer may use; `refuse` means no tool is needed. Also the router's correct routes (arm R) |
| `max_calls` | per-tool caps (the injection case: `graph_find` at most 2); every case also has the global cap of 6 calls |
| `profile` | `seed` (the seed's own build `s<seed>`, default) or `inject` (the tiny bronze with one poisoned `user_name`) |

`python scripts/graph_eval.py cases --seeds 42,7 --show` materialises the cases and REJECTS a case on a seed when
its answer has a tie at the boundary (the top-10 cut, an argmax, the nearest lapse, a name shared by two customers),
when the reference is empty or would be suppressed by the tools (a cell under 5), or when the question text contains
an expected value. A rejection is reported in the eval report, never dropped silently. At seed 42 all 40 materialise.

The severity-mismatch question of PLAN 9.2 needs the radar contract (the full lineage profile with `RADAR_DIR`);
on the core profile `lin-06` asks for the severity of the export range check instead.

## Arms, sampling and graders

Arms: **R** router + one toolset, **M** metrics only, **SE** the same Parquet behind one SELECT-only DuckDB tool
(eval only), **H** all 14 typed tools unrouted. Each (arm, case, seed, trial) is a fresh agent and a fresh MCP
server; trial *t* of seed *s* samples at temperature 0.7 with seed `s * 1000 + t` (router included). An episode
passes when every check passes, the tools it used belong to the case's toolsets (not graded for SE), it made at most
6 tool calls (and respected `max_calls`) and it finished (status `ok`, or a router refusal). Timeouts, servers that
did not start, model API errors and silently truncated contexts are the INVALID class: failures in the pass rates,
also counted on their own.

## `replay/*.jsonl`

One recorded episode per line (schema `lhg-replay/1`), all on the **tiny** build:

| file | class | how it was made |
|---|---|---|
| `pass.jsonl` | a passing episode | real `qwen3:4b` episode, arm R |
| `fail.jsonl` | a failing episode | real `qwen3:4b` episode, arm M on an evidence question |
| `refusal.jsonl` | a router refusal | real `qwen3:4b` router decision, arm R |
| `truncated_context.jsonl` | a silently truncated context | real `qwen3:4b` episode with the server's default 4096 window, arm H |
| `text_tool_call.jsonl` | a tool call written as text | **scripted** (Pydantic AI `FunctionModel`): no real `qwen3:4b` episode has done it |

Each line holds the raw case, the resolved model and settings, the route, the full trace (every model turn, tool
call and tool result) and the expected grade. `scripts/graph_eval.py replay` re-materialises the case on the tiny build
and re-grades the recorded answer (no model, no server); `--live` also re-runs the recorded model turns through the
harness (`agent.replay_model`) against a live MCP server and requires identical status, output, tool calls, tool
results (data and caveats; provenance is volatile), flags and grade. Refresh them deliberately (after a golden
refresh or a grader change) with `scripts/graph_eval.py record --case <id> --arm <arm> --label <class> --out ...`.

## `report.json` (schema `lhg-eval-report/1`)

Written by `scripts/graph_eval.py run` next to `episodes.jsonl` (one row per episode: schema `lhg-eval-episode/1`,
grade, timings, tokens and the full trace) and `report.md`.

| key | content |
|---|---|
| `model`, `resolved_model` | the requested spec and what it resolved to: provider, tag, `profile_from`, `num_ctx` (+ source), parent digest, Ollama version, capabilities, `think_requested`, `think_wire`, `thinking` (what was applied) |
| `settings` | `agent.RunConfig`: temperature, max_tokens, presence_penalty, num_ctx, think, send_back_thinking, router_mode, tool / turn limits, timeouts, max_chars, keep_alive |
| `arms`, `seeds`, `trials`, `episodes`, `started_at`, `finished_at` | the run |
| `builds` | per profile: `build_id`, `lineage_build_id`, cohorts present |
| `cases` | `total`, `materialised` per seed, `rejected` per seed (`[{case, reason}]`) |
| `host`, `preflight`, `speed_before`, `speed_after`, `loaded_context_length` | platform, the PLAN 10.2 checks (refusals, `forced`), generation speed (tok/s) before and after, `/api/ps` context window |
| `router_prompt_sha256`, `instructions_sha256`, `cases_file_sha256` | the measured configuration |
| `results.<arm>` | `episodes`, `items` (case x seed), `k`, `passed`, `invalid`, `pass_at_1`, `pass_at_1_valid_only`, `pass_k`, `pass_k_rate`, `by_shape` / `by_category` (`items`, `pass_k`, `pass_k_rate`, `episodes`, `passed`, `pass_at_1`, `invalid`), `statuses`, `text_tool_call_episodes` / `_rate`, `schema_valid_call_rate`, `tool_calls`, `schema_invalid_calls`, `truncated_context_episodes`, `length_capped_episodes`, `leaked_think_episodes`, `thinking_chars_mean`, `latency_ms` (`p50`, `p95`, `max`, `*_valid`), `tokens`; arm R adds `routing` (`decisions`, `correct`, `accuracy`, `fail_closed`, `route_ms_p50`, `confusion`) |
| `pass_k` | `{"k": k, arm: {shape: [passed, items]}}` |
| `pass3` | the same as `{arm: {shape: [passed, cases]}}`, written only when k >= 3: what `scripts/graph_charts.py` (`charts.eval_data`) draws |
| `pass_at_1` | `{arm: {shape: rate}}` |
| `gate`, `gate_by_arm` | PLAN 8.6 on arm R: per check `{value, threshold, ok}`, `verdict` (`supported` / `experimental`), `basis` (`pass^k`; a smoke run with k < 3 says so) |
| `per_case` | per case: category, shape, question, and per arm `{passed, episodes, invalid, failed_checks}` |
| `notes` | e.g. the Claude arm's missing temperature / seed, a regrade |

`python scripts/graph_eval.py report --out <dir> [--regrade]` rebuilds the report from `episodes.jsonl`
(`--regrade` grades the recorded answers again with the current graders).
