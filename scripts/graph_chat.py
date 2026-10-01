#!/usr/bin/env python3
"""Ask the lakehouse graph a question from the terminal: router -> one toolset -> answer, tool calls, provenance.

  .venv-graph-eval/bin/python scripts/graph_chat.py "What could the model see about Maya at T-7?"
  .venv-graph-eval/bin/python scripts/graph_chat.py                 # interactive: one question per line, Ctrl-D ends
  make graph-chat Q="Lapse rate by plan?"

It uses the harness in src/lakehouse_graph/agent.py (Pydantic AI 2.52): a structured-output router picks graph,
metrics, lineage, cohorts or refuse, and a sub-agent that sees only that toolset answers through the repo's MCP server
(scripts/graph_mcp.sh, stdio, sandboxed on macOS, 4,000-character answers). Defaults: --model ollama:qwen3:4b (Ollama
on 127.0.0.1:11434; the harness creates the derived tag lhg-qwen3-4b:ctx8192 once to pin num_ctx 8192), the build
$GRAPH_ROOT/current (GRAPH_ROOT default data/graph), temperature 0.7, at most 6 tool calls. Run Ollama with
OLLAMA_MAX_LOADED_MODELS=1 and OLLAMA_NUM_PARALLEL=1 and keep Docker stopped (about 4 GB for the model).

The installed qwen3:4b thinks on every turn and cannot be told not to: expect 5-7 s to route and 20-60 s per answer
on an M1 Pro. --model anthropic:claude-opus-5-5 uses the Claude API instead (needs ANTHROPIC_API_KEY; proprietary).

Printed per question: the route, the answer, every tool call (name, arguments, status, ms, what it returned and its
first --rows rows), the provenance of the answer (build id, profile, seed, commit, data_end, contract, sandbox), the
caveats the tools attached, the model settings actually applied (num_ctx, thinking) and the timings. --json prints
the same as one JSON object; --trace FILE appends the full replayable trace (JSONL).

Exit code: 0 answered (a refusal is an answer), 1 when an episode failed (timeout, limit, model error), 2 setup error.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_graph import agent as ag  # noqa: E402 (after the sys.path line above)

PROVENANCE_KEYS = ("build_id", "profile", "seed", "n_users", "seed_n_status", "data_end", "commit", "dirty", "contract",
                   "sandboxed", "lineage_build_id", "build_ids_seen")


def returned(result: str | None) -> tuple[str, list[dict]]:
    """A one-line summary of what a tool answered, and its main rows (from the recorded envelope)."""
    try:
        data = json.loads(result or "").get("data") or {}
    except (ValueError, AttributeError):
        return "", []
    for key in ("rows", "matches", "cells", "counts", "cohorts", "columns", "assertions", "edges"):
        rows = data.get(key)
        if isinstance(rows, list):
            summary = data.get("summary") if isinstance(data.get("summary"), dict) else {}
            extra = ", ".join(f"{k} {v}" for k, v in summary.items() if isinstance(v, (int, float, str)))[:160]
            return f"{len(rows)} {key}" + (f" ({extra})" if extra else ""), [x for x in rows if isinstance(x, dict)]
    return "", []


ROW_KEYS = ("event_date", "relation", "target_id", "feeds_feature", "in_feature_window", "known_by_as_of",
            "declared_exception", "rank", "renewal_id", "dist", "outcome", "id", "kind", "display")


def render(r: ag.AgentResult, show_rows: int = 12) -> str:
    lines = [f"route: {r.route or r.arm} (arm {r.arm})  status: {r.status}", "", r.answer.strip() or "(no answer)", ""]
    lines.append(f"tool calls ({len(r.tool_calls)}):")
    recorded = (r.trace.get("episode") or {}).get("tool_calls") or []
    for i, c in enumerate(r.tool_calls, 1):
        args = json.dumps(c.get("args"), sort_keys=True)
        err = f"  error: {c['error']}" if c.get("error") else ""
        what, rows = returned(recorded[i - 1].get("result")) if i - 1 < len(recorded) else ("", [])
        lines.append(f"  {i}. {c['name']}({args}) -> {c['status']} {c.get('ms')} ms" + (f": {what}" if what else "")
                     + err)
        for row in rows[:show_rows]:
            lines.append("       " + "  ".join(f"{k}={row[k]}" for k in ROW_KEYS if k in row))
        if len(rows) > show_rows:
            lines.append(f"       ... {len(rows) - show_rows} more")
    if not r.tool_calls:
        lines.append("  (none)")
    p = r.provenance or {}
    lines.append("provenance:" + ("" if p else " (no tool answered)"))
    for k in PROVENANCE_KEYS:
        if k in p:
            v = p[k]
            lines.append(f"  {k}: {json.dumps(v) if isinstance(v, (dict, list)) else v}")
    if isinstance(p.get("spec"), dict):
        lines.append("  spec: " + ", ".join(f"{k}={v}" for k, v in p["spec"].items()))
    if r.caveats:
        lines.append("caveats from the tools:")
        lines += [f"  - {c}" for c in r.caveats[:12]]
    m = r.model
    lines.append(f"model: {m.get('requested')} -> {m.get('provider')}:{m.get('name')}  num_ctx {m.get('num_ctx')} "
                 f"({m.get('num_ctx_source')})  thinking: {m.get('thinking')}")
    t = r.timings
    tok = t.get("tokens") or {}
    lines.append(f"timings: total {t.get('wall_ms')} ms, route {t.get('route_ms')} ms, "
                 f"model turns {t.get('llm_ms')} ms, "
                 f"tools {t.get('tool_ms')} ms; tokens in {tok.get('input_tokens')} / out {tok.get('output_tokens')}")
    return "\n".join(lines)


async def chat(args: argparse.Namespace, questions: list[str] | None) -> int:
    cfg = ag.RunConfig(num_ctx=args.num_ctx, max_tool_calls=args.max_tool_calls, temperature=args.temperature,
                       seed=args.seed, router_mode=args.router)
    rm = await asyncio.to_thread(ag.resolve_model, args.model, num_ctx=cfg.num_ctx, think=cfg.think,
                                 max_tokens=cfg.max_tokens)
    build, root = ag.resolve_build_path(args.build, args.graph_root)
    rc = 0
    async with ag.Session(rm, build, root, arm=args.arm, cfg=cfg) as session:
        interactive = questions is None
        while True:
            if interactive:
                try:
                    q = input("question> ").strip()
                except EOFError:
                    break
                if not q:
                    continue
            else:
                if not questions:
                    break
                q = questions.pop(0)
            res = await session.ask(q)
            result = ag.result_from(q, res, rm)
            loaded = ag.ollama_loaded() if rm.provider == "ollama" else []
            if loaded:
                result.model["loaded_context_length"] = [x["context_length"] for x in loaded]
            if args.json:
                print(json.dumps(result.to_dict(), indent=1, default=str))
            else:
                print(render(result, args.rows))
                if loaded:
                    print(f"ollama /api/ps: {loaded}")
                print()
            if args.trace:
                with open(args.trace, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps({"question": q, **result.trace}, default=str) + "\n")
            if result.status not in ("ok", "refused"):
                rc = 1
    return rc


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Terminal Q&A over the lakehouse graph tools (Pydantic AI + MCP).")
    ap.add_argument("question", nargs="*", help="the question (omit for an interactive session)")
    ap.add_argument("--model", default=ag.DEFAULT_MODEL, help="ollama:<model> (default) or anthropic:<model>")
    ap.add_argument("--build", default=None, help="graph build directory (default: $GRAPH_ROOT/current)")
    ap.add_argument("--graph-root", default=None, help="graph root (default: $GRAPH_ROOT, or derived from --build)")
    ap.add_argument("--arm", default="R", choices=ag.ARMS, help="R routed (default), M metrics only, SE one SQL tool "
                                                                 "(eval only), H every tool unrouted")
    ap.add_argument("--router", default="think", choices=["think", "fast"],
                    help="think (default, 5-7 s, more accurate) or fast (thinking off under the grammar, 0.4 s)")
    ap.add_argument("--num-ctx", type=int, default=ag.DEFAULT_NUM_CTX)
    ap.add_argument("--temperature", type=float, default=0.7)
    ap.add_argument("--seed", type=int, default=None, help="sampling seed (Ollama only)")
    ap.add_argument("--max-tool-calls", type=int, default=6)
    ap.add_argument("--json", action="store_true", help="print one JSON object per question")
    ap.add_argument("--rows", type=int, default=12, help="rows of each tool answer to print (default 12)")
    ap.add_argument("--trace", default=None, help="append the full replayable trace of each question (JSONL)")
    args = ap.parse_args(argv)
    questions = [" ".join(args.question)] if args.question else None
    try:
        return asyncio.run(chat(args, questions))
    except ag.AgentUnavailable as exc:
        print(f"graph_chat: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
