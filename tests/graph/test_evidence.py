"""scripts/graph_evidence.py: provenance headers, redaction, absent pieces, the merged index.

* redaction turns the repo root, the GRAPH_ROOT, the home directory and temp dirs into relative / symbolic
  paths and removes anything that looks like a credential;
* every result file starts with status, command, commit + dirty flag, date, host, Python and package versions,
  duration and summary;
* optional pieces that are absent (no build, no eval report, no bench / leakage script, no Iceberg build,
  no Docker record) are recorded as "not run" / "not available", never as failures;
* a recorded Docker run copies timings and summary lines only, never a line with a credential;
* the index is rebuilt from every result file on disk, so a partial (--only) run keeps the full table;
* the committed results hold no home path, scratch path or credential.
"""
from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
RESULTS = REPO / "docs" / "graph" / "results"


def _load():
    spec = importlib.util.spec_from_file_location("graph_evidence", REPO / "scripts" / "graph_evidence.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["graph_evidence"] = mod
    spec.loader.exec_module(mod)
    return mod


ge = _load()


def test_redaction_of_paths_and_credentials(tmp_path):
    home = Path("/Users/someone")
    graph_root = REPO / "scratch-root" / "graph"
    red = ge.Redactor(REPO, graph_root, home)
    text = "\n".join([
        f"build {graph_root}/s42/builds/abc and {graph_root.relative_to(REPO)}/tiny",
        f"script {REPO}/scripts/check_graph_contract.py in {REPO}",
        f"home {home}/.ssh/id_rsa and /Users/other/.aws/credentials",
        "tmp /private/var/folders/x1/abc/T/graph-evidence-1/out.json and /tmp/foo/bar.txt",
        "uri postgresql+psycopg2://iceberg:s3cret@postgres:5432/iceberg",
        "PASSWORD=hunter2 token: abcdef123 api_key=XYZ12345 secret_access_key=abc",
        "key AKIAABCDEFGHIJKLMNOP and minioadmin",
        "scratch .graph-work/impl/docs/graph/s42",
    ])
    out = red(text)
    for leak in ("/Users/", str(REPO), "s3cret", "hunter2", "abcdef123", "XYZ12345", "AKIAABCDEFGHIJKLMNOP",
                 "minioadmin", ".graph-work", "/var/folders", "/tmp/foo"):  # noqa: S108 (a planted string)
        assert leak not in out, f"{leak!r} survived redaction:\n{out}"
    assert "$GRAPH_ROOT/s42/builds/abc" in out and "$GRAPH_ROOT/tiny" in out
    assert "scripts/check_graph_contract.py" in out
    assert "~/.ssh/id_rsa" in out
    assert "postgresql+psycopg2://<redacted>@postgres:5432/iceberg" in out


def test_result_header_carries_the_provenance_fields():
    env = {"commit": "abc1234", "dirty": True, "date": "2026-10-01", "platform": "macOS-26-arm64",
           "platform_tag": "macosx_arm64", "python": "3.12.9", "versions": {"ladybug": "0.21.1"}, "spark_venv": {}}
    res = ge.Result("graph-contract-tiny", "Graph contract (tiny)", "graph-contract", "tiny", "pass", 0,
                    "python scripts/check_graph_contract.py --profile tiny", 1.5, "Graph contract OK",
                    "Graph contract OK")
    text = ge.render(res, env)
    assert text.startswith("# Graph contract (tiny)\n")
    assert "check=graph-contract -->" in text
    for row in ("| Status | **pass** (exit 0) |", "| Profile | tiny |", "| Command | `python scripts/",
                "| Commit | `abc1234` (working tree dirty: yes) |", "| Date | 2026-10-01 (UTC) |",
                "| Host | macOS-26-arm64 (macosx_arm64) |", "| Python | 3.12.9 · ladybug 0.21.1 |",
                "| Duration | 1.5 s |", "| Summary | Graph contract OK |"):
        assert row in text, row


def test_absent_pieces_are_not_run_or_not_available_never_failures(tmp_path, capsys):
    out = tmp_path / "results"
    rc = ge.main(["--graph-root", str(tmp_path / "empty-root"), "--out", str(out), "--docs-dir", str(tmp_path / "docs"),
                  "--no-charts", "--spark-py", str(tmp_path / "no-spark-python"),
                  "--only", "graph-contract,iceberg-contract,lineage-contract,graph-tools,graph-parity,cohorts,"
                            "bench,eval,leakage,docker-e2e"])
    assert rc == 0, capsys.readouterr().out
    index = (out / "index.md").read_text(encoding="utf-8")
    statuses = re.findall(r"^\| [^|]+ \| [^|]+ \| ([^|]+) \|", index, flags=re.M)
    assert statuses and all(s.strip() in ("not run", "not available") for s in statuses[1:]), statuses
    contract = (out / "graph-contract-s42.md").read_text(encoding="utf-8")
    assert "make graph-local PROFILE=s42" in contract
    assert "not available" in (out / "eval.md").read_text(encoding="utf-8")
    assert "not available" in (out / "graph-parity-tiny.md").read_text(encoding="utf-8")
    for f in out.glob("*.md"):
        text = f.read_text(encoding="utf-8")
        assert str(tmp_path) not in text and "/Users/" not in text, f.name


def test_a_check_that_runs_is_recorded_with_its_output(tmp_path):
    out = tmp_path / "results"
    rc = ge.main(["--graph-root", str(tmp_path / "root"), "--out", str(out), "--docs-dir", str(tmp_path / "docs"),
                  "--no-charts", "--only", "repo-contracts,tool-catalogue"])
    assert rc == 0
    text = (out / "repo-contracts.md").read_text(encoding="utf-8")
    assert "| Status | **pass** (exit 0) |" in text
    assert "`python scripts/check_repo_contracts.py`" in text
    assert "Repo contracts OK" in text and "## Output" in text
    assert str(REPO) not in text
    cat = (out / "tool-catalogue.md").read_text(encoding="utf-8")
    for tool in ("graph_renewal_evidence", "metric_lapse_rate", "lineage_trace", "cohort_summary"):
        assert f"`{tool}`" in cat
    index = (out / "index.md").read_text(encoding="utf-8")
    assert "[repo-contracts.md](repo-contracts.md)" in index and "[tool-catalogue.md](tool-catalogue.md)" in index


def test_the_index_merges_results_of_earlier_runs(tmp_path):
    out = tmp_path / "results"
    common = ["--graph-root", str(tmp_path / "root"), "--out", str(out), "--docs-dir", str(tmp_path / "docs"),
              "--no-charts"]
    ge.main([*common, "--only", "eval"])
    ge.main([*common, "--only", "bench"])
    index = (out / "index.md").read_text(encoding="utf-8")
    assert "[eval.md](eval.md)" in index and "[bench.md](bench.md)" in index
    assert index.index("bench.md") < index.index("eval.md"), "fixed order (the CHECKS order), not run order"


def test_results_regions_in_docs_pages_are_filled(tmp_path):
    docs = tmp_path / "docs"
    docs.mkdir()
    page = docs / "page.md"
    page.write_text("a\n<!-- graph-evidence:begin results:checks -->\nold\n"
                    "<!-- graph-evidence:end results:checks -->\nb\n", encoding="utf-8")
    ge.main(["--graph-root", str(tmp_path / "root"), "--out", str(docs / "results"), "--docs-dir", str(docs),
             "--no-charts", "--only", "eval"])
    text = page.read_text(encoding="utf-8")
    assert "old" not in text and "[eval.md](results/eval.md)" in text and text.startswith("a\n")


def test_docker_record_copies_timings_and_summary_lines_only(tmp_path):
    timings = tmp_path / "docker_timings.json"
    timings.write_text(json.dumps({"memory_free_percent_start": 47, "make_up": {"rc": 0, "s": 8.7, "log": "x.log"},
                                   "A_graph_e2e": {"rc": 1, "s": 72.5, "log": "a.log"},
                                   "B_negative_missing_tag": {"rc": 1, "s": 1.5, "log": "b.log"}}), encoding="utf-8")
    log = tmp_path / "a.log"
    log.write_text("\n".join([
        "==> Graph E2E (2026-10-01T10:11Z), profile default",
        "PYICEBERG_CATALOG__LAKEHOUSE__URI=postgresql+psycopg2://iceberg:topsecret@postgres:5432/iceberg",
        "Graph contract OK (renewal-graph/v1, profile default, build ca31c633cbf7): ... strict",
        f"Graph promote OK: {tmp_path}/graph/current -> x",
        "  FAIL  unresolved name: .github/workflows/ci.yml:55: runs scripts/sync_lakehouse_exports.sh",
        "Lineage contract FAILED: password=hunter2",
        "random noise line",
    ]), encoding="utf-8")
    (tmp_path / "b.log").write_text("### B_negative_missing_tag: docker compose ... exec -T graph python ...\n"
                                    "Graph build FAILED: provenance unavailable: tag graph_000000000000 is not on "
                                    "gold.graph_build_manifest\n", encoding="utf-8")
    states = tmp_path / "states.log"
    states.write_text("\n".join([
        "dag_id           execution_date             task_id                 state    start_date",
        "lakehouse_graph  2026-10-01T10:15:44+00:00  publish_gold_graph      success  2026-10-01T10:15:45+00:00",
        "lakehouse_graph  2026-10-01T10:15:44+00:00  check_lineage_contract  failed   2026-10-01T10:16:38+00:00",
        "lakehouse_graph  2026-10-01T10:15:44+00:00  promote                 upstream_failed",
    ]), encoding="utf-8")
    status, summary, md = ge.docker_record(timings, [log, states], ge.Redactor(REPO, tmp_path / "graph"))
    assert status == "partial" and "A_graph_e2e" in summary and "B_negative" not in summary
    assert "| make_up | 0 | 8.7 | ok |" in md and "memory_free_percent_start: 47" in md
    assert "| B_negative_missing_tag | 1 | 1.5 | expected failure (negative test" in md
    assert "| A_graph_e2e | 1 | 72.5 | **failed**" in md
    assert "Graph contract OK" in md and "unresolved name" in md
    assert "Graph build FAILED: provenance unavailable" in md, "a failed or negative step's own log is read"
    assert ("Airflow task states: 1 of 3 tasks `success` (publish_gold_graph success, check_lineage_contract failed, "
            "promote upstream_failed)") in md
    assert "check_lineage_contract  failed" in md
    for leak in ("topsecret", "hunter2", "random noise", str(tmp_path)):
        assert leak not in md, leak


def test_a_negative_step_that_passes_is_a_failure(tmp_path):
    timings = tmp_path / "t.json"
    timings.write_text(json.dumps({"B_negative_missing_tag": {"rc": 0, "s": 1.0}}), encoding="utf-8")
    status, summary, md = ge.docker_record(timings, [], ge.Redactor(REPO, None))
    assert status == "partial" and "B_negative_missing_tag" in summary
    assert "unexpected: a negative test passed" in md


def test_parity_without_a_bronze_sample_is_not_run_and_a_missing_chart_build_is_not_a_failure(tmp_path, capsys):
    fake_spark = tmp_path / "spark-python"
    fake_spark.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_spark.chmod(0o755)
    out = tmp_path / "results"
    rc = ge.main(["--graph-root", str(tmp_path / "empty-root"), "--out", str(out), "--docs-dir", str(tmp_path / "docs"),
                  "--img-dir", str(tmp_path / "img"), "--spark-py", str(fake_spark), "--parity-profiles", "s42",
                  "--only", "graph-parity"])
    printed = capsys.readouterr().out
    assert rc == 0, printed
    parity = (out / "graph-parity-s42.md").read_text(encoding="utf-8")
    assert "| Status | not run |" in parity and "make graph-sample PROFILE=s42" in parity
    index = (out / "index.md").read_text(encoding="utf-8")
    assert "| Charts | not regenerated: no s42 build in this GRAPH_ROOT" in index
    assert not (tmp_path / "img").exists(), "the committed charts are left alone"
    assert str(tmp_path) not in printed and "/Users/" not in printed


@pytest.mark.parametrize("path", sorted(RESULTS.glob("*.md")) + sorted(RESULTS.glob("*.json")), ids=lambda p: p.name)
def test_committed_results_have_no_home_path_scratch_path_or_credential(path):
    text = path.read_text(encoding="utf-8")
    assert not re.search(r"/Users/|/home/[a-z]|/private/var/folders", text), "absolute path"
    assert ".graph-work" not in text, "scratch path"
    assert "minioadmin" not in text
    assert not re.search(r"(?i)\b(password|secret|token|api[_-]?key)\s*[=:]\s*(?!<redacted>)[\w-]{4,}", text)
    assert not re.search(r"://[^/\s:@]+:[^/\s@<]+@", text), "a URI with a password"


def test_committed_results_carry_a_provenance_header():
    files = [f for f in RESULTS.glob("*.md") if f.name not in ("index.md", "palette-validation.md")]
    assert files, "run scripts/graph_evidence.py"
    for f in files:
        text = f.read_text(encoding="utf-8")
        for row in ("| Status |", "| Commit |", "| Date |", "| Host |", "| Python |", "| Summary |"):
            assert row in text, f"{f.name}: {row}"


def test_task_state_lines_parse_with_and_without_a_logical_date():
    """Airflow 3 leaves logical_date empty for a manual run; both layouts count as task-state lines."""
    mod = _load()
    a3 = "lakehouse_graph                  publish_gold_graph      success  2026-10-03T12:49:44+00:00  2026-10-03T12:50:40+00:00"
    a2 = "lakehouse_graph  2026-10-01T10:00:00+00:00  build_graph  failed  2026-10-01T10:00:01+00:00  2026-10-01T10:00:09+00:00"
    assert mod.TASK_STATE.match(a3).groups()[:2] == ("publish_gold_graph", "success")
    assert mod.TASK_STATE.match(a2).groups()[:2] == ("build_graph", "failed")
    assert mod.TASK_STATE.match("dag_id  logical_date  task_id  state  start_date  end_date") is None
