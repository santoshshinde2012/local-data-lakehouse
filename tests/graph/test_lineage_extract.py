"""Tier-0 lineage extraction: the scope walk, the ast extractors and the fail-loudly rule.

No graph engine here: these tests run the extractor and the assembler on the repo itself
and on scratch copies of it that are edited to break one thing at a time. A broken name must
surface as a ``LineageExtractError`` or as an entry in ``graph.unresolved``, never vanish.
"""
from __future__ import annotations

import ast
import re
import shlex
import shutil
import string
from collections import Counter
from pathlib import Path

import pytest
import sqlglot
from conftest import REPO
from sqlglot import exp

from lakehouse_graph import manifest as mf
from lakehouse_graph import spec as gspec
from lakehouse_graph.lineage import build as lbuild
from lakehouse_graph.lineage import extract as ex
from lakehouse_graph.lineage import graph as lg
from lakehouse_graph.lineage import queries, spec
from lakehouse_graph.lineage import scope_walk as sw
from lakehouse_graph.lineage.spec import LineageExtractError

GOLD = "col:" + spec.GOLD_TABLE + "#"
GRAPH_DAG = "airflow/dags/lakehouse_graph.py"                     # the lakehouse-native graph chain
GRAPH_DAG_HELPERS = "airflow/dags/lakehouse_graph_operators.py"   # graph_exec_task, spark_submit_args_task
GRAPH_E2E = "pipelines/run_graph_e2e.sh"                          # the same chain without Airflow
RADAR = "https://github.com/santoshshinde2012/retention-radar"
RADAR_STEP = "Retention Radar consumes the export"
COPIED = ["sql", "src/jobs", "airflow/dags", "pipelines", "Makefile", ".github/workflows/ci.yml", "README.md"]


def fake_repo(tmp_path: Path) -> Path:
    """A scratch copy of everything the extractor reads (edit it freely)."""
    root = tmp_path / "repo"
    for rel in COPIED:
        src, dst = REPO / rel, root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            shutil.copytree(src, dst, ignore=shutil.ignore_patterns("__pycache__"))
        else:
            shutil.copy2(src, dst)
    (root / "scripts").mkdir()
    for p in list((REPO / "scripts").glob("*.py")) + list((REPO / "scripts").glob("*.sh")):
        shutil.copy2(p, root / "scripts" / p.name)
    (root / "data/sample").mkdir(parents=True)
    for name in spec.RETAIL_CSVS:
        shutil.copy2(REPO / "data/sample" / name, root / "data/sample" / name)
    return root


def edit(root: Path, rel: str, old: str, new: str) -> None:
    text = (root / rel).read_text()
    assert text.count(old) == 1, f"{rel}: expected exactly one {old!r}"
    (root / rel).write_text(text.replace(old, new))


@pytest.fixture(scope="module")
def facts() -> dict:
    return ex.extract_all(REPO)


@pytest.fixture(scope="module")
def graph() -> lg.LineageGraph:
    return lg.assemble(REPO, "core")


@pytest.fixture(scope="module")
def gold_lineage(facts) -> sw.GoldLineage:
    bronze = ex.bronze_tables(facts["jobs"][spec.BRONZE_JOB])
    silver = ex.silver_tables(facts["jobs"][spec.SILVER_JOB], bronze)
    sql = string.Template((REPO / spec.GOLD_SQL).read_text()).substitute(silver=spec.SILVER_NAMESPACE)
    return sw.GoldLineage(sql, ex.silver_schema(silver), source=spec.GOLD_SQL)


# --------------------------------------------------------------------------- schema declaration
def test_spec_declares_24_labels_48_edge_types_and_no_reserved_names():
    spec.check_schema()
    assert len(spec.NODE_SCHEMA) == 24 and len(spec.EDGE_SCHEMA) == 48
    assert "DataColumn" in spec.NODE_SCHEMA and "Column" not in spec.NODE_SCHEMA
    assert sorted(n.label for n in spec.NODE_SCHEMA.values() if n.placeholder) == ["Ref", "Run", "Snapshot"]
    assert sorted(e.rel for e in spec.EDGE_SCHEMA.values() if e.placeholder) == [
        "CONSUMED_SNAPSHOT", "HAS_SNAPSHOT", "PARENT", "POINTS_TO", "PRODUCED_BY_RUN", "RAN_AS", "SUPERSEDES"]
    assert spec.SPEC_VERSION == "metadata-graph/0.1"


def test_column_ref_rules():
    assert spec.column_ref("lakehouse.gold.churn_renewal_features", "limit_hits_14d") == \
        "gold.churn_renewal_features.limit_hits_14d"
    assert spec.column_ref("data/sample/churn/limit_events.csv", "hit_at") == "source.limit_events.hit_at"
    assert spec.column_ref("data/export/churn_user_features.csv", "user_id") == "export.churn_user_features.user_id"
    assert spec.column_ref("lakehouse.bronze.churn_usage_raw", "_source_file") == "bronze.churn_usage_raw._source_file"
    # a table name with a digit does not fit the ColumnRef grammar: the column exists but is not addressable
    assert spec.column_ref("data/sample/orders_day1.csv", "order_id") is None
    for bad in ("gold.x", "Gold.t.c", "gold.t.c; DROP", "graph.nodes_renewal.x", "gold..c", "gold.t.C",
                "gold.t.c\n", "\ngold.t.c", "gold.t.c ", None, 7, ["gold.t.c"]):
        assert not spec.is_column_ref(bad), bad
    assert spec.is_column_ref("gold.churn_renewal_features.limit_hits_14d")
    assert spec.COLUMN_REF_RE.match("gold.t.c\n") and not spec.COLUMN_REF_RE.fullmatch("gold.t.c\n")   # why fullmatch


# --------------------------------------------------------------------------- facts
def test_bronze_and_silver_schemas_come_from_the_jobs(facts):
    bronze = ex.bronze_tables(facts["jobs"][spec.BRONZE_JOB])
    silver = ex.silver_tables(facts["jobs"][spec.SILVER_JOB], bronze)
    assert len(bronze) == 10 and len(silver) == 10
    assert bronze["churn_limit_events_raw"]["csv"] == "limit_events.csv"
    limits = silver["churn_limit_events"]
    assert limits["bronze"] == "churn_limit_events_raw"
    assert dict(limits["columns"])["hit_date"] == "date" and limits["derived"]["hit_date"][1] == ["hit_at"]
    assert silver["churn_subscription_snapshots"]["dedupe_keys"] == ["subscription_id", "snapshot_date"]
    assert silver["churn_usage_daily"]["bronze"] == "churn_usage_raw"


def test_spark_jobs_resolve_loop_built_table_names(facts):
    j01, j02, j03, j04 = (facts["jobs"][r] for r in (spec.BRONZE_JOB, spec.SILVER_JOB, spec.GOLD_JOB, spec.EXPORT_JOB))
    assert len(j01["writes"]) == 10 and {m for _, m in j01["writes"]} == {"createOrReplace"}
    assert ("lakehouse.bronze.churn_limit_events_raw", "createOrReplace") in j01["writes"]
    assert len(j01["csv_in"]) == 10 and j01["app_name"] == "churn_01_ingest_bronze"
    assert ("lakehouse.bronze.churn_usage_raw", "spark.table", "input") in j02["reads"]          # through the lambda
    assert ("lakehouse.silver.churn_usage_daily", "createOrReplace") in j02["writes"]
    assert j03["sql_substitution"] == {"silver": "lakehouse.silver"}
    assert ex.canonical_path(j03["sql_file"]) == spec.GOLD_SQL
    names = {ex.canonical_path(f[0]).rsplit("/", 1)[-1]: f for f in j04["files_out"]}
    assert set(names) == set(gspec.EXPORT_FILES)
    assert len(names["churn_renewals_audit.csv"][3]) == 31 and len(names["churn_user_features.csv"][3]) == 25
    assert j04["records"]["record"] == [c for c in names["churn_user_features.csv"][3] if c != "churned"]
    assert all(not j["unresolved"] for j in facts["jobs"].values())


def test_makefile_ci_and_dags_are_parsed(facts):
    make = facts["make"]
    assert [p for p, _ in make["churn-gold-local"]["py"]] == ["scripts/build_churn_gold_local.py",
                                                              "scripts/check_churn_export.py"]
    assert make["churn-gold-local"]["prereqs"] == ["churn-sample"]
    assert make["airflow-trigger-churn"]["sh"] == [("pipelines/airflow_trigger.sh", "lakehouse_churn_features")]
    assert "graph-build" in make and make["graph-build"]["py"][0][0] == "scripts/build_graph_local.py"
    radar = [s for s in facts["ci"] if s["clones"]]
    assert len(radar) == 1 and [c["urls"] for c in radar[0]["clones"]] == [[RADAR]]
    assert radar[0]["clone_problems"] == [] and not any(s["clone_problems"] for s in facts["ci"])
    assert any(s["make"] == ["churn-gold-local"] for s in facts["ci"])
    dags = {d["dag_id"]: d for d in facts["dags"]}
    assert set(dags) >= {"lakehouse_churn_features", "lakehouse_retail_medallion"}
    assert dags["lakehouse_churn_features"]["edges"] == [("bronze", "silver"), ("silver", "gold_features"),
                                                         ("gold_features", "export_features")]


def test_contract_constants_and_checks(facts):
    c = facts["export_contract"]
    assert len(c["train_columns"]) == 25 and len(c["ranges"]) == 18 and "cancel_at_period_end" in c["leaky"]
    assert len(c["checks"]) == 13 and {k["severity"] for k in c["checks"]} == {"error", "warn"}
    assert c["path_vars"]["train_path"].endswith("/churn_user_features.csv")
    p = facts["parity_contract"]
    assert p["columns"][-2:] == ["outcome", "route"] and len(p["columns"]) == 27 and p["atol"] == 1e-4
    params = facts["parameters"]
    assert params["twin"]["as_of_offset_days"] == params["generator"]["as_of_offset_days"] == 7


def test_make_invocations_are_recognised_only_at_a_command_position():
    found = ex.MAKE_COMMAND.findall(
        "          make churn-gold-local\n"
        "          GRAPH_PY=python make graph-local PROFILE=s42\n"
        "          cd repo && make GRAPH_PY=python graph-check; make -s graph-clean\n"
        "        run: make graph-venv\n"
        '          echo "make sure the venv exists, then run make graph-build"\n')
    assert found == ["churn-gold-local", "graph-local", "graph-check", "graph-clean", "graph-venv"]
    assert ex.SUB_MAKE.findall("@$(MAKE) --no-print-directory graph-build ; $(MAKE) PROFILE=tiny graph-check") == [
        "graph-build", "graph-check"]
    runs = ex.command_runs("$(GRAPH_PY) scripts/build_graph_local.py sample --profile x && "
                           "./pipelines/airflow_trigger.sh lakehouse_churn_features; python -m lakehouse_graph.store")
    assert runs["py"] == [("scripts/build_graph_local.py", "sample")]
    assert runs["sh"] == [("pipelines/airflow_trigger.sh", "lakehouse_churn_features")]
    assert runs["modules"] == ["lakehouse_graph.store"]


# --------------------------------------------------------------------------- CI clones (shell variables)
def _ci_line(text: str) -> int:
    """1-based line of ci.yml holding ``text`` (exactly once)."""
    hits = [i for i, line in enumerate((REPO / spec.CI_WORKFLOW).read_text().splitlines(), 1) if text in line]
    assert len(hits) == 1, (text, hits)
    return hits[0]


def test_ci_clone_url_and_both_candidate_refs_resolve_from_the_step(facts):
    """d317368: URL=... and REF chosen by if / else in the same run: block, then git clone "$REF" "$URL"."""
    step = next(s for s in facts["ci"] if s["name"] == RADAR_STEP)
    (clone,) = step["clones"]
    assert clone["urls"] == [RADAR] and clone["url_expr"] == '"$URL"' and clone["problem"] is None
    assert clone["refs"] == ["${GITHUB_HEAD_REF:-${GITHUB_REF_NAME}}", "main"] and clone["ref_expr"] == '"$REF"'
    assert clone["depth"] == 1 and clone["notes"] == [] and clone["line"] == _ci_line("git clone")


def test_ci_clone_is_a_downstream_repo_with_one_clones_edge_per_candidate_ref(graph):
    sid = "ci:churn-gold-local#" + RADAR_STEP.lower().replace(" ", "-")
    rid = "repo:github.com/santoshshinde2012/retention-radar"
    assert graph.unresolved == [] and graph.ids("DownstreamRepo") == [rid]
    assert graph.props(rid)["url"] == RADAR and graph.props(rid)["name"] == "retention-radar"
    clones = sorted((e["props"]["ref"], e["props"]["n_refs"], e["props"]["depth"], e["props"]["ref_expr"],
                     e["props"]["source"]) for e in graph.out(sid, "CLONES"))
    where = f"{spec.CI_WORKFLOW}:{_ci_line('git clone')}"
    assert clones == [("${GITHUB_HEAD_REF:-${GITHUB_REF_NAME}}", 2, 1, '"$REF"', where),
                      ("main", 2, 1, '"$REF"', where)]
    assert sorted(e["dst"] for e in graph.out(rid, "CONSUMES")) == ["export:data/export/churn_user_features.csv",
                                                                    "export:data/export/hero_inference_record.json"]
    # the cloned repo's own script is not ours to resolve (it ran inside the clone)
    assert not any("sync_lakehouse_exports" in u["what"] for u in graph.unresolved)


def _clones(script: str, env: dict | None = None) -> list[dict]:
    return ex.shell_clones(ex.script_lines(list(enumerate(script.splitlines(), 1))), env)


@pytest.mark.parametrize("script, env, urls, refs", [
    ("git clone https://github.com/o/r.git dir", None, ["https://github.com/o/r"], ["default branch"]),
    ('URL=https://github.com/o/r.git\ngit clone --depth 1 "$URL" "$TMP/r"', None, ["https://github.com/o/r"],
     ["default branch"]),
    ("export URL='https://github.com/o/r'\ngit clone ${URL}", None, ["https://github.com/o/r"], ["default branch"]),
    ('git clone "$URL"', {"URL": "https://gitlab.com/g/p.git"}, ["https://gitlab.com/g/p"], ["default branch"]),
    ("git -C /tmp clone --branch=v1.2 git@github.com:o/r.git", None, ["git@github.com:o/r"], ["v1.2"]),
    ('if x; then URL=https://a.example/r; else URL=https://b.example/r; fi\ngit clone -b main "$URL" >/dev/null 2>&1',
     None, ["https://a.example/r", "https://b.example/r"], ["main"]),
    ('B="${GITHUB_HEAD_REF:-main}"\ngit clone --branch "$B" \\\n  https://github.com/o/r', None,
     ["https://github.com/o/r"], ["${GITHUB_HEAD_REF:-main}"]),
    ("REF=${{ github.head_ref }}\ngit clone --branch \"$REF\" https://github.com/o/r", None,   # a run-time value
     ["https://github.com/o/r"], ["${{ github.head_ref }}"]),
    # ${VAR:-default} of a VAR assigned nowhere is its default; an assigned VAR wins over the default
    ('git clone --branch "${BR:-main}" "${URL:-https://github.com/x/y}"', None, ["https://github.com/x/y"], ["main"]),
    ('URL=https://github.com/a/b\ngit clone "${URL:-https://github.com/x/y}"', None, ["https://github.com/a/b"],
     ["default branch"]),
    ('git clone "${URL-${BASE:=https://github.com}/o/r}"', None, ["https://github.com/o/r"], ["default branch"]),
    # every word of a for loop is a candidate
    ('for R in radar "other-repo"; do git clone "https://github.com/o/$R"; done', None,
     ["https://github.com/o/radar", "https://github.com/o/other-repo"], ["default branch"]),
    # the env: of the job / workflow (merged by extract_ci) counts as assigned
    ('git clone -b "$B" "$URL"', {"URL": "https://github.com/o/r", "B": "'release'"}, ["https://github.com/o/r"],
     ["release"]),
    # a command substitution elsewhere on the line does not split or hide the clone
    ('D="$(mktemp -d)"; git clone --depth "$((1 + 0))" https://github.com/o/r "$D/r $(date +%s)"', None,
     ["https://github.com/o/r"], ["default branch"]),
    # gh repo clone: OWNER/REPO on github.com, HOST/OWNER/REPO, a URL; git flags after --
    ("gh repo clone o/r", None, ["https://github.com/o/r"], ["default branch"]),
    ('gh repo clone -u up ghe.example.com/o/r "$D" -- --depth 1 --branch dev', None, ["https://ghe.example.com/o/r"],
     ["dev"]),
    ('R=https://github.com/o/r.git\ngh repo clone "$R"', None, ["https://github.com/o/r"], ["default branch"]),
    ("export GH_HOST=git.corp.example\ngh repo clone o/r", None, ["https://git.corp.example/o/r"], ["default branch"]),
])
def test_shell_clone_forms_the_extractor_resolves(script, env, urls, refs):
    (clone,) = _clones(script, env)
    assert (clone["urls"], clone["refs"], clone["problem"], clone["notes"]) == (urls, refs, None, [])


@pytest.mark.parametrize("script, problem", [
    ('git clone --depth 1 "$URL" dest', "URL is not assigned earlier in this run: block"),
    ('git clone "$URL"\nURL=https://github.com/o/r', "URL is not assigned earlier"),   # assigned only afterwards
    ("URL='$HOME/r'\ngit clone \"$URL\"", "resolves to $HOME/r, which is not a remote repository URL"),
    ("URL=$(cat repo.txt)\ngit clone \"$URL\"",
     "resolves to $(cat repo.txt), which is not a remote repository URL (a command substitution: only the step"),
    ("URL=`cat repo.txt`\ngit clone \"$URL\"", "resolves to `cat repo.txt`, which is not a remote repository URL (a "
                                                "command substitution"),
    ("URL='$(cat repo.txt)'\ngit clone \"$URL\"",   # quoted: a literal string, not a substitution
     "resolves to $(cat repo.txt), which is not a remote repository URL"),
    ('git clone "${URL:?set URL}"', "URL is not assigned earlier in this run: block or in an env: of the step, job"),
    ('git clone "${URL:-$OTHER}"', "OTHER is not assigned earlier"),   # the default needs OTHER
    ('git clone "https://github.com/$GITHUB_REPOSITORY"', "GITHUB_REPOSITORY is set by the CI runner, so the "
                                                         "repository is only known when the workflow runs"),
    ('for R; do git clone "https://github.com/o/$R"; done', "resolves to https://github.com/o/$@, which is not a"),
    ("git clone $(cat repo.txt", "does not tokenise (unbalanced $( command substitution)"),
    ('git clone "$(cat repo.txt"', "does not tokenise (unbalanced \" quote)"),
    ("gh repo clone radar", "resolves to radar, which is not a remote repository URL (a REPO without OWNER/ is the "
                            "authenticated user's"),
    ("gh repo clone", "no repository argument"),
    ('gh repo clone "${{ vars.REPO }}"', "(a GitHub Actions expression: only the workflow run knows its value)"),
    ("git clone ../radar", "resolves to ../radar, which is not a remote repository URL"),
    ('URL=${{ vars.RADAR_URL }}\ngit clone "$URL"', "resolves to ${{ vars.RADAR_URL }}, which is not a remote"),
    ("git clone --depth 1", "no repository argument"),
    ('git clone "https://github.com/o/r', "does not tokenise"),
])
def test_a_clone_that_does_not_resolve_is_kept_with_a_precise_problem(script, problem):
    (clone,) = _clones(script)
    assert clone["urls"] == [] and problem in clone["problem"], clone


def test_shell_words_keep_quotes_escapes_and_command_substitutions_together():
    assert ex._sh_split('a "b c" \'d e\' f\\ g $(h "i j" $(k l)) `m n` "$(o p)" q\\"r') == \
        ['a', '"b c"', "'d e'", 'f\\ g', '$(h "i j" $(k l))', '`m n`', '"$(o p)"', 'q\\"r']
    with pytest.raises(ValueError, match="unbalanced ` quote"):
        ex._sh_split("a `b")


def test_every_clone_of_a_line_is_found_in_order_with_its_tool():
    found = _clones('git clone https://a.example/r && gh repo clone o/r; git -C x clone git@h.example:o/s.git')
    assert [(c["tool"], c["urls"]) for c in found] == [("git clone", ["https://a.example/r"]),
                                                      ("gh repo clone", ["https://github.com/o/r"]),
                                                      ("git clone", ["git@h.example:o/s"])]


def test_a_ref_from_a_variable_nobody_sets_is_recorded_as_written_with_a_note():
    (clone,) = _clones('git clone --branch "$MY_REF" https://github.com/o/r')
    assert clone["refs"] == ["$MY_REF"] and clone["urls"] == ["https://github.com/o/r"]
    assert clone["notes"] == ['--branch "$MY_REF": MY_REF is not assigned in this run: block or in an env: of the '
                              'step, job or workflow, and is not set by the CI runner; the ref is recorded as written']
    (runner,) = _clones('git clone --branch "${GITHUB_HEAD_REF:-$GITHUB_REF_NAME}" https://github.com/o/r')
    assert runner["notes"] == [] and runner["refs"] == ["${GITHUB_HEAD_REF:-$GITHUB_REF_NAME}"]
    (computed,) = _clones('git clone --branch "$(git describe --tags)" https://github.com/o/r')
    assert computed["refs"] == ["$(git describe --tags)"] and computed["notes"] == [
        '--branch "$(git describe --tags)": a command substitution (only the step knows its output); the ref is '
        'recorded as written']


def test_an_unresolved_ci_clone_is_unresolved_never_dropped(tmp_path):
    root = fake_repo(tmp_path)
    edit(root, spec.CI_WORKFLOW, "          URL=https://github.com/santoshshinde2012/retention-radar.git\n", "")
    g = lg.assemble(root)
    line = _ci_line("git clone") - 1   # one line was removed above it
    assert g.unresolved == [{"where": f"{spec.CI_WORKFLOW}:{line}",
                             "what": 'git clone of "$URL": URL is not assigned earlier in this run: block or in an '
                                     "env: of the step, job or workflow (a repository resolves from a literal URL, "
                                     "from VAR=<url> or a for loop before the clone, or from the default of "
                                     "${VAR:-<url>})"}]
    assert g.ids("DownstreamRepo") == [] and not any(e["rel"] == "CLONES" for e in g.edges)


def test_the_url_from_a_job_or_workflow_env_resolves_and_the_job_env_wins(tmp_path):
    root = fake_repo(tmp_path)
    edit(root, spec.CI_WORKFLOW, "          URL=https://github.com/santoshshinde2012/retention-radar.git\n", "")
    edit(root, spec.CI_WORKFLOW, "jobs:\n", "env:\n  URL: https://github.com/elsewhere/radar.git  # overridden\n\n"
                                            "jobs:\n")
    edit(root, spec.CI_WORKFLOW, "  churn-gold-local:\n",
         "  churn-gold-local:\n    env:\n      URL: \"https://github.com/santoshshinde2012/retention-radar\"\n")
    g = lg.assemble(root)
    assert g.unresolved == [] and g.ids("DownstreamRepo") == ["repo:github.com/santoshshinde2012/retention-radar"]
    workflow_env, job_env = ex.ci_env_blocks((root / spec.CI_WORKFLOW).read_text().splitlines())
    assert workflow_env == {"URL": "https://github.com/elsewhere/radar.git"}
    assert job_env["churn-gold-local"] == {"URL": "https://github.com/santoshshinde2012/retention-radar"}


def test_an_actions_checkout_of_another_repository_is_a_clone(tmp_path):
    root = fake_repo(tmp_path)
    edit(root, spec.CI_WORKFLOW, "      - name: Set up Python 3.12\n",
         "      - name: Check out the consumer\n        uses: actions/checkout@v4\n        with:\n"
         "          repository: santoshshinde2012/retention-radar   # the consumer\n"
         "          ref: ${{ github.head_ref }}\n          fetch-depth: 0\n          path: radar\n\n"
         "      - uses: actions/checkout@v4\n        with:\n          repository: ${{ github.repository }}\n\n"
         "      - name: Set up Python 3.12\n")
    g = lg.assemble(root)
    sid = "ci:churn-gold-local#check-out-the-consumer"
    assert g.unresolved == [] and g.has(sid)
    (edge,) = g.out(sid, "CLONES")
    line = next(i for i, x in enumerate((root / spec.CI_WORKFLOW).read_text().splitlines(), 1) if "repository: s" in x)
    assert edge["dst"] == "repo:github.com/santoshshinde2012/retention-radar"
    assert {k: edge["props"][k] for k in ("ref", "ref_expr", "n_refs", "depth", "url_expr", "source")} == {
        "ref": "${{ github.head_ref }}", "ref_expr": "${{ github.head_ref }}", "n_refs": 1, "depth": None,
        "url_expr": "santoshshinde2012/retention-radar", "source": f"{spec.CI_WORKFLOW}:{line}"}
    # the radar step still clones it too (one DownstreamRepo, two steps)
    assert len([e for e in g.edges if e["rel"] == "CLONES"]) == 3
    (own,) = [s for s in ex.extract_ci(ex.SourceFiles(root)) if s["name"] == "Check out the consumer"]
    assert own["clones"][0]["tool"] == "actions/checkout"
    block = [(1, "      - uses: actions/checkout@v4"), (2, "        with:"),
             (3, "          repository: ${{ vars.CONSUMER }}")]
    bad = ex.checkout_clone(block)
    assert bad["urls"] == [] and bad["problem"] == ("actions/checkout of repository: ${{ vars.CONSUMER }}: not "
                                                    "OWNER/REPO on a known host (a GitHub Actions expression: only "
                                                    "the workflow run knows its value)")
    assert ex.checkout_clone(block[:1]) is None   # the workflow's own repository


def test_the_literal_clone_of_3efe31a_still_resolves(tmp_path):
    root = fake_repo(tmp_path)
    path = root / spec.CI_WORKFLOW
    text, n = re.subn(r"          URL=.*?\n(?:.*\n)*?          git clone [^\n]*\n",
                      '          git clone --depth 1 https://github.com/santoshshinde2012/retention-radar.git '
                      '"$RUNNER_TEMP/retention-radar"\n', path.read_text(), count=1)
    assert n == 1
    path.write_text(text)
    g = lg.assemble(root)
    rid = "repo:github.com/santoshshinde2012/retention-radar"
    assert g.unresolved == [] and g.ids("DownstreamRepo") == [rid]
    assert [(e["props"]["ref"], e["props"]["n_refs"], e["props"]["depth"]) for e in g.edges
            if e["rel"] == "CLONES"] == [("default branch", 1, 1)]


def test_sql_the_parser_keeps_as_a_command_still_yields_its_table_names():
    reads, writes, _ddl = ex.sql_tables("ALTER TABLE lakehouse.gold.churn_renewal_features CREATE TAG `graph_0`")
    assert (reads, writes) == (set(), {("lakehouse.gold.churn_renewal_features", "ALTER")})
    assert ex.sql_tables("CALL lakehouse.system.expire_snapshots('gold.x')")[:2] == (set(), set())
    reads, writes, _ddl = ex.sql_tables("SELECT * FROM lakehouse.silver.orders.snapshots")
    assert reads == {"lakehouse.silver.orders#snapshots"} and not writes
    assert ex.sql_tables("this is not sql (((") is None


# --------------------------------------------------------------------------- scope walk
def test_pit_rule_is_read_from_the_sql(gold_lineage):
    rule = gold_lineage.pit_rule((REPO / spec.GOLD_SQL).read_text())
    assert rule["as_of_expr"] == "snapshot_date" and rule["renewal_expr"] == "current_period_end"
    assert rule["as_of_offset_days"] == -7 and rule["source_line"] is not None
    assert gold_lineage.anchor_offset == {"as_of": 0, "renewal_date": 7}


def test_every_gold_sql_column_resolves_with_roles_and_windows(gold_lineage):
    cols = {c.name: c for c in gold_lineage.columns()}
    assert len(cols) == 30 and list(cols)[:3] == ["user_id", "user_name", "plan_tier"]
    assert all(c.leaves for c in cols.values())
    hits = {(x.column, x.role): x for x in cols["limit_hits_14d"].leaves}
    assert hits[("*", "ROWCOUNT")].window == (-14.0, False, 0.0, True)       # (as_of-14, as_of]
    assert hits[("hit_date", "WINDOW_BOUND")].cte == "hits"
    assert ("hit_at", "VALUE") not in hits and not any(x.column == "hit_at" for x in cols["limit_hits_14d"].leaves)
    completed = next(x for x in cols["renewals_completed"].leaves if x.column == "*")
    assert sw.format_window(completed.window) == "(-inf, as_of+7)"            # invoice_date < renewal_date
    first = [x for x in cols["first_renewal_after_pricing_change"].leaves if x.column == "effective_date"]
    assert {sw.format_window(x.window) for x in first} == {"[as_of-23, as_of+7)"}
    assert {x.role for x in first} == {"VALUE", "WINDOW_BOUND"}   # the CASE bound keeps its event column
    cuts = [x for x in cols["allowance_used_pct"].leaves if x.column == "effective_date"]
    assert [(x.role, sw.format_window(x.window)) for x in cuts] == [("WINDOW_BOUND", "(-inf, as_of]")]
    incidents = [x for x in cols["incident_exposed_28d"].leaves if x.table.endswith("churn_incidents")]
    assert {x.column for x in incidents} == {"starts_on", "ends_on"}
    assert all(x.window is None and sw.format_window(x.matched) == "(as_of-28, as_of]" for x in incidents)
    prev = {sw.format_window(x.window) for x in cols["accept_rate_change"].leaves if x.role == "VALUE"}
    assert prev == {"(as_of-28, as_of]", "(as_of-56, as_of-28]"}
    # sqlglot's own lineage() has no source for COUNT(*): the scope walk is what resolves it
    assert cols["limit_hits_14d"].sqlglot_leaves == [] and cols["limit_hits_14d"].sqlglot_unresolved


def test_window_formatting():
    assert sw.format_window(None) == "unbounded"
    assert sw.format_window((-28.0, False, 0.0, True)) == "(as_of-28, as_of]"
    assert sw.format_window((-sw.INF, False, 7.0, False)) == "(-inf, as_of+7)"
    assert sw.format_bounds((("eq", 7.0, True), ("hi", 0.0, True))) == "eq= as_of+7;hi= as_of"
    # queries.window_upper reads the upper bound back from the display (the oracle's recomputation)
    for w in ((-28.0, False, 0.0, True), (-sw.INF, False, 7.0, False), (-23.0, True, 7.0, False),
              (-56.0, False, -28.0, True), (-14.0, False, sw.INF, False)):
        assert queries.window_upper(sw.format_window(w)) == w[2], w
    assert queries.window_upper("unbounded") is None and queries.window_upper(None) is None
    with pytest.raises(ValueError):
        queries.window_upper("(yesterday, today]")
    dim = next(iter(spec.GLOBAL_DIMENSION_TABLES))
    assert queries.read_upper(None, None, dim) is None                      # a column of the as-of row
    assert queries.read_upper("(as_of-7, as_of]", None, "t") == 0
    assert queries.read_upper("unbounded", None, dim) == sw.INF             # declared, but not matched
    assert queries.read_upper("unbounded", "(as_of-28, as_of]", "lakehouse.silver.other") == sw.INF   # not declared
    assert queries.read_upper("unbounded", "(as_of-28, as_of]", dim) == 0
    assert queries.read_upper("unbounded", "(as_of-28, as_of+7)", dim) == 7


def test_bounds_intersect_and_the_exclusive_bound_wins():
    assert sw.intersect([]) is None
    assert sw.intersect([("lo", -28.0, False), ("hi", 0.0, True)]) == (-28.0, False, 0.0, True)
    assert sw.intersect([("hi", 0.0, True), ("hi", 3.0, True)]) == (-sw.INF, False, 0.0, True)      # the tighter
    assert sw.intersect([("hi", 0.0, True), ("hi", 0.0, False)]) == (-sw.INF, False, 0.0, False)
    assert sw.intersect([("hi", 0.0, False), ("hi", 0.0, True)]) == (-sw.INF, False, 0.0, False)
    assert sw.intersect([("eq", 7.0, True)]) == (7.0, True, 7.0, True)
    assert sw.intersect([("lo", -14.0, False)]) == (-14.0, False, sw.INF, False)                   # no upper bound


def test_a_case_branch_is_a_bound_only_when_failing_rows_contribute_nothing():
    one, zero, five = (exp.Literal.number(n) for n in (1, 0, 5))
    null = exp.Null()
    column = exp.column("x", "t")
    for agg in (exp.Sum, exp.Max, exp.Min, exp.Count, exp.Avg):
        assert sw.neutral_default(agg(this=column), one, None) and sw.neutral_default(agg(this=column), one, null)
        assert not sw.neutral_default(agg(this=column), one, five)
    assert sw.neutral_default(exp.Sum(this=column), column, zero)
    assert sw.neutral_default(exp.Max(this=column), one, zero)       # the 0 / 1 indicator
    assert not sw.neutral_default(exp.Max(this=column), column, zero)   # a value that may be negative
    assert not sw.neutral_default(exp.Min(this=column), one, zero)      # MIN(... ELSE 0) sees the failing rows
    assert sw.neutral_default(exp.Min(this=column), exp.Literal.number(-1), zero)
    assert not sw.neutral_default(exp.Count(this=column), one, zero)    # COUNT counts them
    assert not sw.neutral_default(exp.Avg(this=column), one, zero)      # AVG divides by them


# --------------------------------------------------------------------------- the assembled graph
def test_core_graph_has_no_unresolved_name_and_the_plan_sub_graph(graph):
    assert graph.unresolved == [] and graph.profile == "core"
    derived = [e for e in graph.edges if e["rel"] == "DERIVED_FROM" and e["src"].startswith(GOLD)]
    rowcounts = [e for e in graph.edges if e["rel"] == "COUNTS_ROWS_OF" and e["src"].startswith(GOLD)]
    assert (len(derived), len({e["dst"] for e in derived}), len(rowcounts)) == (137, 38, 3)
    assert {e["src"].split("#")[1] for e in rowcounts} == {"renewals_completed", "limit_hits_14d",
                                                           "support_tickets_90d"}
    c = graph.counts()
    assert c["nodes"]["DataColumn"] == 322 and c["nodes"]["GraphElement"] == 21
    assert c["nodes"]["FileSnapshot"] == 0 and c["nodes"]["Snapshot"] == c["nodes"]["Ref"] == c["nodes"]["Run"] == 0
    assert not [i for i in graph.ids("Contract") if i.startswith("contract:radar_")]


def test_core_reads_no_data_file_and_ignores_radar_and_exports(graph, tmp_path, monkeypatch):
    assert not [k for k in graph.inputs if k.startswith(("data/sample/churn/", "data/export/", "data:", "radar:"))]
    assert {spec.GOLD_SQL, spec.MAKEFILE, spec.CI_WORKFLOW, spec.README, spec.GRAPH_SPEC} <= set(graph.inputs)
    monkeypatch.setenv("RADAR_DIR", str(tmp_path))
    again = lg.assemble(REPO, "core", export_dir=REPO / "data/export", radar_dir=tmp_path)
    assert again.counts() == graph.counts() and again.inputs == graph.inputs


def test_the_graph_code_itself_is_lineage(graph):
    for rel in (spec.GRAPH_BUILD_SCRIPT, spec.GRAPH_CONTRACT, spec.LINEAGE_BUILD_SCRIPT, spec.LINEAGE_CONTRACT):
        assert graph.has(f"job:{rel}"), rel
    for cid in ("contract:graph_contract", "contract:lineage_contract", "contract:churn_export",
                "contract:gold_parity"):
        assert graph.props(cid)["modelled"] is True, cid
    tables = [i for i in graph.ids("Dataset") if i.startswith(f"ds:{spec.GRAPH_PARQUET_DIR}/")]
    assert len(tables) == len(gspec.NODE_SCHEMA) + len(gspec.EDGE_SCHEMA) == 21
    assert graph.has("ds:graph/parquet/edges_SIMILAR_TO.parquet")
    # every graph table has exactly one Iceberg twin (planned by the naming convention, or published by
    # the Spark graph job: which one depends on the repo, the fake-repo tests below pin each case)
    mirrored = Counter(e["dst"] for e in graph.edges if e["rel"] == "MIRRORS" and e["dst"] in tables)
    assert mirrored == dict.fromkeys(tables, 1)
    assert all(e["src"].startswith(f"ds:{spec.GRAPH_TWIN_PREFIX}") for e in graph.edges
               if e["rel"] == "MIRRORS" and e["dst"] in tables)
    assert not [w for w in graph.warnings if "Iceberg twin" in w or "partition" in w], graph.warnings
    if graph.has(f"job:{spec.GRAPH_SPARK_JOB}"):   # the lakehouse-native twin of the builder
        assert [e["dst"] for e in graph.out(f"job:{spec.GRAPH_BUILD_SCRIPT}", "PARITY_TWIN_OF")] == \
            [f"job:{spec.GRAPH_SPARK_JOB}"]
    writes = {e["dst"] for e in graph.out(f"job:{spec.GRAPH_BUILD_SCRIPT}", "WRITES")}
    assert set(tables) <= writes and "ds:graph/graph.lbdb" in writes
    assert {e["dst"] for e in graph.out(f"job:{spec.LINEAGE_BUILD_SCRIPT}", "WRITES")} == \
        {"ds:graph/lineage", "ds:graph/lineage.lbdb"}
    imports = {e["dst"] for e in graph.out(f"job:{spec.GRAPH_BUILD_SCRIPT}", "IMPORTS")}
    assert imports == {f"job:{spec.PANDAS_TWIN}"}


def test_bridge_covers_all_21_business_types_with_sources(graph):
    elements = graph.ids("GraphElement")
    want = {f"ge:node:{n}" for n in gspec.NODE_SCHEMA} | {f"ge:edge:{r}" for r in gspec.EDGE_SCHEMA}
    assert set(elements) == want and len(want) == 21
    for ge in elements:
        targets = [graph.label(e["dst"]) for e in graph.out(ge, "SOURCED_FROM")]
        assert "Dataset" in targets and "DataColumn" in targets, ge
        layers = {graph.props(e["dst"])["layer"] for e in graph.out(ge, "SOURCED_FROM")}
        assert layers <= {"silver", "gold"}, (ge, layers)
        assert len(graph.out(ge, "MATERIALIZED_AS")) == 1, ge
    sim = {graph.props(e["dst"])["name"] for e in graph.out("ge:edge:SIMILAR_TO", "SOURCED_FROM")
           if graph.label(e["dst"]) == "DataColumn"}
    assert set(gspec.FEATURES) | {"plan_tier", "route"} <= sim and "agent_requests_28d" not in sim
    assert graph.props("ge:edge:HIT_LIMIT")["feeds_feature"] == "limit_hits_14d"
    assert graph.props("ge:edge:HIT_LIMIT")["pit_window_days"] == 14


def test_feature_cards_agree_with_lineage_pit_status(graph):
    """FEATURE_CARDS (the metrics toolset's source) and the SQL-derived lineage say the same thing."""
    status = {graph.props(i)["name"]: graph.props(i)["pit_status"] for i in graph.ids("DataColumn")
              if i.startswith(GOLD) and graph.props(i)["role"] == "feature"}
    assert set(status) == set(gspec.FEATURE_CARDS) == set(gspec.GOLD_FEATURES) and len(status) == 22
    assert status == {f: c["pit_status"] for f, c in gspec.FEATURE_CARDS.items()}
    assert sorted(f for f, s in status.items() if s != spec.PIT_COMPLIANT) == [
        "first_renewal_after_pricing_change", "renewals_completed"]
    verified = {f for f, c in gspec.FEATURE_CARDS.items() if c["verification"] == "graph-verified"}
    assert {i.split("pit_parity:")[1] for i in graph.ids("Assertion") if "pit_parity:" in i} == verified


# --------------------------------------------------------------------------- fail loudly
def test_missing_required_file_stops_the_extraction(tmp_path):
    root = fake_repo(tmp_path)
    (root / spec.GOLD_SQL).unlink()
    with pytest.raises(LineageExtractError, match="required source file"):
        lg.assemble(root)


def test_gold_sql_reading_an_unknown_silver_column_is_an_error(tmp_path):
    root = fake_repo(tmp_path)
    edit(root, spec.GOLD_SQL, "l.hit_date > date_sub(r.as_of, 14)", "l.hit_day > date_sub(r.as_of, 14)")
    with pytest.raises(LineageExtractError, match="qualify"):
        lg.assemble(root)


def test_bronze_dict_that_is_no_longer_a_literal_is_an_error(tmp_path):
    root = fake_repo(tmp_path)
    edit(root, spec.BRONZE_JOB, "BRONZE = {", "BRONZE = make_tables() or {")
    with pytest.raises(LineageExtractError, match="BRONZE"):
        lg.assemble(root)


def test_unevaluable_table_name_in_a_spark_job_is_unresolved(tmp_path):
    root = fake_repo(tmp_path)
    (root / "src/jobs/churn/05_extra.py").write_text(
        '"""Extra job."""\nfrom pyspark.sql import SparkSession\n\n\ndef main(name):\n'
        '    spark = SparkSession.builder.appName("churn_05_extra").getOrCreate()\n'
        '    spark.table(name).count()\n')
    g = lg.assemble(root)
    assert [u for u in g.unresolved if u["where"].startswith("src/jobs/churn/05_extra.py")
            and "table name" in u["what"]], g.unresolved


def no_graph_jobs(root: Path) -> Path:
    """An empty src/jobs/graph in the scratch copy, whatever Spark graph job the repo has now."""
    jobs = root / "src/jobs/graph"
    shutil.rmtree(jobs, ignore_errors=True)
    jobs.mkdir(parents=True)
    return jobs


def no_graph_runners(root: Path) -> None:
    """Remove what runs the Spark graph job from the scratch copy (the graph DAG, its helper module,
    the e2e pipeline and any Makefile recipe line calling that pipeline): the repo as it was before
    the lakehouse-native twin. Without the job they would run a missing file."""
    for rel in (GRAPH_DAG, GRAPH_DAG_HELPERS, GRAPH_E2E):
        (root / rel).unlink(missing_ok=True)
    make = root / spec.MAKEFILE
    make.write_text("".join(line for line in make.read_text().splitlines(keepends=True)
                            if Path(GRAPH_E2E).name not in line))


def own_graph_sql(root: Path, *names: str) -> None:
    """sql/graph of the scratch copy holds only these files (one plain SELECT each)."""
    sql = root / "sql/graph"
    shutil.rmtree(sql, ignore_errors=True)
    sql.mkdir(parents=True)
    for name in names:
        (sql / name).write_text("SELECT subscription_id FROM $silver.churn_subscription_snapshots;\n")


def test_without_a_spark_graph_job_the_twins_are_the_planned_conventional_ones(tmp_path):
    root = fake_repo(tmp_path)
    no_graph_jobs(root)
    no_graph_runners(root)   # a repo before the lakehouse-native twin: no job, nothing that runs it
    g = lg.assemble(root)
    assert g.unresolved == [] and g.warnings == [], (g.unresolved, g.warnings)
    twins = [i for i in g.ids("Dataset") if i.startswith(f"ds:{spec.GRAPH_TWIN_PREFIX}")]
    assert len(twins) == 21 and {g.props(t)["status"] for t in twins} == {"planned"}
    assert g.has("ds:lakehouse.gold.graph_nodes_renewal") and g.has("ds:lakehouse.gold.graph_edges_similar_to")
    assert not g.has(f"ds:{spec.GRAPH_TWIN_PREFIX}similar_to_scaler")   # the scaler twin is never planned
    assert {e["props"].get("partition") for e in g.edges if e["rel"] == "MIRRORS"} == {None}
    assert not [e for e in g.edges if e["rel"] == "PARITY_TWIN_OF" and e["src"] == f"job:{spec.GRAPH_BUILD_SCRIPT}"]


def test_a_spark_job_its_runners_still_name_is_unresolved_once_per_runner(tmp_path):
    """The graph DAG and the e2e pipeline kept, the Spark graph job deleted: each runner names a
    missing file. One unresolved entry per runner (the core contract fails on it), and the DAG task
    is not also reported as running nothing."""
    root = fake_repo(tmp_path)
    runners = [rel for rel in (GRAPH_DAG, GRAPH_E2E) if (root / rel).is_file()]
    if not runners:
        pytest.skip("nothing in this repo runs the Spark graph job")
    no_graph_jobs(root)
    g = lg.assemble(root)
    assert {u["what"] for u in g.unresolved} == {f"runs {spec.GRAPH_SPARK_JOB}, which does not exist"}, g.unresolved
    assert sorted(u["where"].split(":")[0] for u in g.unresolved) == sorted(runners), g.unresolved
    assert not [w for w in g.warnings if "runs nothing" in w], g.warnings


UNION_JOB = '''"""Publish the graph as two union tables + the scaler + a build manifest."""
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

SQL_DIR = Path(os.environ.get("GRAPH_SQL_DIR", "/opt/sql/graph"))
SQL_FILES = ("nodes.sql", "edges.sql")


def main(nodes, edges, scaler):
    spark = SparkSession.builder.appName("graph_01_publish_gold_graph").getOrCreate()
    spark.table("lakehouse.gold.churn_renewal_features").createOrReplaceGlobalTempView("churn_renewal_features")
    scaler.writeTo("lakehouse.gold.graph_similar_to_scaler").using("iceberg").createOrReplace()
    nodes.writeTo("lakehouse.gold.graph_nodes").using("iceberg").partitionedBy(F.col("label")).createOrReplace()
    edges.writeTo("lakehouse.gold.graph_edges").using("iceberg").partitionedBy("rel_type").createOrReplace()
    spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.gold.graph_build_manifest (build_id STRING) USING iceberg")
'''


def test_union_twins_mirror_every_table_by_partition(tmp_path):
    """gold.graph_nodes / gold.graph_edges hold every node / edge table, one partition per table."""
    root = fake_repo(tmp_path)
    (no_graph_jobs(root) / "01_publish_gold_graph.py").write_text(UNION_JOB)
    own_graph_sql(root, "nodes.sql", "edges.sql")
    g = lg.assemble(root)
    assert g.unresolved == [] and g.warnings == [], (g.unresolved, g.warnings)
    mirrors = {e["dst"]: (e["src"], e["props"]["partition"]) for e in g.edges if e["rel"] == "MIRRORS"
               and e["src"].startswith(f"ds:{spec.GRAPH_TWIN_PREFIX}")}
    assert len(mirrors) == 22   # 21 tables + the scaler
    assert mirrors["ds:graph/parquet/nodes_Renewal.parquet"] == ("ds:lakehouse.gold.graph_nodes", "label = 'Renewal'")
    assert mirrors["ds:graph/parquet/edges_SIMILAR_TO.parquet"] == ("ds:lakehouse.gold.graph_edges",
                                                                    "rel_type = 'SIMILAR_TO'")
    assert mirrors[f"ds:graph/{gspec.SCALER_FILE}"] == ("ds:lakehouse.gold.graph_similar_to_scaler", None)
    assert {src for src, _ in mirrors.values()} == {"ds:lakehouse.gold.graph_nodes", "ds:lakehouse.gold.graph_edges",
                                                    "ds:lakehouse.gold.graph_similar_to_scaler"}
    assert {g.props(src)["status"] for src, _ in mirrors.values()} == {"published"}
    assert g.props("ds:lakehouse.gold.graph_build_manifest")["status"] == "published"   # mirrors nothing
    assert not g.has("ds:lakehouse.gold.graph_nodes_renewal")
    job = f"job:{spec.GRAPH_SPARK_JOB}"
    assert [e["dst"] for e in g.out(job, "EXECUTES_SQL")] == ["sql:sql/graph/nodes.sql", "sql:sql/graph/edges.sql"]
    assert [e["props"]["checked_by"] for e in g.out(f"job:{spec.GRAPH_BUILD_SCRIPT}", "PARITY_TWIN_OF")] == \
        [spec.GRAPH_PARITY]
    assert {e["props"]["verified_by"] for e in g.edges if e["rel"] == "MIRRORS" and e["src"].startswith("ds:")} == \
        {spec.GRAPH_PARITY}


def test_a_union_twin_without_a_partition_column_or_a_sql_file_is_not_silent(tmp_path):
    root = fake_repo(tmp_path)
    (no_graph_jobs(root) / "01_publish_gold_graph.py").write_text(
        UNION_JOB.replace('.partitionedBy("rel_type")', "").replace('"edges.sql")', '"edges.sql", "gone.sql")'))
    own_graph_sql(root, "nodes.sql", "edges.sql")
    g = lg.assemble(root)
    assert sum("holds every edge table but its writer names no single partition column" in w
               for w in g.warnings) == 11, g.warnings
    assert [u["what"] for u in g.unresolved] == [
        "executes ${GRAPH_SQL_DIR:-/opt/sql/graph}/gone.sql (sql/graph/gone.sql), which is not a SQL file of the repo"]


def test_global_temp_views_a_job_binds_itself_are_not_datasets(tmp_path):
    root = fake_repo(tmp_path)
    (root / "scripts/check_views.py").write_text(
        '"""Bind a view, read it back."""\n\n\ndef main(spark, df):\n'
        '    df.createOrReplaceGlobalTempView("churn_renewal_features")\n'
        '    return spark.table("global_temp.churn_renewal_features").count()\n')
    (root / "scripts/read_views.py").write_text(
        '"""Read a view some other application bound."""\n\n\ndef main(spark):\n'
        '    return spark.table("global_temp.churn_renewal_features").count()\n')
    g = lg.assemble(root)
    assert not g.out("job:scripts/check_views.py", "READS")
    assert [u for u in g.unresolved if u["where"] == "scripts/read_views.py"] == [
        {"where": "scripts/read_views.py",
         "what": "table name global_temp.churn_renewal_features is not lakehouse.<namespace>.<table>"}]


def test_table_name_with_a_literal_prefix_matches_known_tables(tmp_path):
    root = fake_repo(tmp_path)
    no_graph_jobs(root)
    (root / "src/jobs/graph/01_publish_gold_graph.py").write_text(
        '"""Publish the graph tables."""\nfrom pyspark.sql import SparkSession\n\nTABLES = load_tables()\n\n\n'
        'def main():\n    spark = SparkSession.builder.appName("graph_01_publish_gold_graph").getOrCreate()\n'
        '    for name, df in TABLES.items():\n'
        '        df.writeTo(f"lakehouse.gold.graph_{name}").createOrReplace()\n')
    g = lg.assemble(root)
    assert g.unresolved == []
    written = {e["dst"] for e in g.out("job:src/jobs/graph/01_publish_gold_graph.py", "WRITES")}
    assert len(written) == 21 and all(w.startswith(f"ds:{spec.GRAPH_TWIN_PREFIX}") for w in written)
    assert g.props("ds:lakehouse.gold.graph_nodes_renewal")["write_mode"] == "createOrReplace"


def test_published_twin_names_replace_the_conventional_ones(tmp_path):
    root = fake_repo(tmp_path)
    no_graph_jobs(root)
    (root / "src/jobs/graph/01_publish_gold_graph.py").write_text(
        '"""Publish two graph tables and tag gold."""\nfrom pyspark.sql import SparkSession\n\n\n'
        'def main(nodes, edges, build_id):\n'
        '    spark = SparkSession.builder.appName("graph_01_publish_gold_graph").getOrCreate()\n'
        '    nodes.writeTo("lakehouse.gold.graph_renewal").createOrReplace()\n'
        '    edges.writeTo("lakehouse.gold.graph_similar_to").createOrReplace()\n'
        '    spark.sql(f"ALTER TABLE lakehouse.gold.churn_renewal_features CREATE TAG `graph_{build_id}`")\n')
    g = lg.assemble(root)
    assert g.unresolved == []
    mirrors = {e["src"]: e["dst"] for e in g.edges if e["rel"] == "MIRRORS" and e["src"].startswith("ds:lakehouse")}
    assert mirrors == {"ds:lakehouse.gold.graph_renewal": "ds:graph/parquet/nodes_Renewal.parquet",
                       "ds:lakehouse.gold.graph_similar_to": "ds:graph/parquet/edges_SIMILAR_TO.parquet"}
    assert g.props("ds:lakehouse.gold.graph_renewal")["status"] == "published"
    assert not g.has("ds:lakehouse.gold.graph_nodes_renewal")
    assert sum("publishes no Iceberg twin" in w for w in g.warnings) == 19
    job = "job:src/jobs/graph/01_publish_gold_graph.py"
    assert {e["dst"]: e["props"]["mode"] for e in g.out(job, "WRITES")} == {
        "ds:lakehouse.gold.graph_renewal": "createOrReplace", "ds:lakehouse.gold.graph_similar_to": "createOrReplace",
        f"ds:{spec.GOLD_TABLE}": "sql:ALTER"}


def test_a_new_sql_file_is_in_the_graph_with_the_tables_it_names(tmp_path):
    root = fake_repo(tmp_path)
    rel = "sql/lineage_probe/renewal_nodes.sql"   # a directory no track creates (sql/graph now exists)
    (root / rel).parent.mkdir(parents=True, exist_ok=True)
    (root / rel).write_text("CREATE OR REPLACE TABLE lakehouse.gold.lineage_probe_renewals AS\n"
                            "SELECT user_id, feature_as_of FROM $gold.churn_renewal_features;\n")
    g = lg.assemble(root)
    sid = f"sql:{rel}"
    assert g.unresolved == [] and g.has(sid) and rel in g.inputs
    assert [e["dst"] for e in g.out(sid, "READS")] == [f"ds:{spec.GOLD_TABLE}"]
    assert [e["dst"] for e in g.out(sid, "DESCRIBES")] == ["ds:lakehouse.gold.lineage_probe_renewals"]


def test_make_ci_and_dag_names_that_do_not_resolve_are_unresolved(tmp_path):
    root = fake_repo(tmp_path)
    with (root / "Makefile").open("a") as f:
        f.write("\nnew-target:\n\tpython3 scripts/does_not_exist.py\n")
    edit(root, spec.CI_WORKFLOW, "          make churn-gold-local\n", "          make no-such-target\n")
    edit(root, "airflow/dags/lakehouse_churn_features.py", '"churn/04_export_features.py"', '"churn/09_gone.py"')
    what = " | ".join(f"{u['where']}: {u['what']}" for u in lg.assemble(root).unresolved)
    assert "scripts/does_not_exist.py, which does not exist" in what
    assert "make no-such-target, which is not a target" in what
    assert "src/jobs/churn/09_gone.py, which does not exist" in what


def dag_runs(g: lg.LineageGraph, dag_id: str) -> list[tuple[str, str, str | None, str | None]]:
    """(task, job or script, args, via) of every RUNS edge of one DAG's tasks."""
    return sorted((e["src"].split(".", 1)[1], e["dst"], e["props"].get("args"), e["props"].get("via"))
                  for e in g.edges if e["rel"] == "RUNS" and e["src"].startswith(f"task:{dag_id}."))


def test_dag_task_helpers_and_their_argument_forms():
    assert ex.command_text("python scripts/x.py build") == "python scripts/x.py build"
    assert ex.command_text(["python", "scripts/x.py", "--root", "a b"]) == "python scripts/x.py --root 'a b'"
    assert ex.command_text(("python", "-m", "lakehouse_graph.store")) == "python -m lakehouse_graph.store"
    assert ex.command_text(["python", None]) is None and ex.command_text([]) is None and ex.command_text(3) is None
    for job in ("graph/01_x.py", "jobs/graph/01_x.py", "/opt/jobs/graph/01_x.py", "opt/jobs/graph/01_x.py"):
        assert ex.spark_job_path(job) == "src/jobs/graph/01_x.py", job
    factories = ex.dag_task_factories(ex.SourceFiles(REPO))
    assert factories["spark_submit_task"] == ["task_id", "job_path"]
    if (REPO / GRAPH_DAG_HELPERS).is_file():
        assert factories["graph_exec_task"][:2] == ["task_id", "command"]
        assert factories["spark_submit_args_task"][:3] == ["task_id", "job_path", "job_args"]


def test_every_dag_task_of_the_repo_runs_a_job_or_a_script(graph):
    tasks = graph.ids("DagTask")
    assert tasks and not [w for w in graph.warnings if "DAG task" in w], graph.warnings
    assert not [t for t in tasks if not graph.out(t, "RUNS")]
    assert dag_runs(graph, "lakehouse_churn_features") == [
        (task, f"job:src/jobs/churn/{job}", None, "spark_submit_task") for task, job in sorted(
            [("bronze", "01_ingest_bronze.py"), ("silver", "02_transform_silver.py"),
             ("gold_features", "03_publish_gold_features.py"), ("export_features", "04_export_features.py")])]
    if not (REPO / GRAPH_DAG).is_file():
        return
    runs = {(task, dst): args for task, dst, args, _via in dag_runs(graph, "lakehouse_graph")}
    assert ("publish_gold_graph", f"job:{spec.GRAPH_SPARK_JOB}") in runs
    assert runs[("build_graph", f"job:{spec.GRAPH_BUILD_SCRIPT}")] == "build"
    assert runs[("promote", f"job:{spec.GRAPH_BUILD_SCRIPT}")] == "promote"
    for task, script in (("check_graph_contract", spec.GRAPH_CONTRACT), ("build_lineage", spec.LINEAGE_BUILD_SCRIPT),
                         ("check_lineage_contract", spec.LINEAGE_CONTRACT)):
        assert (task, f"job:{script}") in runs, (task, sorted(runs))


def test_graph_dag_helpers_are_read_in_every_argument_form_they_accept(tmp_path, graph):
    """graph_exec_task takes a command string or an argv list, by position or keyword, and
    spark_submit_args_task a job path with or without /opt/jobs/: the DAG rewritten into the other
    forms (string <-> argv list, positional -> keyword, /opt/jobs/ prefix) has the same tasks and edges."""
    root = fake_repo(tmp_path)
    if not (root / GRAPH_DAG).is_file():
        pytest.skip(f"{GRAPH_DAG} is not in this repo")
    tree = ast.parse((root / GRAPH_DAG).read_text())
    rewritten: Counter = Counter()
    for call in ast.walk(tree):
        if not isinstance(call, ast.Call) or len(call.args) < 2:
            continue
        name, (task_id, arg) = ast.unparse(call.func), call.args[:2]
        if name == "graph_exec_task" and isinstance(arg, ast.Constant):     # "python scripts/x.py a" -> argv
            other = ast.List([ast.Constant(p) for p in shlex.split(arg.value)], ast.Load())
        elif name == "graph_exec_task" and isinstance(arg, ast.List):       # argv -> "python scripts/x.py a"
            other = ast.Constant(shlex.join(ast.literal_eval(arg)))
        elif name == "spark_submit_args_task" and isinstance(arg, ast.Constant):
            arg.value = f"/opt/jobs/{arg.value.removeprefix('/opt/jobs/')}"
            rewritten[name] += 1
            continue
        else:
            continue
        call.args = call.args[2:]   # graph_exec_task(task_id=..., command=...)
        call.keywords = [ast.keyword("task_id", task_id), ast.keyword("command", other), *call.keywords]
        rewritten[name] += 1
    assert rewritten["graph_exec_task"] >= 1 and rewritten["spark_submit_args_task"] >= 1, rewritten
    (root / GRAPH_DAG).write_text(ast.unparse(ast.fix_missing_locations(tree)) + "\n")
    g = lg.assemble(root)
    assert g.unresolved == [] and not [w for w in g.warnings if "DAG task" in w], (g.unresolved, g.warnings)
    assert dag_runs(g, "lakehouse_graph") == dag_runs(graph, "lakehouse_graph")


def test_dag_task_commands_the_extractor_cannot_read_are_not_silent(tmp_path):
    root = fake_repo(tmp_path)
    (root / "airflow/dags/probe_dag.py").write_text(
        '"""A DAG with one readable and two unreadable tasks."""\nfrom airflow import DAG\n'
        'from lakehouse_operators import spark_submit_task\n\nJOB = "/opt/jobs/churn/04_export_features.py"\n\n'
        'with DAG(dag_id="probe", tags=["probe"]) as dag:\n'
        '    export = spark_submit_task(task_id="export", job_path=JOB)\n'
        '    gone = spark_submit_task("gone", ["churn/09_gone.py"])\n'
        '    mystery = spark_submit_task("mystery", make_job())\n'
        '    export >> gone >> mystery\n')
    g = lg.assemble(root)
    assert dag_runs(g, "probe") == [("export", f"job:{spec.EXPORT_JOB}", None, "spark_submit_task")]
    assert [u["what"] for u in g.unresolved] == ["runs src/jobs/churn/09_gone.py, which does not exist"]
    assert [w for w in g.warnings if "DAG task" in w] == [
        "airflow/dags/probe_dag.py:10: DAG task mystery (spark_submit_task) runs nothing the extractor recognises"]
    assert [(e["src"], e["dst"]) for e in g.edges if e["rel"] == "UPSTREAM_OF" and e["src"].startswith("task:probe")] \
        == [("task:probe.export", "task:probe.gone"), ("task:probe.gone", "task:probe.mystery")]


def test_a_new_unclassified_contract_check_is_unresolved(tmp_path):
    root = fake_repo(tmp_path)
    edit(root, spec.EXPORT_CONTRACT, "    return errors, warnings\n\n\ndef main",
         '    if df["engagement_trend"].sum() < 0:\n        errors.append("negative trend sum")\n'
         "    return errors, warnings\n\n\ndef main")
    g = lg.assemble(root)
    assert any("check not classified" in u["what"] for u in g.unresolved), g.unresolved


def test_a_feature_that_starts_reading_after_as_of_is_an_undeclared_exception(tmp_path):
    root = fake_repo(tmp_path)
    edit(root, spec.GOLD_SQL, "AND l.hit_date <= r.as_of", "AND l.hit_date <= r.renewal_date")
    g = lg.assemble(root)
    col = g.props(GOLD + "limit_hits_14d")
    assert col["pit_status"] == spec.PIT_UNDECLARED_EXCEPTION and col["max_upper_vs_as_of_days"] == 7
    assert col["reads_after_as_of"] is True
    assert g.props(GOLD + "support_tickets_90d")["pit_status"] == spec.PIT_COMPLIANT


# --------------------------------------------------------------------------- point-in-time soundness
# Edits of sql/churn/gold_renewal_features.sql that make a feature read rows after as_of. None may
# leave the feature compliant: each is either an exception (upper bound > as_of) or stops the extraction.
CUTS = "SUM(CASE WHEN p.effective_date <= r.as_of THEN 1 ELSE 0 END) AS cuts_so_far"
LAST = "SELECT r.subscription_id, MAX(u.activity_date) AS last_active"
LAST_ON = "ON u.subscription_id = r.subscription_id AND u.activity_date <= r.as_of"
HITS_ON = "AND l.hit_date > date_sub(r.as_of, 14) AND l.hit_date <= r.as_of"
HITS_TAIL = "   " + HITS_ON + "\n  GROUP BY r.subscription_id\n"
HITS_FROM = "  FROM renewals r JOIN $silver.churn_limit_events l\n"
HITS_SELECT = "  SELECT r.subscription_id, COUNT(*) AS limit_hits_14d\n"
U7_FROM = ("  FROM renewals r JOIN $silver.churn_usage_daily u\n    ON u.subscription_id = r.subscription_id\n"
           "   AND u.activity_date > date_sub(r.as_of, 7) AND u.activity_date <= r.as_of\n")
U7_ON = "    ON u.subscription_id = r.subscription_id\n   AND u.activity_date > date_sub(r.as_of, 7) AND " \
        "u.activity_date <= r.as_of\n"
INCIDENT = ("MAX(CASE WHEN EXISTS(i.windows, w -> u.activity_date BETWEEN w.starts_on AND w.ends_on)\n"
            "                  THEN 1 ELSE 0 END)                                      AS incident_exposed_28d")
RENEWALS = "  FROM $silver.churn_subscription_snapshots\n  WHERE snapshot_date = date_sub(current_period_end, 7)"
LATE_CTE = ("late AS (\n  SELECT r.subscription_id FROM renewals r JOIN $silver.churn_limit_events l\n"
            "    ON l.subscription_id = r.subscription_id AND l.hit_date > r.as_of\n),\nhits AS (\n")
RENEWAL_COLS = "snapshot_date AS as_of, current_period_end AS renewal_date"
AGENT = "CAST(agent_requests AS INT) AS agent_requests_28d"
# plain row filters over the limit events (a CTE, a derived table) that hits reads instead of the table itself
LIM_CTE = ("hits AS (\n", "lim AS (SELECT subscription_id, hit_date, hit_at FROM $silver.churn_limit_events),\n"
                          "hits AS (\n")
FROM_LIM = (HITS_FROM, "  FROM renewals r JOIN lim l\n")


def from_derived(inner: str) -> tuple[str, str]:
    return HITS_FROM, f"  FROM renewals r JOIN ({inner}) l\n"


def renewals_reads(expr: str) -> list[tuple[str, str]]:
    """A renewals CTE that projects ``expr AS extra`` and a feature (agent_requests_28d) that uses it."""
    return [(RENEWAL_COLS, f"{RENEWAL_COLS}, {expr} AS extra"),
            (AGENT, "CAST(agent_requests + datediff(extra, as_of) AS INT) AS agent_requests_28d")]


FILTER = "SELECT subscription_id, hit_date FROM $silver.churn_limit_events"
A, INF = "allowance_used_pct", float("inf")
LEAKY_EDITS = {   # name: (feature, upper bound the walk must report, [(old, new), ...])
    "case_under_not": (A, INF, [(CUTS, "SUM(CASE WHEN NOT (p.effective_date <= r.as_of) THEN 1 ELSE 0 END) AS "
                                       "cuts_so_far")]),
    "case_under_or": (A, INF, [(CUTS, "SUM(CASE WHEN p.effective_date <= r.as_of OR p.effective_date < r.renewal_date "
                                      "THEN 1 ELSE 0 END) AS cuts_so_far")]),
    "two_cases_take_the_loosest": (A, 7, [(CUTS, "SUM(CASE WHEN p.effective_date <= r.as_of THEN 1 ELSE 0 END) + SUM("
                                                 "CASE WHEN p.effective_date < r.renewal_date THEN 1 ELSE 0 END) AS "
                                                 "cuts_so_far")]),
    "column_outside_the_case": (A, INF, [(CUTS, "SUM(CASE WHEN p.effective_date <= r.as_of THEN 1 ELSE 0 END) + "
                                                "COUNT(p.change_id) AS cuts_so_far")]),
    "else_still_contributes": (A, INF, [(CUTS, "SUM(CASE WHEN p.effective_date <= r.as_of THEN 1 ELSE 2 END) AS "
                                               "cuts_so_far")]),
    "else_reads_a_column": (A, INF, [(CUTS, "SUM(CASE WHEN p.effective_date <= r.as_of THEN 1 ELSE "
                                            "length(p.description) END) AS cuts_so_far")]),
    "count_counts_the_else_rows": (A, INF, [(CUTS, "COUNT(CASE WHEN p.effective_date <= r.as_of THEN 1 ELSE 0 END) AS "
                                                   "cuts_so_far")]),
    "second_when": (A, INF, [(CUTS, "SUM(CASE WHEN p.effective_date <= r.as_of THEN 1 WHEN p.effective_date < "
                                    "r.renewal_date THEN 1 ELSE 0 END) AS cuts_so_far")]),
    "case_not_the_aggregate_argument": (A, INF, [(CUTS, "SUM(1 + CASE WHEN p.effective_date <= r.as_of THEN 1 ELSE 0 "
                                                        "END) AS cuts_so_far")]),
    "no_bound_at_all": (A, INF, [(CUTS, "COUNT(p.effective_date) AS cuts_so_far")]),
    "cross_join_counted_without_naming_a_column": (A, INF, [(CUTS, "SUM(1) AS cuts_so_far")]),
    "case_not_then_no_on_bound": ("last_active_days_ago", INF, [
        (LAST, "SELECT r.subscription_id, MAX(CASE WHEN NOT (u.activity_date <= r.as_of) THEN u.activity_date END) "
               "AS last_active"), (LAST_ON, "ON u.subscription_id = r.subscription_id")]),
    "on_bound_at_renewal_date": ("limit_hits_14d", 7, [(HITS_ON, HITS_ON.replace("<= r.as_of", "<= r.renewal_date"))]),
    "on_bound_plus_3_days": ("limit_hits_14d", 3, [
        (HITS_ON, HITS_ON.replace("<= r.as_of", "<= date_add(r.as_of, 3)"))]),
    "on_bound_under_or": ("limit_hits_14d", INF, [(HITS_ON, "AND l.hit_date > date_sub(r.as_of, 14) AND (l.hit_date <= "
                                                           "r.as_of OR l.hit_date <= r.renewal_date)")]),
    "on_bound_dropped": ("limit_hits_14d", INF, [(HITS_ON, "AND l.hit_date > date_sub(r.as_of, 14)")]),
    "on_bound_in_months": ("limit_hits_14d", INF, [
        (HITS_ON, HITS_ON.replace("<= r.as_of", "<= add_months(r.as_of, 1)"))]),
    "bound_moved_to_having": ("limit_hits_14d", INF, [
        (HITS_TAIL, "   AND l.hit_date > date_sub(r.as_of, 14)\n  GROUP BY r.subscription_id, r.as_of\n"
                    "  HAVING MAX(l.hit_date) <= r.as_of\n")]),
    "left_join_preserves_the_event_table": ("active_days_7d", INF, [
        (U7_FROM, "  FROM $silver.churn_usage_daily u LEFT JOIN renewals r\n" + U7_ON)]),
    "full_join_preserves_both_sides": ("active_days_7d", INF, [
        (U7_FROM, "  FROM renewals r FULL JOIN $silver.churn_usage_daily u\n" + U7_ON)]),
    "reference_table_not_matched_to_events": ("incident_exposed_28d", INF, [
        (INCIDENT, "MAX(size(i.windows)) AS incident_exposed_28d")]),
    "reference_table_matched_to_late_events": ("incident_exposed_28d", 7, [
        ("   AND u.activity_date > date_sub(r.as_of, 28) AND u.activity_date <= r.as_of\n  CROSS JOIN",
         "   AND u.activity_date > date_sub(r.as_of, 28) AND u.activity_date <= r.renewal_date\n  CROSS JOIN")]),
    "inner_join_to_a_cte_that_filters_on_late_rows": ("limit_hits_14d", INF, [
        ("hits AS (\n", LATE_CTE),
        (HITS_FROM, "  FROM renewals r JOIN late ON late.subscription_id = r.subscription_id JOIN "
                    "$silver.churn_limit_events l\n")]),
    # a looser WHERE than the CASE is reported too (conservative: the scope does read those rows)
    "where_looser_than_the_case": (A, 7, [
        ("  FROM renewals r CROSS JOIN $silver.churn_pricing_changes p\n  GROUP BY r.subscription_id",
         "  FROM renewals r CROSS JOIN $silver.churn_pricing_changes p\n  WHERE p.effective_date < r.renewal_date\n"
         "  GROUP BY r.subscription_id")]),
    # bounds pushed through a plain row filter keep the loosest bound, and nothing else is pushed
    "row_filter_without_an_upper_bound": ("limit_hits_14d", INF, [
        LIM_CTE, FROM_LIM, (HITS_ON, "AND l.hit_date > date_sub(r.as_of, 14)")]),
    "row_filter_bounded_at_the_renewal_date": ("limit_hits_14d", 7, [
        from_derived(FILTER), (HITS_ON, HITS_ON.replace("<= r.as_of", "<= r.renewal_date"))]),
    "row_filter_computes_the_bounded_column": ("limit_hits_14d", INF, [
        from_derived("SELECT subscription_id, date_sub(hit_date, 30) AS hit_date FROM $silver.churn_limit_events")]),
    "filter_with_limit_is_no_row_filter": ("limit_hits_14d", INF, [
        from_derived(FILTER + " ORDER BY hit_at DESC LIMIT 1000")]),
    "filter_with_distinct_is_no_row_filter": ("limit_hits_14d", INF, [
        from_derived("SELECT DISTINCT subscription_id, hit_date FROM $silver.churn_limit_events")]),
    "row_filter_joined_twice_once_unbounded": ("limit_hits_14d", INF, [
        LIM_CTE, FROM_LIM, (HITS_TAIL, "   " + HITS_ON + "\n  JOIN lim l2 ON l2.subscription_id = r.subscription_id\n"
                                       "  GROUP BY r.subscription_id\n")]),
    "row_filter_preserved_by_an_outer_join": ("limit_hits_14d", INF, [
        (HITS_FROM, "  FROM (" + FILTER + ") l LEFT JOIN renewals r\n")]),
}
UNMODELLED_EDITS = {   # name: (error message fragment, [(old, new), ...]): the extraction stops
    "union": ("UNION is not modelled", [
        (HITS_TAIL, HITS_TAIL + "  UNION ALL SELECT l.subscription_id, 1 FROM $silver.churn_limit_events l\n")]),
    "subquery_in_on": ("subquery inside a WHERE / ON / HAVING", [
        (HITS_ON, HITS_ON + " AND l.hit_at IN (SELECT hit_at FROM $silver.churn_limit_events)")]),
    "subquery_in_having": ("subquery inside a WHERE / ON / HAVING", [
        (HITS_TAIL, HITS_TAIL + "  HAVING MAX(l.hit_at) > (SELECT MIN(hit_at) FROM $silver.churn_limit_events)\n")]),
    "subquery_in_final_where": ("subquery inside a WHERE / ON / HAVING", [
        ("FROM labelled", "FROM labelled WHERE subscription_id IN (SELECT subscription_id FROM "
                          "$silver.churn_subscription_events)")]),
    "lateral_view": ("is not modelled", [
        (HITS_FROM, "  FROM renewals r LATERAL VIEW explode(array(1, 2)) x AS n JOIN $silver.churn_limit_events l\n")]),
    "multi_row_cte_joined_without_a_key": ("joins every_hit without naming any of its columns", [
        ("hits AS (\n", "every_hit AS (SELECT l.subscription_id FROM $silver.churn_limit_events l),\nhits AS (\n"),
        (HITS_FROM, "  FROM renewals r CROSS JOIN every_hit JOIN $silver.churn_limit_events l\n")]),
    "renewal_derived_cte_joined_without_the_key": ("joins late without the condition late.subscription_id", [
        ("hits AS (\n", LATE_CTE),
        (HITS_FROM, "  FROM renewals r CROSS JOIN late JOIN $silver.churn_limit_events l\n")]),
    "renewals_predicate_under_or": ("point-in-time predicate", [
        ("WHERE snapshot_date = date_sub(current_period_end, 7)",
         "WHERE snapshot_date = date_sub(current_period_end, 7) OR snapshot_date = current_period_end")]),
    "renewals_joins_another_table": ("renewals CTE must select from one table", [
        (RENEWALS, RENEWALS.replace("snapshots\n", "snapshots JOIN $silver.churn_incidents ON 1 = 1\n"))]),
    "renewals_as_of_is_not_the_snapshot_date": ("must project both the as_of column", [
        ("snapshot_date AS as_of", "date_add(snapshot_date, 3) AS as_of")]),
    # SUM(CASE ... ELSE 0) is 0 when rows exist only after as_of and NULL when there is none
    "else_zero_bound_read_without_coalesce": (r"joined reads pricing.cuts_so_far without COALESCE\(cuts_so_far, 0\)", [
        ("COALESCE(pricing.cuts_so_far, 0) AS cuts_so_far", "pricing.cuts_so_far AS cuts_so_far")]),
    # leaving the renewal grain: rows bounded by one renewal's as_of would reach another renewal
    "grouped_by_something_else_than_the_renewal_key": ("groups by .* without r.subscription_id", [
        (HITS_SELECT, "  SELECT MAX(r.subscription_id) AS subscription_id, COUNT(*) AS limit_hits_14d\n"),
        (HITS_TAIL, "   " + HITS_ON + "\n  GROUP BY r.plan_tier\n")]),
    "feature_cte_joined_on_something_else_than_the_key": ("joins hits without the condition hits.subscription_id", [
        ("LEFT JOIN hits            ON hits.subscription_id = r.subscription_id",
         "LEFT JOIN hits            ON hits.subscription_id = r.user_name")]),
    "renewals_joined_to_itself": ("joins the renewals CTE to itself", [
        (HITS_FROM, "  FROM renewals r JOIN renewals r2 ON r2.plan_tier = r.plan_tier\n"
                    "  JOIN $silver.churn_limit_events l\n"),
        (HITS_ON, HITS_ON.replace("<= r.as_of", "<= r2.as_of"))]),
    "aggregate_over_all_renewals": ("stats aggregates over all renewals", [
        ("hits AS (\n", "stats AS (SELECT AVG(active_days_7d) AS avg7 FROM u7),\nhits AS (\n"),
        (HITS_FROM, "  FROM renewals r CROSS JOIN stats JOIN $silver.churn_limit_events l\n")]),
    "window_function_across_renewals": ("window function that is not partitioned by r.subscription_id", [
        (HITS_SELECT, "  SELECT r.subscription_id, SUM(COUNT(*)) OVER () AS limit_hits_14d\n")]),
    "window_function_across_renewals_in_qualify": ("window function that is not partitioned by r.subscription_id", [
        (HITS_TAIL, HITS_TAIL + "  QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) <= 100\n")]),
    "window_function_across_renewals_in_order_by": ("window function that is not partitioned by r.subscription_id", [
        (HITS_TAIL, HITS_TAIL + "  ORDER BY RANK() OVER (ORDER BY COUNT(*) DESC)\n")]),
    # which renewals keep a row would depend on the other renewals' rows
    "feature_cte_order_by_limit": ("hits cuts its rows with LIMIT", [
        (HITS_TAIL, HITS_TAIL + "  ORDER BY MAX(l.hit_at) DESC LIMIT 100\n")]),
    "feature_cte_offset": ("hits cuts its rows with LIMIT / OFFSET", [
        (HITS_TAIL, HITS_TAIL + "  LIMIT 100 OFFSET 5\n")]),
    "final_select_limit": ("final_select cuts its rows with LIMIT", [("FROM labelled", "FROM labelled LIMIT 10")]),
    "renewals_cut_in_a_derived_table": ("derived:r cuts its rows with LIMIT", [
        (HITS_FROM, "  FROM (SELECT * FROM renewals LIMIT 10) r JOIN $silver.churn_limit_events l\n")]),
    "subtotals_with_rollup": ("hits groups with subtotals", [
        (HITS_TAIL, "   " + HITS_ON + "\n  GROUP BY r.subscription_id WITH ROLLUP\n")]),
    "distinct_without_the_renewal_key": ("tiers selects DISTINCT without r.subscription_id", [
        ("hits AS (\n", "tiers AS (SELECT DISTINCT r.plan_tier FROM renewals r),\nhits AS (\n")]),
    # the renewals CTE is the as-of snapshot row, exempt from the feature windows: a plain row filter only
    "renewals_window_over_all_renewals": ("renewals CTE must be a plain row filter .* a window function",
                                          renewals_reads("MAX(snapshot_date) OVER ()")),
    "renewals_window_partitioned_by_the_key": ("renewals CTE must be a plain row filter .* a window function",
                                               renewals_reads("COUNT(*) OVER (PARTITION BY subscription_id)")),
    "renewals_lead": ("renewals CTE must be a plain row filter .* a window function", renewals_reads(
        "LEAD(snapshot_date) OVER (PARTITION BY subscription_id ORDER BY snapshot_date)")),
    "renewals_qualify": ("renewals CTE must be a plain row filter .* QUALIFY", [
        (RENEWALS, RENEWALS + "\n  QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY snapshot_date "
                              "DESC) = 1")]),
    "renewals_group_by": ("renewals CTE must be a plain row filter .* GROUP BY, an aggregate", [
        ("SELECT subscription_id, user_name, plan_tier, city, started_at,",
         "SELECT subscription_id, MAX(user_name) AS user_name, MAX(plan_tier) AS plan_tier, MAX(city) AS city, "
         "MIN(started_at) AS started_at,"),
        (RENEWALS, RENEWALS + "\n  GROUP BY subscription_id, snapshot_date, current_period_end")]),
    "renewals_distinct": ("renewals CTE must be a plain row filter .* DISTINCT", [
        ("SELECT subscription_id, user_name, plan_tier, city, started_at,",
         "SELECT DISTINCT subscription_id, user_name, plan_tier, city, started_at,")]),
    "renewals_limit": ("renewals CTE must be a plain row filter .* LIMIT", [(RENEWALS, RENEWALS + " LIMIT 100")]),
    "renewals_order_by": ("renewals CTE must be a plain row filter .* ORDER BY", [
        (RENEWALS, RENEWALS + " ORDER BY snapshot_date")]),
    "renewals_generator": ("renewals CTE must be a plain row filter .* a table-generating function", [
        (RENEWAL_COLS, RENEWAL_COLS + ", explode(array(1, 2)) AS n")]),
    # modifiers that sit on the table reference, and a WITH that could shadow renewals
    "tablesample": ("reads l with TABLESAMPLE", [
        (HITS_FROM, "  FROM renewals r JOIN $silver.churn_limit_events TABLESAMPLE (10 PERCENT) l\n")]),
    "time_travel": (r"reads l with time travel \(AS OF\)", [
        (HITS_FROM, "  FROM renewals r JOIN $silver.churn_limit_events TIMESTAMP AS OF '2030-01-01' l\n")]),
    "nested_with_shadowing_renewals": (r"hits has its own WITH \(renewals\)", [
        (HITS_SELECT, "  WITH renewals AS (SELECT subscription_id, snapshot_date AS as_of FROM "
                      "$silver.churn_subscription_snapshots)\n" + HITS_SELECT)]),
}
SAFE_EDITS = {   # name: [(old, new), ...]: rewrites that keep every window, so nothing may change
    "case_conjuncts_in_parentheses": [(CUTS, "SUM(CASE WHEN (p.effective_date <= r.as_of AND (p.effective_date > "
                                             "date_sub(r.as_of, 400))) THEN 1 ELSE 0 END) AS cuts_so_far")],
    "count_of_a_case_without_else": [(CUTS, "COUNT(CASE WHEN p.effective_date <= r.as_of THEN p.change_id END) AS "
                                            "cuts_so_far")],
    "sum_of_a_cast_case": [(CUTS, "SUM(CAST(CASE WHEN p.effective_date <= r.as_of THEN 1 END AS INT)) AS cuts_so_far")],
    "anchor_written_as_renewal_date_minus_7": [
        (HITS_ON, HITS_ON.replace("<= r.as_of", "<= date_sub(r.renewal_date, 7)"))],
    "having_on_the_bounded_rows": [(HITS_TAIL, HITS_TAIL + "  HAVING COUNT(*) >= 0\n")],
    "ifnull_instead_of_coalesce": [("COALESCE(pricing.cuts_so_far, 0) AS cuts_so_far",
                                    "IFNULL(pricing.cuts_so_far, 0) AS cuts_so_far")],
    "no_else_then_no_coalesce_needed": [
        (CUTS, "SUM(CASE WHEN p.effective_date <= r.as_of THEN 1 END) AS cuts_so_far"),
        ("COALESCE(pricing.cuts_so_far, 0) AS cuts_so_far", "pricing.cuts_so_far AS cuts_so_far")],
    "constant_cte_cross_joined": [
        ("hits AS (\n", "params AS (SELECT 14 AS horizon),\nhits AS (\n"),
        (HITS_FROM, "  FROM renewals r CROSS JOIN params JOIN $silver.churn_limit_events l\n")],
    # the bounds of the reading scope are pushed through a plain row filter (derived table or CTE)
    "bounds_on_a_derived_row_filter": [from_derived(FILTER)],
    "bounds_on_a_cte_row_filter": [LIM_CTE, FROM_LIM],
    "bounds_on_a_row_filter_that_renames_a_date_column": [
        from_derived("SELECT subscription_id, hit_at AS hit_date FROM $silver.churn_limit_events")],
    "row_filter_with_its_own_where": [from_derived(FILTER + " WHERE limit_type = 'daily' OR hit_at IS NOT NULL")],
    "bounds_in_the_where_of_a_left_join_to_a_row_filter": [
        LIM_CTE, (HITS_FROM + "    ON l.subscription_id = r.subscription_id\n" + HITS_TAIL,
                  "  FROM renewals r LEFT JOIN lim l\n    ON l.subscription_id = r.subscription_id\n  WHERE "
                  + HITS_ON.removeprefix("AND ") + "\n  GROUP BY r.subscription_id\n")],
    "distinct_with_the_renewal_key": [(HITS_SELECT, HITS_SELECT.replace("SELECT", "SELECT DISTINCT"))],
    "qualify_within_one_renewal": [
        (HITS_TAIL, HITS_TAIL + "  QUALIFY ROW_NUMBER() OVER (PARTITION BY r.subscription_id ORDER BY COUNT(*)) "
                                "= 1\n")],
    "order_by_without_limit": [(HITS_TAIL, HITS_TAIL + "  ORDER BY r.subscription_id\n")],
    "renewals_extra_row_condition": [(RENEWALS, RENEWALS + " AND plan_tier <> 'free'")],
    "renewals_scalar_projection": [(RENEWAL_COLS, RENEWAL_COLS + ", upper(plan_tier) AS tier_uc")],
}


def edited_sql(edits) -> str:
    text = (REPO / spec.GOLD_SQL).read_text()
    for old, new in edits:
        assert text.count(old) == 1, f"edit no longer applies (the gold SQL changed): {old!r}"
        text = text.replace(old, new)
    return text


@pytest.fixture(scope="module")
def walk(facts):
    """walk(edits) -> the scope walk (GoldLineage) of the edited gold SQL."""
    bronze = ex.bronze_tables(facts["jobs"][spec.BRONZE_JOB])
    schema = ex.silver_schema(ex.silver_tables(facts["jobs"][spec.SILVER_JOB], bronze))

    def run(edits) -> sw.GoldLineage:
        sql = string.Template(edited_sql(edits)).substitute(silver=spec.SILVER_NAMESPACE)
        return sw.GoldLineage(sql, schema, source=spec.GOLD_SQL)
    return run


@pytest.fixture(scope="module")
def uppers(walk):
    """uppers(edits) -> {feature: loosest upper bound vs as_of} from the scope walk of the edited gold SQL."""
    seen: dict[tuple, dict[str, float]] = {}

    def run(edits) -> dict[str, float]:
        key = tuple(tuple(e) for e in edits)
        if key not in seen:   # the unedited SQL is the reference of every equivalent rewrite: walk it once
            gl = walk(edits)
            seen[key] = {f: lg.column_upper(gl.leaves_of(p)) for f, p in gl.final_selects().items()
                         if f in gspec.GOLD_FEATURES}
        return dict(seen[key])
    return run


def test_unedited_gold_sql_has_exactly_the_two_features_that_read_after_as_of(uppers):
    got = uppers([])
    assert len(got) == 22 and {f for f, hi in got.items() if hi > 0} == {
        "renewals_completed", "first_renewal_after_pricing_change"}
    assert got["renewals_completed"] == got["first_renewal_after_pricing_change"] == 7
    assert all(hi == 0 for f, hi in got.items() if gspec.FEATURE_CARDS[f]["pit_status"] == spec.PIT_COMPLIANT)


@pytest.mark.parametrize("name", sorted(LEAKY_EDITS))
def test_a_leaky_edit_of_the_gold_sql_never_stays_compliant(uppers, name):
    feature, upper, edits = LEAKY_EDITS[name]
    assert gspec.FEATURE_CARDS[feature]["pit_status"] == spec.PIT_COMPLIANT   # so the edit is an UNDECLARED exception
    assert uppers(edits)[feature] == upper > 0


@pytest.mark.parametrize("name", sorted(UNMODELLED_EDITS))
def test_a_construct_the_walk_does_not_model_stops_the_extraction(uppers, name):
    message, edits = UNMODELLED_EDITS[name]
    with pytest.raises(LineageExtractError, match=message):
        uppers(edits)


@pytest.mark.parametrize("name", sorted(SAFE_EDITS))
def test_an_equivalent_rewrite_keeps_every_window(uppers, name):
    assert uppers(SAFE_EDITS[name]) == uppers([])


@pytest.mark.parametrize("edits, cte", [([from_derived(FILTER)], "derived:l"), ([LIM_CTE, FROM_LIM], "lim")])
def test_count_star_over_a_row_filter_counts_its_table_in_the_pushed_window(walk, edits, cte):
    """The COUNT(*) row-count lineage survives the rewrite (COUNTS_ROWS_OF of the filter's table, with the
    bounds of the reading ON clause), and every read of the filter's table carries that window."""
    gl = walk(edits)
    leaves = gl.leaves_of(gl.final_selects()["limit_hits_14d"])
    counts = [(x.table, x.role, x.cte, sw.format_window(x.window)) for x in leaves if x.column == "*"]
    assert counts == [("lakehouse.silver.churn_limit_events", "ROWCOUNT", cte, "(as_of-14, as_of]")]
    assert {sw.format_window(x.window) for x in leaves if x.cte == cte} == {"(as_of-14, as_of]"}
    assert gl.plain_filter(gl.renewals) is None                     # renewals is the grain, never an event filter


def test_the_renewals_cte_must_be_a_plain_row_filter(uppers):
    """The verifier's leak: a window function in renewals read other renewals' snapshot rows (later ones
    included) under the renewals exemption, and the feature stayed compliant."""
    with pytest.raises(LineageExtractError) as e:
        uppers(renewals_reads("MAX(snapshot_date) OVER ()"))
    assert "a window function (MAX(" in str(e.value) and "exempt from the feature windows" in str(e.value) \
        and "compute it in a feature CTE" in str(e.value)
    assert sw.beyond_a_row_filter(sqlglot.parse_one("SELECT a, b + 1 AS c FROM t WHERE d > 0")) == []
    assert sorted(sw.beyond_a_row_filter(sqlglot.parse_one(
        "SELECT DISTINCT a, SUM(b) OVER () AS s FROM t JOIN u ON t.k = u.k GROUP BY a LIMIT 3"))) == [
        "DISTINCT", "GROUP BY", "JOIN", "LIMIT", "a window function (SUM(b) OVER ())"]


@pytest.mark.parametrize("name", ["case_under_not", "case_under_or", "two_cases_take_the_loosest"])
def test_a_leaky_case_bound_is_an_undeclared_exception_in_the_assembled_graph(tmp_path, name):
    """The verifier's three rewrites of cuts_so_far, through the whole assembler (pit_status, edges, windows)."""
    feature, upper, edits = LEAKY_EDITS[name]
    root = fake_repo(tmp_path)
    (root / spec.GOLD_SQL).write_text(edited_sql(edits))
    g = lg.assemble(root)
    col = g.props(GOLD + feature)
    assert col["pit_status"] == spec.PIT_UNDECLARED_EXCEPTION and col["reads_after_as_of"] is True
    assert col["max_upper_vs_as_of_days"] == (None if upper == INF else upper)
    subject = [e for e in g.out(GOLD + feature, "SUBJECT_TO")]
    assert [e["props"]["status"] for e in subject] == [spec.PIT_UNDECLARED_EXCEPTION]
    windows = {g.props(e["dst"])["display"] for e in g.out(GOLD + feature, "USES_WINDOW")
               if e["props"]["source_dataset"].endswith("churn_pricing_changes")}
    assert windows == ({"(-inf, as_of]", "(-inf, as_of+7)"} if upper == 7 else {"all history (no time filter)"})
    assert g.props(GOLD + "support_tickets_90d")["pit_status"] == spec.PIT_COMPLIANT


def test_the_reference_table_exemption_needs_the_declaration_and_the_match(graph, monkeypatch):
    incidents = "lakehouse.silver.churn_incidents"
    col = graph.props(GOLD + "incident_exposed_28d")
    assert col["pit_status"] == spec.PIT_COMPLIANT and col["max_upper_vs_as_of_days"] == 0
    edges = [e for e in graph.out(GOLD + "incident_exposed_28d", "DERIVED_FROM") if incidents in e["dst"]]
    assert {(e["props"]["window"], e["props"]["matched_window"]) for e in edges} == {("unbounded", "(as_of-28, as_of]")}
    assert graph.props(f"ds:{incidents}")["reference_data"] == spec.GLOBAL_DIMENSION_TABLES[incidents]
    assert not [e for e in graph.out(GOLD + "incident_exposed_28d", "USES_WINDOW")
                if e["props"]["source_dataset"] == incidents]           # no window of its own (PLAN window usage)
    monkeypatch.setattr(spec, "GLOBAL_DIMENSION_TABLES", {})             # matched, but no longer declared
    undeclared = lg.assemble(REPO, "core").props(GOLD + "incident_exposed_28d")
    assert undeclared["pit_status"] == spec.PIT_UNDECLARED_EXCEPTION and undeclared["max_upper_vs_as_of_days"] is None


def test_inconsistent_parameter_is_flagged(tmp_path):
    root = fake_repo(tmp_path)
    edit(root, spec.PANDAS_TWIN, "CAP_CUT = 0.83", "CAP_CUT = 0.8")
    g = lg.assemble(root)
    assert g.props("param:cap_cut")["consistent"] is False and g.props("param:allowance.pro")["consistent"] is True


def test_schema_violations_raise_instead_of_writing_a_bad_graph():
    g = lg.LineageGraph()
    g.node("Job", "job:x", path="x")
    with pytest.raises(LineageExtractError, match="no property"):
        g.node("Job", "job:y", colour="red")
    with pytest.raises(LineageExtractError, match="does not exist"):
        g.edge("IMPORTS", "job:x", "job:missing")
    g.node("Dataset", "ds:d", name="d")
    with pytest.raises(LineageExtractError, match="does not allow"):
        g.edge("IMPORTS", "job:x", "ds:d")
    with pytest.raises(LineageExtractError, match="unknown node label"):
        g.node("Column", "col:x")


# --------------------------------------------------------------------------- lineage_build_id
def test_lineage_build_id_changes_with_any_extracted_file_and_is_independent_of_the_business_id(tmp_path):
    root = fake_repo(tmp_path)
    tiny = REPO / gspec.TINY_FIXTURE

    def ids() -> tuple[str, str]:
        return (lbuild.lineage_identity(lg.assemble(root))["lineage_build_id"],
                mf.build_identity(tiny, root)["business_build_id"])

    base_lineage, base_business = ids()
    assert ids() == (base_lineage, base_business)                      # deterministic
    seen = {base_lineage}
    for rel in (spec.README, spec.MAKEFILE, spec.CI_WORKFLOW, "airflow/dags/lakehouse_churn_features.py",
                "pipelines/run_churn_e2e.sh", "sql/retail/silver_orders.sql"):
        original = (root / rel).read_text()
        (root / rel).write_text(original + f"\n{'--' if rel.endswith('.sql') else '#'} a harmless trailing comment\n")
        lineage_id, business_id = ids()
        assert lineage_id not in seen, f"{rel} did not change lineage_build_id"
        assert business_id == base_business, f"{rel} changed business_build_id"
        seen.add(lineage_id)
        (root / rel).write_text(original)
    assert ids() == (base_lineage, base_business)
    # the gold SQL shapes both graphs: both ids move
    edit(root, spec.GOLD_SQL, "-- gold.churn_renewal_features:", "-- gold.churn_renewal_features (edited):")
    lineage_id, business_id = ids()
    assert lineage_id not in seen and business_id != base_business
    # bronze bytes are no input of the core lineage build: a different sample dir moves the business id only
    other = mf.build_identity(REPO / "data/sample/churn", root)["business_build_id"] \
        if (REPO / "data/sample/churn/invoices.csv").is_file() else None
    assert other != business_id
    assert not set(mf.CONTENT_CODE) & {spec.README, spec.MAKEFILE, spec.CI_WORKFLOW, *spec.CONTENT_CODE}
