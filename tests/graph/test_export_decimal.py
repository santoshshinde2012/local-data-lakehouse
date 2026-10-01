"""src/jobs/churn/04_export_features.py without Spark: DECIMAL gold values and the all-or-nothing export.

Spark SQL decimal literals (``1.0``, ``4.0`` in the gold SQL) make some gold columns DECIMAL, so
``Row.asDict()`` holds ``decimal.Decimal`` values, which ``json.dumps`` cannot serialise: the job
used to crash on the hero record after it had already overwritten the two CSV exports. It now
turns a Decimal into the same number as a float and writes the three exports to staged siblings,
replacing the previous exports only once all three are written.

pyspark is replaced by a stub module and the gold table by a list of rows (Decimal, date and
datetime values, as a Spark Row holds them), so this runs in the core venv without a JVM.
"""
from __future__ import annotations

import csv
import importlib.util
import json
import sys
import types
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import REPO

from lakehouse_graph import spec as gspec

JOB = REPO / "src/jobs/churn/04_export_features.py"
GOLD = "lakehouse.gold.churn_renewal_features"
EXPORTS = tuple(gspec.EXPORT_FILES)   # churn_renewals_audit.csv, churn_user_features.csv, hero_inference_record.json


class FakeRow:
    def __init__(self, values: dict):
        self._values = values

    def asDict(self) -> dict:  # noqa: N802 (pyspark's name)
        return dict(self._values)


class FakeFrame:
    def __init__(self, rows: list[dict]):
        self.rows = rows

    def orderBy(self, col: str) -> FakeFrame:  # noqa: N802 (pyspark's name)
        return FakeFrame(sorted(self.rows, key=lambda r: r[col]))

    def collect(self) -> list[FakeRow]:
        return [FakeRow(r) for r in self.rows]


class FakeSpark:
    def __init__(self, rows: list[dict]):
        self.rows, self.read, self.stopped = rows, [], False
        self.sparkContext = types.SimpleNamespace(setLogLevel=lambda level: None)

    def table(self, name: str) -> FakeFrame:
        self.read.append(name)
        return FakeFrame(self.rows)

    def stop(self) -> None:
        self.stopped = True


@pytest.fixture
def job(monkeypatch, tmp_path):
    """``job.mod``: the job module imported against a stub pyspark; ``job.run(rows)`` runs its main()
    on those gold rows with EXPORT_DIR = ``job.out`` and returns the fake session."""
    session: dict = {}

    class Builder:
        def appName(self, name: str) -> Builder:  # noqa: N802 (pyspark's name)
            session["app"] = name
            return self

        def getOrCreate(self) -> FakeSpark:  # noqa: N802 (pyspark's name)
            return session["spark"]

    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = types.SimpleNamespace(builder=Builder())
    pyspark = types.ModuleType("pyspark")
    pyspark.sql = sql
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setattr(sys, "dont_write_bytecode", True)   # no __pycache__ next to the user's job
    mspec = importlib.util.spec_from_file_location("churn_04_export_features_under_test", JOB)
    mod = importlib.util.module_from_spec(mspec)
    mspec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "EXPORT_DIR", str(tmp_path / "export"))

    def run(rows: list[dict]) -> FakeSpark:
        session["spark"] = FakeSpark(rows)
        mod.main()
        return session["spark"]

    return types.SimpleNamespace(mod=mod, run=run, out=tmp_path / "export", session=session)


def gold_row(job, user_id: str, route: str, **over) -> dict:
    """One gold row as Spark returns it: DECIMAL columns as Decimal, dates as date, built_at as datetime."""
    row = {c: 0 for c in job.mod.TRAIN_COLUMNS + job.mod.AUDIT_EXTRA}
    row.update(user_id=user_id, user_name=f"name {user_id}", plan_tier="pro", engagement_trend=Decimal("0.8000"),
               allowance_used_pct=Decimal("1.2346"), accept_rate_change=1.0612, overage_usd_28d=Decimal("12.50"),
               outcome="renewed" if route == "model" else "pending", route=route, feature_as_of=date(2026, 9, 24),
               renewal_date=date(2026, 10, 1), city="Pune", built_at=datetime(2026, 9, 30, 7, 8, 9))
    row.update(over)
    return row


def rows(job, hero_route: str = "score_today", **hero_over) -> list[dict]:
    return [gold_row(job, "sub_a", "model"), gold_row(job, "sub_b", "dunning"),
            gold_row(job, job.mod.HERO_ID, hero_route, **hero_over)]


def staged(out: Path) -> list[str]:
    return sorted(p.name for p in out.iterdir() if p.name.endswith(".tmp"))


def test_plain_turns_a_decimal_into_the_same_number(job):
    for text in ("0.8000", "1.2346", "12.50", "0", "-3.1"):
        v = job.mod._plain(Decimal(text))
        assert type(v) is float and v == float(text) and Decimal(repr(v)) == Decimal(text)
    assert json.dumps({"engagement_trend": job.mod._plain(Decimal("0.8000"))}) == '{"engagement_trend": 0.8}'
    assert job.mod._plain(date(2026, 9, 24)) == "2026-09-24"
    assert job.mod._plain(datetime(2026, 9, 30, 7, 8, 9)) == "2026-09-30 07:08:09"
    assert [job.mod._plain(v) for v in (3, 0.5, "pro", None, True)] == [3, 0.5, "pro", None, True]


def test_the_export_writes_all_three_files_from_decimal_columns(job):
    spark = job.run(rows(job))
    out = job.out
    assert spark.read == [GOLD] and spark.stopped and job.session["app"] == "churn_04_export_features"
    assert sorted(p.name for p in out.iterdir()) == sorted(EXPORTS)
    with (out / "churn_renewals_audit.csv").open(newline="") as f:
        audit = list(csv.DictReader(f))
    assert [r["user_id"] for r in audit] == sorted(["sub_a", "sub_b", job.mod.HERO_ID])
    assert list(audit[0]) == job.mod.TRAIN_COLUMNS + job.mod.AUDIT_EXTRA
    assert {(r["engagement_trend"], r["allowance_used_pct"], r["overage_usd_28d"]) for r in audit} == {
        ("0.8", "1.2346", "12.5")}   # the numbers, as the pandas export writes them (not "0.8000")
    assert audit[0]["feature_as_of"] == "2026-09-24" and audit[0]["built_at"] == "2026-09-30 07:08:09"
    with (out / "churn_user_features.csv").open(newline="") as f:
        train = list(csv.DictReader(f))
    assert [r["user_id"] for r in train] == ["sub_a"] and list(train[0]) == job.mod.TRAIN_COLUMNS
    record = json.loads((out / "hero_inference_record.json").read_text())
    assert list(record) == [c for c in job.mod.TRAIN_COLUMNS if c != "churned"]
    assert record["user_id"] == job.mod.HERO_ID and record["engagement_trend"] == 0.8
    assert record["allowance_used_pct"] == 1.2346 and record["overage_usd_28d"] == 12.5
    assert staged(out) == []


@pytest.mark.parametrize("hero_over, error", [
    ({"engagement_trend": object()}, TypeError),   # the hero record cannot be serialised (the old Decimal crash)
    ({"hero_route": "pending"}, SystemExit),        # no T-7 record for the hero today
], ids=["json_fails_after_both_csvs", "no_hero_record"])
def test_a_failed_export_leaves_the_previous_exports_untouched(job, hero_over, error):
    job.out.mkdir(parents=True)
    for name in EXPORTS:
        (job.out / name).write_text(f"previous {name}\n")
    with pytest.raises(error):
        job.run(rows(job, **hero_over))
    assert {name: (job.out / name).read_text() for name in EXPORTS} == {
        name: f"previous {name}\n" for name in EXPORTS}
    assert staged(job.out) == []


def test_a_rename_refused_midway_leaves_no_staged_file(job, monkeypatch):
    """The documented limit of the fix: the three moves are separate renames. When the OS refuses the
    second one (a read-only bind mount, say), the first export is already new, the other two are still
    the previous ones, the error reaches the caller and no staged file is left in the export directory."""
    job.out.mkdir(parents=True)
    for name in EXPORTS:
        (job.out / name).write_text(f"previous {name}\n")
    moved: list[str] = []

    def replace(src, dst):
        if moved:
            raise PermissionError(f"rename refused: {dst}")
        Path(src).replace(dst)
        moved.append(Path(dst).name)

    monkeypatch.setattr(job.mod, "os", types.SimpleNamespace(replace=replace))   # only this module's os
    with pytest.raises(PermissionError):
        job.run(rows(job))
    first, *rest = job.mod.EXPORTS   # the order main() moves them in
    assert sorted(job.mod.EXPORTS) == sorted(EXPORTS) and moved == [first]
    assert (job.out / first).read_text() != f"previous {first}\n"
    assert {name: (job.out / name).read_text() for name in rest} == {name: f"previous {name}\n" for name in rest}
    assert staged(job.out) == []


def test_a_rerun_replaces_every_previous_export(job):
    job.out.mkdir(parents=True)
    for name in EXPORTS:
        (job.out / name).write_text("previous\n")
    job.run(rows(job))
    assert all((job.out / name).read_text() != "previous\n" for name in EXPORTS)
    assert staged(job.out) == []
