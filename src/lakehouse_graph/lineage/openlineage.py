"""Tier-2 lineage facts from OpenLineage: Spark application runs, their parents and their timing
(overlay ``--openlineage`` of scripts/build_lineage_local.py).

Input: the JSON Lines file of the OpenLineage *file* transport (one RunEvent per line, appended
across applications). The optional OPENLINEAGE=1 path writes it: pipelines/run_graph_e2e.sh and
the lakehouse_graph DAG run the Spark graph job with ``--packages
io.openlineage:openlineage-spark_2.13:1.53.0`` and ``spark.openlineage.transport.type=file`` into
``$GRAPH_ROOT/lineage/openlineage.jsonl`` (spec.OPENLINEAGE_FILE).

  read_events(path) -> (events, problems)   a line that is not a RunEvent (not JSON, no string run.runId /
                                            job.name, an eventTime that is not an ISO 8601 time with a UTC
                                            offset; a half-written last line) is named by its line number
                                            in ``problems``, never fatal; a facet of the wrong shape (a
                                            list where an object belongs) is ignored, never fatal
  collapse(events) -> applications          one entry per application run; every action run is
                                            folded into the run its ParentRunFacet names
  overlay(path) -> f(graph)

How the Spark integration (1.53.0) shapes a run, and the collapse rule:
  * one *application* run per spark-submit: job.name = the Spark appName, job.facets.jobType.jobType
    "APPLICATION", a START and a COMPLETE (FAIL / ABORT accepted) event;
  * many *action* runs (jobType SQL_JOB, job.name ``<appName>.<plan node>[.<table>]``, START /
    RUNNING / COMPLETE), each with ``run.facets.parent.run.runId`` = the application run: they are
    collapsed into it (``n_actions``, ``actions`` = runs per plan-node kind, ``n_events``);
  * an action whose application run has no event in the file (a truncated file) gets a run built from
    the facet (state unknown) and an environment warning; nothing is dropped;
  * ``spark_applicationDetails.applicationId`` (on START and action events, not on COMPLETE) is the
    Spark application id: the run's id is ``run:spark:<applicationId>``, the node Iceberg's
    snapshots point at (PRODUCED_BY_RUN, by the summary's spark.app.id), so with --iceberg too the
    run and the snapshots it committed meet; without an application id it is ``run:ol:<runId>``.

Nodes and edges (spec metadata-graph/0.1, Tier 2):
  Run     kind spark_application: started_at / ended_at (START / terminal eventTime), state (the last
          terminal event type), engine + engine_version, spark_app_id, ol_run_id, the collapsed actions;
          kind parent_run / root_run: the orchestrator runs a ParentRunFacet names (an Airflow task run and
          its DAG run when spark.openlineage.parent* / rootParent* are set)
  RAN_AS  Run -> Job: the Job whose appName the Tier-0 extractor read from ``.appName(...)``, else the
          convention <domain>_<stem> -> src/jobs/<domain>/<stem>.py; an appName that maps to no job is an
          environment warning (an in-process run, another repo's job), not an edge
  PARENT  application run -> its parent run (kind parent) -> that run's root (kind root)

What OpenLineage does not give here: Iceberg datasets. With the JDBC catalog the Spark integration
resolves no Iceberg table (OpenLineage issue #4677, PR #4754 open); inputs / outputs hold at most the
source files. They are kept as the ``datasets`` property, not as edges, and a later release that
reports Iceberg datasets is read the same way (nothing asserts that outputs are empty).
"""
from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path

from .. import manifest as mf
from . import extract as ex
from . import spec
from .graph import LineageGraph
from .iceberg_facts import spark_run_id

TERMINAL = ("COMPLETE", "FAIL", "ABORT")
APPLICATION = "APPLICATION"
_AIRFLOW_NAMESPACES = ("airflow",)


# --------------------------------------------------------------------------- reading
def read_events(path: str | Path) -> tuple[list[dict], list[str]]:
    """(events, problems). Each event is the parsed RunEvent plus ``_line`` (its 1-based line)."""
    events: list[dict] = []
    problems: list[str] = []
    with open(path, encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            if not line.strip():
                continue
            try:
                e = json.loads(line)
            except ValueError as err:
                problems.append(f"line {n}: not JSON ({getattr(err, 'msg', err)})")
                continue
            run = _d(e.get("run")) if isinstance(e, dict) else {}
            job = _d(e.get("job")) if isinstance(e, dict) else {}
            if not (isinstance(run.get("runId"), str) and run["runId"] and isinstance(job.get("name"), str)
                    and job["name"] and isinstance(e.get("eventTime"), str)):
                problems.append(f"line {n}: not an OpenLineage RunEvent (needs run.runId, job.name, eventTime)")
                continue
            try:
                when = _when(e["eventTime"])
            except ValueError:
                problems.append(f"line {n}: eventTime {e['eventTime'][:40]!r} is not an ISO 8601 time")
                continue
            if when.tzinfo is None:   # an RFC 3339 date-time has an offset; naive and aware times do not compare
                problems.append(f"line {n}: eventTime {e['eventTime'][:40]!r} has no UTC offset (an OpenLineage "
                                f"eventTime is an RFC 3339 date-time such as 2026-10-01T10:00:00Z)")
                continue
            e["_line"] = n
            events.append(e)
    return events, problems


def _d(x) -> dict:
    """``x`` when it is a JSON object, else ``{}``: a facet of the wrong shape is ignored, never fatal."""
    return x if isinstance(x, dict) else {}


def _s(x) -> str | None:
    """A scalar as text (None for null, an empty string, an object or a list)."""
    return None if x is None or x == "" or isinstance(x, (dict, list)) else str(x)


def _facets(e: dict) -> dict:
    return _d(_d(e.get("run")).get("facets"))


def job_type(e: dict) -> str | None:
    return _s(_d(_d(_d(e.get("job")).get("facets")).get("jobType")).get("jobType"))


def parent_facet(e: dict) -> dict:
    return _d(_facets(e).get("parent"))


def _parent_run(e: dict) -> str | None:
    return _s(_d(parent_facet(e).get("run")).get("runId"))


def _when(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


# --------------------------------------------------------------------------- collapse
def _new_app(run_id: str, job: str, namespace: str | None, synthetic: bool = False) -> dict:
    return {"run_id": run_id, "job": job, "namespace": namespace, "app_id": None, "app_name": None,
            "started_at": None, "ended_at": None, "state": None, "last": None, "n_events": 0,
            "event_types": Counter(), "actions": Counter(), "action_runs": set(), "parent": None, "env": None,
            "engine": None, "engine_version": None, "adapter": None, "inputs": set(), "outputs": set(),
            "lines": [], "synthetic": synthetic}


def _fold(app: dict, e: dict, action: bool) -> None:
    f = _facets(e)
    kind, at = _s(e.get("eventType")) or "OTHER", e["eventTime"]
    app["n_events"] += 1
    app["event_types"][kind] += 1
    app["lines"].append(e["_line"])
    if not action:
        if kind == "START" and (app["started_at"] is None or _when(at) < _when(app["started_at"])):
            app["started_at"] = at
        if kind in TERMINAL and (app["ended_at"] is None or _when(at) >= _when(app["ended_at"])):
            app["ended_at"], app["state"] = at, kind
        if app["last"] is None or _when(at) >= _when(app["last"][0]):
            app["last"] = (at, kind)
        if app["parent"] is None and parent_facet(e) and _parent_run(e) != app["run_id"]:
            app["parent"] = parent_facet(e)
    details = _d(f.get("spark_applicationDetails"))
    if _s(details.get("applicationId")) and app["app_id"] is None:
        app["app_id"] = _s(details["applicationId"])
    if _s(details.get("appName")) and app["app_name"] is None:
        app["app_name"] = _s(details["appName"])
    env = _d(f.get("environment-properties")).get("environment-properties")
    if isinstance(env, dict) and env and app.get("env") is None:
        app["env"] = env
    engine = _d(f.get("processing_engine"))
    if _s(engine.get("name")) and app["engine"] is None:
        app["engine"], app["engine_version"] = _s(engine.get("name")), _s(engine.get("version"))
        app["adapter"] = _s(engine.get("openlineageAdapterVersion"))
    for key in ("inputs", "outputs"):
        for d in e.get(key) if isinstance(e.get(key), list) else []:
            if isinstance(d, dict) and d.get("name"):
                app[key].add(f"{d.get('namespace') or '?'}:{_dataset_name(str(d['name']))}")
    if action:
        rid = e["run"]["runId"]
        if rid not in app["action_runs"]:
            app["action_runs"].add(rid)
            name = str(e["job"]["name"])
            rest = name[len(app["job"]) + 1:] if name.startswith(app["job"] + ".") else name
            app["actions"][rest.split(".", 1)[0] or name] += 1


def _dataset_name(name: str) -> str:
    """A dataset name as OpenLineage reported it, a container mount mapped to its repo path and a
    path inside this repo made relative (``/opt/data/sample/churn/x.csv`` -> ``data/sample/churn/x.csv``)."""
    if not name.startswith("/"):
        return name
    mapped = ex.canonical_path(name)
    return mapped if not mapped.startswith("/") else mf.display_path(mapped)


def collapse(events: list[dict]) -> tuple[dict[str, dict], dict]:
    """(application runs by OpenLineage run id, stats). See the module docstring for the rule."""
    parents = {_parent_run(e) for e in events if _parent_run(e)}
    apps: dict[str, dict] = {}
    is_app: dict[str, bool] = {}
    for e in events:
        rid = e["run"]["runId"]
        jt = job_type(e)
        own = _parent_run(e)
        # an application run, any run without a parent (nothing to fold it into), or, from a producer
        # without the jobType facet, a run other events name as their parent
        app = jt == APPLICATION or not own or own == rid or (jt is None and rid in parents)
        is_app[rid] = is_app.get(rid, False) or app
    for e in events:
        rid = e["run"]["runId"]
        if is_app[rid]:
            app = apps.setdefault(rid, _new_app(rid, str(e["job"]["name"]), _s(e["job"].get("namespace"))))
            _fold(app, e, action=False)
    parent_of = {e["run"]["runId"]: _parent_run(e) for e in events if not is_app[e["run"]["runId"]]}

    def owner(rid: str) -> str:
        """The application run an action belongs to (through nested action runs, if any)."""
        p, seen = parent_of[rid], {rid}
        while p not in apps and p in parent_of and p not in seen:
            seen.add(p)
            p = parent_of[p]
        return p

    orphans: set[str] = set()
    for e in events:
        rid = e["run"]["runId"]
        if is_app[rid]:
            continue
        p = owner(rid)
        if p not in apps:   # the application's own events are not in the file: rebuild it from the facet
            job = _d(parent_facet(e).get("job"))
            apps[p] = _new_app(p, _s(job.get("name")) or "?", _s(job.get("namespace")), synthetic=True)
            orphans.add(p)
        _fold(apps[p], e, action=True)
    stats = {"events": len(events), "application_runs": sum(not a["synthetic"] for a in apps.values()),
             "rebuilt_from_parent_facet": sorted(orphans),
             "action_runs_collapsed": sum(len(a["action_runs"]) for a in apps.values()),
             "action_events_collapsed": sum(1 for e in events if not is_app[e["run"]["runId"]])}
    return apps, stats


# --------------------------------------------------------------------------- the overlay
def run_node_id(app: dict) -> str:
    return spark_run_id(app["app_id"]) if app["app_id"] else f"run:ol:{app['run_id']}"


def job_for(g: LineageGraph, app_name: str) -> tuple[str | None, str | None, list[str]]:
    """(job id, how, ambiguous candidates) for a Spark appName: the Tier-0 Job with that app_name, else
    the convention <domain>_<stem> -> src/jobs/<domain>/<stem>.py."""
    hits = sorted(j for j in g.ids("Job") if g.props(j).get("app_name") == app_name)
    if len(hits) == 1:
        return hits[0], "appName = Job.app_name (read from .appName(...) by the Tier-0 extractor)", []
    if len(hits) > 1:
        return None, None, hits
    m = re.match(rf"^({'|'.join(spec.APP_NAME_DOMAINS)})_(\w+)$", app_name)
    if m and g.has(f"job:src/jobs/{m.group(1)}/{m.group(2)}.py"):
        return f"job:src/jobs/{m.group(1)}/{m.group(2)}.py", "naming convention <domain>_<stem>", []
    return None, None, []


def _orchestrator_run(g: LineageGraph, ref: dict, kind: str, airflow_run: str | None) -> str | None:
    rid = _s(_d(ref.get("run")).get("runId"))
    if not rid:
        return None
    job = _d(ref.get("job"))
    ns = _s(job.get("namespace"))
    return g.node("Run", f"run:ol:{rid}", kind=kind, job=_s(job.get("name")), namespace=ns, ol_run_id=rid,
                  engine="airflow" if ns in _AIRFLOW_NAMESPACES else None, airflow_run_id=airflow_run,
                  source="OpenLineage ParentRunFacet")


def overlay(path: str | Path):
    """``f(graph)`` adding the Tier-2 runs of the OpenLineage file at ``path``."""
    path = Path(path)

    def apply(g: LineageGraph) -> None:
        if not path.is_file():
            raise spec.LineageExtractError(f"--openlineage: {mf.display_path(path)} does not exist (the OPENLINEAGE=1 "
                                           f"path of pipelines/run_graph_e2e.sh writes it)")
        g.inputs[f"openlineage:{path.name}"] = mf.sha256_file(path)
        events, problems = read_events(path)
        apps, stats = collapse(events)
        if problems:
            g.environment_warnings.append(f"openlineage: {len(problems)} line(s) of {path.name} are not RunEvents and "
                                          f"were skipped: {'; '.join(problems[:5])}")
        for p in stats["rebuilt_from_parent_facet"]:
            g.environment_warnings.append(f"openlineage: run {p} ({apps[p]['job']}) has action events but no event of "
                                          f"its own in {path.name}: its run was rebuilt from the ParentRunFacet "
                                          f"(state and timing unknown)")
        ran_as, parent_edges, unmapped, used = 0, 0, [], set()
        producers = sorted({str(e.get("producer")) for e in events if e.get("producer")})
        for rid in sorted(apps, key=lambda r: (apps[r]["started_at"] or "", r)):
            a = apps[rid]
            nid = run_node_id(a)
            if nid in used:   # two OpenLineage runs claim one Spark application id: keep both, apart
                nid = f"run:ol:{rid}"
            used.add(nid)
            parent = a["parent"] or {}
            root = _d(parent.get("root"))
            airflow = _s(_d(root.get("run")).get("runId")) if _d(root.get("job")).get("namespace") in \
                _AIRFLOW_NAMESPACES else None
            state = a["state"] or (None if a["synthetic"] else (a["last"] or (None, None))[1])
            g.node("Run", nid, kind="spark_application", job=a["job"], app_name=a["app_name"] or a["job"],
                   namespace=a["namespace"], engine=a["engine"] or ("spark" if a["app_id"] else None),
                   engine_version=a["engine_version"], spark_app_id=a["app_id"], ol_run_id=rid, airflow_run_id=airflow,
                   started_at=a["started_at"], ended_at=a["ended_at"], state=state, n_actions=len(a["action_runs"]),
                   n_events=a["n_events"], env=ex.dumps(a["env"]) if a["env"] else None,
                   actions=ex.dumps(dict(sorted(a["actions"].items()))) if a["actions"] else None,
                   datasets=ex.dumps({"inputs": sorted(a["inputs"]), "outputs": sorted(a["outputs"])})
                   if a["inputs"] or a["outputs"] else None,
                   source=f"OpenLineage {a['adapter'] or '?'} file transport ({path.name}, lines "
                          f"{min(a['lines'])}-{max(a['lines'])})")
            job, how, ambiguous = job_for(g, a["app_name"] or a["job"])
            if job:
                g.edge("RAN_AS", nid, job, via=how, app_name=a["app_name"] or a["job"])
                ran_as += 1
            else:
                unmapped.append(a["app_name"] or a["job"])
                g.environment_warnings.append(
                    f"openlineage: run {nid} (appName {a['app_name'] or a['job']}) maps to no job of the repo"
                    + (f" (several jobs set that appName: {', '.join(ambiguous)})" if ambiguous else
                       " (no Job sets that appName and src/jobs/<domain>/<stem>.py does not exist; an in-process "
                       "run keeps the harness's appName)") + ": no RAN_AS edge")
            pid = _orchestrator_run(g, parent, "parent_run", airflow) if parent else None
            if pid and pid != nid:
                g.edge("PARENT", nid, pid, kind="parent")
                parent_edges += 1
                root_id = _orchestrator_run(g, root, "root_run", airflow) if root else None
                if root_id and root_id not in (pid, nid):
                    g.edge("PARENT", pid, root_id, kind="root")
                    parent_edges += 1
        joined = sum(1 for nid in used if any(e["rel"] == "PRODUCED_BY_RUN" and e["dst"] == nid for e in g.edges))
        g.overlays["openlineage"] = {
            "file": mf.display_path(path), "sha256": g.inputs[f"openlineage:{path.name}"], "producers": producers,
            "skipped_lines": len(problems), **stats, "ran_as": ran_as, "unmapped_app_names": sorted(set(unmapped)),
            "parent_edges": parent_edges, "runs_joined_to_snapshots": joined}
    return apply


def summary_line(info: dict) -> str:
    """One line for the CLI from the lineage manifest's overlays["openlineage"]."""
    return (f"OpenLineage {info['file']}: {info['events']} events -> {info['application_runs']} application runs "
            f"({info['action_runs_collapsed']} action runs / {info['action_events_collapsed']} events collapsed), "
            f"{info['ran_as']} RAN_AS, {info['parent_edges']} PARENT, {info['runs_joined_to_snapshots']} joined to "
            f"Iceberg snapshots" + (f", {info['skipped_lines']} lines skipped" if info["skipped_lines"] else ""))


__all__ = ["collapse", "overlay", "read_events", "summary_line"]
