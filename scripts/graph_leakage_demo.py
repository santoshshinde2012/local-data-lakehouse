#!/usr/bin/env python3
"""Leakage demo: how a neighbour lapse-rate feature leaks, measured as AUC (numpy only; no scikit-learn).

  python scripts/graph_leakage_demo.py [--graph-root DIR] [--profile s42 | --build DIR] [--json F] [--md F]
                                       [--folds 5] [--repeats 3] [--seed 42] [--check]

The renewal data has NO relationship between subscriptions, so a neighbour feature can only help by leaking.
Four ways to build "the lapse rate of a renewal's 10 nearest renewals" (SIMILAR_TO, similar_to/renewal-v1), each
scored as a single feature (AUC of the feature against the renewal's own voluntary lapse, model-routed renewals):

  self-inclusive    the renewal's own label is counted among its neighbours' ((sum + y) / (n + 1)): pure leakage
  as of today       every neighbour's outcome as of data_end, including outcomes observed after the source's T-7
  temporally safe   only neighbour outcomes observed on or before the source's as_of, smoothed toward the base rate
  random graph      10 random same-plan renewals instead of the nearest ones (the no-structure control)

and as one extra column of a logistic regression on the 21 numeric gold features + plan (L2, C=1, standardised on the
training fold, Newton iterations in numpy), stratified 5-fold x 3 repeats (numpy RNG, --seed). In the regression the
neighbour feature is built out of fold, as a model would see it: neighbours are the nearest TRAINING renewals of the
same plan (re-ranked from the persisted SIMILAR_TO scaler and the quantised key), the prior is the training rate; the
self-inclusive variant still adds the renewal's own label ((9 training neighbours x 10 + y) / 11): the leak.

The planning prototypes measured, at seed 42 (PLAN 2.2 / 13): single feature 0.8525 / 0.6158 / 0.5487 (self-inclusive /
today / safe), logistic regression 0.7198 base, 0.8655 self-inclusive, 0.7223 today, 0.7198 safe, 0.7197 random.
They are reported, never pinned: --check only says whether this run is within +-0.02 of them (seed-42 builds only).

Outputs: a summary table on stdout; --json (schema: generated_by, build_id, profile, seed, n_model, lapses,
variants [{key, label, single_feature, lr, lr_sd}], plan_reference, deltas, within_tolerance, setup, note; the
"variants" list is what scripts/graph_charts.py draws) and --md. Reads only the build's Parquet; writes nothing else.
Exit code: 0, 1 with --check when a seed-42 figure is more than 0.02 from the plan, 3 when there is no build
(nothing measured; the code scripts/graph_charts.py uses for a missing build).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse_graph import spec  # noqa: E402 (after the sys.path line above)

K = spec.K
EXIT_NO_BUILD = 3         # like scripts/graph_charts.py: "the build is missing", not a failed measurement
DEPTH = 120              # neighbours ranked per renewal for the out-of-fold variants (only TRAIN ones are used)
TOLERANCE = 0.02
PLAN_REFERENCE = {        # PLAN.final.md 2.2 / 13, seed 42 (planning prototypes; reported, not pinned)
    "single_feature": {"self_inclusive": 0.8525, "as_of_today": 0.6158, "temporally_safe": 0.5487},
    "lr": {"baseline": 0.7198, "self_inclusive": 0.8655, "as_of_today": 0.7223, "temporally_safe": 0.7198,
           "random_graph": 0.7197},
}
LABELS = {
    "baseline": "gold features only (no neighbour feature)",
    "self_inclusive": "self-inclusive neighbour rate (own label counted)",
    "as_of_today": "neighbour rate as of today (outcomes after T-7 leak in)",
    "temporally_safe": "temporally safe neighbour rate (outcomes known by each as_of)",
    "random_graph": "random same-plan neighbours (control)",
}


# --------------------------------------------------------------------------- numpy statistics
def auc(score: np.ndarray, y: np.ndarray) -> float:
    """ROC AUC (Mann-Whitney U with average ranks for ties)."""
    score = np.asarray(score, dtype=float)
    y = np.asarray(y).astype(bool)
    n1, n0 = int(y.sum()), int((~y).sum())
    if not n1 or not n0:
        return float("nan")
    order = np.argsort(score, kind="mergesort")
    s = score[order]
    ranks = np.empty(len(s))
    i = 0
    while i < len(s):
        j = i
        while j + 1 < len(s) and s[j + 1] == s[i]:
            j += 1
        ranks[i:j + 1] = (i + j) / 2 + 1
        i = j + 1
    r = np.empty(len(s))
    r[order] = ranks
    return float((r[y].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


def fit_logistic(x: np.ndarray, y: np.ndarray, c: float = 1.0, iters: int = 50) -> np.ndarray:
    """L2-penalised logistic regression (sklearn's objective: 0.5 w'w + C * sum logloss; intercept unpenalised)."""
    xb = np.column_stack([np.ones(len(x)), x])
    w = np.zeros(xb.shape[1])
    lam = np.full(xb.shape[1], 1.0 / c)
    lam[0] = 0.0
    for _ in range(iters):
        p = 1 / (1 + np.exp(-np.clip(xb @ w, -35, 35)))
        g = xb.T @ (p - y) + lam * w
        h = (xb * (p * (1 - p))[:, None]).T @ xb + np.diag(lam)
        step = np.linalg.solve(h, g)
        w -= step
        if np.max(np.abs(step)) < 1e-10:
            break
    return w


def predict(w: np.ndarray, x: np.ndarray) -> np.ndarray:
    return 1 / (1 + np.exp(-np.clip(np.column_stack([np.ones(len(x)), x]) @ w, -35, 35)))


def stratified_folds(y: np.ndarray, folds: int, rng: np.random.Generator) -> list[np.ndarray]:
    """Fold index per row, each class spread evenly over the folds."""
    assign = np.empty(len(y), dtype=int)
    for cls in (0, 1):
        idx = np.flatnonzero(y == cls)
        rng.shuffle(idx)
        assign[idx] = np.arange(len(idx)) % folds
    return [np.flatnonzero(assign == f) for f in range(folds)]


# --------------------------------------------------------------------------- data
def _read(path: Path, columns: list[str] | None = None) -> dict[str, np.ndarray]:
    t = pq.read_table(path, columns=columns)
    return {n: t.column(n).to_numpy(zero_copy_only=False) for n in t.column_names}


def _days(a: np.ndarray) -> np.ndarray:
    """date32 / datetime -> int days since epoch (NaT / None -> a large sentinel)."""
    out = np.full(len(a), 10 ** 9, dtype=np.int64)
    for i, v in enumerate(a):
        if v is None:
            continue
        d = np.datetime64(v, "D")
        if not np.isnat(d):
            out[i] = d.astype("int64")
    return out


def load(build: Path) -> dict:
    r = _read(build / "parquet" / "nodes_Renewal.parquet")
    s = _read(build / "parquet" / "edges_SIMILAR_TO.parquet", ["src", "dst", "rank"])
    sc = _read(build / spec.SCALER_FILE)
    return {"r": r, "s": s, "scaler": sc, "manifest": json.loads((build / "manifest.json").read_text())}


# --------------------------------------------------------------------------- the four single-feature variants
def single_feature(d: dict) -> dict:
    r, s = d["r"], d["s"]
    rid = r["renewal_id"]
    pos = {x: i for i, x in enumerate(rid)}
    model = r["route"] == "model"
    y_all = r["churned"].astype(float)
    observed = _days(r["outcome_observed_on"])
    as_of = _days(r["as_of"])
    src = np.array([pos[x] for x in s["src"]])
    dst = np.array([pos[x] for x in s["dst"]])
    n = len(rid)
    nb_sum = np.bincount(src, weights=y_all[dst], minlength=n)
    nb_cnt = np.bincount(src, minlength=n).astype(float)
    safe = observed[dst] <= as_of[src]
    sf_sum = np.bincount(src[safe], weights=y_all[dst][safe], minlength=n)
    sf_cnt = np.bincount(src[safe], minlength=n).astype(float)
    y = y_all[model]
    base = y.mean()
    with np.errstate(invalid="ignore", divide="ignore"):
        today = nb_sum / nb_cnt
    safe_rate = (sf_sum + base) / (sf_cnt + 1)
    self_incl = (nb_sum + y_all) / (nb_cnt + 1)
    rng = np.random.default_rng(0)
    plans = r["plan_tier"]
    rand = np.full(n, np.nan)
    for p in np.unique(plans[model]):
        pool = np.flatnonzero(model & (plans == p))
        for i in pool:
            pick = rng.choice(pool[pool != i], size=min(K, len(pool) - 1), replace=False)
            rand[i] = y_all[pick].mean()
    return {"self_inclusive": auc(self_incl[model], y), "as_of_today": auc(np.nan_to_num(today[model], nan=base), y),
            "temporally_safe": auc(safe_rate[model], y), "random_graph": auc(rand[model], y),
            "safe_neighbours_mean": round(float(sf_cnt[model].mean()), 2), "n_model": int(model.sum()),
            "lapses": int(y.sum())}


# --------------------------------------------------------------------------- the regression (out of fold)
def neighbour_orders(z: np.ndarray, plans: np.ndarray, ids: np.ndarray, depth: int) -> np.ndarray:
    """For every row, the ``depth`` nearest other rows of the same plan, ranked by floor(d2*1e9+0.5) then id."""
    order = np.full((len(z), depth), -1, dtype=np.int64)
    id_rank = np.argsort(np.argsort(ids, kind="mergesort"), kind="mergesort")
    for p in np.unique(plans):
        idx = np.flatnonzero(plans == p)
        zb = z[idx]
        for a in range(0, len(idx), 256):
            zs = zb[a:a + 256]
            d2 = np.zeros((len(zs), len(zb)))
            for f in range(zb.shape[1]):          # feature by feature: a 256 x block matrix, never 3-D
                diff = zs[:, f][:, None] - zb[:, f][None, :]
                d2 += diff * diff
            q = np.floor(d2 * spec.QUANT + 0.5)
            q[np.arange(d2.shape[0]), a + np.arange(d2.shape[0])] = np.inf
            take = min(depth, len(idx) - 1)
            part = np.argpartition(q, take, axis=1)[:, :take + 1] if take + 1 < len(idx) else \
                np.tile(np.arange(len(idx)), (d2.shape[0], 1))
            for k in range(d2.shape[0]):
                cand = part[k][np.isfinite(q[k, part[k]])]
                cand = cand[np.lexsort((id_rank[idx[cand]], q[k, cand]))][:take]
                order[idx[a + k], :len(cand)] = idx[cand]
    return order


def oof_rate(order: np.ndarray, ok: np.ndarray, lab: np.ndarray, prior: float, k: int = K,
             a: float = 1.0) -> np.ndarray:
    """Smoothed lapse rate of each row's first k neighbours with ok (rows x depth booleans)."""
    valid = ok & (order >= 0)
    sel = valid & (np.cumsum(valid, axis=1) <= k)
    labs = np.where(order >= 0, lab[np.clip(order, 0, None)], 0.0)
    return ((labs * sel).sum(axis=1) + a * prior) / (sel.sum(axis=1) + a)


def regression(d: dict, folds: int, repeats: int, seed: int) -> dict:
    r = d["r"]
    model = r["route"] == "model"
    ids = r["renewal_id"][model]
    y = r["churned"][model].astype(float)
    plans = r["plan_tier"][model]
    num = [f for f in spec.NUMERIC_FEATURES]
    x_num = np.column_stack([r[f][model].astype(float) for f in num])
    x = np.column_stack([x_num] + [(plans == p).astype(float) for p in sorted(np.unique(plans))])
    sc = {f: (m, sd) for f, m, sd in zip(d["scaler"]["feature"], d["scaler"]["mean"], d["scaler"]["std"], strict=True)}
    z = np.column_stack([(r[f][model].astype(float) - sc[f][0]) / (sc[f][1] or 1.0) for f in spec.FEATURES])
    t0 = time.perf_counter()
    order = neighbour_orders(z, plans, ids, DEPTH)
    knn_s = time.perf_counter() - t0
    observed = _days(r["outcome_observed_on"][model])
    as_of = _days(r["as_of"][model])
    temporal_ok = np.where(order >= 0, observed[np.clip(order, 0, None)] <= as_of[:, None], False)
    rng = np.random.default_rng(seed)
    res: dict[str, list[float]] = {k: [] for k in ("baseline", "self_inclusive", "as_of_today", "temporally_safe",
                                                   "random_graph")}
    n = len(y)
    for rep in range(repeats):
        for f, te in enumerate(stratified_folds(y, folds, rng)):
            train = np.ones(n, dtype=bool)
            train[te] = False
            lab = np.where(train, y, 0.0)
            prior = float(y[train].mean())
            in_train = np.where(order >= 0, train[np.clip(order, 0, None)], False)
            feats = {
                "as_of_today": oof_rate(order, in_train, lab, prior),
                "temporally_safe": oof_rate(order, in_train & temporal_ok, lab, prior),
                "self_inclusive": (oof_rate(order, in_train, lab, prior, k=K - 1) * K + y) / (K + 1),
                "random_graph": _random_rate(plans, train, lab, prior, np.random.default_rng(1000 * rep + f)),
            }
            for key in res:
                xx = x if key == "baseline" else np.column_stack([x, feats[key]])
                mu, sd = xx[train].mean(axis=0), xx[train].std(axis=0)
                sd[sd == 0] = 1.0
                w = fit_logistic((xx[train] - mu) / sd, y[train])
                res[key].append(auc(predict(w, (xx[te] - mu) / sd), y[te]))
    return {"auc": {k: float(np.mean(v)) for k, v in res.items()}, "sd": {k: float(np.std(v)) for k, v in res.items()},
            "folds_better_than_base": {k: int(sum(a > b for a, b in zip(v, res["baseline"], strict=True)))
                                       for k, v in res.items() if k != "baseline"},
            "knn_s": round(knn_s, 2), "n_features": x.shape[1]}


def _random_rate(plans: np.ndarray, train: np.ndarray, lab: np.ndarray, prior: float, rng: np.random.Generator,
                 a: float = 1.0) -> np.ndarray:
    out = np.empty(len(plans))
    for p in np.unique(plans):
        idx = np.flatnonzero(plans == p)
        pool = idx[train[idx]]
        for i in idx:
            c = pool[pool != i]
            pick = rng.choice(c, size=min(K, len(c)), replace=False)
            out[i] = (lab[pick].sum() + a * prior) / (len(pick) + a)
    return out


# --------------------------------------------------------------------------- report
def run(build: Path, folds: int, repeats: int, seed: int) -> dict:
    t0 = time.perf_counter()
    d = load(build)
    single = single_feature(d)
    reg = regression(d, folds, repeats, seed)
    man = d["manifest"]
    variants = []
    for key in ("baseline", "self_inclusive", "as_of_today", "temporally_safe", "random_graph"):
        variants.append({"key": key, "label": LABELS[key],
                         "single_feature": round(single[key], 4) if key in single else None,
                         "lr": round(reg["auc"][key], 4), "lr_sd": round(reg["sd"][key], 4)})
    is_s42 = man.get("seed") == 42 and man.get("n_users") == 8000
    deltas, ok = {}, {}
    for kind, ref in PLAN_REFERENCE.items():
        for key, want in ref.items():
            got = next(v["single_feature" if kind == "single_feature" else "lr"] for v in variants if v["key"] == key)
            deltas[f"{kind}:{key}"] = round(got - want, 4)
            ok[f"{kind}:{key}"] = abs(got - want) <= TOLERANCE
    return {
        "generated_by": "scripts/graph_leakage_demo.py", "build_id": man.get("business_build_id"),
        "profile": man.get("profile"), "seed": man.get("seed"), "n_users": man.get("n_users"),
        "commit": man.get("commit"), "n_model": single["n_model"], "lapses": single["lapses"], "variants": variants,
        "plan_reference": PLAN_REFERENCE, "deltas": deltas if is_s42 else None,
        "within_tolerance": ({"tolerance": TOLERANCE, "all": all(ok.values()), **ok} if is_s42 else
                             {"tolerance": TOLERANCE, "all": None, "note": "the plan figures are for seed 42, N 8000"}),
        "setup": {"neighbours": f"SIMILAR_TO top {K} (single feature); out-of-fold nearest {K} training "
                                f"renewals of the "
                                f"same plan, re-ranked from the persisted scaler (regression)",
                  "regression": f"logistic, L2 C=1, standardised per training fold, "
                                f"{len(spec.NUMERIC_FEATURES)} numeric "
                                f"gold features + plan one-hot ({reg['n_features']} columns)",
                  "cv": f"stratified {folds}-fold x {repeats} repeats, numpy seed {seed}",
                  "smoothing": "1 pseudo-count toward the training base rate (temporally safe, out-of-fold variants)",
                  "safe_neighbours_mean": single["safe_neighbours_mean"],
                  "folds_better_than_base": reg["folds_better_than_base"], "knn_s": reg["knn_s"],
                  "seconds": round(time.perf_counter() - t0, 1)},
        "note": ("The data has no relationships between subscriptions: any lift from a neighbour feature is leakage "
                 "(self-inclusive, as of today) or noise. Reported, never pinned."),
    }


def markdown(rep: dict) -> str:
    lines = ["# Leakage demo: AUC of a neighbour lapse-rate feature", "",
             f"Build `{rep['build_id']}` (profile {rep['profile']}, seed {rep['seed']}, N {rep['n_users']}); "
             f"{rep['n_model']:,} model-routed renewals, {rep['lapses']:,} voluntary lapses. "
             f"Generated by `scripts/graph_leakage_demo.py`.", "",
             "| variant | single-feature AUC | logistic regression AUC (sd) | plan (single / LR) |",
             "|---|---:|---:|---|"]
    for v in rep["variants"]:
        ref_s = rep["plan_reference"]["single_feature"].get(v["key"])
        ref_l = rep["plan_reference"]["lr"].get(v["key"])
        sf = "-" if v["single_feature"] is None else f"{v['single_feature']:.4f}"
        lines.append(f"| {v['label']} | {sf} | {v['lr']:.4f} ({v['lr_sd']:.4f}) | "
                     f"{'-' if ref_s is None else ref_s} / {'-' if ref_l is None else ref_l} |")
    wt = rep["within_tolerance"]
    lines += ["", f"Within +-{wt['tolerance']} of the plan's seed-42 figures: {wt['all']}.", "",
              f"Setup: {rep['setup']['regression']}; {rep['setup']['cv']}; {rep['setup']['neighbours']}; "
              f"{rep['setup']['smoothing']}.", "", rep["note"], ""]
    return "\n".join(lines)


def resolve(a: argparse.Namespace) -> Path | None:
    if a.build:
        return Path(a.build).resolve()
    root = Path(a.graph_root or os.environ.get("GRAPH_ROOT") or ROOT / "data" / "graph").resolve()
    for prof in ([a.profile] if a.profile else ["s42", "default"]):
        link = root / prof / "latest"
        if (link / "manifest.json").is_file():
            return link.resolve()
        builds = sorted((root / prof / "builds").glob("*/manifest.json")) if (root / prof).is_dir() else []
        if builds:
            return builds[-1].parent.resolve()
    return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Neighbour lapse-rate leakage demo (AUC, numpy only).")
    ap.add_argument("--graph-root", default=None)
    ap.add_argument("--profile", default=None, help="default: s42, else default")
    ap.add_argument("--build", default=None)
    ap.add_argument("--folds", type=int, default=5)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--json", default=None)
    ap.add_argument("--md", default=None)
    ap.add_argument("--check", action="store_true", help="exit 1 when a seed-42 figure is more than 0.02 off the plan")
    a = ap.parse_args(argv)
    build = resolve(a)
    if build is None:
        print(f"graph_leakage_demo: SKIP: no build for {a.profile or 's42 or default'} (make graph-sample "
              f"PROFILE=s42 && make graph-local PROFILE=s42); nothing measured", file=sys.stderr)
        return EXIT_NO_BUILD
    rep = run(build, a.folds, a.repeats, a.seed)
    if a.json:
        Path(a.json).write_text(json.dumps(rep, indent=1) + "\n", encoding="utf-8")
    md = markdown(rep)
    if a.md:
        Path(a.md).write_text(md, encoding="utf-8")
    print(md)
    ok = rep["within_tolerance"].get("all")
    return 1 if a.check and ok is False else 0


if __name__ == "__main__":
    sys.exit(main())
