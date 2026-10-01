"""Entity search for ``graph_find``: a standard-library inverted token index plus difflib typo repair.

What is indexed (and what is not):
  renewal          its id (whole, and the parts: ``sub_maya``, ``maya``, the renewal date), the
                   subscription id and the subscription's user_name words
  subscription     its id and user_name words
  incident         its id (``inc-002``, ``inc``, ``002`` / ``2``), the word "incident", every day of its
                   window and the month (``2026-08``, ``august``, ``aug``)
  pricing_change   its id (``cap-cut-2026-08``, ``cap``, ``cut``), "pricing" / "price" / "change", the
                   effective date and month, and the words of its description
The city is never indexed (and never loaded: context.ToolContext reads Subscription without it), so
"users in <city>" finds nothing. Nothing reaches Cypher: matching is pure Python over Parquet frames.

Scoring (deterministic): every query token adds idf = ln(1 + N / df) for each entity whose tokens hold
it exactly; a token with no exact hit counts 0.7 x idf for each indexed token it is a prefix of (3+
characters), else 0.6 x similarity x idf for its closest words by ``difflib.get_close_matches``
(alphabetic tokens of 3+ characters, cutoff 0.75): "Mya" still finds Maya. A query equal to an entity
id scores on top. Ties order by kind (renewal, subscription, pricing_change, incident), then id.
Stop words (articles, "find", "show", "renewal", "user", ...) are dropped; a query of stop words only
matches nothing.
"""
from __future__ import annotations

import bisect
import difflib
import math
import re
from collections import defaultdict
from dataclasses import dataclass

KINDS = ("renewal", "subscription", "pricing_change", "incident")
_KIND_ORDER = {k: i for i, k in enumerate(KINDS)}
STOP_WORDS = frozenset({
    "a", "an", "the", "of", "for", "in", "on", "at", "to", "and", "or", "by", "with", "about", "from", "is", "was",
    "find", "show", "me", "please", "what", "which", "who", "whose", "look", "up", "get", "id", "ids",
    "renewal", "renewals", "subscription", "subscriptions", "user", "users", "customer", "customers", "account",
    "sub",
})
MONTHS = ("january", "february", "march", "april", "may", "june", "july", "august", "september", "october",
          "november", "december")
_TOKEN = re.compile(r"[a-z0-9][a-z0-9_:%-]*")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ID_NOISE = frozenset({"sub"})
PREFIX_WEIGHT = 0.7
FUZZY_WEIGHT = 0.6
FUZZY_CUTOFF = 0.75
EXACT_ID_BONUS = 100.0
MAX_PREFIX_EXPANSION = 50


def tokens(text: str) -> list[str]:
    """Lower-case tokens of free text. An id stays whole and is also split: at ':' (a date part stays whole,
    plus its month), then at '_' and '-'; a zero-padded number also counts without its zeros (002 -> 2).
    The id prefix "sub" is not a token (every subscription has it)."""
    out: list[str] = []
    for tok in _TOKEN.findall(str(text).lower()):
        tok = tok.strip("_:-")
        if not tok:
            continue
        out.append(tok)
        for piece in tok.split(":"):
            if not piece:
                continue
            out.append(piece)
            if _DATE.match(piece):
                out.append(piece[:7])
                continue
            parts = [p for p in re.split(r"[_-]", piece) if p]
            if len(parts) > 1:
                out += parts
            out += [p.lstrip("0") for p in parts if p.isdigit() and p.lstrip("0") and p != p.lstrip("0")]
    return [t for t in dict.fromkeys(out) if t not in ID_NOISE]


def _month_tokens(date) -> list[str]:
    if date is None:
        return []
    iso = date.isoformat()[:10] if hasattr(date, "isoformat") else str(date)[:10]
    name = MONTHS[int(iso[5:7]) - 1]
    return [iso, iso[:7], name, name[:3]]


@dataclass(frozen=True)
class Entity:
    kind: str
    id: str
    display: str
    as_of: str | None
    route: str | None
    plan_tier: str | None
    date: str | None


class EntityIndex:
    """Inverted token index over renewals, subscriptions, incidents and pricing changes."""

    def __init__(self, entities: list[tuple[Entity, list[str]]]):
        self.entities = [e for e, _ in entities]
        self.by_id = {e.id: i for i, e in enumerate(self.entities)}
        postings: dict[str, set[int]] = defaultdict(set)
        for i, (_, toks) in enumerate(entities):
            for t in toks:
                postings[t].add(i)
        self.postings = {t: sorted(ix) for t, ix in postings.items()}
        self.vocab = sorted(self.postings)
        self.words = sorted(t for t in self.vocab if t.isalpha() and len(t) >= 3)
        n = max(1, len(self.entities))
        self.idf = {t: math.log(1.0 + n / len(ix)) for t, ix in self.postings.items()}

    # ------------------------------------------------------------------ construction
    @classmethod
    def from_context(cls, ctx) -> EntityIndex:
        ren = ctx.renewals().sort_index()
        subs = ctx.subscriptions().sort_index()
        names = dict(zip(subs["subscription_id"], subs["user_name"].fillna(""), strict=True))
        name_toks: dict[str, list[str]] = {}
        items: list[tuple[Entity, list[str]]] = []
        first: dict[str, tuple] = {}
        cols = zip(ren["renewal_id"], ren["subscription_id"], ren["as_of"], ren["route"], ren["plan_tier"],
                   ren["renewal_date"], strict=True)
        for rid, sid, as_of, route, plan, rdate in cols:
            name = names.get(sid, "")
            if name not in name_toks:
                name_toks[name] = _name_tokens(name)
            first.setdefault(sid, (_iso(as_of), route))
            items.append((Entity("renewal", rid, name, _iso(as_of), route, plan, _iso(rdate)),
                          tokens(rid) + tokens(sid) + name_toks[name]))
        for sid, name, plan in zip(subs["subscription_id"], subs["user_name"].fillna(""), subs["plan_tier"],
                                   strict=True):
            as_of, route = first.get(sid, (None, None))
            if name not in name_toks:
                name_toks[name] = _name_tokens(name)
            items.append((Entity("subscription", sid, name, as_of, route, plan, None), tokens(sid) + name_toks[name]))
        for row in ctx.nodes("PricingChange").sort_values("change_id").itertuples():
            desc = str(row.description or "")
            e = Entity("pricing_change", row.change_id, f"{desc} (effective {_iso(row.effective_date)})", None, None,
                       None, _iso(row.effective_date))
            toks = tokens(row.change_id) + ["pricing", "price", "change", "cut", "cap"] + \
                _month_tokens(row.effective_date) + tokens(desc)
            items.append((e, toks))
        for row in ctx.nodes("Incident").sort_values("incident_id").itertuples():
            days = _date_range(row.starts_on, row.ends_on)
            e = Entity("incident", row.incident_id,
                       f"incident {_iso(row.starts_on)} to {_iso(row.ends_on)} ({int(row.days)} days)", None, None,
                       None, _iso(row.starts_on))
            toks = tokens(row.incident_id) + ["incident", "outage"] + [t for d in days for t in _month_tokens(d)]
            items.append((e, toks))
        return cls(items)

    # ------------------------------------------------------------------ query
    def search(self, query: str, kind: str = "any", limit: int = 5) -> tuple[list[dict], int]:
        """(top matches, number of entities that matched at all)."""
        wanted = None if kind == "any" else kind
        q = " ".join(str(query).split()).lower()
        scores: dict[int, float] = defaultdict(float)
        how: dict[int, str] = {}
        exact = self.by_id.get(q)
        if exact is not None:
            scores[exact] += EXACT_ID_BONUS
            how[exact] = "id"
        for tok in [t for t in tokens(q) if t not in STOP_WORDS]:
            hits = self._expand(tok)
            for term, weight, kind_of_match in hits:
                for i in self.postings[term]:
                    scores[i] += weight * self.idf[term]
                    if how.get(i) not in ("id", "token"):
                        how[i] = kind_of_match
        ranked = sorted(((s, i) for i, s in scores.items()
                         if s > 0 and (wanted is None or self.entities[i].kind == wanted)),
                        key=lambda si: (-round(si[0], 9), _KIND_ORDER[self.entities[si[1]].kind],
                                        self.entities[si[1]].id))
        out = []
        for s, i in ranked[:limit]:
            e = self.entities[i]
            row = {"id": e.id, "kind": e.kind, "display": e.display, "as_of": e.as_of, "route": e.route,
                   "plan_tier": e.plan_tier, "match": how.get(i, "token"), "score": round(s, 3)}
            if e.kind in ("incident", "pricing_change"):
                row["date"] = e.date
            elif e.kind == "renewal":
                row["renewal_date"] = e.date
            out.append(row)
        return out, len(ranked)

    def _expand(self, tok: str) -> list[tuple[str, float, str]]:
        if tok in self.postings:
            return [(tok, 1.0, "token")]
        if len(tok) >= 3:
            lo = bisect.bisect_left(self.vocab, tok)
            prefixed = []
            for t in self.vocab[lo:lo + MAX_PREFIX_EXPANSION]:
                if not t.startswith(tok):
                    break
                prefixed.append((t, PREFIX_WEIGHT, "prefix"))
            if prefixed:
                return prefixed
        if tok.isalpha() and len(tok) >= 3:
            close = difflib.get_close_matches(tok, self.words, n=3, cutoff=FUZZY_CUTOFF)
            return [(t, FUZZY_WEIGHT * difflib.SequenceMatcher(None, tok, t).ratio(), "fuzzy") for t in close]
        return []


def _iso(value) -> str | None:
    if value is None:
        return None
    try:
        if value != value:  # NaT / NaN
            return None
    except (TypeError, ValueError):
        return None
    return value.isoformat()[:10] if hasattr(value, "isoformat") else str(value)[:10]


def _name_tokens(name: str) -> list[str]:
    return [t for t in tokens(name) if t not in STOP_WORDS]


def _date_range(start, end) -> list:
    import datetime as dt

    s = start if isinstance(start, dt.date) else dt.date.fromisoformat(str(start)[:10])
    e = end if isinstance(end, dt.date) else dt.date.fromisoformat(str(end)[:10])
    return [s + dt.timedelta(days=i) for i in range((e - s).days + 1)]
