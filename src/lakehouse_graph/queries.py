"""Named Cypher templates for the renewal graph (LadybugDB dialect).

Two kinds of template:
  * TOOL_TEMPLATES   what agent tools may run; ``fetch()`` serves these by default;
  * CONTRACT_ONLY    what only the graph contract may run (``fetch(..., contract=True)``): the
    ``contract_*`` templates, which compute the unbounded ("naive") variants on purpose to
    prove the leak, and the SIMILAR_TO invariants / goldens that read neighbour outcomes with
    no visibility bound (similar_top_k, similar_nearest_lapses, similar_sharing_neighbours,
    similar_edge_checks). ``fetch()`` refuses them unless the caller says it is the contract.

Rules for every template (``lint()``; run by tests/graph/test_queries_lint.py and the contract):
  * ORDER BY and a final LIMIT: result order is undefined without ORDER BY;
  * no UNION: in Ladybug a trailing ORDER BY only orders the last branch, so evidence is
    three templates merged and sorted in Python (``evidence()``);
  * read only: no mutation, procedure call or file statement;
  * ``{label}`` / ``{rel}`` placeholders are filled by ``render()`` from spec names only;
    values always travel as ``$parameters``.

The leak rule, for every tool template (nothing after a renewal's as_of, no label by the back door):
  1. source events: every Subscription->event edge it matches is bound to a variable, starts at
     a named Subscription variable ``s`` and is filtered ``e.event_date <= r.as_of`` (+ the
     feature window where one applies), where ``r`` is the renewal of that same subscription
     (``(s)-[:HAS_RENEWAL]->(r)``), never another renewal's as_of; when the template names a
     source renewal by $renewal_id, ``r`` is that source (a second renewal of the same
     subscription is later, and its as_of bounds nothing for the source). An event node
     (LimitHit, OverageChange, OverageCharge, Ticket, BillingEvent) is only introduced as the head
     of such a bounded edge;
  2. outcome evidence: a template that can match BILLED also filters
     ``NOT coalesce(e.outcome_evidence, false)``: BILLED edges that reveal the outcome are
     never returned;
  3. neighbour outcomes: a template that relates renewals (it walks SIMILAR_TO, or matches more
     than one Renewal variable) binds exactly one source by ``$renewal_id`` and may read another
     renewal's ``outcome``, ``outcome_observed_on``, ``route``, ``churned`` or ``is_reference``
     only under the visibility rule ``visible(n, src)`` = ``n.outcome_observed_on <= src.as_of``:
     either as a row filter, or masked behind ``CASE WHEN visible THEN ...`` where ``visible``
     is ``(<that predicate>) AS visible`` in a WITH, optionally widened by
     ``visible_or_current()``: the caller asked for today's view (``$today``) AND the source
     itself is current (route score_today / pending: its as_of is the current T-7). A
     historical source is never served a neighbour outcome observed after its as_of, whatever
     the parameters. Use similar_top_k_visible and similar_nearest_lapses_known_by_as_of.
     The source is a node variable that can be a Renewal, bound by ``{renewal_id: $renewal_id}``
     in a plain MATCH or by ``src.renewal_id = $renewal_id`` as a row-filter conjunct (a map, or
     a WHERE, of an OPTIONAL MATCH restricts no row, so it never binds a source).
     One identity: the rows of a template that relates renewals describe its source and nothing
     else, whether or not it reads an outcome field, so the rule is never measured against one
     renewal while the rows describe another:
       * every other renewal (named or anonymous) is one of the source's own relations: reached
         from it through the MATCH patterns with no other renewal in between (a Subscription,
         Plan, Incident or PricingChange may be in between; a recursive SIMILAR_TO path is one
         step, its inner renewals are never bound). A disconnected ``(h:Renewal)``, the
         neighbour of a neighbour, or a renewal picked by a WITH ... SKIP / LIMIT and walked on
         from would each let the rows describe that renewal;
       * and it hangs off the source alone: call the source's side the source and the
         subscriptions / hubs it reaches with no renewal in between; beyond that side, what hangs
         off one renewal (its own SIMILAR_TO neighbours, its subscription, a hub only it reaches)
         holds no second renewal. So a relationship between two renewals (SIMILAR_TO, a recursive
         path, a ``{rel}``) has the source as one end, and a renewal reached through a hub is a row
         that is read, never an anchor: ``(r)-[:ON_PLAN]->(p)<-[:ON_PLAN]-(h)``, h pinned by a
         literal or a WITH ... SKIP, then ``(n)-[:ON_PLAN]->(p), (h)-[:SIMILAR_TO]->(n)`` would serve
         h's neighbours under r's as_of. Nor does any expression read two renewals other than the
         source (a WHERE conjunct, a WITH / RETURN / ORDER BY item, an UNWIND or a pattern map
         value, following aliases): ``abs(n.agent_requests_28d - h.agent_requests_28d) < 5``
         relates them as an edge would. With both, the rows on another renewal depend on nothing
         but that renewal and the source, so pinning it (a literal, a WITH ... SKIP) selects which
         of the source's relations come back and describes no one else;
       * its parameters are $renewal_id (the source binding, or ``x.renewal_id <> $renewal_id``),
         $k / $limit only as the value of the final LIMIT / SKIP or in ``<rel>.rank <= $k``, and
         $today only inside visible_or_current(): no parameter can pin a second renewal, whatever
         its name or type ($rid, $subscription_id, $as_of are refused by name; an integer $k
         compared with a feature value by position);
       * an identifying field (renewal_id, subscription_id, user_name, event_id, ticket_id), or
         an alias of one, is only projected (a whole WITH / RETURN item), sorted by in the final
         ORDER BY or compared exactly with $renewal_id (``=`` / ``<>``, both sides whole): never
         wrapped in a function, compared with a literal or another parameter, or sorted by in a
         WITH before its SKIP / LIMIT;
  4. FIRST_RENEWAL_AFTER is the one declared exception: selected by the gold rule and always
     returned with ``known_by_as_of``;
  5. named properties only: a template never returns or passes on a whole node, relationship
     or path (``RETURN n``, ``RETURN *``, ``properties(n)``, ``collect(n)``, ``nodes(p)``,
     ``n AS m``), and never writes ``<anything>.*`` anywhere (``RETURN n.*`` is every property of
     n): every outcome field would travel with it, past rules 1-3. A bare pattern variable is
     allowed as a plain ``WITH`` pass-through and inside ``count()``, ``cost()``, ``length()``
     and ``label()`` only;
  6. the calendar a named renewal sees: a template that names a renewal by $renewal_id sees the
     plan cuts and incidents on or before that source's as_of (spec.PIT_WINDOWS). It walks
     CUT_CAP (or a ``{rel}`` placeholder, which may be rendered as CUT_CAP) only with
     ``c.event_date <= r.as_of`` against that source r, and matches a PricingChange only as the
     tail of such a bounded cut, as the head of the source's own FIRST_RENEWAL_AFTER (rule 4) or
     with ``p.effective_date <= r.as_of``, and an Incident only as the head of an as_of-bounded
     EXPOSED_TO (rule 1) or with ``i.starts_on <= r.as_of`` (each a row-filter conjunct). The
     CUT_CAP edge carries nothing the PricingChange node lacks (effective_date, cap_multiplier),
     so bounding the edge and not the node would bound nothing. When an incident ended
     (``i.ends_on``, ``i.days``) is known only once it is over, so a template that names a renewal
     reads it only with ``i.ends_on <= r.as_of`` as well (an incident still running at as_of ends
     after it). A template that names no renewal reads the public calendar (see below);
  7. population rows: a template that reads the outcome fields of renewals it does not name by
     $renewal_id, and relates no renewals, returns aggregates grouped by cohort keys:
       * its RETURN has count / sum and it has no collect();
       * every grouping is keyed by cohort values: each item of the RETURN, and of any WITH that
         aggregates, is an aggregate or a grouping key, and a grouping key reads (outside the
         aggregate calls of its own clause, following WITH / UNWIND aliases, map aliases
         included) only COHORT_KEYS, properties with a handful of values (plan_tier, route,
         outcome, a flag). So an outcome field reaches the rows only inside an aggregate or as
         the cohort it groups by; outcome_observed_on, a date, an amount or a feature value
         never is a key (a group per date is a row per renewal). A WITH may keep the renewal
         itself as its key (``WITH r, count(f) > 0 AS first_after``: one group per renewal,
         which the RETURN regroups), and an alias an earlier WITH defined by an aggregate is a
         per-group value: used as a key it reads what it aggregates (``min(n.as_of) AS day`` is
         a date), and a count or sum is a number, a key only as the flag ``<aggregate> <op> 0``
         where it is written (``count(f) > 0``; the number of a renewal's events is a feature
         value, and ``count(e) = 7`` picks renewals by it as a literal pin would);
       * no row is chosen before the final RETURN: no SKIP / LIMIT in a WITH (``WITH n ORDER BY
         n.as_of SKIP $k LIMIT 1`` serves one renewal at a time);
       * it neither returns nor filters on an identifying field, and every parameter (except the
         value of the final LIMIT / SKIP) is one whole side of a comparison whose other whole
         side is ``<pattern variable>.<key>``, or the whole value of ``key`` in a MATCH pattern
         map, where key is a cohort key or a hub key (HUB_KEYS: plan_tier, incident_id,
         change_id): so a parameter cannot narrow the population to one renewal
         (``cast(n.as_of, 'STRING') + n.plan_tier = $p`` pins a date: arithmetic, a
         concatenation or a function on either side is refused, and so is a parameter in a map
         literal or compared with an alias);
       * a literal is read like a parameter: a comparison that holds a string, a number or an
         alias of one (in a WHERE, a CASE, an aggregate), a simple ``CASE x WHEN ...`` and a
         MATCH pattern map read only cohort or hub keys (``n.agent_requests_28d = 211`` and
         ``sum(CASE WHEN n.as_of = date('...') ...)`` are one renewal). The one exception is the
         PIT window ``e.event_date > r.as_of - INTERVAL('<n> DAYS')``;
       * an aggregate is a cell statistic, the count of the renewals of a (sub-)cell: count or
         sum. avg is sum / count, two statistics in one number (a rate); min / max return the
         extreme renewal's own value (``min(n.as_of)`` grouped by outcome is that renewal and its
         label); collect and the engine's other aggregates (histogram, percentiles) carry values of
         single rows: all refused, anywhere in the template. Its argument is a vetted shape:
         ``*``; a pattern variable (count only, also ``count(DISTINCT v)``); a cohort-key property
         (sum: a 0/1 cohort flag, COHORT_FLAGS); a literal (sum: 0 or 1); ``CASE WHEN <cohort
         predicate> THEN <value> ... [ELSE <value>] END`` or ``CASE <cohort-key property> WHEN
         <literal> THEN <value> ... END``, a value being a literal (sum: 0 or 1), null or such a
         property, and a cohort predicate comparing cohort or hub keys with each other, literals
         or parameters (AND / OR / XOR / NOT, IS [NOT] NULL, IN [literals]); or an alias of one of
         these, or of such a count / sum in a WITH grouped by cohort keys only (a cell of an
         earlier grouping, a value there, never a predicate operand: ``c = 1`` picks groups by
         size; in a WITH that keeps a pattern variable the aggregate is a per-renewal value, a
         feature, unless it is the flag ``count(f) > 0``). So every renewal adds 0 or 1 to a sum,
         which counts a sub-cell. No arithmetic, concatenation or function inside:
         ``max(n.agent_requests_28d * 10 + n.churned)``, ``sum(n.churned * pow(2.0,
         n.agent_requests_28d % 50))`` and ``max(cast(n.as_of, 'STRING') + n.outcome)`` read
         cohort keys only beside a near-unique value, yet encode single renewals' labels in cells
         of any size, and ``sum(CASE WHEN <sub-cell> AND n.churned = 1 THEN 1001 WHEN <sub-cell>
         THEN 1 ELSE 0 END)`` is a sub-cell's lapses times 1000 plus its size;
       * one statistic per column: an item of the RETURN, of a WITH that aggregates, or of an
         ORDER BY, that holds an aggregate is that aggregate alone; only in a WITH that keeps a
         pattern variable (one group per renewal) may it be the flag ``<aggregate> <op> 0``. No
         arithmetic, function, comparison or CASE around or between aggregates (``sum(a) * 1000 +
         sum(b)`` packs two cells into one number; the RETURN flag ``sum(<sub-cell>) = 1`` says a
         sub-cell holds one renewal, and ``sum(<sub-cell lapsed>) = 1`` its label), and an alias of
         an aggregate travels on whole in every later clause: a whole item, the argument of count /
         sum, a CASE value or a flag condition, never ``l * 1000 + c`` in a WITH, a WHERE or an
         ORDER BY;
       * every aggregate counts each renewal once: count(*) and sum count rows, so the rows a
         grouping counts hold one per renewal. A MATCH keeps that when its relationships are the
         renewal's HAS_RENEWAL (its subscription), ON_PLAN (its plan) and FIRST_RENEWAL_AFTER to a
         pricing change pinned by change_id, and every node is the renewal, one of those or their
         end; a cartesian MATCH, an UNWIND, an edge to events or to the calendar hubs can repeat a
         renewal a number of times the template chooses (``MATCH (p:Plan)`` beside the renewal with
         ``WHERE p.plan_tier = 'pro' OR <sub-cell lapsed>`` counts those renewals three times), so
         such rows are collapsed first: ``WITH DISTINCT r``, or a WITH that keeps only the renewal
         (or its subscription / plan) and aggregates per renewal (``WITH r, count(f) > 0 AS flag``).
     What no static rule can see is a cell that happens to hold few renewals (a hub hit by one
     subscription, a tiny plan, a coincidence between two properties such as ``n.a = n.b``, a
     sub-cell counted by ``sum(CASE ...)``), or two columns (or two templates) whose difference is
     one: the TOOL LAYER suppresses every count of fewer than 5 renewals before it serves one, with
     complements over the margins it publishes. With the rules above every aggregate column is
     such a count (rule 7 vets the argument, the column and the rows; tests/graph/test_queries_lint.py
     plants, executes and decodes each refused packing on the tiny build and fuzzes the rest).
     Population templates are served by tools.py only, through ``incident_table`` /
     ``pricing_table`` (graph_exposure: every plan x route count and the known_by_as_of split,
     under metrics.MIN_CELL with complements proven by ``metrics.protect()``); viz.py sums them
     into the always-shown totals. The other population answers come from metrics.py (MIN_CELL,
     ``metrics.protect()``), which computes them in pandas. tests/graph/test_queries_lint.py fails
     when a module under src/lakehouse_graph, at any depth, reaches a population template
     (population_templates()) in a fetch(), render() or TEMPLATES[...] other than through those, or
     reaches any template by a computed name or by reading the catalog, outside the two places
     that forward a caller's literal name (ToolContext.fetch, and the lineage package's own
     catalog). scripts/ are not agent surfaces (the contract prints these totals in its report).
A bound or filter counts only when it is a whole conjunct of a WHERE that filters rows: the
exact predicate, not negated, with nothing added to the as_of side; not inside a CASE, not
beside an OR / XOR, not after a LIMIT / SKIP (the rows were chosen before it), and not in the
WHERE of an OPTIONAL MATCH that did not introduce the variable (that WHERE only decides whether
the optional part matches; the rows stay).
Names: a name means one thing, so ``r.as_of`` in a bound means the renewal r. No WITH / UNWIND /
RETURN alias re-uses the name of a pattern variable (``{as_of: date('2999-12-31')} AS r`` would
satisfy the exact bound while bounding nothing; after RETURN, ORDER BY would read the alias), no
alias is defined twice (WITH, UNWIND and RETURN together), no name is a keyword, and no variable
is used after a WITH dropped it (nor re-bound by a later MATCH).
What the lint cannot read it refuses: the dialect is case-insensitive and permissive, so a tool
template has no comments, back-ticked names, backslashes in strings, subqueries (EXISTS / COUNT
blocks), comprehensions or lambdas, patterns outside MATCH (pattern predicates, size(pattern)),
and no untyped or undirected event relationships.
Population templates (count_*, routes, exposure_*, motif_*, first_renewal_after_by_plan) return
totals and cohort aggregates "as of today" (rule 7); they name no source renewal, return no event
or neighbour rows, and are descriptive (renewal_header returns the named renewal's own route).
``()-[e:TYPE]->()`` / ``(n:Label)`` used only inside ``count()`` is such a total. Plan, Incident
and PricingChange are hubs on the public calendar: with no renewal named the lint leaves them
alone (the pricing and incident calendar is public), and for a named renewal rule 6 cuts the
calendar at its as_of (Plan carries no date).

Vetted shapes (an allowlist on top of the rules): ``lint()`` of this module's catalog also requires
every tool template to be one of VETTED_TOOL_TEMPLATES, by name and by ``fingerprint()`` (its tokens
as the lint reads them: layout and keyword case do not count, anything else does). The rules above
decide what MAY be served; the allowlist pins what IS served, so a tool template is never edited
or added silently: a new or changed one passes the rules, the executed leak tests and the
differential fuzz in tests/graph/test_queries_lint.py, and then its fingerprint is added here.

Later phases may ADD templates (and their fingerprints); existing ones are part of the contract and
are not edited. Ladybug reserved words to avoid in new templates: ``Column``, ``on``.
"""
from __future__ import annotations

import hashlib
import itertools
import re
from typing import NamedTuple

from . import spec

_W = spec.PIT_WINDOWS
_EVENTS = "|".join(spec.EVENT_RELATIONS)
ROW_LIMIT = 1_000_000  # default $limit for whole-population contract templates
OUTCOME_FIELDS = ("outcome_observed_on", "outcome", "route", "churned")  # what reveals a renewal's label
# What the lint guards on another renewal: the outcome fields and is_reference (= route is 'model').
LABEL_FIELDS = (*OUTCOME_FIELDS, "is_reference")
NOT_YET_OBSERVED = "not_yet_observed"  # what a masked neighbour outcome reads as


class ContractOnlyError(RuntimeError):
    """A contract-only template was requested without ``contract=True``."""


def visible(nbr: str, src: str) -> str:
    """The visibility rule: the neighbour's outcome was already observed at the source's as_of."""
    return f"{nbr}.outcome_observed_on <= {src}.as_of"


def visible_or_current(nbr: str, src: str) -> str:
    """visible(), or the caller wants today's view ($today) and the source itself is current."""
    return f"{visible(nbr, src)} OR ($today AND ({src}.route = 'score_today' OR {src}.route = 'pending'))"


def _win(rel: str, var: str = "e") -> str:
    """PIT window predicate for a Subscription->event relation (as_of upper bound included)."""
    days = _W[rel].days
    return f"{var}.event_date <= r.as_of AND {var}.event_date > r.as_of - INTERVAL('{days} DAYS')"


def _naive(rel: str, var: str = "e") -> str:
    """The same window WITHOUT the as_of upper bound (what a naive traversal computes)."""
    return f"{var}.event_date > r.as_of - INTERVAL('{_W[rel].days} DAYS')"


TEMPLATES: dict[str, str] = {
    # ---- counts ----------------------------------------------------------------------
    # the column is ``n`` (callers read row["n"]); the pattern variable is x: a name means one thing
    "count_nodes": "MATCH (x:{label}) RETURN count(x) AS n ORDER BY n LIMIT 1",
    "count_rels": "MATCH ()-[e:{rel}]->() RETURN count(e) AS n ORDER BY n LIMIT 1",
    "routes": """
MATCH (r:Renewal)
RETURN r.route AS route, count(*) AS n, sum(r.churned) AS lapses
ORDER BY route LIMIT 20""",
    "renewals_per_subscription": """
MATCH (s:Subscription)
OPTIONAL MATCH (s)-[h:HAS_RENEWAL]->(:Renewal)
WITH s, count(h) AS n
RETURN n AS renewals, count(*) AS subscriptions
ORDER BY renewals LIMIT 20""",

    # ---- point-in-time parity (contract): per renewal, gold vs windowed vs naive -------
    "contract_pit_limit_hits_14d": f"""
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal)
OPTIONAL MATCH (s)-[e:HIT_LIMIT]->(:LimitHit)
WITH r, sum(CASE WHEN {_win('HIT_LIMIT')} THEN 1 ELSE 0 END) AS pit,
        sum(CASE WHEN {_naive('HIT_LIMIT')} THEN 1 ELSE 0 END) AS naive
RETURN r.renewal_id AS renewal_id, r.limit_hits_14d AS gold, pit, naive
ORDER BY renewal_id LIMIT $limit""",
    "contract_pit_support_tickets_90d": f"""
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal)
OPTIONAL MATCH (s)-[e:OPENED]->(:Ticket)
WITH r, sum(CASE WHEN {_win('OPENED')} THEN 1 ELSE 0 END) AS pit,
        sum(CASE WHEN {_naive('OPENED')} THEN 1 ELSE 0 END) AS naive
RETURN r.renewal_id AS renewal_id, r.support_tickets_90d AS gold, pit, naive
ORDER BY renewal_id LIMIT $limit""",
    "contract_pit_incident_exposed_28d": f"""
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal)
OPTIONAL MATCH (s)-[e:EXPOSED_TO]->(:Incident)
WITH r, sum(CASE WHEN {_win('EXPOSED_TO')} THEN 1 ELSE 0 END) AS pit_days,
        sum(CASE WHEN {_naive('EXPOSED_TO')} THEN 1 ELSE 0 END) AS naive_days
RETURN r.renewal_id AS renewal_id, r.incident_exposed_28d AS gold,
       CASE WHEN pit_days > 0 THEN 1 ELSE 0 END AS pit, CASE WHEN naive_days > 0 THEN 1 ELSE 0 END AS naive
ORDER BY renewal_id LIMIT $limit""",
    "contract_pit_overage_usd_28d": f"""
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal)
OPTIONAL MATCH (s)-[e:CHARGED_OVERAGE]->(:OverageCharge)
WITH r, sum(CASE WHEN {_win('CHARGED_OVERAGE')} THEN e.amount_usd ELSE 0.0 END) AS pit
RETURN r.renewal_id AS renewal_id, r.overage_usd_28d AS gold, round(pit, 2) AS pit
ORDER BY renewal_id LIMIT $limit""",
    "contract_pit_overage_toggled_off": """
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal)
OPTIONAL MATCH (s)-[e:CHANGED_OVERAGE]->(:OverageChange)
WITH r, max(CASE WHEN e.event_date <= r.as_of AND e.state = 'disabled' THEN e.event_date END) AS last_off,
        max(CASE WHEN e.event_date <= r.as_of AND e.state = 'enabled' THEN e.event_date END) AS last_on
RETURN r.renewal_id AS renewal_id, r.overage_toggled_off AS gold,
       CASE WHEN last_off IS NOT NULL AND last_on IS NOT NULL AND last_off > last_on THEN 1 ELSE 0 END AS pit
ORDER BY renewal_id LIMIT $limit""",
    # Declared exception: the gold rule is "an edge exists" (no as_of filter). The as_of
    # filtered count is returned too, so the contract can assert exactly how many differ.
    "contract_pit_first_renewal_after": """
MATCH (r:Renewal)
OPTIONAL MATCH (r)-[f:FIRST_RENEWAL_AFTER]->(:PricingChange)
WITH r, count(f) AS gold_rule,
        sum(CASE WHEN f.event_date <= r.as_of THEN 1 ELSE 0 END) AS as_of_filtered,
        sum(CASE WHEN f.known_by_as_of <> (f.event_date <= r.as_of) THEN 1 ELSE 0 END) AS flag_inconsistent
RETURN r.renewal_id AS renewal_id, r.first_renewal_after_pricing_change AS gold,
       CASE WHEN gold_rule > 0 THEN 1 ELSE 0 END AS pit,
       CASE WHEN as_of_filtered > 0 THEN 1 ELSE 0 END AS as_of_filtered, flag_inconsistent
ORDER BY renewal_id LIMIT $limit""",
    "contract_billed_by_renewal": """
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal)
OPTIONAL MATCH (s)-[e:BILLED]->(:BillingEvent)
WITH r, sum(CASE WHEN e.event_date <= r.as_of THEN 1 ELSE 0 END) AS on_or_before,
        sum(CASE WHEN e.event_date <= r.as_of AND e.event_type = 'cancel_scheduled' THEN 1 ELSE 0 END) AS scheduled,
        sum(CASE WHEN e.outcome_evidence = false THEN 1 ELSE 0 END) AS not_outcome_evidence
RETURN r.renewal_id AS renewal_id, r.route AS route, on_or_before, scheduled, not_outcome_evidence
ORDER BY renewal_id LIMIT $limit""",
    "contract_post_as_of_edges": """
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal)
MATCH (s)-[e:{rel}]->()
WHERE e.event_date > r.as_of
WITH r, count(e) AS n
RETURN sum(n) AS edges, count(*) AS renewals
ORDER BY edges LIMIT 1""",
    "contract_first_renewal_after_unknown": """
MATCH (r:Renewal)-[f:FIRST_RENEWAL_AFTER]->(p:PricingChange)
WHERE f.known_by_as_of = false
RETURN p.change_id AS change_id, count(f) AS edges
ORDER BY change_id LIMIT 20""",

    # ---- SIMILAR_TO invariants -------------------------------------------------------------
    "similar_out_degree": """
MATCH (r:Renewal)
OPTIONAL MATCH (r)-[e:SIMILAR_TO]->(:Renewal)
WITH r, count(e) AS out_degree
RETURN out_degree, count(*) AS sources
ORDER BY out_degree LIMIT 50""",
    "similar_edge_checks": """
MATCH (a:Renewal)-[e:SIMILAR_TO]->(b:Renewal)
RETURN count(e) AS edges,
       sum(CASE WHEN a.plan_tier <> b.plan_tier THEN 1 ELSE 0 END) AS cross_plan,
       sum(CASE WHEN b.route <> 'model' THEN 1 ELSE 0 END) AS dst_not_model,
       sum(CASE WHEN a.renewal_id = b.renewal_id THEN 1 ELSE 0 END) AS self_loops,
       sum(CASE WHEN e.mutual THEN 1 ELSE 0 END) AS mutual_flagged,
       count(DISTINCT b.renewal_id) AS distinct_dst
ORDER BY edges LIMIT 1""",
    "similar_mutual_edges": """
MATCH (a:Renewal)-[:SIMILAR_TO]->(b:Renewal)-[:SIMILAR_TO]->(a)
RETURN count(*) AS mutual_edges
ORDER BY mutual_edges LIMIT 1""",
    "similar_max_in_degree": """
MATCH (:Renewal)-[e:SIMILAR_TO]->(d:Renewal)
WITH d, count(e) AS in_degree
RETURN d.renewal_id AS renewal_id, in_degree
ORDER BY in_degree DESC, renewal_id LIMIT 1""",

    # ---- evidence for one renewal (PIT-safe; merged by evidence()) ---------------------------
    "evidence_events": f"""
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal {{renewal_id: $renewal_id}})
MATCH (s)-[e:{_EVENTS}]->(x)
WHERE e.event_date <= r.as_of AND NOT coalesce(e.outcome_evidence, false)
RETURN e.event_date AS event_date, label(e) AS relation,
       coalesce(x.event_id, x.ticket_id, x.incident_id) AS target_id,
       x.limit_type AS limit_type, e.state AS state, e.amount_usd AS amount_usd, e.event_type AS event_type,
       r.as_of AS as_of, r.renewal_date AS renewal_date
ORDER BY event_date, relation, target_id LIMIT $limit""",
    "evidence_first_renewal_after": """
MATCH (r:Renewal {renewal_id: $renewal_id})-[f:FIRST_RENEWAL_AFTER]->(p:PricingChange)
RETURN f.event_date AS event_date, 'FIRST_RENEWAL_AFTER' AS relation, p.change_id AS target_id,
       f.known_by_as_of AS known_by_as_of, NOT f.known_by_as_of AS declared_exception,
       r.as_of AS as_of, r.renewal_date AS renewal_date
ORDER BY event_date, target_id LIMIT $limit""",
    "evidence_cut_cap": """
MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(pl:Plan)<-[c:CUT_CAP]-(p:PricingChange)
WHERE c.event_date <= r.as_of
RETURN c.event_date AS event_date, 'CUT_CAP' AS relation, p.change_id AS target_id, pl.plan_tier AS plan_tier,
       r.as_of AS as_of, r.renewal_date AS renewal_date
ORDER BY event_date, target_id LIMIT $limit""",
    "contract_evidence_hidden_after_as_of": f"""
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal {{renewal_id: $renewal_id}})
MATCH (s)-[e:{_EVENTS}]->()
WHERE e.event_date > r.as_of
RETURN count(e) AS hidden
ORDER BY hidden LIMIT 1""",

    # ---- neighbours ------------------------------------------------------------------------------
    "renewal_header": """
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal {renewal_id: $renewal_id})
RETURN r.renewal_id AS renewal_id, r.subscription_id AS subscription_id, r.plan_tier AS plan_tier,
       r.as_of AS as_of, r.renewal_date AS renewal_date, r.route AS route, s.city AS city
ORDER BY renewal_id LIMIT 1""",
    "similar_top_k": """
MATCH (r:Renewal {renewal_id: $renewal_id})-[e:SIMILAR_TO]->(n:Renewal)
RETURN e.rank AS rank, n.renewal_id AS renewal_id, e.d2_q AS d2_q, e.dist AS dist, e.mutual AS mutual,
       n.outcome AS outcome, n.route AS route, n.outcome_observed_on AS outcome_observed_on
ORDER BY rank LIMIT $k""",
    "similar_nearest_lapses": """
MATCH p = (a:Renewal)-[e:SIMILAR_TO* WSHORTEST(dist)]->(b:Renewal)
WHERE a.renewal_id = $renewal_id AND b.outcome = 'voluntary_lapse'
RETURN b.renewal_id AS renewal_id, round(cost(e), 4) AS path_dist
ORDER BY path_dist, renewal_id LIMIT $k""",
    # The tool-facing variant of similar_top_k: same rows, but a neighbour's outcome is masked
    # ('not_yet_observed', null date) unless it is visible under the rule in the module docstring.
    "similar_top_k_visible": f"""
MATCH (r:Renewal {{renewal_id: $renewal_id}})-[e:SIMILAR_TO]->(n:Renewal)
WITH r, e, n, ({visible_or_current('n', 'r')}) AS visible
RETURN e.rank AS rank, n.renewal_id AS renewal_id, e.d2_q AS d2_q, e.dist AS dist, e.mutual AS mutual,
       CASE WHEN visible THEN n.outcome ELSE '{NOT_YET_OBSERVED}' END AS outcome,
       CASE WHEN visible THEN n.outcome_observed_on END AS outcome_observed_on, visible AS outcome_visible
ORDER BY rank LIMIT $k""",
    # The tool-facing variant: a neighbour's lapse is an outcome, so it is only served when it was
    # already observed at the source renewal's as_of (no outcome from the source's future).
    "similar_nearest_lapses_known_by_as_of": """
MATCH p = (a:Renewal)-[e:SIMILAR_TO* WSHORTEST(dist)]->(b:Renewal)
WHERE a.renewal_id = $renewal_id AND b.outcome = 'voluntary_lapse' AND b.outcome_observed_on <= a.as_of
RETURN b.renewal_id AS renewal_id, round(cost(e), 4) AS path_dist, b.route AS route,
       b.outcome_observed_on AS outcome_observed_on
ORDER BY path_dist, renewal_id LIMIT $k""",
    "similar_sharing_neighbours": """
MATCH (r:Renewal {renewal_id: $renewal_id})-[:SIMILAR_TO]->(n:Renewal)<-[:SIMILAR_TO]-(o:Renewal)
WHERE o.renewal_id <> $renewal_id
WITH DISTINCT o
RETURN o.route AS route, count(*) AS renewals
ORDER BY route LIMIT 20""",

    # ---- hub exposure ----------------------------------------------------------------------------
    "exposure_incident_by_plan": f"""
MATCH (i:Incident {{incident_id: $incident_id}})<-[e:EXPOSED_TO]-(s:Subscription)-[:HAS_RENEWAL]->(r:Renewal)
WHERE {_win('EXPOSED_TO')}
WITH DISTINCT r
RETURN r.plan_tier AS plan_tier, count(*) AS exposed,
       sum(CASE WHEN r.route = 'model' THEN 1 ELSE 0 END) AS model,
       sum(CASE WHEN r.route = 'model' THEN r.churned ELSE 0 END) AS voluntary_lapses,
       sum(CASE WHEN r.route = 'cancel_flow' THEN 1 ELSE 0 END) AS cancel_flow,
       sum(CASE WHEN r.route = 'dunning' THEN 1 ELSE 0 END) AS dunning
ORDER BY plan_tier LIMIT 10""",
    "contract_exposure_incident_naive": """
MATCH (i:Incident {incident_id: $incident_id})<-[e:EXPOSED_TO]-(s:Subscription)-[:HAS_RENEWAL]->(r:Renewal)
WITH r, min(e.event_date) AS first_seen
WHERE first_seen > r.as_of
RETURN count(*) AS naive_additional
ORDER BY naive_additional LIMIT 1""",
    "exposure_pricing_change": """
MATCH (r:Renewal)-[f:FIRST_RENEWAL_AFTER]->(p:PricingChange {change_id: $change_id})
RETURN r.plan_tier AS plan_tier, r.route AS route, f.known_by_as_of AS known_by_as_of, count(*) AS renewals,
       sum(r.churned) AS voluntary_lapses
ORDER BY plan_tier, route, known_by_as_of LIMIT 100""",

    # ---- cohort goldens ----------------------------------------------------------------------------
    "motif_limit_hit_then_overage_off": """
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal), (s)-[h:HIT_LIMIT]->(:LimitHit),
      (s)-[o:CHANGED_OVERAGE]->(:OverageChange)
WHERE r.route = 'model' AND o.state = 'disabled' AND h.event_date <= o.event_date AND o.event_date <= r.as_of
WITH DISTINCT r
RETURN count(*) AS renewals, sum(r.churned) AS lapses
ORDER BY renewals LIMIT 1""",
    "first_renewal_after_by_plan": """
MATCH (r:Renewal)
WHERE r.route = 'model'
OPTIONAL MATCH (r)-[f:FIRST_RENEWAL_AFTER]->(:PricingChange)
WITH r, count(f) > 0 AS first_after
RETURN r.plan_tier AS plan_tier, first_after, count(*) AS n, sum(r.churned) AS lapses
ORDER BY plan_tier, first_after LIMIT 20""",
}

# Templates only the graph contract may run (fetch(..., contract=True)); agent tools never do.
#   contract_*                  compute a value WITHOUT the as_of bound on purpose (leak fixtures)
#   the four SIMILAR_TO ones    read neighbour outcome fields with no visibility rule: they pin
#                               invariants and the hero goldens; tools use similar_top_k_visible
#                               and similar_nearest_lapses_known_by_as_of instead
UNBOUNDED_NEIGHBOUR_OUTCOMES = ("similar_top_k", "similar_nearest_lapses", "similar_sharing_neighbours",
                                "similar_edge_checks")
CONTRACT_ONLY = sorted({k for k in TEMPLATES if k.startswith("contract_")} | set(UNBOUNDED_NEIGHBOUR_OUTCOMES))
TOOL_TEMPLATES = sorted(set(TEMPLATES) - set(CONTRACT_ONLY))
# The allowlist of vetted tool template shapes (name -> fingerprint(); see the module docstring):
# lint() of the catalog refuses a tool template that is not here, or that changed since it was vetted.
VETTED_TOOL_TEMPLATES = {
    "count_nodes": "6812e10ebffe0bc4",
    "count_rels": "36c6e60c742c584b",
    "evidence_cut_cap": "43a86c013c959286",
    "evidence_events": "03229e9ef9431a2e",
    "evidence_first_renewal_after": "767a2d800e0a1d6f",
    "exposure_incident_by_plan": "33fe3bd4916b0377",
    "exposure_pricing_change": "0e08803df5de7dbc",
    "first_renewal_after_by_plan": "0c2534269a0b65ec",
    "motif_limit_hit_then_overage_off": "b1fc69e69366afcf",
    "renewal_header": "98de4eeb63f68eed",
    "renewals_per_subscription": "c59ba20adc0a09f5",
    "routes": "baaf54701321c5ca",
    "similar_max_in_degree": "6b2f32b374113545",
    "similar_mutual_edges": "b7dbaea195b6959b",
    "similar_nearest_lapses_known_by_as_of": "58ac6357cfc885a8",
    "similar_out_degree": "34d395d2040830bc",
    "similar_top_k_visible": "fb33404896f168c8",
}
PARAM_DEFAULTS = {"limit": ROW_LIMIT, "today": False}  # $today: only a current source ever sees today's view


def render(name: str, **identifiers: str) -> str:
    """Template text with ``{label}`` / ``{rel}`` filled from spec names (never from user input)."""
    q = TEMPLATES[name]
    for key, value in identifiers.items():
        allowed = spec.NODE_SCHEMA if key == "label" else spec.EDGE_SCHEMA if key == "rel" else None
        if allowed is None or value not in allowed:
            raise ValueError(f"{key}={value!r} is not a graph {key}")
    return q.format(**identifiers) if identifiers else q


def fetch(conn, name: str, params: dict | None = None, *, contract: bool = False, **identifiers: str) -> list[dict]:
    """Run a named template; rows as dicts keyed by the RETURN aliases.

    CONTRACT_ONLY templates are refused unless ``contract=True`` (only the graph contract and
    its tests pass it), so a tool cannot reach an unbounded template by name.
    """
    if name in CONTRACT_ONLY and not contract:
        raise ContractOnlyError(
            f"template {name!r} is contract-only (it reads post-as_of events or neighbour outcomes with no "
            f"visibility rule): tools use the bounded templates (queries.TOOL_TEMPLATES), e.g. similar_top_k_visible "
            f"or similar_nearest_lapses_known_by_as_of")
    q = render(name, **identifiers)
    given = dict(params or {})
    for key, default in PARAM_DEFAULTS.items():
        if f"${key}" in q:
            given.setdefault(key, default)
    used = {k: v for k, v in given.items() if f"${k}" in q}
    res = conn.execute(q, used)
    cols = res.get_column_names()
    out = []
    while res.has_next():
        out.append(dict(zip(cols, res.get_next(), strict=True)))
    return out


# --------------------------------------------------------------------------- lint (the rules above)
# Mutations, procedures and file I/O keywords. (Engine-extension statements are asserted absent
# from every graph source file, not only the templates, by tests/graph/test_queries_lint.py.)
_WRITE_OR_FILE = re.compile(r"\b(CREATE|MERGE|DELETE|SET|DROP|ALTER|COPY|CALL|LOAD|EXPORT|IMPORT)\b", re.IGNORECASE)


def _lint_shape(name: str, q: str) -> list[str]:
    order = [m.start() for m in re.finditer(r"\bORDER\s+BY\b", q, re.IGNORECASE)]
    limit = [m.start() for m in re.finditer(r"\bLIMIT\b", q, re.IGNORECASE)]
    out = []
    if not order or not limit or order[-1] > limit[-1] or \
            not re.search(r"\bLIMIT\s+(\$\w+|\d+)\s*$", q.rstrip().splitlines()[-1], re.IGNORECASE):
        out.append(f"{name}: needs ORDER BY and must end with LIMIT <n | $param>")
    if re.search(r"\bUNION\b", q, re.IGNORECASE):
        out.append(f"{name}: UNION is not allowed (ORDER BY binds to the last branch)")
    if _WRITE_OR_FILE.search(q):
        out.append(f"{name}: must be read only, with no procedure call or file statement")
    return out


# The leak rule is checked on tokens, clauses and parsed MATCH patterns, never on substrings. The
# dialect ignores case in keywords, variables, labels, relationship types and property names, and
# accepts RETURN *, properties(n), back-ticked names, comments, untyped relationships, inline
# property maps and subqueries: a substring match is satisfied by text that filters nothing.
_ANY = "__any__"  # what a {label} / {rel} placeholder becomes: any node label / relationship type
_TOKEN = re.compile(r"""
    (?P<ws>\s+) | (?P<comment>//|/\*)
  | (?P<str>'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")
  | (?P<param>\$\w+) | (?P<num>\d+(?:\.\d+)?) | (?P<id>[A-Za-z_]\w*)
  | (?P<op><=|>=|<>|<-|->|[-+*/%^=<>(){}\[\],.:|])
""", re.VERBOSE)
_OPEN, _CLOSE = ("(", "[", "{"), (")", "]", "}")
_CLAUSE_WORDS = ("match", "optional", "where", "with", "return", "unwind", "order", "limit", "skip", "union", "call")
_SECOND_WORD = {"optional": "match", "order": "by"}
_MATCHES = ("match", "optional match")
_BARE_OK = ("count", "cost", "length", "label")  # the only functions that may take a whole node / relationship / path
_EVENT_RELS = frozenset(r.lower() for r in spec.EVENT_RELATIONS)
# Per-subscription event nodes. Incident is left out: it is a hub on the public incident calendar
# (the per-subscription fact is the EXPOSED_TO edge), so exposure templates match it by id.
_EVENT_NODES = frozenset(spec.EDGE_SCHEMA[r].dst.lower() for r in spec.EVENT_RELATIONS) - {"incident"}
_ENDS = {e.rel.lower(): (e.src.lower(), e.dst.lower()) for e in spec.EDGE_SCHEMA.values()}
# Fields that name one subscription or renewal: the keys of Subscription, Renewal and the event
# nodes (an event id embeds its subscription id), and the user's name.
_IDENTIFYING = tuple(sorted({n.key for n in spec.NODE_SCHEMA.values()
                             if n.label.lower() in _EVENT_NODES | {"subscription", "renewal"}} | {"user_name"}))
_NEIGHBOUR_PARAMS = ("$renewal_id", "$k", "$limit", "$today")  # all a template relating renewals takes
_AGGREGATES = ("count", "sum", "min", "max", "avg")
# Rule 7: the aggregates a population template may use (cell statistics: each counts the renewals of a
# cell), and every aggregate the engine has (CALL show_functions(): type AGGREGATE FUNCTION, ladybug
# 0.21.1), which it may not. avg is sum / count: two cell statistics in one number.
CELL_STATISTICS = ("count", "sum")
ENGINE_AGGREGATES = ("avg", "collect", "count", "count_star", "histogram", "max", "min", "percentilecont",
                     "percentiledisc", "sum")
# Rule 7 grouping keys: properties with a handful of values (tiers, routes, outcomes, 0/1 flags,
# categorical event attributes). A date, an amount, a feature value or an id is never one.
COHORT_KEYS = ("plan_tier", "city", "route", "outcome", "churned", "is_reference", "known_by_as_of",
               "overage_toggled_off", "incident_exposed_28d", "first_renewal_after_pricing_change", "limit_type",
               "state", "event_type", "mutual")
# The 0/1 (boolean) cohort keys: what sum / avg may add up in a population template (rule 7).
COHORT_FLAGS = ("churned", "is_reference", "known_by_as_of", "overage_toggled_off", "incident_exposed_28d",
                "first_renewal_after_pricing_change", "mutual")
# The keys of the hub nodes on the public calendar: what a population template's parameter may select.
HUB_KEYS = tuple(sorted(spec.NODE_SCHEMA[label].key for label in ("Plan", "Incident", "PricingChange")))
# Words the lint reads as syntax (CASE ... END nests, AND / OR split a WHERE): never a variable or alias.
_KEYWORDS = frozenset({"case", "when", "then", "else", "end", "and", "or", "xor", "not", "as", "distinct", "in", "is",
                       "null", "true", "false", "by", "asc", "desc", "ascending", "descending", "starts", "ends",
                       "contains", "exists", *_CLAUSE_WORDS})
_COMPARE = ("=", "<>", "<", ">", "<=", ">=")
# What may stand just outside a whole comparison operand (nothing continues the operand there).
# Arithmetic, '.', '||', a comparison (chained), IS / IN / STARTS WITH (they bind tighter than '=')
# and '[' after an operand (an index) are never edges.
_EDGE_BEFORE = frozenset({"and", "or", "xor", "not", "where", "when", "then", "else", "case", "by", "distinct",
                          *_CLAUSE_WORDS})
_EDGE_AFTER = frozenset({"and", "or", "xor", "then", "else", "end", "when", "as", "asc", "desc", "ascending",
                         "descending", *_CLAUSE_WORDS})
# Rule 6: the calendar hubs and the date that says when each one was known; what says when an incident ended.
_CALENDAR = {"pricingchange": "effective_date", "incident": "starts_on"}
_INCIDENT_END = ("ends_on", "days")
_COUNT_KEY = "count(...)"  # what a count of an earlier WITH group reads as a grouping key (rule 7)
# ... and a sum or mean of one (a per-group number, never a cohort value): rule 7 refuses all three as keys
_STATISTIC_KEYS = frozenset({_COUNT_KEY, "sum(...)", "avg(...)"})
# What a CASE value may be in sum(): 0 or 1 (with null and the 0/1 COHORT_FLAGS), so every renewal adds 0 or 1
# and the sum is the count of a sub-cell; sum(CASE WHEN ... THEN 1001 ...) packs a second count into it.
_UNIT_VALUES = (0.0, 1.0)


class _Tok(NamedTuple):
    kind: str  # str | param | num | id | op
    text: str
    low: str   # identifiers lower-cased (the dialect is case-insensitive); everything else as written


class _Clause(NamedTuple):
    kind: str  # match | optional match | where | with | return | unwind | order by | limit | skip | union | call
    lo: int    # tokens[lo:hi] is the clause body (keyword excluded)
    hi: int


class _Node(NamedTuple):
    var: str | None    # lower-cased variable; None when anonymous
    shown: str | None  # the variable as written
    labels: frozenset  # lower-cased; _ANY for a placeholder
    keys: frozenset    # keys of the inline property map
    source: bool       # the map binds {renewal_id: $renewal_id}
    alone: bool        # a pattern part of its own: no relationship attached


class _Rel(NamedTuple):
    var: str | None
    shown: str | None
    types: tuple       # lower-cased; _ANY for a placeholder; empty when untyped
    what: str          # the types as written, for messages
    recursive: bool
    has_map: bool
    left: _Node
    right: _Node
    direction: str     # out: (left)-[]->(right) | in: (left)<-[]-(right) | none
    lone: bool         # the only relationship of its pattern part

    @property
    def tail(self) -> _Node | None:
        return {"out": self.left, "in": self.right}.get(self.direction)

    @property
    def head(self) -> _Node | None:
        return {"out": self.right, "in": self.left}.get(self.direction)


class _NoParse(Exception):
    """A MATCH pattern the lint cannot read: the template is refused."""


def _tokenise(q: str) -> tuple[list[_Tok], list[str]]:
    """(tokens, unreadable pieces): comments, back-ticks, ';', unterminated strings and backslashes
    in strings (where the lint's idea of the string's end could differ from the engine's) are unreadable."""
    q = q.replace("{label}", _ANY).replace("{rel}", _ANY)
    toks, bad, pos = [], [], 0
    while pos < len(q):
        m = _TOKEN.match(q, pos)
        if m is None or m.lastgroup == "comment":
            bad.append(q[pos] if m is None else m.group())
            pos = pos + 1 if m is None else m.end()
            continue
        if m.lastgroup == "str" and "\\" in m.group():
            bad.append("\\")
        pos = m.end()
        if m.lastgroup != "ws":
            toks.append(_Tok(m.lastgroup, m.group(), m.group().lower() if m.lastgroup == "id" else m.group()))
    return toks, sorted(set(bad))


def _canon(toks) -> str:
    return " ".join(t.low for t in toks)


def _canon_text(text: str) -> str:
    """Canonical form of an expression: what two spellings of the same predicate share."""
    return _canon(_tokenise(text)[0])


def _is(toks, i: int, text: str) -> bool:
    return 0 <= i < len(toks) and toks[i].kind == "op" and toks[i].text == text


def _word(toks, i: int) -> str:
    """The lower-cased identifier at i; '' when there is none."""
    return toks[i].low if 0 <= i < len(toks) and toks[i].kind == "id" else ""


def _comma(t: _Tok) -> bool:
    return t.kind == "op" and t.text == ","


def _num_in(toks, i: int, values: tuple[float, ...]) -> bool:
    """The token at i is a number literal whose value is one of ``values``."""
    if not (0 <= i < len(toks) and toks[i].kind == "num"):
        return False
    try:
        return float(toks[i].text) in values
    except ValueError:
        return False


def _split(toks, sep) -> list[list[_Tok]]:
    """The runs of tokens between the depth-0 tokens for which ``sep`` holds.

    Brackets nest, and so does CASE ... END: an AND between CASE and END belongs to the CASE
    expression, not to the WHERE around it. Unbalanced nesting (``{end: 1}``, a stray END) is
    unreadable: _NoParse.
    """
    parts, cur, depth = [], [], 0
    for n, t in enumerate(toks):
        keyword = t.low if t.kind == "id" and not _is(toks, n - 1, ".") else ""
        if (t.kind == "op" and t.text in _OPEN) or keyword == "case":
            depth += 1
        elif (t.kind == "op" and t.text in _CLOSE) or keyword == "end":
            depth -= 1
            if depth < 0:
                raise _NoParse
        if depth == 0 and sep(t):
            parts.append(cur)
            cur = []
        else:
            cur.append(t)
    if depth:
        raise _NoParse
    return [*parts, cur]


def _close(toks, i: int) -> int:
    """Index of the bracket that closes the one at i."""
    depth = 0
    for j in range(i, len(toks)):
        if toks[j].kind == "op" and toks[j].text in _OPEN:
            depth += 1
        elif toks[j].kind == "op" and toks[j].text in _CLOSE:
            depth -= 1
            if depth == 0:
                return j
    raise _NoParse


def _conjuncts(toks) -> list[str]:
    """The conditions a WHERE body requires together, in canonical form.

    Split at top-level ANDs (through redundant brackets). A part with a top-level OR / XOR stays
    one condition: ``a OR b`` requires neither a nor b. ``NOT a`` is the condition ``not a``.
    """
    toks = list(toks)
    while len(toks) >= 2 and _is(toks, 0, "(") and _close(toks, 0) == len(toks) - 1:
        toks = toks[1:-1]
    if not toks:
        return []
    if len(_split(toks, lambda t: t.kind == "id" and t.low in ("or", "xor"))) > 1:
        return [_canon(toks)]
    parts = [p for p in _split(toks, lambda t: t.kind == "id" and t.low == "and") if p]
    return [_canon(toks)] if len(parts) <= 1 else [c for p in parts for c in _conjuncts(p)]


def _clauses(toks) -> tuple[list[_Clause], list[str], int]:
    """(clauses at bracket depth 0, clause keywords nested inside brackets, tokens before the first clause)."""
    starts, nested, depth, i = [], [], 0, 0
    while i < len(toks):
        t = toks[i]
        if t.kind == "op" and t.text in _OPEN:
            depth += 1
        elif t.kind == "op" and t.text in _CLOSE:
            depth -= 1
        elif t.kind == "id" and t.low in _CLAUSE_WORDS and not _is(toks, i - 1, ".") \
                and not (t.low == "with" and _word(toks, i - 1) in ("starts", "ends")):
            second = _SECOND_WORD.get(t.low)  # OPTIONAL MATCH, ORDER BY
            if second is None or _word(toks, i + 1) == second:
                width = 2 if second else 1
                if depth:
                    nested.append(t.text.upper())
                else:
                    starts.append((f"{t.low} {second}" if second else t.low, i, i + width))
                i += width - 1
        i += 1
    ends = [kw for _, kw, _ in starts[1:]] + [len(toks)]
    clauses = [_Clause(kind, lo, hi) for (kind, _, lo), hi in zip(starts, ends, strict=True)]
    return clauses, sorted(set(nested)), starts[0][1] if starts else len(toks)


def _names(toks, i: int) -> tuple[list[_Tok], int]:
    """':A', ':A|B', ':A|:B' or ':A:B' at i -> (name tokens, next index)."""
    out = []
    while _is(toks, i, ":") or (out and _is(toks, i, "|")):
        i += 2 if _is(toks, i, "|") and _is(toks, i + 1, ":") else 1
        if not _word(toks, i):
            raise _NoParse
        out.append(toks[i])
        i += 1
    return out, i


def _node(toks, i: int) -> tuple[_Node, int]:
    if not _is(toks, i, "("):
        raise _NoParse
    i += 1
    var = toks[i] if _word(toks, i) else None
    i += var is not None
    labels, i = _names(toks, i)
    keys, source = [], False
    if _is(toks, i, "{"):
        j = _close(toks, i)
        pairs = _split(toks[i + 1:j], _comma)
        if any(len(p) < 3 or p[0].kind != "id" or not _is(p, 1, ":") for p in pairs):
            raise _NoParse
        keys = [p[0].low for p in pairs]
        source = any([t.low for t in p] == ["renewal_id", ":", "$renewal_id"] for p in pairs)
        i = j + 1
    if not _is(toks, i, ")"):
        raise _NoParse
    return _Node(var.low if var else None, var.text if var else None, frozenset(t.low for t in labels),
                 frozenset(keys), source, False), i + 1


def _rel(toks, i: int):
    """The relationship at i -> (variable token, type tokens, recursive, has_map, direction, next index).

    None when no relationship starts at i.
    """
    if not (_is(toks, i, "<-") or _is(toks, i, "-")):
        return None
    left = toks[i].text == "<-"
    i += 1
    var, types, recursive, has_map = None, [], False, False
    if _is(toks, i, "["):
        j = _close(toks, i)
        inner, k = toks[:j], i + 1
        if _word(inner, k):
            var = inner[k]
            k += 1
        types, k = _names(inner, k)
        rest = inner[k:]
        recursive = any(_is(rest, n, "*") for n in range(len(rest)))
        has_map = any(_is(rest, n, "{") for n in range(len(rest)))
        if rest and not (recursive or has_map):
            raise _NoParse
        i = j + 1
    if not (_is(toks, i, "->") or _is(toks, i, "-")):
        raise _NoParse
    right = toks[i].text == "->"
    if left and right:
        raise _NoParse
    return var, types, recursive, has_map, "in" if left else "out" if right else "none", i + 1


def _patterns(toks) -> tuple[list[_Tok], list[_Node], list[_Rel]]:
    """A MATCH body -> (path variable tokens, node patterns, relationship patterns)."""
    paths, nodes, rels, i = [], [], [], 0
    while True:
        if _word(toks, i) and _is(toks, i + 1, "="):
            paths.append(toks[i])
            i += 2
        node, i = _node(toks, i)
        chain, links = [node], []
        while (r := _rel(toks, i)) is not None:
            nxt, i = _node(toks, r[-1])
            links.append((r, chain[-1], nxt))
            chain.append(nxt)
        for (var, types, recursive, has_map, direction, _), left, right in links:
            rels.append(_Rel(var.low if var else None, var.text if var else None, tuple(t.low for t in types),
                             "|".join("{rel}" if t.low == _ANY else t.text for t in types), recursive, has_map,
                             left, right, direction, len(links) == 1))
        nodes += chain if links else [node._replace(alone=True)]
        if i >= len(toks):
            return paths, nodes, rels
        if not _is(toks, i, ","):
            raise _NoParse
        i += 1


class _Template:
    """One tool template read as tokens, clauses and MATCH patterns; ``problems`` lists its leak-rule violations."""

    def __init__(self, name: str, q: str):
        self.name, self.problems = name, []
        self.toks, unreadable = _tokenise(q)
        if unreadable:
            self._bad(f"has text the lint cannot read ({' '.join(unreadable)}): a tool template has no comments, "
                      f"back-ticked names, ';' or backslashes in strings")
        self.clauses, nested, lead = _clauses(self.toks)
        if nested:
            self._bad(f"nests {', '.join(nested)} inside brackets (a subquery or comprehension): the leak rule cannot "
                      f"see into it")
        if lead:
            self._bad("does not start with a clause keyword")
        self.clause_at = [-1] * len(self.toks)     # token index -> index of the clause it belongs to (keyword included)
        for idx, c in enumerate(self.clauses):
            for j in range(c.lo - (2 if " " in c.kind else 1), c.hi):   # OPTIONAL MATCH / ORDER BY: two words
                self.clause_at[j] = idx
        self.last_return = max((idx for idx, c in enumerate(self.clauses) if c.kind == "return"), default=-1)
        self.kind: dict[str, str] = {}             # pattern variable -> node | relationship | path
        self.shown: dict[str, str] = {}            # pattern variable as first written
        self.first: dict[str, int] = {}            # pattern variable -> index of the clause that declares it
        self.spots: dict[str, list[_Node]] = {}    # node variable -> its node patterns
        self.nodes: list[_Node] = []
        self.rels: list[_Rel] = []
        self.matched: list[tuple[int, list[_Node], list[_Rel]]] = []   # (MATCH clause, its nodes, its rels)
        self.filters: list[tuple[int, str, list[str]]] = []   # (clause a WHERE belongs to, its kind, conditions)
        self.passed: set[int] = set()              # token indexes of plain WITH pass-through items
        self.map_sources: set[str] = set()         # node variables with {renewal_id: $renewal_id} in a plain MATCH
        self.bounded_events: list[_Rel] = []       # event edges rule 1 found bounded (source_events() fills it)
        aliases: list[tuple[str, str, str]] = []   # (alias, as written, clause kind) of every ... AS alias
        declared: set[str] = set()
        dropped: set[str] = set()
        cut = False  # a LIMIT / SKIP was seen: a WHERE after it filters rows that were already chosen
        for idx, c in enumerate(self.clauses):
            body = self.toks[c.lo:c.hi]
            cut = cut or c.kind in ("limit", "skip")
            if c.kind in _MATCHES:
                try:
                    paths, nodes, rels = _patterns(body)
                except (_NoParse, IndexError):
                    self._bad("has a MATCH pattern the lint cannot read (write plain (var:Label {key: value}) nodes "
                              "and -[var:TYPE]-> relationships)")
                    continue
                self.nodes += nodes
                self.rels += rels
                self.matched.append((idx, nodes, rels))
                found = [(p.low, p.text, "path") for p in paths] + [(n.var, n.shown, "node") for n in nodes] + \
                        [(r.var, r.shown, "relationship") for r in rels]
                for var, shown, kind in found:
                    if var is None:
                        continue
                    if var in dropped:
                        self._bad(f"re-binds {shown} after a WITH dropped it: a filter on the new {shown} does not "
                                  f"bound what was read from the old one")
                    if self.kind.get(var, kind) != kind:
                        self._bad(f"declares {shown} both as a {self.kind[var]} and as a {kind}")
                    self.kind.setdefault(var, kind)
                    self.shown.setdefault(var, shown)
                    self.first.setdefault(var, idx)
                    declared.add(var)
                for n in nodes:
                    if n.var:
                        self.spots.setdefault(n.var, []).append(n)
                        if n.source and c.kind == "match":  # an OPTIONAL MATCH map restricts no row
                            self.map_sources.add(n.var)
                continue
            for j in range(c.lo, c.hi):  # a dropped variable is out of scope: nothing after its WITH may use it
                t = self.toks[j]
                if t.kind == "id" and t.low in dropped and not _is(self.toks, j - 1, ".") and \
                        _word(self.toks, j - 1) != "as" and not _is(self.toks, j + 1, "("):
                    self._bad(f"uses {t.text} after a WITH dropped it: it is out of scope there, and a bound or "
                              f"filter on it bounds nothing that was read before")
            if c.kind in ("with", "return"):
                for item in self._items(c):
                    words = [self.toks[j] for j in item]
                    if len(words) >= 3 and words[-2].low == "as" and words[-1].kind == "id":
                        aliases.append((words[-1].low, words[-1].text, c.kind))
            elif c.kind == "unwind" and len(body) >= 3 and _word(body, len(body) - 2) == "as" and \
                    body[-1].kind == "id":
                aliases.append((body[-1].low, body[-1].text, c.kind))
            if c.kind == "with":
                kept = set()
                for item in self._items(c):
                    words = [self.toks[j] for j in item]
                    if len(words) == 1 and words[0].kind == "id":
                        kept.add(words[0].low)
                        self.passed.add(item[0])
                    elif len(words) == 1 and _is(words, 0, "*"):
                        kept |= declared
                    elif len(words) >= 3 and words[-2].low == "as" and words[-1].kind == "id":
                        kept.add(words[-1].low)
                dropped |= declared - kept
            elif c.kind == "where" and idx and self.clauses[idx - 1].kind in (*_MATCHES, "with") and not cut:
                try:
                    self.filters.append((idx - 1, self.clauses[idx - 1].kind, _conjuncts(body)))
                except _NoParse:
                    self._bad("has a WHERE with unbalanced brackets or CASE ... END")
        self._names(aliases)
        # the source renewal: a node variable that can be a Renewal, named by $renewal_id in a plain
        # MATCH map or by a row-filter conjunct (never by an OPTIONAL MATCH, which restricts no row)
        named = self.map_sources | {m.group(1) for cond in self._filters()
                                    if (m := re.fullmatch(r"(\w+) \. renewal_id = \$renewal_id", cond))}
        self.sources = {v for v in named if self.kind.get(v) == "node" and self._can_be_renewal(v)}

    def _names(self, aliases: list[tuple[str, str, str]]) -> None:
        """A name means one thing: no WITH / UNWIND / RETURN alias re-uses a pattern variable's name,
        no alias is defined twice (the three clause kinds together), and no name is a keyword."""
        for var in sorted(v for v in self.kind if v in _KEYWORDS):
            self._bad(f"names a pattern variable {self.shown[var]}, which is a keyword to the lint and the engine")
        seen: dict[str, int] = {}
        for low, shown, kind in aliases:
            if low in _KEYWORDS:
                self._bad(f"names an alias {shown}, which is a keyword to the lint and the engine")
            if low in self.kind:
                after = "an ORDER BY after it would read the alias" if kind == "return" else \
                    f"a bound, filter or mask written on {shown} would test the alias, not the {self.kind[low]}"
                self._bad(f"re-uses the name of the pattern variable {self.shown[low]} as an alias ({kind.upper()} ... "
                          f"AS {shown}): {after}")
            seen[low] = seen.get(low, 0) + 1
            if seen[low] == 2:
                self._bad(f"defines the alias {shown} more than once: a later definition would change what an earlier "
                          f"filter, mask or sort meant")

    def _can_be_renewal(self, node: _Node | str) -> bool:
        labels = self._labels(node)
        return not labels or bool(labels & {"renewal", _ANY})

    def _bad(self, msg: str) -> None:
        if f"{self.name}: {msg}" not in self.problems:
            self.problems.append(f"{self.name}: {msg}")

    def _items(self, c: _Clause) -> list[list[int]]:
        """Projection items of a WITH / RETURN as lists of token indexes (a leading DISTINCT removed)."""
        items, cur, depth = [], [], 0
        for j in range(c.lo, c.hi):
            t = self.toks[j]
            if t.kind == "op" and t.text in _OPEN:
                depth += 1
            elif t.kind == "op" and t.text in _CLOSE:
                depth -= 1
            if depth == 0 and _comma(t):
                items.append(cur)
                cur = []
            else:
                cur.append(j)
        items.append(cur)
        if items[0] and _word(self.toks, items[0][0]) == "distinct":
            items[0] = items[0][1:]
        return items

    def _filters(self, var: str | None = None) -> set[str]:
        """Conditions that really restrict ``var``: every WHERE of a MATCH or WITH (they filter rows),
        and the WHERE of the OPTIONAL MATCH that introduced ``var`` (it nulls var when it fails)."""
        return {cond for idx, kind, conds in self.filters for cond in conds
                if kind != "optional match" or (var is not None and self.first.get(var) == idx)}

    def _labels(self, node: _Node | str) -> set[str]:
        """Labels a node variable (or one anonymous node pattern) can have: those written on it,
        else what its typed relationships imply. Empty = unknown, which the rules read as "any"."""
        spots = self.spots.get(node, []) if isinstance(node, str) else [node]
        written = set().union(*(n.labels for n in spots))
        if written:
            return written
        out = set()
        for r in self.rels:
            for t in r.types:
                src, dst = _ENDS.get(t, (_ANY, _ANY))
                ends = {"out": ((r.left, src), (r.right, dst)), "in": ((r.left, dst), (r.right, src))}.get(
                    r.direction, ((r.left, src), (r.left, dst), (r.right, src), (r.right, dst)))
                out |= {label for end, label in ends if any(end is n for n in spots)}
        return out

    def _wrapped(self, j: int, functions: tuple[str, ...]) -> bool:
        """The variable at j is the whole argument of one of ``functions`` (count also takes DISTINCT)."""
        t = self.toks
        if not _is(t, j + 1, ")"):
            return False
        if _is(t, j - 1, "(") and _word(t, j - 2) in functions:
            return True
        return "count" in functions and _word(t, j - 1) == "distinct" and _is(t, j - 2, "(") and \
            _word(t, j - 3) == "count"

    def _only_counted(self, var: str) -> bool:
        """Outside its MATCH the variable appears inside count() only (a population total)."""
        for c in self.clauses:
            if c.kind in _MATCHES:
                continue
            for j in range(c.lo, c.hi):
                if self.toks[j].kind != "id" or self.toks[j].low != var or _is(self.toks, j - 1, "."):
                    continue
                if _is(self.toks, j + 1, "."):
                    return False
                if _word(self.toks, j - 1) == "as" or c.kind == "order by":  # an alias of the same name
                    continue
                if not self._wrapped(j, ("count",)):
                    return False
        return True

    # ---- rule 5: named properties only
    def named_properties_only(self) -> None:
        whole = []
        toks = self.toks
        for j in range(len(toks) - 1):  # anywhere: RETURN / WITH / UNWIND / WHERE / ORDER BY, a MATCH map, ...
            if _is(toks, j, ".") and _is(toks, j + 1, "*"):
                owner = toks[j - 1].text if j and toks[j - 1].kind == "id" else "an expression"
                self._bad(f"projects {owner}.* (every property of {owner}, outcome fields included): project "
                          f"named properties")
        for c in self.clauses:
            if c.kind in ("with", "return") and any(len(i) == 1 and _is(self.toks, i[0], "*") for i in self._items(c)):
                self._bad(f"{c.kind.upper()} * passes on every whole node and relationship: project named properties")
            for j in range(c.lo, c.hi) if c.kind not in _MATCHES else ():
                quantifier = _is(toks, j, "(") and _word(toks, j - 1) in ("any", "all", "none", "single")
                if (_is(toks, j, "[") or quantifier) and _word(toks, j + 1) and _word(toks, j + 2) == "in":
                    self._bad(f"binds a local variable {toks[j + 1].text} (a list comprehension or quantifier): the "
                              f"leak rule cannot follow what it names")
            if c.kind not in _MATCHES and any(
                    _is(self.toks, j, "->") or _is(self.toks, j, "<-") or (_is(self.toks, j, "-") and (
                        _is(self.toks, j + 1, "[") or _is(self.toks, j + 1, "-") or _is(self.toks, j - 1, "]")))
                    for j in range(c.lo, c.hi)):
                self._bad(f"has a relationship pattern outside MATCH (in its {c.kind.upper()}): a pattern predicate or "
                          f"size(pattern) walks edges the leak rule cannot bound")
            if c.kind not in ("where", "with", "return", "unwind"):
                continue
            for j in range(c.lo, c.hi):
                t = self.toks[j]
                if t.kind != "id" or t.low not in self.kind or j in self.passed or t.low in whole:
                    continue
                if _is(self.toks, j - 1, ".") or _word(self.toks, j - 1) == "as" or _is(self.toks, j + 1, "."):
                    continue  # a property name, an alias, a property access
                if not self._wrapped(j, _BARE_OK):
                    whole.append(t.low)
        for var in whole:
            self._bad(f"returns or passes on the whole {self.kind[var]} {self.shown[var]}: project named properties "
                      f"(a bare variable is allowed as a plain WITH pass-through and inside "
                      f"{' / '.join(f + '()' for f in _BARE_OK)} only)")

    # ---- rules 1 and 2: source events bounded by the renewal's as_of; no BILLED outcome evidence
    def source_events(self) -> None:
        anchors: dict[str, set[str]] = {}  # Subscription variable -> renewal variables matched from it via HAS_RENEWAL
        for r in self.rels:
            if r.types == ("has_renewal",) and not r.recursive:
                ends = [(r.tail, r.head)] if r.tail else [(r.left, r.right), (r.right, r.left)]
                for sub, ren in ends:
                    if sub.var and ren.var:
                        anchors.setdefault(sub.var, set()).add(ren.var)
        if self.sources:  # a named renewal: its subscription's events, bounded by its own as_of only
            anchors = {sub: rens & self.sources for sub, rens in anchors.items()}
        events = []
        for r in self.rels:
            if not r.types:
                self._bad(f"has a relationship without a type ({r.shown or 'anonymous'}): it matches event edges too, "
                          f"and the leak rule cannot bound what it cannot name")
                continue
            if _ANY not in r.types and not set(r.types) & _EVENT_RELS:
                continue
            untied = not (r.left.var or r.right.var or r.left.keys or r.right.keys or r.has_map or r.recursive)
            if r.lone and untied and (r.var is None or self._only_counted(r.var)):
                continue  # a population total: ()-[e:TYPE]->() that is only counted
            if r.var is None:
                self._bad(f"the {r.what} edge is anonymous, so it cannot be bounded by as_of")
                if _ANY in r.types or "billed" in r.types:
                    self._bad("can return BILLED outcome evidence (filter NOT coalesce(e.outcome_evidence, false))")
            elif r.recursive or r.tail is None or r.tail.var is None:
                self._bad(f"walks {r.what} as {r.shown} without a direction from a named Subscription variable "
                          f"(write (s)-[{r.shown}:{r.what}]->(x)), so no renewal's as_of can bound it")
            else:
                events.append(r)
        bounded: dict[str, str] = {}  # bounded event edge variable -> its Subscription variable
        grew = True
        while grew:  # h.event_date <= o.event_date bounds h once o (same subscription) is bounded
            grew = False
            for r in events:
                have = self._filters(r.var)
                if r.var not in bounded and (
                        any(f"{r.var} . event_date <= {a} . as_of" in have for a in anchors.get(r.tail.var, ())) or
                        any(f"{r.var} . event_date <= {o} . event_date" in have
                            for o, sub in bounded.items() if sub == r.tail.var)):
                    bounded[r.var] = r.tail.var
                    grew = True
        self.bounded_events = [r for r in events if r.var in bounded]   # rule 6 reads it (Incident heads)
        for r in events:
            if r.var not in bounded:
                self._bad(f"walks {r.what} without {r.shown}.event_date <= <renewal>.as_of as a WHERE conjunct "
                          f"(<renewal> = the renewal matched from the same Subscription variable via HAS_RENEWAL, "
                          f"and the source itself when the template names one by $renewal_id; nothing may be added "
                          f"to its as_of)")
            if (_ANY in r.types or "billed" in r.types) and not self._filters(r.var) & {
                    f"not coalesce ( {r.var} . outcome_evidence , false )", f"{r.var} . outcome_evidence = false"}:
                self._bad(f"can return BILLED outcome evidence (filter NOT coalesce({r.shown}.outcome_evidence, "
                          f"false))")
        t = self.toks
        if any(x.low == "event_date" and _is(t, j - 1, ".") and _is(t, j + 1, ">") and _word(t, j + 2)
               and _is(t, j + 3, ".") and _word(t, j + 4) == "as_of" and not _is(t, j + 5, "-")
               for j, x in enumerate(t)):
            self._bad("selects events after as_of")
        for var, spots in self.spots.items():
            labels = self._labels(var)
            if labels and _ANY not in labels and not labels & _EVENT_NODES:
                continue
            if any(r.head is spots[0] for r in events if r.var in bounded):
                continue  # introduced as the head of a bounded event edge: it only ever holds bounded events
            if all(n.alone for n in spots) and self._only_counted(var):
                continue  # a population total: (n:Label) that is only counted
            self._bad(f"matches {self.shown[var]}, which can be an event node, outside an as_of-bounded event edge "
                      f"(label it, or introduce it as (s)-[e:TYPE]->({self.shown[var]}) with e bounded)")

    # ---- rule 6: the calendar a named renewal sees stops at its as_of (plan cuts, pricing changes, incidents)
    def plan_cuts(self) -> None:
        if not self.sources:
            return  # a population template: CUT_CAP / PricingChange / Incident are the public calendar
        src = self.shown[min(self.sources)]
        cuts = []  # CUT_CAP edges bounded by the source's as_of
        for r in self.rels:
            if "cut_cap" not in r.types and _ANY not in r.types:  # {rel} may be rendered as CUT_CAP
                continue
            if r.var is None or r.recursive:
                self._bad(f"walks {r.what} for a renewal named by $renewal_id without a variable on the edge, so it "
                          f"cannot be bounded by as_of (write -[c:CUT_CAP]- and filter c.event_date <= {src}.as_of)")
            elif not any(f"{r.var} . event_date <= {s} . as_of" in self._filters(r.var) for s in self.sources):
                self._bad(f"walks {r.what} as {r.shown} for a renewal named by $renewal_id without "
                          f"{r.shown}.event_date <= {src}.as_of as a WHERE conjunct "
                          f"(spec.PIT_WINDOWS: the cuts "
                          f"effective on or before as_of; the bound is against that renewal, not a neighbour)")
            else:
                cuts.append(r)
        # the hub nodes: bounding the CUT_CAP edge bounds nothing when the PricingChange node (same
        # effective_date, same cap_multiplier) is read on its own; incidents likewise
        patterns = [*((v, s[0]) for v, s in self.spots.items()), *((None, n) for n in self.nodes if n.var is None)]
        for var, first in patterns:
            labels = self._labels(var if var else first)
            for hub, date in _CALENDAR.items():
                if not labels & {hub, _ANY} or self._on_calendar(var, first, hub, date, cuts):
                    continue
                label = "a PricingChange" if hub == "pricingchange" else "an Incident"
                how = (f"the tail of a CUT_CAP edge bounded by {src}.as_of, the head of {src}'s own FIRST_RENEWAL_AFTER"
                       if hub == "pricingchange" else "the head of an EXPOSED_TO edge bounded by rule 1")
                self._bad(f"matches {(var and self.shown[var]) or 'an anonymous node'}, which can be {label}, for a "
                          f"renewal named by $renewal_id without cutting the calendar at its as_of (introduce it as "
                          f"{how}, or filter {(var and self.shown[var]) or 'it'}.{date} <= {src}.as_of as a WHERE "
                          f"conjunct): the node carries the same date the edge does")
        # when an incident ended (ends_on, days) is known once it is over: an incident still running at
        # the source's as_of ends after it, so its end is read only for incidents over by then
        t, ended = self.toks, set()
        for j, x in enumerate(t):
            if x.kind == "id" and x.low in _INCIDENT_END and _is(t, j - 1, ".") and _word(t, j - 2):
                ended.add((t[j - 2].low, x.low))
        ended |= {(n.var, key) for n in self.nodes for key in n.keys & set(_INCIDENT_END)}
        for var, field in sorted(ended, key=str):
            labels = self._labels(var) if var is not None else set()
            if var is not None and (self.kind.get(var) != "node" or (labels and not labels & {"incident", _ANY})):
                continue   # not a node that can be an Incident (an alias, another label)
            if var is not None and any(f"{var} . ends_on <= {s} . as_of" in self._filters(var) for s in self.sources):
                continue
            shown = (var and self.shown[var]) or "an anonymous node"
            self._bad(f"reads {shown}.{field} (when an incident ended) for a renewal named by $renewal_id without "
                      f"{shown}.ends_on <= {src}.as_of as a WHERE conjunct: an incident still running at as_of ends "
                      f"after it")

    def _on_calendar(self, var: str | None, first: _Node, hub: str, date: str, cuts: list[_Rel]) -> bool:
        """Rule 6: a hub node the source could see at its as_of (see plan_cuts). The node's first
        pattern is the one that introduces it: a later, bounded pattern would only join it (and in an
        OPTIONAL MATCH not even that). A total over the calendar is no exception: for a named renewal
        it would count the cuts and incidents after its as_of."""
        if hub == "pricingchange" and (
                any(r.tail is first for r in cuts) or
                any(r.types == ("first_renewal_after",) and not r.recursive and r.head is first and
                    r.tail is not None and r.tail.var in self.sources for r in self.rels)):
            return True
        if hub == "incident" and any(r.head is first for r in self.bounded_events):
            return True
        return var is not None and any(f"{var} . {date} <= {s} . as_of" in self._filters(var) for s in self.sources)

    # ---- rules 3 and 7: another renewal's outcome only under the visibility rule, or aggregated
    def _relating(self) -> bool:
        """The template relates renewals: it walks SIMILAR_TO (or a {rel}) or can match two renewals."""
        # every renewal the patterns can match: each variable once, each anonymous node pattern on its own
        renewals = [n for n in [*self.spots, *(n for n in self.nodes if n.var is None)] if self._can_be_renewal(n)]
        return len(renewals) >= 2 or any({"similar_to", _ANY} & set(r.types) for r in self.rels)

    def _label_reads(self) -> list[tuple]:
        """(variable, as written, field, token index | None) of every label field read on a renewal
        other than a named source: ``v.<field>`` anywhere, and pattern map keys."""
        t = self.toks
        reads = [(t[j - 2].low, t[j - 2].text, x.low, j - 2) for j, x in enumerate(t)
                 if x.kind == "id" and x.low in LABEL_FIELDS and _is(t, j - 1, ".") and _word(t, j - 2)]
        reads += [(n.var, n.shown, key, None) for n in self.nodes for key in sorted(n.keys & set(LABEL_FIELDS))]
        return [r for r in reads if r[0] not in self.sources]

    def is_population(self) -> bool:
        """Rule 7 applies: the template reads outcome fields of renewals it does not name and relates none."""
        return bool(self._label_reads()) and not self._relating()

    def neighbour_outcomes(self) -> None:
        relating = self._relating()
        if relating:
            self._one_identity()  # outcome fields read or not: no renewal is pinned by another parameter
        sources = self.sources
        t = self.toks
        reads = self._label_reads()
        if not reads:
            return  # no outcome field of a renewal other than the named one
        if not relating:
            self._population()
            return
        if len(sources) != 1:
            self._bad("reads outcome fields of another renewal but binds no single source renewal by $renewal_id")
            return
        src = next(iter(sources))
        for nbr in sorted({r[0] for r in reads}, key=str):
            mine = [r for r in reads if r[0] == nbr]
            shown = mine[0][1]
            if nbr is None:
                self._bad(f"filters an anonymous node on {mine[0][2]} in its pattern: an outcome field of another "
                          f"renewal is read only under the visibility rule")
                continue
            forms = (_canon_text(visible(nbr, src)), _canon_text(visible_or_current(nbr, src)))
            if set(forms) & self._filters(nbr):
                continue  # a row filter: only neighbours whose outcome is visible are returned at all
            mask = self._mask(forms)
            if mask is None:
                self._bad(f"reads {shown}'s outcome fields without the visibility rule "
                          f"({visible(shown, self.shown.get(src, src))})")
                continue
            alias, lo, hi = mask
            for _, _, field, j in mine:
                guarded = j is not None and (lo <= j < hi or
                                             [x.low for x in t[max(j - 4, 0):j]] == ["case", "when", alias, "then"])
                if not guarded:
                    self._bad(f"{shown}.{field} is returned outside CASE WHEN {alias} THEN ...")

    def _population(self) -> None:
        """Rule 7: outcome fields of renewals the template does not name come back aggregated only."""
        t = self.toks
        why = "while reading outcome fields of renewals it does not name by $renewal_id"
        returns = [j for c in self.clauses if c.kind == "return" for j in range(c.lo, c.hi)]
        if not any(_word(t, j) in _AGGREGATES and _is(t, j + 1, "(") for j in returns):
            self._bad(f"returns rows, not aggregates, {why}: a population template's RETURN has "
                      f"{' / '.join(_AGGREGATES)}")
        if any(_word(t, j) == "collect" and _is(t, j + 1, "(") for j in range(len(t))):
            self._bad(f"collects values into a list (collect()) {why}: a list carries the rows it collected")
        named = sorted({x.low for j, x in enumerate(t) if x.kind == "id" and x.low in _IDENTIFYING
                        and (_is(t, j - 1, ".") or _is(t, j + 1, ":"))})
        if named:
            self._bad(f"returns or filters on {', '.join(named)} {why}: population rows never identify a renewal "
                      f"(name the one renewal by $renewal_id instead)")
        # rows are chosen after the grouping, never before it: WITH ... ORDER BY ... SKIP $k LIMIT 1 is one renewal
        early = sorted({c.kind.upper() for idx, c in enumerate(self.clauses)
                        if c.kind in ("limit", "skip") and idx < self.last_return})
        if early:
            self._bad(f"chooses rows with {' / '.join(early)} before its final RETURN {why}: a WITH ... ORDER BY ... "
                      f"SKIP / LIMIT keeps some renewals and drops the rest, so its aggregates describe the ones it "
                      f"kept (with LIMIT 1, one renewal at a time)")
        # every grouping (the RETURN, and any WITH that aggregates) is keyed by cohort keys: a group per
        # date is a row per renewal, whether the RETURN or an earlier WITH groups by the date
        defs = self._definitions()
        keys: set[str] = set()
        for c in self.clauses:
            if c.kind not in ("with", "return"):
                continue
            exprs = [item[:-2] if len(item) >= 3 and _word(t, item[-2]) == "as" else item for item in self._items(c)]
            if c.kind == "with" and not any(self._aggregated(e) for e in exprs):
                continue  # a projection, not a grouping: its aliases are read where they become keys
            for e in exprs:
                try:
                    keys |= self._key_fields(e, defs, frozenset())
                except _NoParse:
                    self._bad(f"has a {c.kind.upper()} item with unbalanced brackets")
        loose = sorted(keys - set(COHORT_KEYS))
        if loose:
            self._bad(f"groups its rows by {', '.join(loose)} {why}: every item of the RETURN, and of a WITH that "
                      f"aggregates, is an aggregate or a cohort key (queries.COHORT_KEYS: {', '.join(COHORT_KEYS)}); "
                      f"an outcome field reaches the rows only inside an aggregate or as the cohort it groups by, and "
                      f"an earlier WITH's aggregate used as a key reads what it aggregates ({_COUNT_KEY} / sum(...): a "
                      f"count or sum is a number, a key only as the per-renewal flag <aggregate> > 0, count(x) > 0)")
        self._cell_statistics(defs, why)
        self._counted_once(why)
        # parameters select hubs and cohorts, never one renewal
        for j, x in enumerate(t):
            if x.kind != "param" or self._paging(j):
                continue
            field = self._param_field(j)
            if field not in (*COHORT_KEYS, *HUB_KEYS):
                self._bad(f"compares {x.text} with {field or 'an expression'} {why}: a population template's parameter "
                          f"is one whole side of a comparison whose other whole side is <variable>.<key> (or the "
                          f"value of key in a MATCH pattern map), key a hub ({', '.join(HUB_KEYS)}) or a cohort key, "
                          f"so it cannot narrow the population to one renewal (no arithmetic, concatenation, function "
                          f"or alias beside it)")
        # a literal written into the template selects exactly as a parameter would
        fields = self._selected_by_value(defs)
        if fields:
            self._bad(f"selects renewals by value on {', '.join(fields)} {why}: a literal (or an alias of one) is read "
                      f"like a parameter, so a comparison, a simple CASE or a MATCH pattern map that holds one reads "
                      f"only cohort or hub keys (the one exception is the PIT window <edge>.event_date > "
                      f"<renewal>.as_of - INTERVAL('<n> DAYS')); agent_requests_28d = 211 is one renewal")

    # ---- rule 7: what an aggregate may compute (one cell statistic per column, over vetted shapes)
    def _calls(self, span: list[int]) -> list[tuple[int, int]]:
        """(name token, closing bracket) of every engine aggregate call in ``span`` (outermost first)."""
        t, out = self.toks, []
        for j in span:
            if _word(t, j) in ENGINE_AGGREGATES and _is(t, j + 1, "(") and not _is(t, j - 1, "."):
                out.append((j, _close(t, j + 1)))
        return out

    def _cell_statistics(self, defs: dict[str, list[int]], why: str) -> None:
        """Rule 7: every aggregate is count / sum over a vetted argument; an item of a WITH / RETURN /
        ORDER BY that holds one is that aggregate alone (in a WITH that keeps a pattern variable, also
        the per-renewal flag ``<aggregate> <op> 0``); and an aggregate's alias travels on whole."""
        t = self.toks
        try:
            calls = self._calls(list(range(len(t))))
        except _NoParse:
            self._bad(f"has an aggregate call with unbalanced brackets {why}")
            return
        for j, close in calls:
            name = t[j].low
            if name in ("min", "max"):
                self._bad(f"aggregates with {t[j].text}() {why}: min / max return the extreme renewal's own value "
                          f"(grouped by outcome, that renewal and its label), not a cell statistic; a population "
                          f"template aggregates with {' / '.join(CELL_STATISTICS)} only")
            elif name == "avg":
                self._bad(f"aggregates with {t[j].text}() {why}: avg is sum / count, two cell statistics in one "
                          f"number (a rate: beside a published count it gives a suppressed sum back, and a rate of "
                          f"1 / k is a count the tool layer cannot see); a population template aggregates with "
                          f"{' / '.join(CELL_STATISTICS)} only")
            elif name not in CELL_STATISTICS and name != "collect":     # collect() is reported on its own
                self._bad(f"aggregates with {t[j].text}() {why}: a population template aggregates with "
                          f"{' / '.join(CELL_STATISTICS)} only (an engine aggregate such as histogram or a percentile "
                          f"carries the values of single rows)")
            elif name in CELL_STATISTICS and not self._vetted_argument(name, j + 2, close, defs):
                shown = _canon([t[k] for k in range(j, close + 1)])
                self._bad(f"aggregates {shown[:120]} {why}: the argument of {name}() is a vetted shape (*, a pattern "
                          f"variable, a cohort-key property, sum of a cohort flag ({', '.join(COHORT_FLAGS)}), a "
                          f"literal (0 or 1 in a sum), CASE over cohort keys whose values are literals (0 or 1 in a "
                          f"sum), null or such properties, or an alias of one): no arithmetic, concatenation or "
                          f"function inside, and no feature, date or amount (max(n.agent_requests_28d * 10 + "
                          f"n.churned) singles out renewals with their label in a cell of any size, and sum(CASE WHEN "
                          f"<sub-cell> AND n.churned = 1 THEN 1001 WHEN <sub-cell> THEN 1 ELSE 0 END) packs a "
                          f"sub-cell's lapses beside its size)")
        for c in self.clauses:
            if c.kind not in ("with", "return", "order by"):
                continue
            items = self._order_items(c) if c.kind == "order by" else self._items(c)
            for item in items:
                expr = item[:-2] if len(item) >= 3 and _word(t, item[-2]) == "as" else item
                inner = [(j, close) for j, close in calls if expr and expr[0] <= j <= expr[-1]]
                if inner and not self._one_statistic(expr, inner, c):
                    self._bad(f"combines an aggregate with other terms in one {c.kind.upper()} item "
                              f"({_canon([t[k] for k in expr])[:120]}) {why}: an item that aggregates is one count / "
                              f"sum alone (in a WITH that keeps a pattern variable, one group per renewal, also the "
                              f"flag <aggregate> <op> 0), so every column is one cell statistic the tool layer can "
                              f"suppress: sum(a) * 1000 + sum(b) packs two cells into one number, and the flag "
                              f"sum(<sub-cell>) = 1 in a RETURN singles out a renewal of a cell")
        self._statistics_travel_whole(why)

    def _keeps_variable(self, c: _Clause) -> bool:
        """The WITH keeps a pattern variable (or ``*``): it groups per renewal (or per pattern row)."""
        for item in self._items(c):
            words = [self.toks[k] for k in item]
            if len(words) == 1 and (_is(words, 0, "*") or (words[0].kind == "id" and words[0].low in self.kind)):
                return True
        return False

    def _one_statistic(self, expr: list[int], inner: list[tuple[int, int]], clause: _Clause) -> bool:
        """The item is one aggregate call alone; in a WITH that keeps a pattern variable, also the flag
        ``<call> <op> 0`` / ``0 <op> <call>`` (a per-renewal boolean, count(f) > 0)."""
        t = self.toks
        j, close = inner[0]
        if j == expr[0] and close == expr[-1]:
            return True
        if clause.kind != "with" or not self._keeps_variable(clause):
            return False
        lo, hi = expr[0], expr[-1]
        after = j == lo and close + 2 == hi and t[close + 1].kind == "op" and t[close + 1].text in _COMPARE and \
            _num_in(t, hi, (0.0,))
        before = close == hi and j - 2 == lo and t[j - 1].kind == "op" and t[j - 1].text in _COMPARE and \
            _num_in(t, lo, (0.0,))
        return after or before

    def _statistics_travel_whole(self, why: str) -> None:
        """Rule 7: an alias of an aggregate (a WITH / RETURN item that aggregates) is used only whole: a
        whole WITH / RETURN / ORDER BY item, the whole argument of an aggregate, a whole CASE value, or a
        whole condition (a flag). Arithmetic, a function or a comparison around it is refused, in any
        clause: ``WHERE l * 1000 + c > 5`` and ``ORDER BY l * 1000 + c`` pack two cells, and ``c = 1``
        picks groups by size."""
        t = self.toks
        defined: dict[str, int] = {}                # alias -> the token that names it (after AS)
        for c in self.clauses:
            for item in self._items(c) if c.kind in ("with", "return") else ():
                if len(item) >= 3 and _word(t, item[-2]) == "as" and t[item[-1]].kind == "id" and \
                        self._aggregated(item[:-2]):
                    defined.setdefault(t[item[-1]].low, item[-1])
        for j, x in enumerate(t):
            if x.kind != "id" or x.low not in defined or j == defined[x.low] or _is(t, j - 1, ".") or \
                    _is(t, j + 1, "(") or _word(t, j - 1) == "as":
                continue
            if not self._whole_use(j):
                lo, hi = max(j - 3, 0), min(j + 4, len(t))
                self._bad(f"uses the aggregate alias {x.text} inside an expression (... "
                          f"{_canon(t[lo:hi])} ...) {why}: an aggregate travels on only whole (a WITH / RETURN / "
                          f"ORDER BY item, the argument of count / sum, a CASE value or a flag condition): "
                          f"l * 1000 + c packs two cells into one number, and c = 1 picks groups by their size")

    def _whole_use(self, j: int) -> bool:
        """The identifier at j (through brackets that hold only it) is a whole item, a whole aggregate
        argument, a whole CASE value or a whole condition (see _statistics_travel_whole)."""
        t = self.toks
        lo, hi = j, j

        def grouping(k: int) -> bool:               # the '(' at k groups (it opens no call)
            if k - 1 < 0 or t[k - 1].kind == "op":
                return True
            return t[k - 1].kind == "id" and t[k - 1].low in _KEYWORDS and not _is(t, k - 2, ".")

        while _is(t, lo - 1, "(") and _is(t, hi + 1, ")") and grouping(lo - 1):
            lo, hi = lo - 1, hi + 1                 # (x): brackets around it alone, not a call's
        if _is(t, lo - 1, "(") and _is(t, hi + 1, ")") and _word(t, lo - 2) in ENGINE_AGGREGATES and \
                not _is(t, lo - 3, "."):
            return True                             # sum(x), count(x)
        c = self.clauses[self.clause_at[j]] if self.clause_at[j] >= 0 else None
        if c is not None and c.kind in ("with", "return", "order by"):
            items = self._order_items(c) if c.kind == "order by" else self._items(c)
            for item in items:
                expr = item[:-2] if len(item) >= 3 and _word(t, item[-2]) == "as" else item
                if expr == list(range(lo, hi + 1)):
                    return True                     # a whole item: x, x AS y, ORDER BY x DESC
        if _is(t, lo - 1, ".") or _is(t, hi + 1, "."):
            return False
        before, after = _word(t, lo - 1), _word(t, hi + 1)
        if before in ("then", "else") and after in ("when", "else", "end"):
            return True                             # CASE ... THEN x [WHEN | ELSE | END], ELSE x END
        if before == "when" and after == "then":
            return self._searched_case(lo)          # WHEN x THEN of a searched CASE: x is a condition
        ends = ("and", "or", "xor", *_CLAUSE_WORDS)
        starts = ("where", "and", "or", "xor", "not")
        return before in starts and (after in ends or hi + 1 >= len(t))   # WHERE x [AND ...]

    def _searched_case(self, j: int) -> bool:
        """The CASE whose WHEN stands just before token j is a searched CASE (``CASE WHEN ...``), not
        the simple form ``CASE <subject> WHEN <value>``, where a WHEN operand is compared with the subject."""
        t, depth = self.toks, 0
        for k in range(j - 1, -1, -1):
            keyword = _word(t, k) if not _is(t, k - 1, ".") else ""
            if (t[k].kind == "op" and t[k].text in _CLOSE) or keyword == "end":
                depth += 1
            elif (t[k].kind == "op" and t[k].text in _OPEN) or keyword == "case":
                if depth == 0:
                    return keyword == "case" and _word(t, k + 1) == "when"
                depth -= 1
        return False

    def _vetted_argument(self, name: str, lo: int, hi: int, defs: dict[str, list[int]]) -> bool:
        """Tokens lo..hi-1, the argument of the cell statistic ``name``, are a vetted shape (rule 7)."""
        t, span = self.toks, list(range(lo, hi))
        distinct = bool(span) and _word(t, span[0]) == "distinct"
        span = span[1:] if distinct else span
        whole = len(span) == 1 and t[span[0]].kind == "id" and t[span[0]].low in self.kind
        if name == "count" and (whole or (not distinct and len(span) == 1 and _is(t, span[0], "*"))):
            return True
        try:
            return not distinct and self._cohort_value(span, defs, numeric=name != "count")
        except _NoParse:
            return False

    def _strip(self, span: list[int]) -> list[int]:
        """``span`` without brackets that enclose all of it."""
        t = self.toks
        while len(span) >= 2 and _is(t, span[0], "(") and _close(t, span[0]) == span[-1]:
            span = span[1:-1]
        return span

    def _top(self, span: list[int]) -> list[int]:
        """The positions in ``span`` at depth 0: outside brackets and outside nested CASE ... END."""
        t, out, depth = self.toks, [], 0
        for pos, j in enumerate(span):
            keyword = _word(t, j) if not _is(t, j - 1, ".") else ""
            if (t[j].kind == "op" and t[j].text in _CLOSE) or keyword == "end":
                depth -= 1
                if depth < 0:
                    raise _NoParse
                continue
            if depth == 0:
                out.append(pos)
            if (t[j].kind == "op" and t[j].text in _OPEN) or keyword == "case":
                depth += 1
        if depth:
            raise _NoParse
        return out

    def _counted_once(self, why: str) -> None:
        """Rule 7: count(*) and sum count rows, so the rows an aggregate counts hold each renewal once.

        A MATCH keeps one row per renewal when each of its relationships is one per renewal (the
        renewal's HAS_RENEWAL from its subscription, its ON_PLAN, its FIRST_RENEWAL_AFTER to a pricing
        change pinned by change_id: the contract counts one subscription and one plan per renewal, and
        build.py gives a renewal at most one edge per change, while EXPOSED_TO is one edge per active
        day of an incident) and every node is the renewal, a
        variable bound by one of them (``units``), or an end of one. Anything else
        (a cartesian MATCH, an UNWIND, an edge to events or to the calendar hubs) can repeat a renewal,
        a number of times the template chooses: MATCH (p:Plan) beside the renewal with WHERE
        p.plan_tier = 'pro' OR <sub-cell lapsed> counts those renewals three times, so count(*) is the
        cell plus twice a sub-cell's lapses. Such rows are collapsed before a cohort grouping counts
        them: WITH DISTINCT <renewal>, or a WITH that keeps the renewal (or its subscription / plan) and
        aggregates per renewal (WITH r, count(f) > 0 AS flag).
        """
        t = self.toks
        renewals = [n for n in [*self.spots, *(n for n in self.nodes if n.var is None)] if self._can_be_renewal(n)]
        r = renewals[0] if len(renewals) == 1 else None

        def is_r(node: _Node | None) -> bool:
            return node is not None and r is not None and (node.var == r if isinstance(r, str) else node is r)

        units = {r} if isinstance(r, str) else set()          # bound once per renewal: it, its subscription, plan
        for rel in self.rels:
            if not rel.recursive and rel.types == ("has_renewal",) and is_r(rel.head) and rel.tail.var:
                units.add(rel.tail.var)
            if not rel.recursive and rel.types == ("on_plan",) and is_r(rel.tail) and rel.head.var:
                units.add(rel.head.var)

        def once(rel: _Rel) -> bool:
            if rel.recursive or len(rel.types) != 1 or rel.direction not in ("out", "in"):
                return False
            typ = rel.types[0]
            return (typ == "has_renewal" and is_r(rel.head)) or (typ == "on_plan" and is_r(rel.tail)) or \
                (typ == "first_renewal_after" and is_r(rel.tail) and "change_id" in rel.head.keys)

        state, cause = "renewals", ""                           # renewals | repeated | groups
        for idx, c in enumerate(self.clauses):
            if c.kind in _MATCHES:
                for at, nodes, rels in self.matched:
                    if at != idx:
                        continue
                    ends = {id(x) for rel in rels if once(rel) for x in (rel.left, rel.right)}
                    bad_rel = next((rel for rel in rels if not once(rel)), None)
                    bad_node = next((n for n in nodes if not (is_r(n) or n.var in units or id(n) in ends)), None)
                    if state == "groups" or bad_rel or bad_node:
                        what = ("a MATCH after a grouping" if state == "groups" else
                                f"a {bad_rel.what or 'untyped'} relationship" if bad_rel else
                                f"the node {bad_node.shown or ':' + '/'.join(sorted(bad_node.labels))} beside the "
                                f"renewal")
                        state, cause = "repeated", cause if state == "repeated" else what
            elif c.kind == "unwind":
                state, cause = "repeated", cause if state == "repeated" else "an UNWIND"
            elif c.kind in ("with", "return"):
                exprs = [item[:-2] if len(item) >= 3 and _word(t, item[-2]) == "as" else item for item in self._items(c)]
                aggregates = any(self._aggregated(e) for e in exprs)
                distinct = _word(t, c.lo) == "distinct"
                if not (aggregates or distinct):
                    continue                                   # a projection: the same rows
                star = any(len(e) == 1 and _is(t, e[0], "*") for e in exprs)
                kept = set(self.kind) if star else {t[e[0]].low for e in exprs
                                                    if len(e) == 1 and t[e[0]].kind == "id" and t[e[0]].low in self.kind}
                if kept:
                    state = "renewals" if kept <= units else "repeated"
                    cause = "" if state == "renewals" else f"a {c.kind.upper()} that keeps {', '.join(sorted(kept))}"
                    continue
                if aggregates and state == "repeated":
                    self._bad(f"aggregates rows that can repeat a renewal ({cause}) {why}: count(*) and sum count "
                              f"rows, so a pattern that repeats renewals (a cartesian MATCH, an UNWIND, an edge to "
                              f"events or to a calendar hub) adds a multiple of a sub-cell to a count (MATCH (p:Plan) "
                              f"beside the renewal and WHERE p.plan_tier = 'pro' OR <sub-cell lapsed> counts those "
                              f"renewals three times); collapse the rows first, WITH DISTINCT <renewal> or a WITH "
                              f"that keeps the renewal (WITH r, count(f) > 0 AS flag)")
                    return
                state = "groups"

    def _cohort_value(self, span: list[int], defs: dict[str, list[int]], *, numeric: bool,
                      seen: frozenset = frozenset()) -> bool:
        """A vetted aggregate argument or CASE value: a literal, null, a cohort-key property, a
        cohort-valued alias, or a CASE over cohort keys. When ``numeric`` (sum), every renewal adds 0 or 1:
        a literal is 0 or 1 and a property is a 0/1 cohort flag, so the sum counts a sub-cell (a CASE
        value of 1001 would add a second count, scaled, to the first)."""
        t = self.toks
        try:
            span = self._strip(span)
        except _NoParse:
            return False
        if not span:
            return False
        if len(span) == 1:
            x = t[span[0]]
            if x.kind == "num":
                return not numeric or _num_in(t, span[0], _UNIT_VALUES)
            if x.kind == "id" and x.low == "null":
                return True
            if x.kind == "str" or (x.kind == "id" and x.low in ("true", "false")):
                return not numeric
            if x.kind == "id" and x.low in defs and x.low not in self.kind and x.low not in seen:
                return self._cohort_alias(x.low, defs, numeric=numeric, seen=seen | {x.low})
            return False
        if len(span) == 3 and t[span[0]].kind == "id" and self.kind.get(t[span[0]].low) in ("node", "relationship") \
                and _is(t, span[1], ".") and _word(t, span[2]):
            return t[span[2]].low in (COHORT_FLAGS if numeric else COHORT_KEYS)
        if _word(t, span[0]) == "case" and _word(t, span[-1]) == "end":
            return self._cohort_case(span, defs, numeric=numeric, seen=seen)
        return False

    def _cohort_case(self, span: list[int], defs: dict[str, list[int]], *, numeric: bool, seen: frozenset) -> bool:
        """``CASE WHEN <cohort predicate> THEN <value> ... [ELSE <value>] END`` or the simple form
        ``CASE <cohort-key property> WHEN <literal> THEN <value> ... END`` (values: _cohort_value)."""
        t = self.toks
        body = span[1:-1]
        try:
            if self._top(span) != [0]:
                return False                                        # CASE ... END <op> CASE ... END: not one CASE
            top = self._top(body)
        except _NoParse:
            return False
        marks = [p for p in top if _word(t, body[p]) in ("when", "then", "else")]
        if not marks or _word(t, body[marks[0]]) != "when":
            return False
        subject = body[:marks[0]]
        parts: list[tuple[str, list[int]]] = []
        for a, b in zip(marks, [*marks[1:], len(body)], strict=True):
            parts.append((_word(t, body[a]), body[a + 1:b]))
        kinds = [k for k, _ in parts]
        pairs = kinds[:-1] if kinds[-1] == "else" else kinds
        if pairs != ["when", "then"] * (len(pairs) // 2) or kinds.count("else") > 1:
            return False
        for kind, part in parts:
            if not part:
                return False
            if kind == "when":
                ok = self._cohort_operand(part, defs, seen) if subject else self._cohort_predicate(part, defs, seen)
            else:
                ok = self._cohort_value(part, defs, numeric=numeric, seen=seen)
            if not ok:
                return False
        return not subject or self._cohort_operand(subject, defs, seen)

    def _cohort_operand(self, span: list[int], defs: dict[str, list[int]], seen: frozenset) -> bool:
        """One side of a cohort predicate: a literal, a parameter, a cohort or hub key property, or a
        cohort-valued alias (a flag such as ``count(f) > 0 AS first_after``)."""
        t = self.toks
        try:
            span = self._strip(span)
        except _NoParse:
            return False
        if len(span) == 1:
            x = t[span[0]]
            if x.kind in ("num", "str", "param") or (x.kind == "id" and x.low in ("null", "true", "false")):
                return True
            if x.kind == "id" and x.low in defs and x.low not in self.kind and x.low not in seen:
                return self._cohort_alias(x.low, defs, numeric=False, seen=seen | {x.low}, operand=True)
            return False
        return len(span) == 3 and t[span[0]].kind == "id" and \
            self.kind.get(t[span[0]].low) in ("node", "relationship") and _is(t, span[1], ".") and \
            _word(t, span[2]) in (*COHORT_KEYS, *HUB_KEYS)

    def _cohort_predicate(self, span: list[int], defs: dict[str, list[int]], seen: frozenset) -> bool:
        """A condition over cohort operands: comparisons, IS [NOT] NULL, IN [literals], AND / OR / XOR /
        NOT and brackets; a lone operand is a flag."""
        t = self.toks
        try:
            span = self._strip(span)
            top = self._top(span)
        except _NoParse:
            return False
        if not span:
            return False
        for words in (("or", "xor"), ("and",)):
            cuts = [p for p in top if _word(t, span[p]) in words]
            if cuts:
                return all(self._cohort_predicate(span[a + 1:b], defs, seen)
                           for a, b in itertools.pairwise([-1, *cuts, len(span)]))
        if _word(t, span[0]) == "not":
            return self._cohort_predicate(span[1:], defs, seen)
        if len(span) >= 3 and _word(t, span[-1]) == "null" and _word(t, span[-2]) in ("is", "not"):
            k = len(span) - (3 if _word(t, span[-2]) == "not" else 2)
            return _word(t, span[k]) == "is" and self._cohort_operand(span[:k], defs, seen)
        inside = [p for p in top if _word(t, span[p]) == "in"]
        if inside:
            p, rest = inside[0], span[inside[0] + 1:]
            if len(inside) > 1 or len(rest) < 2 or not _is(t, rest[0], "[") or _close(t, rest[0]) != rest[-1]:
                return False
            values = _split([t[k] for k in rest[1:-1]], _comma)
            return self._cohort_operand(span[:p], defs, seen) and all(
                len(v) == 1 and (v[0].kind in ("num", "str", "param") or v[0].low in ("null", "true", "false"))
                for v in values)
        ops = [p for p in top if t[span[p]].kind == "op" and t[span[p]].text in _COMPARE]
        if len(ops) == 1:
            p = ops[0]
            return self._cohort_operand(span[:p], defs, seen) and self._cohort_operand(span[p + 1:], defs, seen)
        return not ops and self._cohort_operand(span, defs, seen)

    def _cohort_alias(self, alias: str, defs: dict[str, list[int]], *, numeric: bool, seen: frozenset,
                      operand: bool = False) -> bool:
        """An alias is a cohort value when its definition is one (``_cohort_value``), the flag
        ``<cell statistic> <op> 0`` of a WITH that keeps a pattern variable (a per-renewal boolean), or,
        as a value (not an ``operand`` of a predicate), a cell statistic of a WITH grouped by cohort keys
        only: it keeps no pattern variable (a WITH that keeps one aggregates per renewal, a feature value).
        A cell statistic is a number, not a cohort: compared with a literal it picks groups by size."""
        t = self.toks
        expr = defs[alias]
        try:
            calls = self._calls(expr)
        except _NoParse:
            return False
        if not calls:
            return self._cohort_value(expr, defs, numeric=numeric, seen=seen)
        j, close = calls[0]
        name = t[j].low
        if name not in CELL_STATISTICS or not self._vetted_argument(name, j + 2, close, defs):
            return False
        clause = self.clauses[self.clause_at[j]]
        if clause.kind != "with" or not self._one_statistic(expr, calls, clause):
            return False
        if not (j == expr[0] and close == expr[-1]):
            return True                                             # <statistic> <op> 0 per renewal: a flag
        return not operand and not self._keeps_variable(clause)

    def _selected_by_value(self, defs: dict[str, list[int]]) -> list[str]:
        """Rule 7, literals read like parameters: the non-cohort, non-hub properties that a comparison
        mixes with a value (a string, number or parameter, or an alias defined without a pattern
        property), that a simple CASE compares with its WHEN values, or that a MATCH pattern map filters."""
        t, allowed = self.toks, {*COHORT_KEYS, *HUB_KEYS, *_IDENTIFYING}   # identifying: reported on their own
        found: set[str] = set()
        for c in self.clauses:  # node and relationship pattern maps: {key: value, ...}
            for k in range(c.lo, c.hi) if c.kind in _MATCHES else ():
                if _is(t, k, ":") and _word(t, k - 1) and (_is(t, k - 2, "{") or _is(t, k - 2, ",")):
                    found.add(t[k - 1].low)
        found -= allowed
        for j, x in enumerate(t):
            word = _word(t, j)
            if (x.kind == "op" and x.text in _COMPARE) or word in ("in", "contains") or \
                    (word in ("starts", "ends") and _word(t, j + 1) == "with"):
                span = self._operand(j - 1, -1) + self._operand(j + (2 if word in ("starts", "ends") else 1), +1)
                window = re.fullmatch(r"\w+ \. event_date > \w+ \. as_of - interval \( '\d+ days' \)",
                                      _canon([t[k] for k in sorted([*span, j])]), re.IGNORECASE)
                read, value = self._reads(span, defs)
                if value and not window:
                    found |= read - allowed
            elif word == "case" and not _is(t, j - 1, ".") and _word(t, j + 1) != "when":   # CASE <subject> WHEN v
                subject, k, depth = [], j + 1, 0
                while k < len(t) and not (depth == 0 and _word(t, k) == "when"):
                    depth += (t[k].kind == "op" and t[k].text in _OPEN) - (t[k].kind == "op" and t[k].text in _CLOSE)
                    subject.append(k)
                    k += 1
                found |= self._reads(subject, defs)[0] - allowed
        return sorted(found)

    def _operand(self, j: int, step: int) -> list[int]:
        """Token indexes of the operand that starts at j and extends in ``step`` direction (-1 left,
        +1 right) up to the nearest AND / OR / keyword / clause, comma, unmatched bracket or comparison."""
        t, out, depth, k = self.toks, [], 0, j
        opens, closes = (_CLOSE, _OPEN) if step < 0 else (_OPEN, _CLOSE)
        while 0 <= k < len(t):
            x = t[k]
            if x.kind == "op" and x.text in opens:
                depth += 1
            elif x.kind == "op" and x.text in closes:
                if depth == 0:
                    break
                depth -= 1
            elif depth == 0 and (_comma(x) or (x.kind == "op" and x.text in _COMPARE) or
                                 (x.kind == "id" and not _is(t, k - 1, ".") and
                                  x.low in _EDGE_BEFORE | _EDGE_AFTER | {"in", "is", "starts", "ends", "contains"})):
                break
            out.append(k)
            k += step
        return out

    def _reads(self, span: list[int], defs: dict[str, list[int]]) -> tuple[set[str], bool]:
        """(pattern properties the tokens read, following aliases; whether they hold a value: a string,
        a number, a parameter, or an alias whose definition reads no pattern property)."""
        t, read, value = self.toks, set(), False
        for k in span:
            x = t[k]
            if x.kind in ("str", "num", "param"):
                value = True
            elif x.kind == "id" and not _is(t, k - 1, ".") and x.low in defs and x.low not in self.kind:
                via = self._key_fields(defs[x.low], defs, frozenset({x.low}), nested=True) - _STATISTIC_KEYS
                read |= via
                value = value or not via
            elif x.kind == "id" and x.low in self.kind and _is(t, k + 1, ".") and _word(t, k + 2):
                read.add(t[k + 2].low)
        return read, value

    def _aggregated(self, expr: list[int]) -> bool:
        return any(_word(self.toks, j) in _AGGREGATES and _is(self.toks, j + 1, "(") for j in expr)

    def _definitions(self) -> dict[str, list[int]]:
        """alias -> token indexes of the expression a WITH item or an UNWIND defines it as."""
        t, out = self.toks, {}
        for c in self.clauses:
            items = self._items(c) if c.kind == "with" else [list(range(c.lo, c.hi))] if c.kind == "unwind" else []
            for item in items:
                if len(item) >= 3 and _word(t, item[-2]) == "as" and t[item[-1]].kind == "id":
                    out.setdefault(t[item[-1]].low, item[:-2])
        return out

    def _key_fields(self, expr: list[int], defs: dict[str, list[int]], seen: frozenset,
                    nested: bool = False) -> set[str]:
        """The properties a grouping key reads, following WITH / UNWIND aliases (map aliases too).

        The aggregate calls of the key's own clause are not keys and are skipped. An alias is read
        ``nested``: what an earlier WITH aggregated is a per-group value there, so min / max read their
        argument (over a group of one they are its value), and a count, sum or mean reads as
        ``count(...)`` / ``sum(...)`` / ``avg(...)`` (_STATISTIC_KEYS: a number, never a cohort) unless
        it is the flag ``<aggregate> <op> 0`` where it is written (count(f) > 0: a boolean).
        """
        t, out, inside = self.toks, set(), -1
        for j in expr:
            if j <= inside:
                continue
            if _word(t, j) in _AGGREGATES and _is(t, j + 1, "("):
                inside = _close(t, j + 1)
                if nested and _word(t, j) in ("min", "max"):
                    out |= self._key_fields(list(range(j + 2, inside)), defs, seen, nested=True)
                elif nested and not self._flag(j, inside):
                    out.add(f"{_word(t, j)}(...)")
                continue
            if t[j].kind != "id" or _is(t, j - 1, "."):
                continue
            if t[j].low in defs and t[j].low not in self.kind and not _is(t, j + 1, "("):
                if t[j].low not in seen:  # an alias, with or without .key: whatever its definition reads
                    out |= self._key_fields(defs[t[j].low], defs, seen | {t[j].low}, nested=True)
            elif _is(t, j + 1, ".") and _word(t, j + 2):
                out.add(t[j + 2].low)
        return out

    def _flag(self, j: int, close: int) -> bool:
        """The call at tokens j..close is compared with 0 where it is written (``count(f) > 0``): whether
        a renewal has any, a boolean. Compared with another number it selects by a count (count(e) = 7:
        renewals with exactly seven tickets), a feature value, as a literal pin would."""
        t = self.toks
        after = close + 2 < len(t) and t[close + 1].kind == "op" and t[close + 1].text in _COMPARE and \
            _num_in(t, close + 2, (0.0,)) and self._edge(close + 3, "after")
        before = j >= 2 and t[j - 1].kind == "op" and t[j - 1].text in _COMPARE and _num_in(t, j - 2, (0.0,)) and \
            self._edge(j - 3, "before")
        return after or before

    def _edge(self, k: int, side: str) -> bool:
        """Nothing continues an operand at token k, just before it (side 'before') or after it ('after')."""
        t = self.toks
        if k < 0 or k >= len(t):
            return True
        if t[k].kind == "op":
            return t[k].text in (("(", "[", "{", ",") if side == "before" else (")", "]", "}", ","))
        return t[k].kind == "id" and not _is(t, k - 1, ".") and \
            t[k].low in (_EDGE_BEFORE if side == "before" else _EDGE_AFTER)

    def _compared(self, j: int) -> tuple[str, str, str, str] | None:
        """(variable, key, operator, side) when the token at j is one whole side of a comparison whose
        other whole side is ``<pattern variable>.<key>``: side 'right' for ``v.key <op> $x``, 'left'
        for ``$x <op> v.key``. None for any other use (arithmetic, concatenation, a function, an
        index, an alias or map literal on either side)."""
        t = self.toks

        def op(k: int) -> bool:
            return 0 <= k < len(t) and ((t[k].kind == "op" and t[k].text in _COMPARE) or _word(t, k) == "in")

        got = None
        if op(j - 1) and self._edge(j + 1, "after") and _word(t, j - 2) and _is(t, j - 3, ".") and \
                _word(t, j - 4) and self._edge(j - 5, "before"):
            got = (t[j - 4].low, t[j - 2].low, t[j - 1].low if t[j - 1].kind == "id" else t[j - 1].text, "right")
        elif op(j + 1) and self._edge(j - 1, "before") and _word(t, j + 2) and _is(t, j + 3, ".") and \
                _word(t, j + 4) and self._edge(j + 5, "after"):
            got = (t[j + 2].low, t[j + 4].low, t[j + 1].low if t[j + 1].kind == "id" else t[j + 1].text, "left")
        return got if got and self.kind.get(got[0]) in ("node", "relationship") else None

    def _map_value(self, j: int) -> str | None:
        """The key when the token at j is the whole value of an entry of a MATCH pattern's property map."""
        t = self.toks
        c = self.clause_at[j] if 0 <= j < len(t) else -1
        if c >= 0 and self.clauses[c].kind in _MATCHES and _is(t, j - 1, ":") and _word(t, j - 2) and \
                (_is(t, j - 3, "{") or _is(t, j - 3, ",")) and (_is(t, j + 1, ",") or _is(t, j + 1, "}")):
            return t[j - 2].low
        return None

    def _param_field(self, j: int) -> str | None:
        """The key the parameter at j selects: ``v.key <op> $x``, ``$x <op> v.key`` (both sides whole) or
        ``{key: $x}`` in a MATCH pattern; None when it is used any other way."""
        key = self._map_value(j)
        if key is not None:
            return key
        got = self._compared(j)
        return got[1] if got else None

    def _paging(self, j: int) -> bool:
        """The token at j is the whole value of the final LIMIT / SKIP (after the last RETURN)."""
        c = self.clause_at[j] if 0 <= j < len(self.toks) else -1
        return c > self.last_return >= 0 and self.clauses[c].kind in ("limit", "skip") and \
            self.clauses[c].hi - self.clauses[c].lo == 1

    def _one_identity(self) -> None:
        """Rule 3: the rows of a template that relates renewals describe its source only (parameters,
        identifying fields and the shape of the patterns; see the module docstring)."""
        t = self.toks
        extra = sorted({x.text for x in t if x.kind == "param"} - set(_NEIGHBOUR_PARAMS))
        if extra:
            self._bad(f"takes {', '.join(extra)}: a template that relates renewals takes only "
                      f"{', '.join(_NEIGHBOUR_PARAMS)} (another parameter could pin a second renewal), so the "
                      f"visibility rule cannot be measured against one renewal while the rows describe another")
        for j, x in enumerate(t):
            if x.kind != "param" or x.text not in _NEIGHBOUR_PARAMS:
                continue
            if x.text in ("$k", "$limit"):
                got = self._compared(j)
                rank = got is not None and got[1] == "rank" and self.kind.get(got[0]) == "relationship" and \
                    got[2] in (("<=", "<") if got[3] == "right" else (">=", ">"))
                if not (self._paging(j) or rank):
                    self._bad(f"uses {x.text} other than as the value of its final LIMIT / SKIP or in "
                              f"<relationship>.rank <= {x.text}: in a template that relates renewals {x.text} only "
                              f"pages the source's relations (anywhere else a string or an integer {x.text} could pin "
                              f"a second renewal)")
            elif x.text == "$today":
                if not self._in_visible_or_current(j):
                    self._bad("uses $today outside visible_or_current(<neighbour>, <source>): $today only widens the "
                              "visibility rule for a current source")
            else:
                got = self._compared(j)
                if not (self._map_value(j) == "renewal_id" or (got is not None and got[1] == "renewal_id" and
                                                              got[2] in ("=", "<>"))):
                    self._bad("uses $renewal_id other than to bind the source ({renewal_id: $renewal_id}, "
                              "<var>.renewal_id = $renewal_id or <> $renewal_id, both sides whole): a value derived "
                              "from it (a substring, a concatenation) could name another renewal or subscription")
        self._identifying_projected_only()
        if self.sources:
            self._direct_relations()
            self._value_relations()

    def _in_visible_or_current(self, j: int) -> bool:
        """The parameter at j is the $today of ``visible_or_current(<neighbour>, <a source>)``, written out."""
        t, lo = self.toks, j - 9  # $today is the tenth token of the rule
        width = len(_tokenise(visible_or_current("n", "r"))[0])
        if lo < 0 or lo + width > len(t) or t[lo].kind != "id":
            return False
        written = _canon(t[lo:lo + width])
        return any(written == _canon_text(visible_or_current(t[lo].low, s)) for s in self.sources)

    def _identifying(self, j: int, idents: set[str]) -> tuple[int, int, str] | None:
        """(first token, last token, how) when the token at j is an identifying value: ``v.<field>``
        ('property'), a pattern map key ('key') or an alias of an identifying value ('alias')."""
        t, x = self.toks, self.toks[j]
        if x.kind != "id" or _is(t, j + 1, "("):
            return None
        if x.low in _IDENTIFYING and _is(t, j - 1, ".") and _word(t, j - 2):
            return j - 2, j, "property"
        if _is(t, j - 1, ".") or _word(t, j - 1) == "as":
            return None
        if x.low in _IDENTIFYING and _is(t, j + 1, ":") and (_is(t, j - 1, "{") or _is(t, j - 1, ",")):
            return j, j, "key"
        return (j, j, "alias") if x.low in idents else None

    def _order_items(self, c: _Clause) -> list[list[int]]:
        """ORDER BY items as token index lists, without their ASC / DESC."""
        items = [[]]
        depth = 0
        for j in range(c.lo, c.hi):
            x = self.toks[j]
            depth += (x.kind == "op" and x.text in _OPEN) - (x.kind == "op" and x.text in _CLOSE)
            if depth == 0 and _comma(x):
                items.append([])
            else:
                items[-1].append(j)
        return [i[:-1] if i and _word(self.toks, i[-1]) in ("asc", "desc", "ascending", "descending") else i
                for i in items]

    def _identifying_projected_only(self) -> None:
        """Rule 3: in a template that relates renewals an identifying field, or an alias of one, is only
        projected (a whole WITH / RETURN item), sorted by in the final ORDER BY, or compared exactly
        with $renewal_id: never wrapped, compared with a literal or another parameter, or sorted by in
        a WITH (before its SKIP / LIMIT)."""
        t = self.toks
        idents: set[str] = set()
        for c in self.clauses:  # aliases of an identifying value, in clause order
            items = self._items(c) if c.kind == "with" else [list(range(c.lo, c.hi))] if c.kind == "unwind" else []
            for item in items:
                if len(item) >= 3 and _word(t, item[-2]) == "as" and t[item[-1]].kind == "id" and \
                        any(self._identifying(k, idents) for k in item[:-2]):
                    idents.add(t[item[-1]].low)
        whole: set[tuple[int, ...]] = set()
        for idx, c in enumerate(self.clauses):
            if c.kind in ("with", "return"):
                whole |= {tuple(i[:-2] if len(i) >= 3 and _word(t, i[-2]) == "as" else i) for i in self._items(c)}
            elif c.kind == "order by" and idx > self.last_return:
                whole |= {tuple(i) for i in self._order_items(c)}
        for j in range(len(t)):
            got = self._identifying(j, idents)
            if got is None or got[1] != j:
                continue
            lo, hi, how = got
            if how == "key":
                ok = t[j].low == "renewal_id" and j + 2 < len(t) and t[j + 2].text == "$renewal_id" and \
                    self._map_value(j + 2) == "renewal_id"
            else:
                ok = tuple(range(lo, hi + 1)) in whole or (how == "property" and self._compared_to_source(lo, hi))
            if not ok:
                what = f"{t[lo].text}.{t[j].text}" if how == "property" else \
                    f"the pattern map key {t[j].text}" if how == "key" else \
                    f"{t[j].text} (an alias of an identifying field)"
                self._bad(f"uses {what} other than as a whole projection, a final sort key or an exact comparison with "
                          f"$renewal_id: in a template that relates renewals an identifying field is never wrapped, "
                          f"compared with a literal or another value, or sorted by before a SKIP / LIMIT, so no "
                          f"renewal but the source can be pinned")

    def _compared_to_source(self, lo: int, hi: int) -> bool:
        """Tokens lo..hi (``v.renewal_id``) are one whole side of ``= $renewal_id`` or ``<> $renewal_id``."""
        t = self.toks
        for k in (hi + 2, lo - 2):
            if 0 <= k < len(t) and t[k].text == "$renewal_id":
                got = self._compared(k)
                if got is not None and got[:2] == (t[lo].low, "renewal_id") and got[2] in ("=", "<>"):
                    return True
        return False

    def _direct_relations(self) -> None:
        """Rule 3: every renewal but the source is one of the source's own relations, reached from it
        with no other renewal in between (hubs and subscriptions may be; a recursive path is one step),
        and hangs off the source alone: beyond the source's side (the source and the subscriptions /
        hubs it reaches with no renewal in between) no pattern joins it to another renewal."""
        def key(n: _Node):
            return n.var if n.var else id(n)

        renewals = {key(n): n for n in self.nodes if self._can_be_renewal(n.var if n.var else n)}
        adjacent: dict = {}
        for r in self.rels:
            adjacent.setdefault(key(r.left), set()).add(key(r.right))
            adjacent.setdefault(key(r.right), set()).add(key(r.left))
        reached, todo = set(self.sources), list(self.sources)
        while todo:
            for nxt in adjacent.get(todo.pop(), ()):
                if nxt not in reached:
                    reached.add(nxt)
                    if nxt not in renewals:  # a subscription or a hub: walk on through it
                        todo.append(nxt)
        src = self.shown[min(self.sources)]
        for k, n in renewals.items():
            if k not in reached:
                shown = n.shown or "an anonymous renewal pattern"
                self._bad(f"matches {shown}, a renewal that is not one of the source's own relations ({src} reaches it "
                          f"only through another renewal, or not at all): the rows would describe {shown}, or the "
                          f"renewal it hangs off, while the visibility rule reads {src}.as_of (relate every other "
                          f"renewal to {src} directly: SIMILAR_TO, a recursive SIMILAR_TO path, or a shared "
                          f"Subscription, Plan or hub)")
        # the source's side: the source and what it reaches with no renewal in between. Beyond it, what
        # hangs off one renewal (its SIMILAR_TO neighbours, its subscription, a hub only it reaches) holds
        # no second renewal: else the rows describe that renewal's relations, while the rule reads src.as_of
        own = set(self.sources) | {k for k in reached if k not in renewals}
        seen: set = set()
        for k in renewals:
            if k in own or k in seen:
                continue
            part, todo = {k}, [k]
            while todo:
                for nxt in adjacent.get(todo.pop(), ()):
                    if nxt not in own and nxt not in part:
                        part.add(nxt)
                        todo.append(nxt)
            seen |= part
            together = [renewals[x].shown or "an anonymous renewal pattern" for x in renewals if x in part]
            if len(together) > 1:
                names = " and ".join(sorted(together, key=str))
                self._bad(f"joins {names} other than through {src} (a SIMILAR_TO, a path, a {{rel}} or a "
                          f"subscription / hub only they share): the rows would describe one renewal's relations "
                          f"while the visibility rule reads {src}.as_of. Every other renewal hangs off {src} alone: a "
                          f"relationship between two renewals has {src} as one end, and a renewal reached through a "
                          f"hub is a row that is read, never an anchor for another renewal")

    def _value_relations(self) -> None:
        """Rule 3: no expression reads two renewals other than the source (following aliases): a
        comparison, arithmetic, CASE, sort key, aggregate or UNWIND that mixes them relates them as
        an edge would (``n.agent_requests_28d = h.agent_requests_28d``); and a MATCH pattern map
        holds literals and parameters only (``(n {agent_requests_28d: h.agent_requests_28d})``)."""
        t = self.toks
        defs = self._definitions()
        for c in self.clauses:   # RETURN aliases too: the final ORDER BY reads them
            for item in self._items(c) if c.kind == "return" else ():
                if len(item) >= 3 and _word(t, item[-2]) == "as" and t[item[-1]].kind == "id":
                    defs.setdefault(t[item[-1]].low, item[:-2])
        src = self.shown[min(self.sources)]
        try:
            units, maps = self._units()
        except _NoParse:
            self._bad("has a WHERE with unbalanced brackets or CASE ... END")
            return
        for value in maps:
            read = sorted(self.shown.get(v, v) for v in self._vars_read(value, defs, frozenset()))
            if read:
                self._bad(f"has a MATCH pattern map value that reads {', '.join(read)} "
                          f"({_canon([t[j] for j in value])[:80]}): in a template that relates renewals a pattern map "
                          f"holds literals and parameters only (a value read from another pattern joins two renewals "
                          f"by value, as an edge would)")
        others = {v for v in self.spots if v not in self.sources and self.kind.get(v) == "node"
                  and self._can_be_renewal(v)}
        if len(others) < 2:
            return
        # a relationship's properties describe its ends: reading e of (h)-[e]->(c) reads h
        ends = {r.var: {n.var for n in (r.left, r.right)} & others for r in self.rels if r.var}
        for unit in units:
            read = self._vars_read(unit, defs, frozenset())
            read = sorted((read & others) | {x for v in read for x in ends.get(v, ())})
            if len(read) > 1:
                names = " and ".join(self.shown[v] for v in read)
                self._bad(f"reads {names} in one expression ({_canon([t[j] for j in unit])[:120]}): comparing or "
                          f"combining two renewals other than {src} relates them as an edge would, so the rows would "
                          f"describe one renewal's neighbourhood while the visibility rule reads {src}.as_of")

    def _units(self) -> tuple[list[list[int]], list[list[int]]]:
        """(the expressions rule 3 reads one at a time: each conjunct of a WHERE, each WITH / RETURN
        item, each ORDER BY item, an UNWIND; the values of the MATCH patterns' property maps)."""
        t, out, maps = self.toks, [], []
        for c in self.clauses:
            if c.kind == "where":
                out += self._conjunct_spans(list(range(c.lo, c.hi)))
            elif c.kind in ("with", "return"):
                out += [i[:-2] if len(i) >= 3 and _word(t, i[-2]) == "as" else i for i in self._items(c)]
            elif c.kind == "order by":
                out += self._order_items(c)
            elif c.kind == "unwind":
                out.append(list(range(c.lo, c.hi)))
            elif c.kind in _MATCHES:
                for j in range(c.lo, c.hi):   # {key: value, ...}: the value runs to the next ',' or '}'
                    if _is(t, j, ":") and _word(t, j - 1) and (_is(t, j - 2, "{") or _is(t, j - 2, ",")):
                        span, k, depth = [], j + 1, 0
                        while k < c.hi and not (depth == 0 and (_is(t, k, ",") or _is(t, k, "}"))):
                            depth += (t[k].kind == "op" and t[k].text in _OPEN) - \
                                (t[k].kind == "op" and t[k].text in _CLOSE)
                            span.append(k)
                            k += 1
                        maps.append(span)
        return [u for u in out if u], [m for m in maps if m]

    def _conjunct_spans(self, idx: list[int]) -> list[list[int]]:
        """_conjuncts() on token indexes: the parts of a WHERE body split at top-level ANDs (through
        redundant brackets); a part with a top-level OR / XOR stays one."""
        t = self.toks
        while len(idx) >= 2 and _is(t, idx[0], "(") and _close(t, idx[0]) == idx[-1]:
            idx = idx[1:-1]
        if not idx:
            return []
        try:
            toks = [t[j] for j in idx]
            if len(_split(toks, lambda x: x.kind == "id" and x.low in ("or", "xor"))) > 1:
                return [idx]
        except _NoParse:
            return [idx]
        parts, cur, depth = [], [], 0
        for j in idx:
            x = t[j]
            keyword = x.low if x.kind == "id" and not _is(t, j - 1, ".") else ""
            if (x.kind == "op" and x.text in _OPEN) or keyword == "case":
                depth += 1
            elif (x.kind == "op" and x.text in _CLOSE) or keyword == "end":
                depth -= 1
            if depth == 0 and keyword == "and":
                parts.append(cur)
                cur = []
            else:
                cur.append(j)
        parts = [p for p in [*parts, cur] if p]
        return [idx] if len(parts) <= 1 else [u for p in parts for u in self._conjunct_spans(p)]

    def _vars_read(self, span: list[int], defs: dict[str, list[int]], seen: frozenset) -> set[str]:
        """Pattern variables an expression reads (``v.key`` or a bare ``v``), following WITH / UNWIND /
        RETURN aliases to what their definitions read."""
        t, out = self.toks, set()
        for j in span:
            x = t[j]
            if x.kind != "id" or _is(t, j - 1, ".") or _is(t, j + 1, "(") or _word(t, j - 1) == "as":
                continue
            if x.low in self.kind:
                out.add(x.low)
            elif x.low in defs and x.low not in seen:
                out |= self._vars_read(defs[x.low], defs, seen | {x.low})
        return out

    def _mask(self, forms: tuple[str, ...]) -> tuple[str, int, int] | None:
        """(alias, first token, end token) of the WITH item ``(<visibility rule>) AS alias``.

        None when there is none, or when the alias is defined more than once (a later
        ``true AS visible`` would unmask everything) or shadows a pattern variable.
        """
        wanted = {f"( {f} )" for f in forms}
        for c in self.clauses:
            for item in self._items(c) if c.kind == "with" else ():
                words = [self.toks[j] for j in item]
                if len(words) > 4 and words[-2].low == "as" and words[-1].kind == "id" and _canon(words[:-2]) in wanted:
                    alias = words[-1].low
                    defined = sum(1 for j, x in enumerate(self.toks)
                                  if x.kind == "id" and x.low == alias and _word(self.toks, j - 1) == "as")
                    if defined == 1 and alias not in self.kind:
                        return alias, item[0], item[-1] + 1
        return None


def _lint_leaks(name: str, q: str) -> list[str]:
    """Violations of the leak rule (module docstring, rules 1-7 and the naming rules) in one tool template."""
    t = _Template(name, q)
    t.named_properties_only()
    t.source_events()
    t.plan_cuts()
    t.neighbour_outcomes()
    return t.problems


def population_templates(templates: dict[str, str] | None = None, contract_only=None) -> list[str]:
    """The tool templates rule 7 governs (they read outcome fields of renewals they do not name and
    relate none): their rows are population cells, which the tool layer suppresses count by count
    before it serves them (see the module docstring)."""
    templates = TEMPLATES if templates is None else templates
    contract_only = set(CONTRACT_ONLY if contract_only is None else contract_only)
    return sorted(name for name, q in templates.items()
                  if name not in contract_only and _Template(name, q).is_population())


def fingerprint(q: str) -> str:
    """16 hex of sha256 over a template's tokens as the lint reads them (identifiers lower-cased,
    whitespace and layout dropped): two spellings with the same tokens share it, any other edit not."""
    return hashlib.sha256(_canon(_tokenise(q)[0]).encode()).hexdigest()[:16]


def _lint_vetted(templates: dict[str, str], contract_only: set, vetted: dict[str, str]) -> list[str]:
    tools = {name: q for name, q in templates.items() if name not in contract_only}
    out = [f"{name}: is vetted (VETTED_TOOL_TEMPLATES) but is not a tool template of the catalog"
           for name in sorted(set(vetted) - set(tools))]
    for name, q in tools.items():
        fp = fingerprint(q)
        if vetted.get(name) != fp:
            how = "a new tool template" if name not in vetted else f"edited since it was vetted ({vetted[name]})"
            out.append(f"{name}: is not a vetted tool template shape ({how}; fingerprint {fp}): it passes the leak "
                       f"rule, the executed leak tests and the differential fuzz of tests/graph/test_queries_lint.py, "
                       f"then its fingerprint is added to queries.VETTED_TOOL_TEMPLATES")
    return out


def lint(templates: dict[str, str] | None = None, contract_only=None,
         vetted: dict[str, str] | None = None) -> list[str]:
    """Every violation of the template rules in the module docstring (empty list = clean).

    All templates: shape (ORDER BY, LIMIT, no UNION, read only). Tool templates (not in
    ``contract_only``): the leak rule as well, and, with ``vetted`` (name -> fingerprint), the
    allowlist: each is a vetted shape. The defaults lint this module's catalog against
    VETTED_TOOL_TEMPLATES; a module that keeps its own tool templates passes them (and its own
    allowlist) in.
    """
    if templates is None:
        templates = TEMPLATES
        vetted = VETTED_TOOL_TEMPLATES if vetted is None else vetted
    contract_only = set(CONTRACT_ONLY if contract_only is None else contract_only)
    problems = [f"{name}: contract-only template is not in the catalog"
                for name in sorted(contract_only - set(templates))]
    for name, q in templates.items():
        problems += _lint_shape(name, q)
        if name not in contract_only:
            problems += _lint_leaks(name, q)
    if vetted is not None:
        problems += _lint_vetted(templates, contract_only, vetted)
    return problems


# --------------------------------------------------------------------------- evidence rows
def evidence_row(relation: str, event_date, target_id: str, as_of, renewal_date, detail: str | None = None,
                 known_by_as_of: bool | None = None) -> dict:
    """The one evidence row shape, shared by the pandas oracle and the Cypher path.

    ``event_date`` / ``as_of`` / ``renewal_date`` are datetime.date. Keys: event_date,
    relation, target_id, detail, feeds_feature, in_feature_window, known_by_as_of,
    declared_exception.
    """
    import datetime as dt

    w = _W[relation]
    known = (event_date <= as_of) if known_by_as_of is None else bool(known_by_as_of)
    if relation == "BILLED":
        in_window = None
    elif relation == "FIRST_RENEWAL_AFTER":
        lo = renewal_date - dt.timedelta(days=spec.FIRST_RENEWAL_WINDOW_DAYS)
        in_window = lo <= event_date < renewal_date
    elif w.days is None:
        in_window = event_date <= as_of
    else:
        in_window = as_of - dt.timedelta(days=w.days) < event_date <= as_of
    return {"event_date": event_date.isoformat(), "relation": relation, "target_id": target_id, "detail": detail,
            "feeds_feature": w.feature, "in_feature_window": in_window, "known_by_as_of": known,
            "declared_exception": relation == "FIRST_RENEWAL_AFTER" and not known}


def event_detail(relation: str, *, limit_type=None, state=None, amount_usd=None, event_type=None) -> str | None:
    if relation == "HIT_LIMIT":
        return limit_type
    if relation == "CHANGED_OVERAGE":
        return state
    if relation == "CHARGED_OVERAGE":
        return None if amount_usd is None else f"{float(amount_usd):.2f}"
    if relation == "BILLED":
        return event_type
    return None


def evidence_sort_key(row: dict) -> tuple:
    return (row["event_date"], row["relation"], row["target_id"])


def evidence(conn, renewal_id: str, limit: int = ROW_LIMIT) -> list[dict]:
    """PIT evidence for one renewal from Ladybug, ordered by (event_date, relation, target_id).

    Subscription->event rows on or before as_of (never outcome evidence), CUT_CAP via the
    renewal's plan on or before as_of, and FIRST_RENEWAL_AFTER by the gold rule (flagged).
    """
    p = {"renewal_id": renewal_id, "limit": limit}
    out = []
    for r in fetch(conn, "evidence_events", p):
        out.append(evidence_row(r["relation"], r["event_date"], r["target_id"], r["as_of"], r["renewal_date"],
                                detail=event_detail(r["relation"], limit_type=r["limit_type"], state=r["state"],
                                                    amount_usd=r["amount_usd"], event_type=r["event_type"])))
    for r in fetch(conn, "evidence_cut_cap", p):
        out.append(evidence_row("CUT_CAP", r["event_date"], r["target_id"], r["as_of"], r["renewal_date"],
                                detail=f"via plan {r['plan_tier']}"))
    for r in fetch(conn, "evidence_first_renewal_after", p):
        out.append(evidence_row("FIRST_RENEWAL_AFTER", r["event_date"], r["target_id"], r["as_of"],
                                r["renewal_date"], known_by_as_of=r["known_by_as_of"]))
    return sorted(out, key=evidence_sort_key)[:limit]
