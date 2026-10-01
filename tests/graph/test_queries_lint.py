"""Static lint for the Cypher templates and for every Ladybug open in the graph code.

* every template has ORDER BY and LIMIT (result order is undefined without ORDER BY);
* no UNION (a trailing ORDER BY only orders the last branch in Ladybug);
* tool templates (queries.TOOL_TEMPLATES) obey the leak rule, checked statically by
  ``queries.lint()`` and against a real build: Subscription->event edges carry the as_of bound,
  BILLED outcome evidence is never returned, another renewal's outcome / route /
  outcome_observed_on is only served under the visibility rule, and no whole node, relationship
  or path is returned. The lint reads tokens, clauses and MATCH patterns (the dialect ignores
  case and accepts RETURN *, n.*, properties(n), back-ticks, comments, untyped relationships):
  BAD_TEMPLATES plants 200+ leaks it must refuse (incl. `.*` anywhere, WITH / UNWIND / RETURN
  aliases that shadow a pattern variable or repeat, a source bound only in an OPTIONAL MATCH,
  a second identity or a second renewal pinned by $k / $limit / a literal / a WITH ... SKIP,
  renewals that hang off another renewal, directly or through a hub the source shares, or are
  related to another by value, unbounded CUT_CAP / PricingChange / Incident for a
  named renewal, events bounded by another renewal of the subscription, population rows grouped
  by dates in a RETURN or a WITH, or pinned by a parameter or literal beside an expression, and
  population aggregates that encode a label beside a near-unique value, take an extreme, pack
  two cells into one number (around the aggregates or inside a CASE value), flag a sub-cell, or
  count rows that repeat a renewal), GOOD_TEMPLATES the shapes it must accept, and both sets run on the
  tiny build to show the refused ones leak (outcomes, events and cuts after the as_of of the
  renewal the rows describe, masked renewals served one at a time or decoded from rows of 5 or
  more) and the accepted ones do not; REFUSED_CELL_STATISTICS / VETTED_CELL_STATISTICS pin rule
  7's aggregate grammar; a seeded differential fuzz checks that no generated template the lint
  accepts leaks. The unbounded variants are contract-only and ``queries.fetch`` refuses them
  without ``contract=True``;
* population templates (queries.population_templates()) are served only through the tool layer's
  small-cell suppression (tools.incident_table / pricing_table) or as viz.py's summed totals;
* every ``Database(...)`` open caps buffer_pool_size and max_num_threads;
* nothing installs or loads an engine extension, and templates never touch files.
"""
from __future__ import annotations

import ast
import re

import pandas as pd
import pytest
from conftest import REPO

from lakehouse_graph import queries, spec

GRAPH_SOURCES = sorted((REPO / "src/lakehouse_graph").rglob("*.py")) + [
    REPO / "scripts/build_graph_local.py", REPO / "scripts/check_graph_contract.py",
    REPO / "scripts/check_repo_contracts.py"]
# ATTACH is matched as a statement (it always names a database: ATTACH '<path>' / ATTACH $path), so
# the English verb in a message ("checks attach to columns") is not a false positive.
FORBIDDEN_STATEMENT = re.compile(r"\bINSTALL\b|\bLOAD\s+EXTENSION\b|\bLOAD\s+FROM\b|\bATTACH\s+['\"$]|"
                                 r"\bEXPORT\s+DATABASE\b", re.IGNORECASE)


def _string_constants(path):
    """(lineno, text) for every string literal that is not a docstring."""
    tree = ast.parse(path.read_text(), filename=str(path))
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.body and \
                isinstance(node.body[0], ast.Expr) and isinstance(node.body[0].value, ast.Constant) and \
                isinstance(node.body[0].value.value, str):
            docstrings.add(id(node.body[0].value))
    return [(n.lineno, n.value) for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docstrings]


@pytest.mark.parametrize("name", sorted(queries.TEMPLATES))
def test_template_has_order_by_and_limit(name):
    q = queries.TEMPLATES[name]
    order = [m.start() for m in re.finditer(r"\bORDER\s+BY\b", q, re.IGNORECASE)]
    limit = [m.start() for m in re.finditer(r"\bLIMIT\b", q, re.IGNORECASE)]
    assert order and limit, f"{name} needs ORDER BY and LIMIT"
    assert order[-1] < limit[-1], f"{name}: LIMIT must follow the final ORDER BY"
    assert re.search(r"\bLIMIT\s+(\$\w+|\d+)\s*$", q.rstrip().splitlines()[-1]), \
        f"{name}: the statement must end with LIMIT <n | $param>"
    assert not re.search(r"\bUNION\b", q, re.IGNORECASE), \
        f"{name}: UNION is not allowed (ORDER BY binds to the last branch)"
    assert not re.search(r"\b(CREATE|MERGE|DELETE|SET|DROP|ALTER|COPY|CALL)\b", q, re.IGNORECASE), \
        f"{name} must be read-only"
    assert not FORBIDDEN_STATEMENT.search(q), name


def test_templates_are_a_reasonable_catalog():
    assert len(queries.TEMPLATES) >= 25
    unbounded = {"similar_top_k", "similar_nearest_lapses", "similar_sharing_neighbours", "similar_edge_checks"}
    assert set(queries.UNBOUNDED_NEIGHBOUR_OUTCOMES) == unbounded
    assert queries.CONTRACT_ONLY == sorted({k for k in queries.TEMPLATES if k.startswith("contract_")} | unbounded)
    assert queries.TOOL_TEMPLATES == sorted(set(queries.TEMPLATES) - set(queries.CONTRACT_ONLY))
    for name in ("evidence_events", "evidence_first_renewal_after", "evidence_cut_cap", "similar_top_k_visible",
                 "similar_nearest_lapses_known_by_as_of", "exposure_incident_by_plan", "count_nodes", "count_rels",
                 "renewal_header"):
        assert name in queries.TOOL_TEMPLATES, name
    assert queries.lint() == []                             # the whole catalog obeys the documented rules


def test_agent_usable_event_templates_are_bounded_by_as_of():
    """Any tool template that walks a Subscription->event edge must filter <= r.as_of."""
    rel_pattern = re.compile(r"\[\w*:(?:[A-Z_]+\|)*(?:" + "|".join(spec.EVENT_RELATIONS) + r")\b")
    checked = 0
    for name in queries.TOOL_TEMPLATES:
        q = queries.TEMPLATES[name]
        if not rel_pattern.search(q):
            continue
        checked += 1
        assert re.search(r"\.event_date <= r\.as_of", q), f"{name} walks event edges without the as_of bound"
        assert "outcome_evidence" in q or "BILLED" not in q, f"{name} may return BILLED outcome evidence"
    assert checked >= 3
    naive = [k for k, q in queries.TEMPLATES.items() if re.search(r"\.event_date > r\.as_of\b(?! -)", q)]
    assert all(k.startswith("contract_") for k in naive), naive


def test_no_tool_template_reads_a_neighbour_outcome_without_the_visibility_rule():
    """Independent of queries.lint(): every tool template that walks SIMILAR_TO and mentions an
    outcome field of another renewal carries `<n>.outcome_observed_on <= <src>.as_of`."""
    field = re.compile(r"\b(\w+)\.(outcome_observed_on|outcome|route|churned)\b")
    seen = []
    for name in queries.TOOL_TEMPLATES:
        q = queries.TEMPLATES[name]
        if "SIMILAR_TO" not in q or not field.search(q):
            continue
        seen.append(name)
        m = re.search(r"\b(\w+)\.outcome_observed_on <= (\w+)\.as_of\b", q)
        assert m, f"{name} returns neighbour outcome fields with no visibility rule"
        nbr, src = m.groups()
        assert f"{src}:Renewal {{renewal_id: $renewal_id}}" in q or f"{src}.renewal_id = $renewal_id" in q, name
        assert nbr != src, name
    assert seen == ["similar_nearest_lapses_known_by_as_of", "similar_top_k_visible"]
    assert queries.visible("n", "r") == "n.outcome_observed_on <= r.as_of"
    assert queries.visible("n", "r") in queries.TEMPLATES["similar_top_k_visible"]
    assert queries.visible_or_current("n", "r") in queries.TEMPLATES["similar_top_k_visible"]
    assert queries.visible("b", "a") in queries.TEMPLATES["similar_nearest_lapses_known_by_as_of"]
    for name in queries.UNBOUNDED_NEIGHBOUR_OUTCOMES:      # and these really are unbounded: contract-only
        assert "outcome_observed_on <=" not in queries.TEMPLATES[name], name


BAD_TEMPLATES = {
    # what similar_top_k does: a neighbour's outcome, route and observation date with no bound
    "tool_top_k": ("""
MATCH (r:Renewal {renewal_id: $renewal_id})-[e:SIMILAR_TO]->(n:Renewal)
RETURN e.rank AS rank, n.renewal_id AS renewal_id, n.outcome AS outcome, n.route AS route
ORDER BY rank LIMIT $k""", "reads n's outcome fields without the visibility rule"),
    # filtering on the outcome leaks it just as returning it does
    "tool_lapses": ("""
MATCH (a:Renewal)-[e:SIMILAR_TO]->(b:Renewal)
WHERE a.renewal_id = $renewal_id AND b.outcome = 'voluntary_lapse'
RETURN b.renewal_id AS renewal_id
ORDER BY renewal_id LIMIT $k""", "reads b's outcome fields without the visibility rule"),
    # the rule must be a conjunct: an OR lets every row through
    "tool_or": ("""
MATCH (a:Renewal)-[e:SIMILAR_TO]->(b:Renewal)
WHERE a.renewal_id = $renewal_id AND (b.outcome_observed_on <= a.as_of OR b.churned = 1)
RETURN b.renewal_id AS renewal_id, b.outcome AS outcome
ORDER BY renewal_id LIMIT $k""", "reads b's outcome fields without the visibility rule"),
    # masked, but one field escapes the CASE
    "tool_half_masked": ("""
MATCH (r:Renewal {renewal_id: $renewal_id})-[e:SIMILAR_TO]->(n:Renewal)
WITH r, e, n, (n.outcome_observed_on <= r.as_of) AS visible
RETURN e.rank AS rank, CASE WHEN visible THEN n.outcome ELSE 'not_yet_observed' END AS outcome, n.route AS route
ORDER BY rank LIMIT $k""", "n.route is returned outside CASE WHEN visible THEN"),
    # a widened predicate that is not the documented one
    "tool_wide": ("""
MATCH (r:Renewal {renewal_id: $renewal_id})-[e:SIMILAR_TO]->(n:Renewal)
WITH r, e, n, (n.outcome_observed_on <= r.as_of OR $today) AS visible
RETURN e.rank AS rank, CASE WHEN visible THEN n.outcome END AS outcome
ORDER BY rank LIMIT $k""", "reads n's outcome fields without the visibility rule"),
    # outcomes of renewals that merely share a neighbour (what similar_sharing_neighbours does)
    "tool_sharing": ("""
MATCH (r:Renewal {renewal_id: $renewal_id})-[:SIMILAR_TO]->(n:Renewal)<-[:SIMILAR_TO]-(o:Renewal)
RETURN o.route AS route, count(*) AS renewals
ORDER BY route LIMIT 20""", "reads o's outcome fields without the visibility rule"),
    "tool_no_source": ("""
MATCH (a:Renewal)-[e:SIMILAR_TO]->(b:Renewal)
RETURN b.route AS route, count(e) AS edges
ORDER BY route LIMIT 20""", "binds no single source renewal"),
    # source events
    "tool_naive_hits": ("""
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal {renewal_id: $renewal_id})
MATCH (s)-[e:HIT_LIMIT]->(x:LimitHit)
RETURN x.event_id AS event_id, e.event_date AS event_date
ORDER BY event_date, event_id LIMIT $limit""", "walks HIT_LIMIT without e.event_date <="),
    "tool_anonymous_edge": ("""
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal {renewal_id: $renewal_id})
MATCH (s)-[:OPENED]->(x:Ticket)
RETURN x.ticket_id AS ticket_id
ORDER BY ticket_id LIMIT $limit""", "the OPENED edge is anonymous"),
    "tool_post_as_of": ("""
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal {renewal_id: $renewal_id})
MATCH (s)-[e:OPENED]->(x:Ticket)
WHERE e.event_date <= r.as_of OR e.event_date > r.as_of
RETURN x.ticket_id AS ticket_id
ORDER BY ticket_id LIMIT $limit""", "selects events after as_of"),
    # BILLED rows that reveal the outcome (invoice_paid / canceled after as_of are filtered by date,
    # but the rule is explicit: outcome evidence is excluded by its flag as well)
    "tool_billed": ("""
MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal {renewal_id: $renewal_id})
MATCH (s)-[e:BILLED]->(x:BillingEvent)
WHERE e.event_date <= r.as_of
RETURN x.event_id AS event_id, e.event_type AS event_type
ORDER BY event_id LIMIT $limit""", "can return BILLED outcome evidence"),
    # shape
    "tool_no_order": ("MATCH (r:Renewal) RETURN r.renewal_id AS renewal_id LIMIT 5", "needs ORDER BY"),
    "tool_union": ("MATCH (r:Renewal) RETURN r.renewal_id AS id ORDER BY id LIMIT 5 UNION "
                   "MATCH (p:Plan) RETURN p.plan_tier AS id ORDER BY id LIMIT 5", "UNION is not allowed"),
    "tool_write": ("MATCH (r:Renewal) SET r.route = 'x' RETURN r.renewal_id AS id ORDER BY id LIMIT 5",
                   "must be read only"),
}

# More planted leaks, each a shape the engine runs. The dialect is case-insensitive and permissive,
# so the lint reads tokens and patterns, not substrings. The names in LEAKS_ON_TINY are also
# executed against the tiny build to show that they really serve what the rule forbids.
SRC = "MATCH (r:Renewal {renewal_id: $renewal_id})-[e:SIMILAR_TO]->(n:Renewal)\n"
EV = "MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal {renewal_id: $renewal_id})\n"
NBR = "RETURN n.renewal_id AS renewal_id, n.outcome AS outcome"
TAIL = "\nORDER BY renewal_id LIMIT 50"
TICKETS = "RETURN DISTINCT x.ticket_id AS id, e.event_date AS event_date\nORDER BY event_date, id LIMIT $limit"
NO_RULE = "outcome fields without the visibility rule"
MASK = "WITH r, e, n, (n.outcome_observed_on <= r.as_of) AS visible\n"
BAD_TEMPLATES |= {
    # rule 5: a whole node, relationship or path carries every outcome field with it
    "tool_whole_node": (SRC + "RETURN n\nORDER BY e.rank LIMIT 10", "returns or passes on the whole node n"),
    "tool_whole_node_alias": (SRC + "RETURN n AS neighbour, e.rank AS rank\nORDER BY rank LIMIT 10",
                              "returns or passes on the whole node n"),
    "tool_properties_fn": (SRC + "RETURN properties(n) AS p, e.rank AS rank\nORDER BY rank LIMIT 10",
                           "returns or passes on the whole node n"),
    "tool_collect_nodes": (SRC + "WITH r, collect(n) AS ns\nRETURN ns\nORDER BY r.renewal_id LIMIT 1",
                           "returns or passes on the whole node n"),
    "tool_two_hop_node": (SRC + "MATCH (n)-[:SIMILAR_TO]->(m:Renewal)\nWHERE n.outcome_observed_on <= r.as_of\n"
                          "RETURN m.renewal_id AS id, m\nORDER BY id LIMIT 10",
                          "returns or passes on the whole node m"),
    "tool_return_star": (SRC + "RETURN *\nORDER BY e.rank LIMIT 10", "RETURN * passes on every whole node"),
    "tool_with_star": (SRC + "WITH *\nRETURN n.renewal_id AS renewal_id" + TAIL, "WITH * passes on every whole node"),
    "tool_struct_extract": (SRC + "RETURN n.renewal_id AS renewal_id, struct_extract(n, 'outcome') AS outcome" + TAIL,
                            "returns or passes on the whole node n"),
    "tool_node_alias_in_with": (SRC + "WITH r, n AS m\nRETURN m.renewal_id AS renewal_id, m.outcome AS outcome" + TAIL,
                                "returns or passes on the whole node n"),
    "tool_unwind_alias": (SRC + "WITH r, collect(n) AS ns\nUNWIND ns AS m\n"
                          "RETURN m.renewal_id AS renewal_id, m.outcome AS outcome" + TAIL,
                          "returns or passes on the whole node n"),
    "tool_path_nodes": ("MATCH p = (a:Renewal)-[e:SIMILAR_TO]->(b:Renewal)\nWHERE a.renewal_id = $renewal_id\n"
                        "RETURN nodes(p) AS ns\nORDER BY b.renewal_id LIMIT 10",
                        "returns or passes on the whole path p"),
    "tool_recursive_rel_nodes": ("MATCH (a:Renewal)-[e:SIMILAR_TO*1..2]->(b:Renewal)\n"
                                 "WHERE a.renewal_id = $renewal_id\n"
                                 "RETURN properties(nodes(e), 'outcome') AS outcomes\nORDER BY b.renewal_id LIMIT 10",
                                 "returns or passes on the whole relationship e"),
    # the dialect: case-insensitive names, back-ticks, comments, string literals
    "tool_upper_property": (SRC + "RETURN n.renewal_id AS renewal_id, n.OUTCOME AS outcome" + TAIL,
                            f"reads n's {NO_RULE}"),
    "tool_upper_variable": (SRC + "RETURN N.renewal_id AS renewal_id, N.Outcome AS outcome" + TAIL,
                            f"reads N's {NO_RULE}"),
    "tool_lowercase": ("match (r:renewal {renewal_id: $renewal_id})-[e:similar_to]->(n:renewal)\n"
                       "return n.renewal_id as renewal_id, n.outcome as outcome\norder by renewal_id limit 50",
                       f"reads n's {NO_RULE}"),
    "tool_backtick": (SRC + "RETURN n.renewal_id AS renewal_id, n.`outcome` AS outcome" + TAIL,
                      "has text the lint cannot read (`)"),
    "tool_comment_hides_bound": (SRC + "WHERE e.rank > 0 // AND n.outcome_observed_on <= r.as_of\n" + NBR + TAIL,
                                 "has text the lint cannot read (//)"),
    "tool_string_hides_bound": (SRC + "WHERE n.plan_tier <> ' AND n.outcome_observed_on <= r.as_of AND '\n"
                                + NBR + TAIL,
                                f"reads n's {NO_RULE}"),
    # a visibility rule that is weakened, negated, misplaced or overwritten
    "tool_as_of_plus": (SRC + "WHERE n.outcome_observed_on <= r.as_of + INTERVAL('999 DAYS')\n" + NBR + TAIL,
                        f"reads n's {NO_RULE}"),
    "tool_not_visible": (SRC + "WHERE NOT n.outcome_observed_on <= r.as_of\n" + NBR + TAIL, f"reads n's {NO_RULE}"),
    "tool_xor": (SRC + "WHERE n.outcome_observed_on <= r.as_of XOR true\n" + NBR + TAIL, f"reads n's {NO_RULE}"),
    "tool_swapped_rule": (SRC + "WHERE r.outcome_observed_on <= n.as_of\n" + NBR + TAIL, f"reads n's {NO_RULE}"),
    # the WHERE of an OPTIONAL MATCH only decides whether the optional part matches: n's rows all stay
    "tool_optional_where": (SRC + "OPTIONAL MATCH (n)-[:ON_PLAN]->(p:Plan)\nWHERE n.outcome_observed_on <= r.as_of\n"
                            + NBR + TAIL, f"reads n's {NO_RULE}"),
    # the outcomes are aggregated first; the filter lands on a second, new n
    "tool_rebind": (SRC + "WITH r, collect(n.outcome) AS outcomes\nMATCH (r)-[:SIMILAR_TO]->(n:Renewal)\n"
                    "WHERE n.outcome_observed_on <= r.as_of\nRETURN outcomes\nORDER BY r.renewal_id LIMIT 1",
                    "re-binds n after a WITH dropped it"),
    "tool_mask_redefined": (SRC + MASK + "WITH r, e, n, true AS visible\n"
                            "RETURN n.renewal_id AS renewal_id, CASE WHEN visible THEN n.outcome END AS outcome" + TAIL,
                            f"reads n's {NO_RULE}"),
    "tool_mask_negated": (SRC + MASK + "RETURN n.renewal_id AS renewal_id, "
                          "CASE WHEN NOT visible THEN n.outcome END AS outcome" + TAIL,
                          "n.outcome is returned outside CASE WHEN visible THEN"),
    "tool_mask_else": (SRC + MASK + "RETURN n.renewal_id AS renewal_id, "
                       "CASE WHEN visible THEN 'known' ELSE n.outcome END AS outcome" + TAIL,
                       "n.outcome is returned outside CASE WHEN visible THEN"),
    "tool_mask_simple_case": (SRC + MASK + "RETURN n.renewal_id AS renewal_id, "
                              "CASE visible WHEN false THEN n.outcome END AS outcome" + TAIL,
                              "n.outcome is returned outside CASE WHEN visible THEN"),
    # an AND between CASE and END belongs to the CASE, not to the WHERE: this filter is always true
    "tool_case_hides_bound": (SRC + "WHERE CASE WHEN true AND n.outcome_observed_on <= r.as_of AND true THEN true "
                              "ELSE true END\n" + NBR + TAIL, f"reads n's {NO_RULE}"),
    # three neighbours are chosen first; the filter only thins what was chosen (a row-count side channel)
    "tool_limit_before_filter": (SRC + "WITH r, e, n\nORDER BY e.rank LIMIT 3\nMATCH (n)-[:ON_PLAN]->(p:Plan)\n"
                                 "WHERE n.outcome_observed_on <= r.as_of\n" + NBR + TAIL, f"reads n's {NO_RULE}"),
    "tool_source_in_optional_where": ("MATCH (a:Renewal)-[e:SIMILAR_TO]->(b:Renewal)\n"
                                      "OPTIONAL MATCH (a)-[:ON_PLAN]->(p:Plan)\n"
                                      "WHERE a.renewal_id = $renewal_id AND b.outcome_observed_on <= a.as_of\n"
                                      "RETURN b.renewal_id AS renewal_id, b.outcome AS outcome" + TAIL,
                                      "binds no single source renewal"),
    "tool_cast_node": (SRC + "RETURN n.renewal_id AS renewal_id, cast(n AS STRING) AS outcome" + TAIL,
                       "returns or passes on the whole node n"),
    "tool_order_by_outcome": (SRC + "RETURN n.renewal_id AS renewal_id\nORDER BY n.outcome, renewal_id LIMIT 50",
                              f"reads n's {NO_RULE}"),
    "tool_is_reference": (SRC + "RETURN n.renewal_id AS renewal_id, n.is_reference AS is_reference" + TAIL,
                          f"reads n's {NO_RULE}"),
    "tool_map_filter": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[e:SIMILAR_TO]->"
                        "(n:Renewal {outcome: 'voluntary_lapse'})\nRETURN n.renewal_id AS renewal_id" + TAIL,
                        f"reads n's {NO_RULE}"),
    "tool_map_filter_anonymous": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[e:SIMILAR_TO]->"
                                  "(:Renewal {churned: 1})\n"
                                  "RETURN count(e) AS lapses\nORDER BY lapses LIMIT 1",
                                  "filters an anonymous node on churned"),
    "tool_exists_subquery": ("MATCH (r:Renewal {renewal_id: $renewal_id})\nWHERE EXISTS { MATCH (r)-[:SIMILAR_TO]->"
                             "(n:Renewal) WHERE n.outcome = 'voluntary_lapse' }\n"
                             "RETURN r.renewal_id AS renewal_id" + TAIL,
                             "nests MATCH, WHERE inside brackets"),
    # a pattern outside MATCH walks edges too: "does the source have a lapsed neighbour?"
    "tool_pattern_predicate": ("MATCH (r:Renewal {renewal_id: $renewal_id})\nWHERE (:Renewal {renewal_id: $renewal_id})"
                               "-[:SIMILAR_TO]->(:Renewal {outcome: 'voluntary_lapse'})\n"
                               "RETURN r.renewal_id AS renewal_id" + TAIL,
                               "has a relationship pattern outside MATCH (in its WHERE)"),
    "tool_pattern_size": ("MATCH (r:Renewal {renewal_id: $renewal_id})\nRETURN r.renewal_id AS renewal_id, "
                          "size((r)-[:SIMILAR_TO]->(:Renewal {outcome: 'voluntary_lapse'})) AS lapsed" + TAIL,
                          "has a relationship pattern outside MATCH (in its RETURN)"),
    # other renewals reached without SIMILAR_TO, or from a source that is not bound by $renewal_id
    "tool_same_plan": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(p:Plan)<-[:ON_PLAN]-(o:Renewal)\n"
                       "RETURN o.renewal_id AS renewal_id, o.outcome AS outcome" + TAIL, f"reads o's {NO_RULE}"),
    "tool_same_plan_anonymous_source": ("MATCH (:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(:Plan)<-[:ON_PLAN]-"
                                        "(o:Renewal)\nRETURN o.renewal_id AS renewal_id, o.outcome AS outcome" + TAIL,
                                        "binds no single source renewal"),
    "tool_other_param": ("MATCH (a:Renewal {subscription_id: $subscription_id})-[:SIMILAR_TO]->(b:Renewal)\n"
                         "RETURN b.renewal_id AS renewal_id, b.outcome AS outcome" + TAIL,
                         "binds no single source renewal"),
    "tool_untyped_from_renewal": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[e]->(n)\n"
                                  "RETURN label(n) AS label, count(*) AS n_rows\nORDER BY label LIMIT 10",
                                  "has a relationship without a type (e)"),
    # rule 1: the bound is the whole conjunct, against the as_of of the same subscription's renewal
    "tool_event_as_of_plus": (EV + "MATCH (s)-[e:OPENED]->(x:Ticket)\n"
                              "WHERE e.event_date <= r.as_of + INTERVAL('999 DAYS')\n" + TICKETS,
                              "walks OPENED without e.event_date <="),
    "tool_event_not": (EV + "MATCH (s)-[e:OPENED]->(x:Ticket)\nWHERE NOT e.event_date <= r.as_of\n" + TICKETS,
                       "walks OPENED without e.event_date <="),
    "tool_event_other_renewal": (EV + "MATCH (s)-[e:OPENED]->(x:Ticket), (o:Renewal)\nWHERE e.event_date <= o.as_of\n"
                                 + TICKETS, "walks OPENED without e.event_date <="),
    "tool_event_other_subscription": (EV + "MATCH (s2:Subscription)-[e:OPENED]->(x:Ticket)\n"
                                      "WHERE e.event_date <= r.as_of\n" + TICKETS,
                                      "walks OPENED without e.event_date <="),
    "tool_event_optional_where": (EV + "MATCH (s)-[e:OPENED]->(x:Ticket)\n"
                                  "OPTIONAL MATCH (s)-[h:HIT_LIMIT]->(y:LimitHit)\n"
                                  "WHERE e.event_date <= r.as_of AND h.event_date <= r.as_of\n" + TICKETS,
                                  "walks OPENED without e.event_date <="),
    "tool_event_case_hides_bound": (EV + "MATCH (s)-[e:OPENED]->(x:Ticket)\nWHERE CASE WHEN true AND e.event_date <= "
                                    "r.as_of AND true THEN true ELSE true END\n" + TICKETS,
                                    "walks OPENED without e.event_date <="),
    # every ticket is matched first; the bounded edge is only an optional extra
    "tool_event_node_joined_late": (EV + "MATCH (x:Ticket)\nOPTIONAL MATCH (s)-[e:OPENED]->(x)\n"
                                    "WHERE e.event_date <= r.as_of\n"
                                    "RETURN x.ticket_id AS id, x.event_date AS event_date\n"
                                    "ORDER BY event_date, id LIMIT $limit", "matches x, which can be an event node"),
    "tool_event_count_for_renewal": (EV + "MATCH (s)-[e:OPENED]->(:Ticket)\nRETURN count(e) AS tickets\n"
                                     "ORDER BY tickets LIMIT 1", "walks OPENED without e.event_date <="),
    "tool_event_anonymous_subscription": ("MATCH (r:Renewal {renewal_id: $renewal_id})<-[:HAS_RENEWAL]-()"
                                          "-[e:OPENED]->()\n"
                                          "RETURN count(e) AS tickets\nORDER BY tickets LIMIT 1",
                                          "without a direction from a named Subscription variable"),
    "tool_event_undirected": (EV + "MATCH (s)-[e:OPENED]-(x:Ticket)\nWHERE e.event_date <= r.as_of\n" + TICKETS,
                              "without a direction from a named Subscription variable"),
    "tool_event_untyped": (EV + "MATCH (s)-[e]->(x)\nRETURN x\nORDER BY x.event_id LIMIT 10",
                           "has a relationship without a type (e)"),
    "tool_event_rows_by_type": ("MATCH ()-[e:{rel}]->()\nRETURN e.event_date AS event_date, count(*) AS n_rows\n"
                                "ORDER BY event_date LIMIT 50",
                                "walks {rel} as e without a direction from a named Subscription variable"),
    # event nodes matched directly: a canceled BillingEvent names its subscription in its id
    "tool_event_node_direct": ("MATCH (b:BillingEvent)\nWHERE b.event_type = 'canceled'\nRETURN b.event_id AS id\n"
                               "ORDER BY id LIMIT $limit", "matches b, which can be an event node"),
    "tool_event_label_rows": ("MATCH (n:{label})\nRETURN n.event_type AS event_type, count(*) AS n_rows\n"
                              "ORDER BY event_type LIMIT 50", "matches n, which can be an event node"),
    "tool_event_unlabelled_node": ("MATCH (x)\nWHERE x.event_type = 'canceled'\nRETURN x.event_id AS id\n"
                                   "ORDER BY id LIMIT $limit", "matches x, which can be an event node"),
    # rule 2
    "tool_billed_flag_true": (EV + "MATCH (s)-[e:BILLED]->(x:BillingEvent)\n"
                              "WHERE e.event_date <= r.as_of AND e.outcome_evidence = true\n"
                              "RETURN x.event_id AS id, e.event_type AS event_type\nORDER BY id LIMIT $limit",
                              "can return BILLED outcome evidence"),
}
FAR, NEVER = "{as_of: date('2999-12-31')}", "date('1900-01-01')"
BAD_TEMPLATES |= {
    # rule 5: n.* is every property of n, outcome fields included
    "tool_dot_star": (SRC + "RETURN n.*\nORDER BY n.renewal_id LIMIT 10", "projects n.*"),
    "tool_dot_star_after_rank": (SRC + "RETURN e.rank AS rank, n.*\nORDER BY rank LIMIT 10", "projects n.*"),
    "tool_dot_star_after_with": (SRC + "WITH r, e, n\nRETURN e.rank AS rank, n.*\nORDER BY rank LIMIT 10",
                                 "projects n.*"),
    "tool_edge_dot_star": (EV + "MATCH (s)-[e:BILLED]->(x:BillingEvent)\n"
                           "WHERE e.event_date <= r.as_of AND NOT coalesce(e.outcome_evidence, false)\n"
                           "RETURN e.*\nORDER BY e.event_date LIMIT 10", "projects e.*"),
    # names: an alias that re-uses a pattern variable's name satisfies the exact bound, filter or
    # mask the lint looks for while bounding nothing (the struct is compared, not the renewal)
    "tool_alias_shadows_source_filter": (SRC + f"WITH n, e, {FAR} AS r\nWHERE n.outcome_observed_on <= r.as_of\n"
                                         + NBR + TAIL, "re-uses the name of the pattern variable r as an alias"),
    "tool_alias_shadows_source_mask": (SRC + f"WITH n, e, {FAR} AS r\n" + MASK + "RETURN n.renewal_id AS renewal_id, "
                                       "CASE WHEN visible THEN n.outcome END AS outcome" + TAIL,
                                       "re-uses the name of the pattern variable r as an alias"),
    "tool_alias_shadows_event_source": (EV + f"MATCH (s)-[e:OPENED]->(x:Ticket)\nWITH s, e, x, {FAR} AS r\n"
                                        "WHERE e.event_date <= r.as_of\n" + TICKETS,
                                        "re-uses the name of the pattern variable r as an alias"),
    "tool_alias_shadows_event_edge": (EV + "MATCH (s)-[e:BILLED]->(x:BillingEvent)\n"
                                      "WITH r, e.event_type AS event_type, e.event_date AS event_date, "
                                      f"{{event_date: {NEVER}, outcome_evidence: false}} AS e\n"
                                      "WHERE e.event_date <= r.as_of AND NOT coalesce(e.outcome_evidence, false)\n"
                                      "RETURN event_type, event_date\nORDER BY event_date LIMIT $limit",
                                      "re-uses the name of the pattern variable e as an alias"),
    "tool_alias_shadows_neighbour": (SRC + "WITH r, n.renewal_id AS renewal_id, n.outcome AS outcome, "
                                     f"{{outcome_observed_on: {NEVER}}} AS n\nWHERE n.outcome_observed_on <= r.as_of\n"
                                     "RETURN renewal_id, outcome" + TAIL,
                                     "re-uses the name of the pattern variable n as an alias"),
    "tool_alias_shadows_neighbour_mask": (SRC + "WITH r, e, n.renewal_id AS renewal_id, n.outcome AS outcome, "
                                          f"{{outcome_observed_on: {NEVER}}} AS n\n"
                                          "WITH r, e, n, renewal_id, outcome, (n.outcome_observed_on <= r.as_of) AS "
                                          "visible\nRETURN renewal_id, CASE WHEN visible THEN outcome END AS outcome"
                                          + TAIL, "re-uses the name of the pattern variable n as an alias"),
    "tool_unwind_shadows_source": (SRC + f"UNWIND [{FAR}] AS r\nWITH r, e, n\nWHERE n.outcome_observed_on <= r.as_of\n"
                                   + NBR + TAIL, "re-uses the name of the pattern variable r as an alias (UNWIND"),
    "tool_alias_twice": (SRC + "WHERE n.outcome_observed_on <= r.as_of\nWITH r, e, n, e.rank AS k2\n"
                         "WITH r, e, n, k2 + 1 AS k2\nRETURN n.renewal_id AS renewal_id, k2" + TAIL,
                         "defines the alias k2 more than once"),
    "tool_keyword_alias": (SRC + "WITH r, e, n, 1 AS end\nWHERE end = 1 OR (true AND n.outcome_observed_on <= r.as_of "
                           "AND true)\n" + NBR + TAIL, "names an alias end"),
    "tool_map_key_end": (SRC + "WHERE {end: 1}.end = 1 OR (true AND n.outcome_observed_on <= r.as_of AND true)\n"
                         + NBR + TAIL, "unbalanced brackets or CASE ... END"),
    "tool_dropped_variable": (SRC + "WITH r, sum(n.churned) AS lapses\nWHERE n.outcome_observed_on <= r.as_of\n"
                              "RETURN lapses\nORDER BY lapses LIMIT 1", "uses n after a WITH dropped it"),
    # the engine reads \' inside a string as a quote: the rule below is part of the string
    "tool_backslash_string": (SRC + "WHERE n.plan_tier <> 'a\\' AND n.outcome_observed_on <= r.as_of AND \\''\n"
                              + NBR + TAIL, "has text the lint cannot read (\\)"),
    "tool_list_comprehension": (SRC + MASK + "RETURN n.renewal_id AS renewal_id, "
                                "[visible IN [true] | CASE WHEN visible THEN n.outcome END] AS outcome" + TAIL,
                                "binds a local variable visible"),
    # rule 3: the source is bound where it restricts rows, and it is the only renewal the template names
    "tool_optional_map_source": ("MATCH (r:Renewal)-[e:SIMILAR_TO]->(n:Renewal)\nWHERE r.renewal_id = $rid\n"
                                 "OPTIONAL MATCH (n {renewal_id: $renewal_id})-[:ON_PLAN]->(p:Plan)\n" + NBR + TAIL,
                                 "binds no single source renewal"),
    "tool_optional_other_source": ("MATCH (r:Renewal)-[e:SIMILAR_TO]->(n:Renewal)\nWHERE r.renewal_id = $rid\n"
                                   "OPTIONAL MATCH (z:Renewal {renewal_id: $renewal_id})\nWITH r, e, n, z\n"
                                   "WHERE n.outcome_observed_on <= z.as_of\n" + NBR + TAIL,
                                   "binds no single source renewal"),
    "tool_second_identity": ("MATCH (a:Renewal {subscription_id: $subscription_id})-[e:SIMILAR_TO]->(n:Renewal), "
                             "(r:Renewal {renewal_id: $renewal_id})\nWHERE n.outcome_observed_on <= r.as_of\n"
                             + NBR + TAIL, "takes $subscription_id"),
    "tool_second_identity_where": ("MATCH (r:Renewal {renewal_id: $renewal_id}), (a:Renewal)-[e:SIMILAR_TO]->"
                                   "(n:Renewal)\nWHERE a.renewal_id = $rid AND n.outcome_observed_on <= r.as_of\n"
                                   + NBR + TAIL, "takes $rid"),
    "tool_second_identity_literal": ("MATCH (a:Renewal {renewal_id: 'sub_00000:2026-07-27'})-[e:SIMILAR_TO]->"
                                     "(n:Renewal), (r:Renewal {renewal_id: $renewal_id})\n"
                                     "WHERE n.outcome_observed_on <= r.as_of\n" + NBR + TAIL,
                                     "uses the pattern map key renewal_id other than as a whole projection"),
    "tool_second_identity_derived": ("MATCH (r:Renewal {renewal_id: $renewal_id}), (a:Renewal)-[e:SIMILAR_TO]->"
                                     "(n:Renewal)\nWHERE a.renewal_id = 'x' + $renewal_id AND "
                                     "n.outcome_observed_on <= r.as_of\n" + NBR + TAIL,
                                     "uses $renewal_id other than to bind the source"),
    # rule 6: the plan cuts a named renewal sees are the ones effective on or before its as_of
    "tool_cut_cap_unbounded": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(pl:Plan)<-[c:CUT_CAP]-"
                               "(p:PricingChange)\nRETURN c.event_date AS event_date, p.change_id AS id\n"
                               "ORDER BY event_date LIMIT 10", "walks CUT_CAP as c for a renewal named by $renewal_id"),
    "tool_cut_cap_anonymous": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(pl:Plan)<-[:CUT_CAP]-"
                               "(p:PricingChange)\nRETURN p.effective_date AS event_date, p.change_id AS id\n"
                               "ORDER BY event_date LIMIT 10",
                               "walks CUT_CAP for a renewal named by $renewal_id without"),
    # rule 7: outcomes of renewals the template does not name come back aggregated, never row by row
    "tool_population_rows": ("MATCH (n:Renewal)\nRETURN n.renewal_id AS renewal_id, n.outcome AS outcome\n"
                             "ORDER BY renewal_id LIMIT 500", "returns rows, not aggregates"),
    "tool_population_by_ids": ("MATCH (n:Renewal)\nWHERE n.renewal_id IN $ids\n"
                               "RETURN n.outcome AS outcome, count(*) AS n_rows\nORDER BY outcome LIMIT 10",
                               "returns or filters on renewal_id"),
    "tool_population_by_subscription": ("MATCH (s:Subscription {subscription_id: $subscription_id})-[:HAS_RENEWAL]->"
                                        "(r:Renewal)\nRETURN r.route AS route, count(*) AS n_rows\n"
                                        "ORDER BY route LIMIT 10", "returns or filters on subscription_id"),
    "tool_population_collect": ("MATCH (r:Renewal)\nRETURN r.plan_tier AS plan, collect(r.outcome) AS outcomes, "
                                "count(*) AS n\nORDER BY plan LIMIT 20", "collects values into a list"),
}
# Hardening round (verifier round 2): `.*` anywhere, every alias kind, a second identity in a
# template that relates renewals, CUT_CAP bounds and population keys / parameters.
CUT = "MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(pl:Plan)<-[c:CUT_CAP]-(p:PricingChange)\n"
CUTS = "RETURN c.event_date AS event_date, p.change_id AS id\nORDER BY event_date, id LIMIT 10"
CUT_NO_BOUND = "walks CUT_CAP as c for a renewal named by $renewal_id without c.event_date <= r.as_of"
POP = "MATCH (n:Renewal)\n"
BAD_TEMPLATES |= {
    # rule 5: `<anything>.*` is refused wherever it is written, not only in a projection
    "tool_dot_star_event_node": (EV + "MATCH (s)-[e:OPENED]->(x:Ticket)\nWHERE e.event_date <= r.as_of\nRETURN x.*\n"
                                 "ORDER BY x.ticket_id LIMIT 10", "projects x.*"),
    "tool_dot_star_in_where": (SRC + "WHERE n.* IS NOT NULL AND n.outcome_observed_on <= r.as_of\n" + NBR + TAIL,
                               "projects n.*"),
    "tool_dot_star_in_order_by": (SRC + "WHERE n.outcome_observed_on <= r.as_of\nRETURN n.renewal_id AS renewal_id\n"
                                  "ORDER BY n.*, renewal_id LIMIT 50", "projects n.*"),
    "tool_dot_star_in_unwind": (SRC + "WHERE n.outcome_observed_on <= r.as_of\nUNWIND [n.*] AS v\n"
                                "RETURN n.renewal_id AS renewal_id, v" + TAIL, "projects n.*"),
    "tool_dot_star_in_match_map": (SRC + "MATCH (m:Renewal {renewal_id: n.*})\nWHERE n.outcome_observed_on <= r.as_of\n"
                                   + NBR + TAIL, "projects n.*"),
    # names: a RETURN alias is a name too (ORDER BY reads it), and no alias is defined twice
    "tool_return_alias_shadows_count": ("MATCH (n:Renewal)\nRETURN count(n) AS n\nORDER BY n LIMIT 1",
                                        "re-uses the name of the pattern variable n as an alias (RETURN"),
    "tool_return_alias_shadows_neighbour": (SRC + "WHERE n.outcome_observed_on <= r.as_of\n"
                                            "RETURN n.renewal_id AS renewal_id, e.rank AS n\nORDER BY n LIMIT 5",
                                            "re-uses the name of the pattern variable n as an alias (RETURN"),
    "tool_return_alias_shadows_edge": (EV + "MATCH (s)-[e:OPENED]->(x:Ticket)\nWHERE e.event_date <= r.as_of\n"
                                       "RETURN x.ticket_id AS id, e.event_date AS e\nORDER BY e LIMIT 10",
                                       "re-uses the name of the pattern variable e as an alias (RETURN"),
    "tool_return_alias_twice": (SRC + "WHERE n.outcome_observed_on <= r.as_of\n"
                                "RETURN n.renewal_id AS renewal_id, e.rank AS renewal_id" + TAIL,
                                "defines the alias renewal_id more than once"),
    "tool_with_then_return_alias": (SRC + "WHERE n.outcome_observed_on <= r.as_of\nWITH r, e, n, e.rank AS k\n"
                                    "RETURN n.renewal_id AS renewal_id, k + 1 AS k" + TAIL,
                                    "defines the alias k more than once"),
    "tool_unwind_after_with_alias": (SRC + "WHERE n.outcome_observed_on <= r.as_of\nWITH r, e, n, e.rank AS k\n"
                                     "UNWIND [1, 2] AS k\nRETURN n.renewal_id AS renewal_id, k" + TAIL,
                                     "defines the alias k more than once"),
    "tool_path_alias_shadow": ("MATCH p = (r:Renewal {renewal_id: $renewal_id})-[e:SIMILAR_TO]->(n:Renewal)\n"
                               "WHERE n.outcome_observed_on <= r.as_of\nWITH r, e, n, e.rank AS p\n" + NBR + TAIL,
                               "re-uses the name of the pattern variable p as an alias (WITH"),
    # rule 3: a template that relates renewals is pointed at its source only, outcome fields read or not
    "tool_relating_other_param": ("MATCH (a:Renewal {renewal_id: $rid})-[e:SIMILAR_TO]->(n:Renewal)\n"
                                  "RETURN n.renewal_id AS renewal_id, e.rank AS rank\nORDER BY rank LIMIT 10",
                                  "takes $rid"),
    "tool_relating_pins_as_of": (SRC + "WHERE n.outcome_observed_on <= r.as_of AND n.as_of = $as_of\n" + NBR + TAIL,
                                 "takes $as_of"),
    "tool_relating_by_subscription": ("MATCH (s:Subscription {subscription_id: $subscription_id})-[:HAS_RENEWAL]->"
                                      "(a:Renewal)-[e:SIMILAR_TO]->(n:Renewal)\n"
                                      "RETURN n.renewal_id AS renewal_id, e.rank AS rank\nORDER BY rank LIMIT 10",
                                      "takes $subscription_id"),
    "tool_optional_map_only_source": ("MATCH (a:Renewal)-[e:SIMILAR_TO]->(n:Renewal)\n"
                                      "OPTIONAL MATCH (r:Renewal {renewal_id: $renewal_id})\nWITH r, e, n\n"
                                      "WHERE n.outcome_observed_on <= r.as_of\n" + NBR + TAIL,
                                      "binds no single source renewal"),
    # rule 6: CUT_CAP for a named renewal, bounded exactly by that renewal's as_of
    "tool_cut_cap_or": (CUT + "WHERE c.event_date <= r.as_of OR true\n" + CUTS, CUT_NO_BOUND),
    "tool_cut_cap_as_of_plus": (CUT + "WHERE c.event_date <= r.as_of + INTERVAL('90 DAYS')\n" + CUTS, CUT_NO_BOUND),
    "tool_cut_cap_renewal_date": (CUT + "WHERE c.event_date <= r.renewal_date\n" + CUTS, CUT_NO_BOUND),
    "tool_cut_cap_neighbour_as_of": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[:SIMILAR_TO]->(n:Renewal), "
                                     "(r)-[:ON_PLAN]->(pl:Plan)<-[c:CUT_CAP]-(p:PricingChange)\n"
                                     "WHERE c.event_date <= n.as_of\n" + CUTS, CUT_NO_BOUND),
    "tool_cut_cap_optional_where": (CUT + "OPTIONAL MATCH (p)-[:CUT_CAP]->(q:Plan)\nWHERE c.event_date <= r.as_of\n"
                                    + CUTS, CUT_NO_BOUND),
    "tool_cut_cap_alias_shadow": (CUT + f"WITH pl, c, p, {FAR} AS r\nWHERE c.event_date <= r.as_of\n" + CUTS,
                                  "re-uses the name of the pattern variable r as an alias"),
    "tool_cut_cap_placeholder": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(pl:Plan)<-[c:{rel}]-(p)\n"
                                 + CUTS, "walks {rel} as c for a renewal named by $renewal_id"),
    # rule 7: population rows are aggregates grouped by cohort keys; parameters select hubs and cohorts
    "tool_population_by_dates": (POP + "RETURN n.as_of AS as_of, n.plan_tier AS plan, n.outcome AS outcome, "
                                 "count(*) AS n_rows\nORDER BY as_of, plan, outcome LIMIT 500",
                                 "groups its rows by as_of"),
    "tool_population_observed_on": (POP + "RETURN n.outcome_observed_on AS observed_on, count(*) AS n_rows, "
                                    "sum(n.churned) AS lapses\nORDER BY observed_on LIMIT 500",
                                    "groups its rows by outcome_observed_on"),
    "tool_population_feature_key": (POP + "RETURN n.plan_tier AS plan, n.allowance_used_pct AS used, n.route AS route, "
                                    "count(*) AS n_rows\nORDER BY plan, used, route LIMIT 500",
                                    "groups its rows by allowance_used_pct"),
    "tool_population_alias_key": (POP + "WITH n, n.renewal_date AS d\nRETURN d, n.route AS route, count(*) AS n_rows\n"
                                  "ORDER BY d, route LIMIT 500", "groups its rows by renewal_date"),
    "tool_population_case_key": (POP + "RETURN CASE WHEN n.as_of > date('2026-08-01') THEN n.as_of END AS late, "
                                 "n.outcome AS outcome, count(*) AS n_rows\nORDER BY late, outcome LIMIT 500",
                                 "groups its rows by as_of"),
    "tool_population_param_pins": (POP + "WHERE n.as_of = $as_of AND n.plan_tier = $plan\n"
                                   "RETURN n.route AS route, count(*) AS n_rows, sum(n.churned) AS lapses\n"
                                   "ORDER BY route LIMIT 10", "compares $as_of with as_of"),
    "tool_population_param_expression": (POP + "WHERE n.as_of > date($since)\n"
                                         "RETURN n.route AS route, count(*) AS n_rows\nORDER BY route LIMIT 10",
                                         "compares $since with an expression"),
    "tool_population_subscription_param": ("MATCH (s:Subscription)-[:HAS_RENEWAL]->(n:Renewal)\n"
                                           "WHERE s.started_at = $started\n"
                                           "RETURN n.route AS route, count(*) AS n_rows\nORDER BY route LIMIT 10",
                                           "compares $started with started_at"),
}
# Fix round (p1-hardening verifier round 1): a second renewal pinned by a paging parameter, a wrapped
# identifying field, a literal or a WITH ... SKIP; a population grouped in a WITH or pinned beside an
# expression; the calendar hubs for a named renewal; events bounded by another renewal's as_of.
CUR = "MATCH (r:Renewal {renewal_id: $renewal_id})\n"
PINNED = "MATCH (h:Renewal)-[e:SIMILAR_TO]->(n:Renewal)\n"
LATE = "RETURN n.renewal_id AS renewal_id, n.outcome AS outcome\nORDER BY renewal_id LIMIT 50"
NOT_OWN = "a renewal that is not one of the source's own relations"
VIS = "n.outcome_observed_on <= r.as_of"
HIST = "'__rid__'"   # a literal renewal id: the leak tests write the pinned historical renewal's id in its place
BY_PLAN = "MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(:Plan)<-[:ON_PLAN]-(h:Renewal)"
BAD_TEMPLATES |= {
    # rule 3: no parameter, literal or selection pins a renewal but the source (the verifier's A, A2, B,
    # K_skip, K_feature and their neighbours); each is executed by test_pinned_second_renewals_really_leak
    "tool_k_pins_second_renewal": (CUR + PINNED + f"WHERE lower(h.renewal_id) = lower($k) AND {VIS}\n" + LATE,
                                   "uses $k other than as the value of its final LIMIT / SKIP"),
    "tool_k_concat_pins_second_renewal": (CUR + PINNED + f"WHERE h.renewal_id + '' = $k AND {VIS}\n" + LATE,
                                          "uses $k other than"),
    "tool_int_k_pins_by_feature": (CUR + PINNED + f"WHERE h.agent_requests_28d = $k AND {VIS}\n" + LATE,
                                   "uses $k other than"),
    "tool_limit_pins_second_renewal": (CUR + PINNED + f"WHERE h.renewal_id = $limit AND {VIS}\n" + LATE,
                                       "uses $limit other than"),
    "tool_skip_picks_second_renewal": (CUR + "MATCH (h:Renewal)\nWITH r, h ORDER BY h.renewal_id SKIP $k LIMIT 1\n"
                                       "MATCH (h)-[e:SIMILAR_TO]->(n:Renewal)\n"
                                       + MASK.replace("r, e, n", "r, h, e, n") +
                                       "RETURN h.renewal_id AS source, n.renewal_id AS renewal_id, "
                                       "CASE WHEN visible THEN n.outcome END AS outcome\nORDER BY renewal_id LIMIT 50",
                                       "uses $k other than"),
    "tool_literal_pin_wrapped": (CUR + PINNED + f"WHERE lower(h.renewal_id) = {HIST} AND {VIS}\n" + LATE,
                                 "uses h.renewal_id other than as a whole projection"),
    "tool_literal_pin_disconnected": (CUR + PINNED + f"WHERE h.renewal_id = {HIST} AND {VIS}\n" + LATE,
                                      f"matches h, {NOT_OWN}"),
    "tool_literal_feature_pin": (CUR + PINNED + f"WHERE h.agent_requests_28d = 211 AND {VIS}\n" + LATE,
                                 f"matches h, {NOT_OWN}"),
    "tool_literal_skip_picks": (CUR + "MATCH (h:Renewal)\nWITH r, h ORDER BY h.agent_requests_28d SKIP 7 LIMIT 1\n"
                                "MATCH (h)-[e:SIMILAR_TO]->(n:Renewal)\n" + MASK.replace("r, e, n", "r, h, e, n") +
                                "RETURN n.renewal_id AS renewal_id, CASE WHEN visible THEN n.outcome END AS outcome"
                                + TAIL, f"matches h, {NOT_OWN}"),
    # connected to the source through a hub, but the outcomes read hang off the pinned renewal
    "tool_hub_pins_second_renewal": (BY_PLAN + "-[e:SIMILAR_TO]->(n:Renewal)\n"
                                     f"WHERE h.agent_requests_28d = 211 AND {VIS}\n" + LATE, f"matches n, {NOT_OWN}"),
    "tool_neighbour_of_neighbour": (SRC + f"WHERE {VIS}\nMATCH (n)-[f:SIMILAR_TO]->(m:Renewal)\n"
                                    "WHERE m.outcome_observed_on <= r.as_of\n"
                                    "RETURN n.renewal_id AS via, m.renewal_id AS renewal_id, m.outcome AS outcome\n"
                                    "ORDER BY via, renewal_id LIMIT 500", f"matches m, {NOT_OWN}"),
    "tool_anonymous_second_renewal": (CUR + "MATCH (:Renewal {agent_requests_28d: 211})-[e:SIMILAR_TO]->(n:Renewal)\n"
                                      f"WHERE {VIS}\n" + LATE, f"matches an anonymous renewal pattern, {NOT_OWN}"),
    # identifying fields and parameters in a template that relates renewals
    "tool_identifying_alias_compared": (SRC + "WITH r, e, n, n.renewal_id AS nid\n"
                                        f"WHERE nid <> {HIST} AND {VIS}\n"
                                        "RETURN nid AS renewal_id, n.outcome AS outcome" + TAIL,
                                        "uses nid (an alias of an identifying field) other than"),
    "tool_identifying_sorted_before_skip": (SRC + f"WHERE {VIS}\nWITH r, e, n ORDER BY n.renewal_id SKIP 2 LIMIT 1\n"
                                            + NBR + TAIL, "uses n.renewal_id other than"),
    "tool_renewal_id_param_derived": (CUR + "MATCH (s:Subscription)-[:HAS_RENEWAL]->(h:Renewal)-[e:SIMILAR_TO]->"
                                      "(n:Renewal)\n"
                                      f"WHERE s.subscription_id = substring($renewal_id, 0, 9) AND {VIS}\n" + LATE,
                                      "uses $renewal_id other than to bind the source"),
    "tool_today_outside_rule": (SRC + f"WHERE {VIS} AND e.rank <= CASE WHEN $today THEN 10 ELSE 5 END\n" + NBR + TAIL,
                                "uses $today outside visible_or_current"),
    "tool_with_skip_param": (SRC + f"WHERE {VIS}\nWITH r, e, n ORDER BY e.rank SKIP $k LIMIT 1\n" + NBR + TAIL,
                             "uses $k other than"),
    # rule 7: a WITH that groups by a fine key hands it on as an aggregate alias; a count per renewal
    # as a key; one renewal chosen by SKIP / LIMIT; a parameter beside a concatenation or a map alias
    "tool_population_with_group_by_as_of": (POP + "WITH n.as_of AS d, n.outcome AS o, min(n.as_of) AS day, "
                                            "count(*) AS c\nRETURN o, day, sum(c) AS k\nORDER BY day, o LIMIT 1000",
                                            "groups its rows by as_of"),
    "tool_population_with_group_by_features": (POP + "WITH n.overage_usd_28d AS a, n.agent_requests_28d AS b, "
                                               "n.outcome AS o, min(n.overage_usd_28d) AS amt, "
                                               "min(n.agent_requests_28d) AS req, count(*) AS c\n"
                                               "RETURN o, amt, req, sum(c) AS k\nORDER BY amt, req LIMIT 1000",
                                               "groups its rows by agent_requests_28d, overage_usd_28d"),
    "tool_population_min_alias_key": (POP + "WITH n, n.outcome AS o, min(n.as_of) AS day\n"
                                      "RETURN o, day, count(*) AS k\nORDER BY day, o LIMIT 1000",
                                      "groups its rows by as_of"),
    "tool_population_map_alias_key": (POP + "WITH n, {plan_tier: n.as_of} AS m\n"
                                      "RETURN m.plan_tier AS day, n.outcome AS o, count(*) AS k\n"
                                      "ORDER BY day, o LIMIT 1000", "groups its rows by as_of"),
    "tool_population_count_key": ("MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal)\n"
                                  "OPTIONAL MATCH (s)-[e:HIT_LIMIT]->(x:LimitHit)\nWHERE e.event_date <= r.as_of\n"
                                  "WITH r, count(e) AS hits\n"
                                  "RETURN hits, r.outcome AS o, r.plan_tier AS plan, count(*) AS k\n"
                                  "ORDER BY hits, o, plan LIMIT 1000", "groups its rows by count(...)"),
    "tool_population_skip_one": (POP + "WITH n ORDER BY n.agent_requests_28d SKIP $pos LIMIT 1\n"
                                 "RETURN n.outcome AS o, count(*) AS k\nORDER BY o LIMIT 10",
                                 "chooses rows with LIMIT / SKIP before its final RETURN"),
    "tool_population_param_concat": (POP + "WHERE cast(n.as_of, 'STRING') + n.plan_tier = $p\n"
                                     "RETURN n.outcome AS o, count(*) AS k\nORDER BY o LIMIT 10",
                                     "compares $p with an expression"),
    "tool_population_map_alias_param": (POP + "WITH n, {plan_tier: n.as_of} AS m\nWHERE m.plan_tier = $day\n"
                                        "RETURN n.outcome AS o, count(*) AS k\nORDER BY o LIMIT 10",
                                        "compares $day with an expression"),
    "tool_population_param_in_map_literal": (POP + "WITH n, {plan_tier: $plan} AS m\nWHERE n.as_of = m.plan_tier\n"
                                             "RETURN n.outcome AS o, count(*) AS k\nORDER BY o LIMIT 10",
                                             "compares $plan with an expression"),
    "tool_population_simple_case_param": (POP + "WHERE CASE n.agent_requests_28d WHEN $req THEN true ELSE false END\n"
                                          "RETURN n.outcome AS o, count(*) AS k\nORDER BY o LIMIT 10",
                                          "compares $req with an expression"),
    # rule 7: a literal written into the template selects exactly as a parameter would (987654321 is
    # written as the pinned renewal's agent_requests_28d when executed)
    "tool_population_literal_pin": (POP + "WHERE n.plan_tier = 'pro' AND n.as_of = date('2026-07-20')\n"
                                    "RETURN n.outcome AS o, count(*) AS k\nORDER BY o LIMIT 10",
                                    "selects renewals by value on as_of"),
    "tool_population_literal_map": ("MATCH (n:Renewal {agent_requests_28d: 987654321})\n"
                                    "RETURN n.outcome AS o, count(*) AS k\nORDER BY o LIMIT 10",
                                    "selects renewals by value on agent_requests_28d"),
    "tool_population_literal_in_aggregate": (POP + "RETURN n.plan_tier AS plan, "
                                             "sum(CASE WHEN n.agent_requests_28d = 987654321 THEN 1 ELSE 0 END) AS k, "
                                             "max(CASE WHEN n.agent_requests_28d = 987654321 THEN n.outcome END) AS o\n"
                                             "ORDER BY plan LIMIT 10",
                                             "selects renewals by value on agent_requests_28d"),
    "tool_population_literal_offset": (POP + "WHERE n.agent_requests_28d = n.limit_hits_14d + 987654321\n"
                                       "RETURN n.outcome AS o, count(*) AS k\nORDER BY o LIMIT 10",
                                       "selects renewals by value on agent_requests_28d, limit_hits_14d"),
    "tool_population_simple_case_literal": (POP + "RETURN CASE n.agent_requests_28d WHEN 211 THEN 'x' ELSE 'y' END AS "
                                            "kind, n.outcome AS o, count(*) AS k\nORDER BY kind, o LIMIT 10",
                                            "selects renewals by value on agent_requests_28d"),
    "tool_population_literal_alias": (POP + "WITH n, date('2026-07-20') AS d\nWHERE n.as_of = d\n"
                                      "RETURN n.outcome AS o, count(*) AS k\nORDER BY o LIMIT 10",
                                      "selects renewals by value on as_of"),
    # rule 6: the calendar a named renewal sees stops at its as_of, node or edge
    "tool_pricing_by_property": (CUR + "MATCH (p:PricingChange)\nWHERE p.effective_date > r.as_of\n"
                                 "RETURN p.change_id AS id, p.effective_date AS event_date\n"
                                 "ORDER BY event_date LIMIT 10",
                                 "matches p, which can be a PricingChange, for a renewal named by $renewal_id"),
    "tool_pricing_node_unbounded": (CUR + "MATCH (p:PricingChange)\nRETURN p.change_id AS id, p.effective_date AS "
                                    "event_date, p.cap_multiplier AS multiplier\nORDER BY event_date LIMIT 10",
                                    "matches p, which can be a PricingChange"),
    "tool_pricing_optional_cut": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(pl:Plan), "
                                  "(p:PricingChange)\n"
                                  "OPTIONAL MATCH (p)-[c:CUT_CAP]->(pl)\nWHERE c.event_date <= r.as_of\n"
                                  "RETURN p.change_id AS id, p.effective_date AS event_date\n"
                                  "ORDER BY event_date LIMIT 10",
                                  "matches p, which can be a PricingChange"),
    "tool_pricing_total": (CUR + "MATCH (p:PricingChange)\nRETURN count(p) AS cuts\nORDER BY cuts LIMIT 1",
                           "matches p, which can be a PricingChange"),
    "tool_incident_by_property": (CUR + "MATCH (i:Incident)\nWHERE i.starts_on > r.as_of\n"
                                  "RETURN i.incident_id AS id, i.starts_on AS event_date\nORDER BY event_date LIMIT 10",
                                  "matches i, which can be an Incident, for a renewal named by $renewal_id"),
    # when an incident ended is known once it is over (fix round 2): an incident running at as_of ends after it
    "tool_incident_end_after_as_of": (CUR + "MATCH (i:Incident)\nWHERE i.starts_on <= r.as_of\n"
                                      "RETURN i.incident_id AS id, i.ends_on AS event_date\n"
                                      "ORDER BY event_date LIMIT 10",
                                      "reads i.ends_on (when an incident ended) for a renewal named by $renewal_id"),
    "tool_incident_days_via_exposure": (EV + "MATCH (s)-[e:EXPOSED_TO]->(i:Incident)\nWHERE e.event_date <= r.as_of\n"
                                        "RETURN DISTINCT i.incident_id AS id, i.days AS days\nORDER BY id LIMIT 10",
                                        "reads i.days (when an incident ended)"),
    "tool_incident_end_in_map": (CUR + "MATCH (i:Incident {ends_on: date('2026-07-22')})\n"
                                 "WHERE i.starts_on <= r.as_of\nRETURN i.incident_id AS id\nORDER BY id LIMIT 10",
                                 "reads i.ends_on"),
    # rule 1: the source's events are bounded by the source's own as_of, not a later renewal's of the
    # same subscription (latent on tiny and s42: every subscription has one renewal there)
    "tool_event_other_renewal_anchor": (EV + "MATCH (s)-[:HAS_RENEWAL]->(r2:Renewal)\n"
                                        "MATCH (s)-[e:OPENED]->(x:Ticket)\n"
                                        "WHERE e.event_date <= r2.as_of\n" + TICKETS,
                                        "walks OPENED without e.event_date <="),
}
# Fix round 2 (p1-hardening-2 verifier): a second renewal reached from the source through a hub (Plan,
# PricingChange), pinned by a literal or a WITH ... SKIP, its neighbours joined through the same hub (so
# each one is "one of the source's relations" too) and through it; a subscription or path only they share;
# two renewals related by value (a comparison, a sort key, an alias, a list, a pattern map) instead of an
# edge. The rows describe the pinned renewal's neighbourhood while the rule reads the source's as_of.
PLAN_H = "MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(p:Plan)<-[:ON_PLAN]-(h:Renewal)\n"
PLAN_N = "MATCH (n:Renewal)-[:ON_PLAN]->(p)\n"
HUB_V = "WITH r, h, e, n, (n.outcome_observed_on <= r.as_of) AS visible\n"
HUB_N = "MATCH (n:Renewal)-[:ON_PLAN]->(p), (h)-[e:SIMILAR_TO]->(n)\n"
PIN_H = "WHERE h.agent_requests_28d = 211 AND n.outcome_observed_on <= r.as_of\n"
VIA = ("RETURN h.renewal_id AS via, n.renewal_id AS renewal_id, n.outcome AS outcome\n"
       "ORDER BY via, renewal_id LIMIT 500")
RANKED = "RETURN e.rank AS rank, n.renewal_id AS renewal_id, "
JOINS = "joins h and n other than through r"
TWO = "reads h and n in one expression"
BAD_TEMPLATES |= {
    # the verifier's HUB_plan_literal_pin / _skip_literal / _asof_literal / _rowfilter / HUB_pricing_literal_pin
    "tool_hub_plan_literal_pin": (PLAN_H + HUB_N + "WHERE h.agent_requests_28d = 211\n" + HUB_V +
                                  "RETURN e.rank AS rank, h.as_of AS h_as_of, n.renewal_id AS renewal_id, "
                                  "CASE WHEN visible THEN n.outcome END AS outcome,\n"
                                  "       CASE WHEN visible THEN n.outcome_observed_on END AS seen\n"
                                  "ORDER BY rank LIMIT 20", JOINS),
    "tool_hub_plan_skip_literal": (PLAN_H + "WITH r, p, h ORDER BY h.as_of, h.agent_requests_28d SKIP 0 LIMIT 1\n"
                                   + HUB_N + HUB_V + "RETURN h.renewal_id AS via, e.rank AS rank, n.renewal_id AS "
                                   "renewal_id, CASE WHEN visible THEN n.outcome END AS outcome\n"
                                   "ORDER BY rank LIMIT 20", JOINS),
    "tool_hub_plan_asof_literal": (PLAN_H + HUB_N + "WHERE h.as_of = date('2026-07-20') AND "
                                   "h.agent_requests_28d > 200\n" + HUB_V + RANKED +
                                   "CASE WHEN visible THEN n.outcome END AS outcome\nORDER BY rank LIMIT 20", JOINS),
    "tool_hub_plan_rowfilter": (PLAN_H + HUB_N + PIN_H + RANKED + "n.outcome AS outcome\nORDER BY rank LIMIT 20",
                                JOINS),
    # 987654321 is written as the pinned renewal's agent_requests_28d when executed
    "tool_hub_pricing_literal_pin": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[f:FIRST_RENEWAL_AFTER]->"
                                     "(c:PricingChange)<-[g:FIRST_RENEWAL_AFTER]-(h:Renewal)\n"
                                     "MATCH (h)-[e:SIMILAR_TO]->(n:Renewal)-[:ON_PLAN]->(p:Plan)<-[:ON_PLAN]-(r)\n"
                                     "WHERE h.agent_requests_28d = 987654321 AND n.outcome_observed_on <= r.as_of\n"
                                     "RETURN f.known_by_as_of AS k, e.rank AS rank, n.renewal_id AS renewal_id, "
                                     "n.outcome AS outcome\nORDER BY rank LIMIT 20", JOINS),
    "tool_hub_recursive_path": (PLAN_H + "MATCH (h)-[e:SIMILAR_TO*1..1]->(n:Renewal)-[:ON_PLAN]->(p)\n" + PIN_H + VIA,
                                JOINS),
    "tool_hub_shared_neighbour": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[:SIMILAR_TO]->(n:Renewal)"
                                  "<-[:SIMILAR_TO]-(h:Renewal)-[:ON_PLAN]->(p:Plan)<-[:ON_PLAN]-(r)\n" + PIN_H + VIA,
                                  JOINS),
    # latent on tiny and s42 (one renewal per subscription): another renewal of the pinned one's subscription
    "tool_hub_sibling_subscription": (PLAN_H + "MATCH (s2:Subscription)-[:HAS_RENEWAL]->(h), (s2)-[:HAS_RENEWAL]->"
                                      "(n:Renewal), (n)-[:ON_PLAN]->(p)\n" + PIN_H + VIA, JOINS),
    # related by value: the pinned renewal's nearest plan-mates by a feature, a window around its value,
    # an alias of it, a sort key, a list of it, a pattern map, an edge property of it, a CASE
    "tool_value_relation_nearest": (PLAN_H + PLAN_N + PIN_H +
                                    "WITH r, h, n ORDER BY abs(n.agent_requests_28d - h.agent_requests_28d), "
                                    "n.agent_requests_28d LIMIT 10\n" + VIA, TWO),
    "tool_value_relation_window": (PLAN_H + PLAN_N + "WHERE h.agent_requests_28d = 211 AND "
                                   "abs(n.agent_requests_28d - h.agent_requests_28d) <= 40 AND "
                                   "n.outcome_observed_on <= r.as_of\n" + VIA, TWO),
    "tool_value_relation_alias": (PLAN_H + "WITH r, p, h, h.agent_requests_28d AS x\n" + PLAN_N +
                                  "WHERE x = 211 AND n.agent_requests_28d <= x + 40 AND "
                                  "n.agent_requests_28d >= x - 40 AND n.outcome_observed_on <= r.as_of\n" + VIA, TWO),
    "tool_value_relation_sort_alias": (PLAN_H + PLAN_N + PIN_H +
                                       "RETURN h.agent_requests_28d AS hx, n.agent_requests_28d AS nx, n.renewal_id AS "
                                       "renewal_id, n.outcome AS outcome\nORDER BY abs(nx - hx) LIMIT 10", TWO),
    "tool_value_relation_list": (PLAN_H + "WHERE h.agent_requests_28d = 211\nWITH r, p, collect(h.as_of) AS days\n"
                                 + PLAN_N + "WHERE n.as_of IN days AND n.outcome_observed_on <= r.as_of\n" + NBR + TAIL,
                                 TWO),
    "tool_value_relation_map": (PLAN_H + "MATCH (n:Renewal {agent_requests_28d: h.agent_requests_28d})"
                                "-[:ON_PLAN]->(p)\n" + PIN_H + VIA, "has a MATCH pattern map value that reads h"),
    "tool_value_relation_edge_property": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[:FIRST_RENEWAL_AFTER]->"
                                          "(c:PricingChange)<-[f:FIRST_RENEWAL_AFTER]-(h:Renewal)\n"
                                          "MATCH (r)-[:ON_PLAN]->(p:Plan)<-[:ON_PLAN]-(n:Renewal)\n"
                                          "WHERE h.agent_requests_28d = 552 AND f.known_by_as_of = n.churned AND "
                                          "n.outcome_observed_on <= r.as_of\n" + VIA, TWO),
    "tool_value_relation_case": (PLAN_H + PLAN_N + PIN_H + "RETURN n.renewal_id AS renewal_id, "
                                 "CASE WHEN n.as_of < h.as_of THEN n.outcome END AS outcome\n"
                                 "ORDER BY renewal_id LIMIT 50", TWO),
}
# Fix round 3 (p1-hardening-2 verifier round 2, the open major): rule 7 also limits what an aggregate
# computes. Each of these reads cohort keys only outside its aggregates, yet an aggregate that puts a
# label beside a near-unique value (an extreme, a weighted sum, a date-plus-outcome string), an extreme
# grouped by outcome, or two cell statistics packed into one number singles out renewals and their labels
# in cells of any size. The POPULATION_ENCODED ones are executed and decoded on tiny.
SUBS = "MATCH (s:Subscription)-[:HAS_RENEWAL]->(n:Renewal)\n"
ENC = "n.agent_requests_28d * 10 + n.churned"          # a label beside a near-unique value
BOTH = "n.overage_toggled_off = 1 AND n.incident_exposed_28d = 1"   # a sub-cell of a plan x city cell
UNVETTED = "the argument of"                          # rule 7: not a vetted aggregate argument
PACKED = "combines an aggregate with other terms in one"
BAD_TEMPLATES |= {
    # the verifier's POP_max_encodes_churned, POP_sum_bitmask_by_cell, POP_max_string_asof_outcome,
    # POP_minmax_by_outcome; F_aggregate_date (verify-1) and POP_order_by_feature_return_first (v1)
    "tool_population_encoded_extremes": (SUBS + f"RETURN n.plan_tier AS pl, s.city AS city, count(*) AS k, max({ENC}) "
                                         f"AS hi, min({ENC}) AS lo\nORDER BY pl, city LIMIT 1000",
                                         "aggregates with max()"),
    "tool_population_bitmask_sum": (SUBS + "RETURN n.plan_tier AS pl, s.city AS city, n.route AS rt, count(*) AS k, "
                                    "sum(n.churned * pow(2.0, n.agent_requests_28d % 50)) AS mask\n"
                                    "ORDER BY pl, city, rt LIMIT 1000",
                                    "aggregates sum ( n . churned * pow ( 2.0 , n . agent_requests_28d % 50 ) )"),
    "tool_population_string_extremes": (POP + "RETURN n.plan_tier AS pl, count(*) AS k, max(cast(n.as_of, 'STRING') + "
                                        "n.outcome) AS hi, min(cast(n.as_of, 'STRING') + n.outcome) AS lo\n"
                                        "ORDER BY pl LIMIT 100", "aggregates with max()"),
    "tool_population_extremes_by_outcome": (POP + "RETURN n.plan_tier AS pl, n.outcome AS o, count(*) AS k, "
                                            "min(n.as_of) AS first_day, max(n.as_of) AS last_day, "
                                            "min(n.agent_requests_28d) AS lo, max(n.agent_requests_28d) AS hi\n"
                                            "ORDER BY pl, o LIMIT 100", "aggregates with min()"),
    "tool_population_packed_columns": (SUBS + f"RETURN n.plan_tier AS pl, s.city AS city, count(*) AS k, "
                                       f"sum(CASE WHEN {BOTH} THEN n.churned ELSE 0 END) * 1000 + "
                                       f"sum(CASE WHEN {BOTH} THEN 1 ELSE 0 END) AS packed\n"
                                       "ORDER BY pl, city LIMIT 1000", PACKED),
    "tool_population_max_observed": (POP + "RETURN n.plan_tier AS tier, max(n.outcome_observed_on) AS last_seen, "
                                     "count(*) AS k\nORDER BY tier LIMIT 10", "aggregates with max()"),
    "tool_population_min_observed": (POP + "RETURN n.plan_tier AS plan, min(n.outcome_observed_on) AS first_seen, "
                                     "count(*) AS n_rows\nORDER BY plan LIMIT 10", "aggregates with min()"),
    "tool_population_min_feature_by_outcome": (POP + "RETURN n.outcome AS outcome, min(n.agent_requests_28d) AS lo, "
                                               "count(*) AS c\nORDER BY outcome LIMIT 10", "aggregates with min()"),
    "tool_population_percentile": (POP + f"RETURN n.plan_tier AS pl, count(*) AS k, percentiledisc({ENC}, 1.0) AS p\n"
                                   "ORDER BY pl LIMIT 10", "aggregates with percentiledisc()"),     # = max()
    # a lapsed renewal's own feature, when its sub-cell holds one: the sum is that renewal's value
    "tool_population_case_fine_value": (POP + "RETURN n.plan_tier AS pl, count(*) AS k, sum(CASE WHEN n.churned = 1 "
                                        "THEN n.agent_requests_28d ELSE 0 END) AS s\nORDER BY pl LIMIT 10", UNVETTED),
    # two cell statistics in one number (inside one aggregate, through an alias, or around aggregates):
    # the tool layer suppresses a count of fewer than 5 column by column, and a packed sub-cell escapes it
    "tool_population_flag_arithmetic": (POP + "RETURN n.plan_tier AS pl, sum(n.churned * 1000 + n.overage_toggled_off) "
                                        "AS s, count(*) AS k\nORDER BY pl LIMIT 10", UNVETTED),
    "tool_population_case_arithmetic_value": (POP + "RETURN n.plan_tier AS pl, count(*) AS k, sum(CASE WHEN "
                                              "n.route = 'model' THEN n.churned * 1000 ELSE n.churned END) AS s\n"
                                              "ORDER BY pl LIMIT 10", UNVETTED),
    "tool_population_alias_packed": (POP + "WITH n, n.churned * 256 + n.overage_toggled_off AS v\n"
                                     "RETURN n.plan_tier AS pl, sum(v) AS packed, count(*) AS k\nORDER BY pl LIMIT 10",
                                     "aggregates sum ( v )"),
    "tool_population_with_packed": (POP + "WITH n.plan_tier AS pl, sum(n.churned) * 1000 + count(*) AS x\n"
                                    "RETURN pl, sum(x) AS packed\nORDER BY pl LIMIT 10", PACKED),
    "tool_population_histogram": (POP + "RETURN n.plan_tier AS pl, count(*) AS k, histogram(n.outcome) AS h\n"
                                  "ORDER BY pl LIMIT 10", "aggregates with histogram()"),        # a count per value
}
# Fix round 4 (p3-hardening verifier round 1, the open major): the packing moved inside a CASE, a flag
# column, and rows that repeat a renewal. A CASE value of 1001 in a sum adds a sub-cell's lapses, times
# 1000, to its size (packed_in_case is tool_population_packed_columns with the * 1000 moved into the CASE);
# the flag sum(<sub-cell>) = 1 in a RETURN says a sub-cell holds one renewal and whether it lapsed; a
# CASE value of 1000001 puts a small cell's lapses and size into one number that is not a small count;
# and a MATCH (p:Plan) beside the renewal, filtered p.plan_tier = 'pro' OR <sub-cell lapsed>, counts the
# lapsed renewals of the sub-cell three times, so count(*) is the cell plus twice their number. None is a
# count under 5, so the tool layer's column-by-column n<5 rule would serve each. All executed and decoded.
REPEATED = "aggregates rows that can repeat a renewal"
ALIAS = "uses the aggregate alias"
BAD_TEMPLATES |= {
    "tool_population_packed_in_case": (SUBS + "RETURN n.plan_tier AS pl, s.city AS city, count(*) AS k, "
                                       f"sum(CASE WHEN {BOTH} AND n.churned = 1 THEN 1001 WHEN {BOTH} THEN 1 "
                                       "ELSE 0 END) AS packed\nORDER BY pl, city LIMIT 1000", UNVETTED),
    "tool_population_packed_simple_case": (SUBS + "RETURN n.plan_tier AS pl, s.city AS city, count(*) AS k, "
                                           f"sum(CASE WHEN {BOTH} THEN CASE n.churned WHEN 1 THEN 1001 ELSE 1 END "
                                           "ELSE 0 END) AS packed\nORDER BY pl, city LIMIT 1000", UNVETTED),
    "tool_population_flag_columns": (SUBS + "RETURN n.plan_tier AS pl, s.city AS city, count(*) AS k, "
                                     f"sum(CASE WHEN {BOTH} THEN 1 ELSE 0 END) = 1 AS one, "
                                     f"sum(CASE WHEN {BOTH} THEN n.churned ELSE 0 END) = 1 AS lapsed\n"
                                     "ORDER BY pl, city LIMIT 1000", PACKED),
    "tool_population_cell_lapses_and_size": (SUBS + "WHERE n.route = 'model'\nRETURN n.plan_tier AS pl, s.city AS "
                                             "city, n.overage_toggled_off AS off, n.incident_exposed_28d AS inc, "
                                             "sum(CASE WHEN n.churned = 1 THEN 1000001 ELSE 1 END) AS packed\n"
                                             "ORDER BY pl, city, off, inc LIMIT 1000", UNVETTED),
    "tool_population_repeated_rows": ("MATCH (s:Subscription)-[:HAS_RENEWAL]->(n:Renewal), (p:Plan)\n"
                                      f"WHERE p.plan_tier = 'pro' OR ({BOTH} AND n.churned = 1)\n"
                                      "RETURN n.plan_tier AS pl, s.city AS city, count(*) AS k\n"
                                      "ORDER BY pl, city LIMIT 1000", REPEATED),
}
# Rule 7's vetted grammar refuses these too, although none leaks on its own: an allowlist refuses what it
# has not vetted (a sum or mean of a feature, date or amount; a predicate over features; a function,
# concatenation or DISTINCT of a value inside; a string summed; a rate or CASE around aggregates; any
# min / max). test_rule_7_vets_what_an_aggregate_computes checks each, and the vetted forms beside them.
REFUSED_CELL_STATISTICS = {
    "sum_feature": ("RETURN n.outcome AS o, sum(n.agent_requests_28d) AS s, count(*) AS k\nORDER BY o",
                    "aggregates sum ( n . agent_requests_28d ) while"),
    "avg_amount": ("RETURN n.plan_tier AS pl, n.outcome AS o, avg(n.overage_usd_28d) AS a, count(*) AS k\nORDER BY pl",
                   "aggregates with avg()"),
    "sum_date_case": ("RETURN n.plan_tier AS pl, sum(CASE WHEN n.churned = 1 THEN n.as_of END) AS s\nORDER BY pl",
                      UNVETTED),
    "sum_cohort_string": ("RETURN n.plan_tier AS pl, sum(n.route) AS s, count(*) AS k\nORDER BY pl", UNVETTED),
    "function_inside": ("RETURN n.plan_tier AS pl, sum(coalesce(n.churned, 0)) AS s, count(*) AS k\nORDER BY pl",
                        UNVETTED),
    "concat_in_count": ("RETURN n.plan_tier AS pl, count(n.outcome + n.route) AS s, count(*) AS k\nORDER BY pl",
                        UNVETTED),
    "case_fine_predicate": ("RETURN n.plan_tier AS pl, count(*) AS k, sum(CASE WHEN n.agent_requests_28d > "
                            "n.limit_hits_14d THEN n.churned ELSE 0 END) AS s\nORDER BY pl", UNVETTED),
    "case_starts_with": ("RETURN n.plan_tier AS pl, sum(CASE WHEN n.outcome STARTS WITH 'v' THEN 1 ELSE 0 END) AS s, "
                         "count(*) AS k\nORDER BY pl", UNVETTED),
    "count_distinct_value": ("RETURN n.outcome AS o, count(DISTINCT n.as_of) AS days, count(*) AS k\nORDER BY o",
                             UNVETTED),
    "count_distinct_cohort_key": ("RETURN n.outcome AS o, count(DISTINCT n.plan_tier) AS plans, count(*) AS k\n"
                                  "ORDER BY o", UNVETTED),
    "sum_per_renewal_count": ("OPTIONAL MATCH (n)-[f:FIRST_RENEWAL_AFTER]->(:PricingChange)\nWITH n, count(f) AS cuts\n"
                              "RETURN n.outcome AS o, sum(cuts) AS total, count(*) AS k\nORDER BY o",
                              "aggregates sum ( cuts )"),
    "sum_map_alias": ("WITH n, {a: n.churned} AS m\nRETURN n.plan_tier AS pl, sum(m.a) AS s, count(*) AS k\n"
                      "ORDER BY pl", UNVETTED),
    "rate_column": ("RETURN n.plan_tier AS pl, sum(n.churned) * 1.0 / count(*) AS rate\nORDER BY pl", PACKED),
    "case_around_aggregate": ("RETURN n.plan_tier AS pl, count(*) AS k, CASE WHEN count(*) < 5 THEN 0 ELSE "
                              "sum(n.churned) END AS l\nORDER BY pl", PACKED),
    "max_flag": ("RETURN n.plan_tier AS pl, max(n.churned) AS any_lapse, count(*) AS k\nORDER BY pl",
                 "aggregates with max()"),
    "min_in_with": ("WITH n.plan_tier AS pl, min(n.churned) AS m, count(*) AS c\nRETURN pl, sum(c) AS k\nORDER BY pl",
                    "aggregates with min()"),
    # fix round 4: avg is a rate (sum / count); a sum adds 0 or 1 per renewal; a flag is a per-renewal
    # "has any" (<aggregate> <op> 0 in a WITH that keeps the renewal), never a column; an aggregate's
    # alias travels whole (no arithmetic, function or comparison around it, in any clause); a count or
    # sum of an earlier grouping is never a key; and every aggregate counts each renewal once
    "avg_flag": ("RETURN n.plan_tier AS pl, avg(n.churned) AS r, count(*) AS k\nORDER BY pl", "aggregates with avg()"),
    "sum_large_literal": ("RETURN n.plan_tier AS pl, sum(1001) AS s, sum(n.churned) AS l\nORDER BY pl",
                          "aggregates sum ( 1001 )"),
    "case_large_literal": ("RETURN n.plan_tier AS pl, sum(CASE WHEN n.churned = 1 THEN 1000001 ELSE 1 END) AS s\n"
                           "ORDER BY pl", UNVETTED),
    "case_negative_value": ("RETURN n.plan_tier AS pl, sum(CASE WHEN n.route = 'model' THEN 1 WHEN n.churned = 1 "
                            "THEN -1 ELSE 0 END) AS s\nORDER BY pl", UNVETTED),
    "flag_column": ("RETURN n.plan_tier AS pl, count(*) AS k, sum(n.churned) = 1 AS one\nORDER BY pl", PACKED),
    "flag_in_cohort_grouping": ("WITH n.plan_tier AS pl, sum(n.churned) > 0 AS any, count(*) AS c\n"
                                "RETURN pl, any, sum(c) AS k\nORDER BY pl", PACKED),
    "flag_not_zero": ("OPTIONAL MATCH (n)-[f:FIRST_RENEWAL_AFTER]->(:PricingChange)\nWITH n, count(f) = 2 AS two\n"
                      "RETURN two, n.outcome AS o, count(*) AS k\nORDER BY two, o", PACKED),
    "order_by_packed_calls": ("RETURN n.plan_tier AS pl, count(*) AS k\nORDER BY sum(n.churned) * 1000 + count(*)",
                              PACKED),
    "order_by_packed_aliases": ("RETURN n.plan_tier AS pl, count(*) AS k, sum(n.churned) AS l\nORDER BY l * 1000 + k",
                                ALIAS),
    "having_packed": ("WITH n.plan_tier AS pl, sum(n.churned) AS l, count(*) AS c\nWHERE l * 1000 + c > 5\n"
                      "RETURN pl, sum(c) AS k\nORDER BY pl", ALIAS),
    "having_group_size": ("WITH n.plan_tier AS pl, n.route AS rt, sum(n.churned) AS l, count(*) AS c\nWHERE c = 1\n"
                          "RETURN pl, sum(l) AS k\nORDER BY pl", ALIAS),
    "case_on_group_size": ("WITH n.plan_tier AS pl, n.route AS rt, sum(n.churned) AS l, count(*) AS c\n"
                           "RETURN pl, sum(CASE WHEN c = 1 THEN l ELSE 0 END) AS k\nORDER BY pl", ALIAS),
    "packed_key_later": ("WITH n.plan_tier AS pl, sum(n.churned) AS l, count(*) AS c\n"
                         "WITH pl, l * 1000 + c AS packed, count(*) AS g\nRETURN pl, packed, sum(g) AS k\nORDER BY pl",
                         ALIAS),
    "function_of_alias": ("WITH n.plan_tier AS pl, sum(n.churned) AS l, count(*) AS c\n"
                          "RETURN pl, sum(abs(c)) AS k, sum(l) AS lapses\nORDER BY pl", ALIAS),
    "sum_as_key": ("WITH n.plan_tier AS pl, n.route AS rt, sum(n.churned) AS l, count(*) AS c\n"
                   "RETURN pl, l, sum(c) AS k\nORDER BY pl", "groups its rows by sum(...)"),
    "cartesian_rows": ("MATCH (p:Plan)\nWHERE p.plan_tier = 'pro' OR n.churned = 1\n"
                       "RETURN n.route AS rt, count(*) AS k\nORDER BY rt", REPEATED),
    "unwind_rows": ("UNWIND [1, 2, 3] AS x\nWITH n, x\nWHERE x = 1 OR n.churned = 1\n"
                    "RETURN n.route AS rt, count(*) AS k\nORDER BY rt", REPEATED),
    "calendar_hub_rows": ("MATCH (n)-[:ON_PLAN]->(pl:Plan)<-[c:CUT_CAP]-(p:PricingChange)\n"
                          "RETURN n.route AS rt, count(*) AS k, sum(n.churned) AS l\nORDER BY rt", REPEATED),
    "event_rows": ("MATCH (s:Subscription)-[:HAS_RENEWAL]->(n), (s)-[e:OPENED]->(x:Ticket)\n"
                   "WHERE e.event_date <= n.as_of\nRETURN n.route AS rt, count(*) AS k, sum(n.churned) AS l\nORDER BY rt",
                   REPEATED),
    "distinct_pair_rows": ("MATCH (n)-[:ON_PLAN]->(pl:Plan)<-[c:CUT_CAP]-(p:PricingChange)\nWITH DISTINCT n, p\n"
                           "RETURN n.route AS rt, count(*) AS k\nORDER BY rt", REPEATED),
    "unpinned_hub_edge_rows": ("MATCH (n)-[f:FIRST_RENEWAL_AFTER]->(p:PricingChange)\n"
                               "RETURN n.route AS rt, count(*) AS k, sum(n.churned) AS l\nORDER BY rt", REPEATED),
}
# ... and the shapes it vets (each a population template on its own, accepted and executed on tiny)
VETTED_CELL_STATISTICS = {
    "count_star_and_variables": "RETURN n.route AS rt, count(*) AS k, count(n) AS c, count(DISTINCT n) AS d\n"
                                "ORDER BY rt",
    "sum_flags": "RETURN n.route AS rt, sum(n.churned) AS l, sum(n.overage_toggled_off) AS t\nORDER BY rt",
    "count_cohort_key_and_literals": "RETURN n.plan_tier AS pl, count(n.outcome) AS o, sum(1) AS k, count('x') AS x\n"
                                     "ORDER BY pl",
    "searched_case": "RETURN n.plan_tier AS pl, sum(CASE WHEN n.route = 'model' AND NOT (n.churned = 0) THEN 1 "
                     "WHEN n.route = 'dunning' THEN n.churned ELSE 0 END) AS s\nORDER BY pl",
    "simple_case": "RETURN n.plan_tier AS pl, sum(CASE n.route WHEN 'model' THEN n.churned ELSE null END) AS s\n"
                   "ORDER BY pl",
    "in_is_null_parameter": "RETURN n.route AS rt, count(CASE WHEN n.outcome IN ['renewed', 'voluntary_lapse'] OR "
                            "n.outcome IS NULL OR n.plan_tier = $plan THEN 1 END) AS c\nORDER BY rt",
    "cohort_group_aliases": "WITH n.plan_tier AS pl, n.outcome AS o, count(*) AS c, sum(n.churned) AS l\n"
                            "RETURN pl, sum(c) AS k, sum(l) AS lapses, sum(CASE WHEN o = 'renewed' THEN c ELSE 0 END) "
                            "AS renewed\nORDER BY pl",
    "projection_alias": "WITH n, CASE WHEN n.route = 'model' THEN 1 ELSE 0 END AS modelled\n"
                        "RETURN n.plan_tier AS pl, sum(CASE WHEN modelled = 1 THEN n.churned ELSE 0 END) AS l, "
                        "sum(modelled) AS m\nORDER BY pl",
    # fix round 4: count() may count any literal (it counts rows, not values); whole aggregate aliases
    # in ORDER BY; renewals counted once after WITH DISTINCT, or after a WITH that keeps the renewal
    "count_any_literal": "RETURN n.plan_tier AS pl, count(CASE WHEN n.churned = 1 THEN 1001 END) AS l\nORDER BY pl",
    "order_by_whole_statistics": "RETURN n.route AS rt, count(*) AS k, sum(n.churned) AS l\nORDER BY k DESC, l, rt",
    "distinct_after_events": "MATCH (s:Subscription)-[:HAS_RENEWAL]->(n)\nMATCH (s)-[e:OPENED]->(x:Ticket)\n"
                             "WHERE e.event_date <= n.as_of\nWITH DISTINCT n\n"
                             "RETURN n.plan_tier AS pl, count(*) AS k, sum(n.churned) AS l\nORDER BY pl",
    "per_renewal_flag_cells": "OPTIONAL MATCH (n)-[f:FIRST_RENEWAL_AFTER]->(:PricingChange)\n"
                              "WITH n, count(f) > 0 AS fa\nRETURN n.plan_tier AS pl, fa, count(*) AS k, "
                              "sum(n.churned) AS l\nORDER BY pl, fa",
}
# Accepted by the lint, refused by the engine's binder ("nested aggregation": it inlines the flag), so not run.
VETTED_NOT_RUN = {
    "statistic_flag_alias": "OPTIONAL MATCH (n)-[f:FIRST_RENEWAL_AFTER]->(:PricingChange)\n"
                            "WITH n, count(f) > 0 AS first_after\nRETURN n.plan_tier AS pl, "
                            "sum(CASE WHEN first_after THEN n.churned ELSE 0 END) AS l, count(*) AS k\nORDER BY pl",
}
SHAPE_ONLY = ("tool_no_order", "tool_union", "tool_write")
# Two identities: executed on the tiny build with $rid / $subscription_id naming a historical source
# and $renewal_id naming another renewal (test_second_identity_shapes_really_leak).
SECOND_IDENTITY = ("tool_optional_map_source", "tool_optional_other_source", "tool_second_identity",
                   "tool_second_identity_where")
# Executed on the tiny build: every one of these serves an outcome or an event from after as_of.
LEAKS_ON_TINY = {
    "tool_top_k", "tool_whole_node", "tool_properties_fn", "tool_struct_extract", "tool_node_alias_in_with",
    "tool_unwind_alias", "tool_upper_property", "tool_upper_variable", "tool_lowercase", "tool_backtick",
    "tool_comment_hides_bound", "tool_string_hides_bound", "tool_as_of_plus", "tool_not_visible", "tool_xor",
    "tool_optional_where", "tool_mask_redefined", "tool_mask_negated", "tool_mask_else", "tool_same_plan",
    "tool_event_as_of_plus", "tool_event_not", "tool_event_other_renewal", "tool_event_optional_where",
    "tool_naive_hits", "tool_mask_simple_case", "tool_case_hides_bound", "tool_event_case_hides_bound",
    "tool_event_node_joined_late", "tool_same_plan_anonymous_source",
    "tool_dot_star", "tool_dot_star_after_rank", "tool_dot_star_after_with", "tool_alias_shadows_source_filter",
    "tool_alias_shadows_source_mask", "tool_alias_shadows_event_source", "tool_alias_shadows_event_edge",
    "tool_alias_shadows_neighbour", "tool_backslash_string", "tool_cut_cap_unbounded", "tool_population_rows",
    "tool_cut_cap_or", "tool_cut_cap_as_of_plus", "tool_cut_cap_renewal_date", "tool_cut_cap_neighbour_as_of",
    "tool_cut_cap_optional_where", "tool_cut_cap_alias_shadow",
    "tool_pricing_by_property", "tool_pricing_node_unbounded", "tool_pricing_optional_cut", "tool_incident_by_property",
    "tool_incident_end_after_as_of",
}
# Executed on the tiny build by test_population_keys_and_parameters_that_single_out_a_renewal_really_do.
POPULATION_LEAKS = ("tool_population_by_dates", "tool_population_observed_on", "tool_population_param_pins")
# Executed on the tiny build by test_population_groupings_and_pins_single_out_masked_renewals: each
# serves, row by row, the outcome of a renewal that similar_top_k_visible masks for some source.
POPULATION_SINGLES = ("tool_population_with_group_by_as_of", "tool_population_with_group_by_features",
                      "tool_population_min_alias_key", "tool_population_map_alias_key", "tool_population_count_key",
                      "tool_population_skip_one", "tool_population_param_concat", "tool_population_map_alias_param",
                      "tool_population_simple_case_param", "tool_population_literal_map",
                      "tool_population_literal_in_aggregate")
# Executed on the tiny build by test_pinned_second_renewals_really_leak: $renewal_id names the current
# renewal (its as_of is the latest), and the rows describe a historical renewal pinned by $k / $limit (its
# id, its position or its feature value) or by a literal ('__rid__' is written as its id): they serve
# neighbour outcomes observed after that renewal's as_of, which similar_top_k_visible masks for it.
PINNED_SECOND = ("tool_k_pins_second_renewal", "tool_k_concat_pins_second_renewal", "tool_int_k_pins_by_feature",
                 "tool_limit_pins_second_renewal", "tool_skip_picks_second_renewal", "tool_literal_pin_wrapped",
                 "tool_literal_pin_disconnected", "tool_literal_feature_pin", "tool_hub_pins_second_renewal",
                 "tool_neighbour_of_neighbour", "tool_anonymous_second_renewal",
                 # fix round 2: reached through the Plan hub and anchoring its neighbours there; related by value
                 "tool_hub_plan_literal_pin", "tool_hub_plan_skip_literal", "tool_hub_plan_asof_literal",
                 "tool_hub_plan_rowfilter", "tool_hub_recursive_path", "tool_value_relation_nearest",
                 "tool_value_relation_window", "tool_value_relation_alias", "tool_value_relation_map")
# Executed by test_a_renewal_pinned_through_the_pricing_hub_really_leaks (its own source and pinned renewal:
# the one historical renewal of tiny's sub_maya plan is not a first renewal after a cut).
PINNED_THROUGH_PRICING = ("tool_hub_pricing_literal_pin",)
# Executed and decoded on the tiny build by test_aggregates_that_encode_a_label_really_decode_masked_renewals.
POPULATION_ENCODED = ("tool_population_encoded_extremes", "tool_population_bitmask_sum",
                      "tool_population_string_extremes", "tool_population_extremes_by_outcome",
                      "tool_population_packed_columns", "tool_population_packed_in_case",
                      "tool_population_packed_simple_case", "tool_population_flag_columns",
                      "tool_population_cell_lapses_and_size", "tool_population_repeated_rows")

# Shapes the rule allows (the lint must not refuse what is safe): executed on the tiny build too.
GOOD_TEMPLATES = {
    "ok_row_filter": SRC + "WHERE n.outcome_observed_on <= r.as_of\n" + NBR + TAIL,
    "ok_brackets_and_case": ("match (R:renewal {renewal_id: $renewal_id})-[E:similar_to]->(N:renewal)\n"
                             "where (N.OUTCOME_OBSERVED_ON <= R.AS_OF) and E.rank <= 5\n"
                             "return N.renewal_id as renewal_id, N.Outcome as outcome\norder by renewal_id limit 50"),
    "ok_with_filter": SRC + "WITH r, e, n\nWHERE n.outcome_observed_on <= r.as_of\n" + NBR + TAIL,
    "ok_optional_neighbour": ("MATCH (r:Renewal {renewal_id: $renewal_id})\n"
                              "OPTIONAL MATCH (r)-[e:SIMILAR_TO]->(n:Renewal)\nWHERE n.outcome_observed_on <= r.as_of\n"
                              + NBR + TAIL),
    "ok_masked": (SRC + "WITH r, e, n, (n.outcome_observed_on <= r.as_of) AS known\n"
                  "RETURN n.renewal_id AS renewal_id, CASE WHEN known THEN n.outcome END AS outcome, "
                  "CASE WHEN known THEN n.route ELSE 'hidden' END AS route" + TAIL),
    "ok_event_rows": EV + "MATCH (s)-[e:OPENED]->(x:Ticket)\nWHERE e.event_date <= r.as_of\n" + TICKETS,
    "ok_event_window_count": (EV + "OPTIONAL MATCH (s)-[e:HIT_LIMIT]->(x:LimitHit)\n"
                              "WHERE e.event_date <= r.as_of AND e.event_date > r.as_of - INTERVAL('14 DAYS')\n"
                              "RETURN r.renewal_id AS renewal_id, count(e) AS hits" + TAIL),
    "ok_billed": (EV + "MATCH (s)-[e:BILLED]->(x:BillingEvent)\n"
                  "WHERE e.event_date <= r.as_of AND e.outcome_evidence = false\n"
                  "RETURN x.event_id AS id, e.event_date AS event_date, e.event_type AS event_type\n"
                  "ORDER BY id LIMIT $limit"),
    "ok_chained_events": (EV + "MATCH (s)-[h:HIT_LIMIT]->(:LimitHit), (s)-[e:CHANGED_OVERAGE]->(x:OverageChange)\n"
                          "WHERE h.event_date <= e.event_date AND e.event_date <= r.as_of\n"
                          "RETURN DISTINCT x.event_id AS id, h.event_date AS event_date\n"
                          "ORDER BY event_date, id LIMIT $limit"),
    "ok_population": ("MATCH (r:Renewal)\nWHERE r.plan_tier = $plan\n"
                      "RETURN r.route AS route, count(*) AS renewals, sum(r.churned) AS lapses\n"
                      "ORDER BY route LIMIT 20"),
    "ok_population_total": ("MATCH (:Subscription)-[e:OPENED]->(:Ticket)\nRETURN count(e) AS tickets\n"
                            "ORDER BY tickets LIMIT 1"),
    "ok_own_fields": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(p:Plan)\n"
                      "RETURN r.route AS route, r.outcome AS outcome, p.price_usd AS price_usd\n"
                      "ORDER BY route LIMIT 1"),
    "ok_not_self": (SRC + "WHERE n.outcome_observed_on <= r.as_of AND n.renewal_id <> $renewal_id\n" + NBR + TAIL),
    "ok_cut_cap_bounded": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(pl:Plan)<-[c:CUT_CAP]-"
                           "(p:PricingChange)\nWHERE c.event_date <= r.as_of\n"
                           "RETURN c.event_date AS event_date, p.change_id AS id\nORDER BY event_date LIMIT 10"),
    "ok_population_grouped": ("MATCH (r:Renewal)\nRETURN r.plan_tier AS plan, r.route AS route, count(*) AS renewals, "
                              "sum(r.churned) AS lapses\nORDER BY plan, route LIMIT 20"),
    "ok_population_count_alias": "MATCH (x:Renewal)\nRETURN count(x) AS n\nORDER BY n LIMIT 1",   # count_nodes' shape
    "ok_cut_cap_window": (CUT + "WHERE c.event_date <= r.as_of AND c.event_date > r.as_of - INTERVAL('365 DAYS')\n"
                          + CUTS),
    # the OPTIONAL MATCH introduces c: its WHERE decides which cuts come back at all
    "ok_cut_cap_optional": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(pl:Plan)\n"
                            "OPTIONAL MATCH (pl)<-[c:CUT_CAP]-(p:PricingChange)\nWHERE c.event_date <= r.as_of\n"
                            + CUTS),
    # no renewal named: the public pricing calendar
    "ok_calendar_cuts": ("MATCH (p:PricingChange)-[c:CUT_CAP]->(pl:Plan)\n"
                         "RETURN p.change_id AS id, c.event_date AS event_date, pl.plan_tier AS plan\n"
                         "ORDER BY event_date, id, plan LIMIT 50"),
    "ok_population_case_key": (POP + "RETURN CASE WHEN n.route = 'model' THEN 'model' ELSE 'other' END AS kind, "
                               "count(*) AS n_rows, sum(n.churned) AS lapses\nORDER BY kind LIMIT 10"),
    "ok_population_flag_alias": ("MATCH (r:Renewal)\nOPTIONAL MATCH (r)-[f:FIRST_RENEWAL_AFTER]->(:PricingChange)\n"
                                 "WITH r, count(f) > 0 AS first_after\n"
                                 "RETURN r.plan_tier AS plan, first_after, count(*) AS n_rows, "
                                 "sum(r.churned) AS lapses\n"
                                 "ORDER BY plan, first_after LIMIT 20"),
    "ok_population_hub_param": ("MATCH (i:Incident {incident_id: $incident_id})<-[e:EXPOSED_TO]-(s:Subscription)"
                                "-[:HAS_RENEWAL]->(r:Renewal)\nWHERE e.event_date <= r.as_of\nWITH DISTINCT r\n"
                                "RETURN r.plan_tier AS plan, r.outcome AS outcome, count(*) AS n_rows\n"
                                "ORDER BY plan, outcome LIMIT 20"),
    "ok_population_city_param": ("MATCH (s:Subscription)-[:HAS_RENEWAL]->(n:Renewal)\nWHERE s.city = $city\n"
                                 "RETURN n.route AS route, count(*) AS n_rows\nORDER BY route LIMIT 10"),
    # fix round: paging by rank, the source's plan-mates under the rule, the calendar cut at as_of,
    # cohort groupings in a WITH, a per-renewal flag
    "ok_rank_param": SRC + f"WHERE e.rank <= $k AND {VIS}\n" + NBR + "\nORDER BY renewal_id LIMIT $limit",
    "ok_same_plan_visible": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(p:Plan)"
                             "<-[:ON_PLAN]-(o:Renewal)\n"
                             "WHERE o.outcome_observed_on <= r.as_of AND o.renewal_id <> $renewal_id\n"
                             "RETURN o.renewal_id AS renewal_id, o.outcome AS outcome\nORDER BY renewal_id LIMIT 200"),
    "ok_pricing_by_property": (CUR + "MATCH (p:PricingChange)\nWHERE p.effective_date <= r.as_of\n"
                               "RETURN p.change_id AS id, p.effective_date AS event_date\n"
                               "ORDER BY event_date LIMIT 10"),
    "ok_incident_by_property": (CUR + "MATCH (i:Incident)\nWHERE i.starts_on <= r.as_of\n"
                                "RETURN i.incident_id AS id, i.starts_on AS event_date\nORDER BY event_date LIMIT 10"),
    "ok_incident_over_by_as_of": (CUR + "MATCH (i:Incident)\nWHERE i.starts_on <= r.as_of AND i.ends_on <= r.as_of\n"
                                  "RETURN i.incident_id AS id, i.ends_on AS event_date, i.days AS days\n"
                                  "ORDER BY event_date LIMIT 10"),
    "ok_first_renewal_after_hub": ("MATCH (r:Renewal {renewal_id: $renewal_id})-[f:FIRST_RENEWAL_AFTER]->"
                                   "(p:PricingChange)\n"
                                   "RETURN p.change_id AS id, f.known_by_as_of AS known_by_as_of\n"
                                   "ORDER BY id LIMIT 10"),
    "ok_population_with_cohort_group": (POP + "WITH n.plan_tier AS plan, n.outcome AS o, count(*) AS c\n"
                                        "RETURN plan, o, sum(c) AS k\nORDER BY plan, o LIMIT 50"),
    "ok_population_with_ticket_flag": ("MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal)\n"
                                       "OPTIONAL MATCH (s)-[e:OPENED]->(t:Ticket)\nWHERE e.event_date <= r.as_of\n"
                                       "WITH r, count(e) > 0 AS any_ticket\n"
                                       "RETURN any_ticket, r.outcome AS outcome, count(*) AS n_rows\n"
                                       "ORDER BY any_ticket, outcome LIMIT 20"),
    # fix round 2: a renewal reached through a hub (or a neighbour's own plan) is a row that is read: it may
    # be selected by value, and sit beside the source's neighbours, as long as it anchors no other renewal
    "ok_plan_mates_selected_by_value": (PLAN_H + "WHERE h.agent_requests_28d > 200 AND "
                                        "h.outcome_observed_on <= r.as_of\n"
                                        "RETURN h.renewal_id AS renewal_id, h.outcome AS outcome\n"
                                        "ORDER BY renewal_id LIMIT 200"),
    "ok_neighbour_own_plan": (SRC.replace("(n:Renewal)\n", "(n:Renewal)-[:ON_PLAN]->(q:Plan)\n")
                              + f"WHERE {VIS}\nRETURN n.renewal_id AS renewal_id, q.plan_tier AS plan, "
                                "n.outcome AS outcome" + TAIL),
    "ok_pinned_plan_mate_beside_neighbours": (PLAN_H + "MATCH (r)-[e:SIMILAR_TO]->(n:Renewal)\n"
                                              f"WHERE h.agent_requests_28d = 211 AND {VIS}\n"
                                              "RETURN h.as_of AS pinned_as_of, n.renewal_id AS renewal_id, "
                                              "n.outcome AS outcome" + TAIL),
    # fix round 3: the vetted cell statistics (rule 7): count / sum of *, a variable, a cohort flag, a
    # literal, CASE over cohort keys (searched or simple, IN, IS NULL, NOT, a parameter), and aliases of them
    "ok_population_cell_statistics": (SUBS + "WHERE s.city = $city\n"
                                      "RETURN n.plan_tier AS plan, count(*) AS k, count(DISTINCT s) AS subs, "
                                      "sum(n.churned) AS lapses, count(n.outcome) AS known, "
                                      "sum(CASE WHEN n.route = 'model' AND NOT n.churned = 0 THEN 1 ELSE 0 END) AS a, "
                                      "sum(CASE n.route WHEN 'model' THEN n.churned ELSE 0 END) AS b, "
                                      "count(CASE WHEN n.outcome IN ['renewed', 'voluntary_lapse'] THEN 1 END) AS c, "
                                      "sum(CASE WHEN n.outcome IS NULL OR n.plan_tier = $plan THEN 0 ELSE 1 END) AS d, "
                                      "sum(1) AS e\nORDER BY plan LIMIT 10"),
    "ok_population_statistic_aliases": ("MATCH (r:Renewal)\n"
                                        "OPTIONAL MATCH (r)-[f:FIRST_RENEWAL_AFTER]->(:PricingChange)\n"
                                        "WITH r, count(f) > 0 AS first_after, r.churned AS lapsed\n"
                                        "WITH r.plan_tier AS plan, first_after, count(*) AS cell, "
                                        "sum(lapsed) AS lapses\n"
                                        "RETURN plan, first_after, sum(cell) AS k, sum(lapses) AS lapses_total\n"
                                        "ORDER BY plan, first_after LIMIT 10"),
}


@pytest.mark.parametrize("name", sorted(BAD_TEMPLATES))
def test_lint_catches_a_planted_leak(name):
    text, want = BAD_TEMPLATES[name]
    problems = queries.lint({name: text}, contract_only=())
    assert problems and any(want in p for p in problems), problems
    assert all(p.startswith(f"{name}: ") for p in problems)
    if name not in SHAPE_ONLY:
        assert queries.lint({name: text}, contract_only=(name,)) == []     # allowed only as contract-only


def test_lint_accepts_the_shapes_the_rule_allows():
    assert queries.lint(GOOD_TEMPLATES, contract_only=()) == []
    assert LEAKS_ON_TINY <= set(BAD_TEMPLATES) and len(BAD_TEMPLATES) >= 175
    executed = [set(SECOND_IDENTITY), set(POPULATION_LEAKS), set(POPULATION_SINGLES), set(PINNED_SECOND),
                set(PINNED_THROUGH_PRICING), set(POPULATION_ENCODED), LEAKS_ON_TINY]
    assert all(group <= set(BAD_TEMPLATES) for group in executed)
    assert sum(map(len, executed)) == len(set().union(*executed))      # each planted leak is executed by one test


def test_names_mean_one_thing_and_dot_star_is_refused_anywhere():
    """Verifier round 2: an alias of any clause kind (WITH, UNWIND, RETURN) never re-uses a pattern
    variable's name and is never defined twice, and `<var>.*` is refused wherever it is written."""
    for kind in ("WITH", "UNWIND", "RETURN"):
        for name in ("r", "e", "n"):
            body = {"WITH": f"WITH r, e, n, 1 AS {name}\nRETURN e.rank AS rank",
                    "UNWIND": f"UNWIND [1] AS {name}\nRETURN e.rank AS rank",
                    "RETURN": f"RETURN e.rank AS {name}"}[kind]
            text = SRC + "WHERE n.outcome_observed_on <= r.as_of\n" + body + "\nORDER BY 1 LIMIT 5"
            problems = queries.lint({"t": text}, contract_only=())
            assert any(f"re-uses the name of the pattern variable {name} as an alias ({kind}" in p for p in problems), \
                (kind, name, problems)
    for first, second in (("WITH", "WITH"), ("WITH", "RETURN"), ("WITH", "UNWIND"), ("UNWIND", "RETURN")):
        a = "WITH r, e, n, e.rank AS k" if first == "WITH" else "UNWIND [1] AS k\nWITH r, e, n, k"
        b = {"WITH": "WITH r, e, n, 2 AS k\nRETURN n.renewal_id AS id", "RETURN": "RETURN n.renewal_id AS id, 2 AS k",
             "UNWIND": "UNWIND [2] AS k\nRETURN n.renewal_id AS id"}[second]
        text = SRC + "WHERE n.outcome_observed_on <= r.as_of\n" + a + "\n" + b + "\nORDER BY id LIMIT 5"
        assert any("defines the alias k more than once" in p for p in queries.lint({"t": text}, contract_only=())), \
            (first, second)
    for where in ("RETURN n.*", "RETURN e.rank AS rank, n.*", "WITH r, e, n\nRETURN n.*", "RETURN x.*"):
        text = SRC + "WHERE n.outcome_observed_on <= r.as_of\n" + where + "\nORDER BY 1 LIMIT 5"
        owner = where.split(".")[-2].split()[-1]
        assert any(f"projects {owner}.*" in p for p in queries.lint({"t": text}, contract_only=())), where
    # the catalog's count_nodes keeps its column n and names its pattern variable x
    assert queries.render("count_nodes", label="Renewal") == "MATCH (x:Renewal) RETURN count(x) AS n ORDER BY n LIMIT 1"


def _run(conn, text, params):
    """Rows of raw template text as dicts (what queries.fetch does for a catalog template).

    Placeholders a planted or generated template carries for the renewal it pins are written in
    first: '__rid__' (its id, a string literal), 987654321 (its agent_requests_28d) and 987654322
    (its position in renewal_id order), from params["pin"] / ["pin_requests"] / ["pin_position"]."""
    text = text.replace("{rel}", "OPENED").replace("{label}", "BillingEvent")
    for mark, key in (("__rid__", "pin"), ("987654321", "pin_requests"), ("987654322", "pin_position")):
        if mark in text:
            text = text.replace(mark, str(params[key]))
    res = conn.execute(text, {k: v for k, v in params.items() if re.search(rf"\${k}\b", text)})
    cols = res.get_column_names()
    out = []
    while res.has_next():
        out.append(dict(zip(cols, res.get_next(), strict=True)))
    return out


def _served(value):
    """Every (renewal_id, outcome) a result value carries: row dicts (``n.*`` columns are named
    ``n.outcome``), whole nodes, lists of nodes."""
    if isinstance(value, dict):
        low = {str(k).lower().rsplit(".", 1)[-1]: v for k, v in value.items()}
        if low.get("renewal_id") is not None and low.get("outcome") not in (None, queries.NOT_YET_OBSERVED):
            yield low["renewal_id"], low["outcome"]
        for v in value.values():
            yield from _served(v)
    elif isinstance(value, (list, tuple)):
        for v in value:
            yield from _served(v)


def _pins(ren, rid, text) -> dict:
    """What pins the renewal ``rid`` in a template that relates renewals: $k / $limit as its id, its
    position in renewal_id order (SKIP $k) or its agent_requests_28d (compared with $k), and the
    values _run writes for the placeholders. Paging values (k 10, limit 1000) otherwise."""
    order = sorted(ren.index)
    pin = {"pin": rid, "pin_requests": int(ren.at[rid, "agent_requests_28d"]), "pin_position": order.index(rid)}
    head = text.rsplit("\nRETURN", 1)[0]
    by_id = any(form in head for form in ("lower($k)", "'' = $k", "renewal_id = $k"))
    k = rid if by_id else pin["pin_position"] if "SKIP $k" in head else \
        pin["pin_requests"] if "agent_requests_28d = $k" in head else 10
    return {**pin, "k": k, "limit": rid if "renewal_id = $limit" in head else 1000}


def _leaks(db, sources, text, ren=None, renewal_id=None):
    """(outcomes served that the source could not know, events served from after as_of, rows),
    summed over the ``sources`` rows of the Renewal frame ``ren`` (default: sources is all of it).

    The source is passed as $renewal_id, or, when ``renewal_id`` names a second identity for
    $renewal_id, as $rid or pinned by $k / $limit / a literal (_pins): the rows describe that
    source while the bound reads another renewal. One fresh Connection per statement text: the
    engine keeps every distinct parameterised statement prepared for the life of a Connection
    (store.connect explains the cost).
    """
    from lakehouse_graph import store

    ren = sources if ren is None else ren
    outcomes = events = rows = 0
    conn = store.connect(db)
    try:
        for rid in sources.index:
            as_of = ren.at[rid, "as_of"]
            got = _run(conn, text, {"renewal_id": renewal_id or rid, "rid": rid, "plan": ren.at[rid, "plan_tier"],
                                    **_pins(ren, rid, text)})
            rows += len(got)
            for row in got:
                outcomes += sum(1 for nid, _ in _served(row)
                                if nid != rid and not ren.at[nid, "outcome_observed_on"] <= as_of)
                date = {k.rsplit(".", 1)[-1]: v for k, v in row.items()}.get("event_date")   # e.* -> e.event_date
                events += int(date is not None and pd.Timestamp(date) > as_of)
    finally:
        conn.close()
    return outcomes, events, rows


def test_planted_leaks_really_leak_and_the_allowed_shapes_do_not(tiny_build):
    """The lint is not a style check: the refused shapes serve outcomes / events from after the
    source's as_of on the tiny build, and the accepted ones never do."""
    from lakehouse_graph import oracle, store

    bdir, _ = tiny_build
    tables = oracle.load_tables(bdir)
    ren = tables["Renewal"].set_index("renewal_id")
    ren_city = tables["Subscription"]["city"].iloc[0]
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    try:
        for name in sorted(LEAKS_ON_TINY):
            outcomes, events, rows = _leaks(db, ren, BAD_TEMPLATES[name][0])
            assert outcomes + events > 0, f"{name} is refused by the lint but leaked nothing in {rows} rows"
        served = 0
        for name, text in GOOD_TEMPLATES.items():
            if name.startswith(("ok_population", "ok_calendar")) or name == "ok_own_fields":   # no source-relative rows
                p = {"renewal_id": ren.index[0], "plan": "pro", "incident_id": "inc-002", "city": ren_city}
                assert _run(conn, text, p), name
                continue
            outcomes, events, rows = _leaks(db, ren, text)
            assert (outcomes, events) == (0, 0) and rows > 0, (name, outcomes, events, rows)
            served += rows
        assert served > 1000
        # the windowed count is the gold feature itself, renewal by renewal
        for rid in ren.index:
            got = _run(conn, GOOD_TEMPLATES["ok_event_window_count"], {"renewal_id": rid})
            assert [r["hits"] for r in got] == [ren.at[rid, "limit_hits_14d"]], rid
    finally:
        conn.close()
        db.close()


def test_second_identity_shapes_really_leak(tiny_build):
    """The two-identity shapes the lint refuses: the rows describe a historical renewal ($rid /
    $subscription_id) while the visibility rule is measured against another one ($renewal_id,
    here the current renewal or the late neighbour itself). Executed, they serve the historical
    renewal outcomes it could not know."""
    from lakehouse_graph import oracle, store

    bdir, _ = tiny_build
    t = oracle.load_tables(bdir)
    ren = t["Renewal"].set_index("renewal_id")
    sim = t["SIMILAR_TO"].merge(ren[["as_of"]], left_on="src", right_index=True) \
        .merge(ren[["outcome_observed_on"]], left_on="dst", right_index=True)
    late = sim[sim["outcome_observed_on"] > sim["as_of"]]
    historical = late["src"].iloc[0]
    hidden = set(late.loc[late["src"] == historical, "dst"])
    params = {"rid": historical, "subscription_id": ren.at[historical, "subscription_id"],
              "renewal_id": "sub_maya:2026-10-07"}
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    served = {}
    try:
        for name in SECOND_IDENTITY:
            p = {**params, "renewal_id": sorted(hidden)[0]} if name == "tool_optional_map_source" else params
            served[name] = {nid for row in _run(conn, BAD_TEMPLATES[name][0], p) for nid, _ in _served(row)} & hidden
    finally:
        conn.close()
        db.close()
    assert all(served.values()), served


def test_population_keys_and_parameters_that_single_out_a_renewal_really_do(tiny_build):
    """Rule 7's refused shapes are not style: an aggregate grouped by a date, or pinned by a
    parameter on a non-cohort field, describes one renewal at a time. Executed on tiny they serve
    the outcome of neighbours that similar_top_k_visible masks for some source (the back door a
    second, single-renewal template would open)."""
    from lakehouse_graph import oracle, store

    bdir, _ = tiny_build
    ren = oracle.load_tables(bdir)["Renewal"].set_index("renewal_id")
    alone = {key for key, size in ren.groupby(["as_of", "plan_tier"]).size().items() if size == 1}
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    try:
        masked = {r["renewal_id"] for rid in ren.index
                  for r in queries.fetch(conn, "similar_top_k_visible", {"renewal_id": rid, "k": 10})
                  if not r["outcome_visible"]}
        identified = {}
        for row in _run(conn, BAD_TEMPLATES["tool_population_by_dates"][0], {}):
            key = (pd.Timestamp(row["as_of"]), row["plan"])
            if key in alone:                                 # a group of one renewal: its outcome, row by row
                rid = ren.index[(ren["as_of"] == key[0]) & (ren["plan_tier"] == key[1])][0]
                identified[rid] = row["outcome"]
        assert set(identified) & masked, (len(identified), len(masked))
        assert all(ren.at[rid, "outcome"] == outcome for rid, outcome in identified.items())
        singles = [r for r in _run(conn, BAD_TEMPLATES["tool_population_observed_on"][0], {}) if r["n_rows"] == 1]
        assert singles                                       # a date observed once is one renewal's lapse flag
        nid = sorted(rid for rid in set(identified) & masked)[0]
        pinned = _run(conn, BAD_TEMPLATES["tool_population_param_pins"][0],
                      {"as_of": ren.at[nid, "as_of"].date(), "plan": ren.at[nid, "plan_tier"]})
        want = {"route": str(ren.at[nid, "route"]), "n_rows": 1, "lapses": int(ren.at[nid, "churned"])}
        assert [dict(r) for r in pinned] == [want], (pinned, want)
    finally:
        conn.close()
        db.close()


def _masked(conn, ren) -> set:
    """Renewals whose outcome similar_top_k_visible masks for at least one source."""
    return {r["renewal_id"] for rid in ren.index
            for r in queries.fetch(conn, "similar_top_k_visible", {"renewal_id": rid, "k": 10})
            if not r["outcome_visible"]}


def _targets(ren, masked, n=3) -> list:
    """Masked renewals a value pins alone: a unique agent_requests_28d, as_of and (as_of, plan_tier)."""
    once = ((ren.groupby("agent_requests_28d")["plan_tier"].transform("size") == 1)
            & (ren.groupby("as_of")["plan_tier"].transform("size") == 1)
            & (ren.groupby(["as_of", "plan_tier"])["route"].transform("size") == 1))
    return sorted(set(ren.index[once]) & masked)[:n]


def _target_params(ren, x) -> dict:
    """What pins the renewal x in a population template: its (as_of, plan), its feature value and its
    position in agent_requests_28d order, as parameters and as the values _run writes for placeholders."""
    as_of, plan = ren.at[x, "as_of"], ren.at[x, "plan_tier"]
    req = int(ren.at[x, "agent_requests_28d"])
    pos = int((ren["agent_requests_28d"] < req).sum())
    return {"plan": plan, "day": as_of.date(), "dayplan": f"{as_of.date()}{plan}", "p": f"{as_of.date()}{plan}",
            "req": req, "pos": pos, "pin": x, "pin_requests": req, "pin_position": pos}


def _hits_by_renewal(t) -> pd.Series:
    """HIT_LIMIT edges on or before each renewal's as_of (what count(e) per renewal is)."""
    ren = t["Renewal"][["renewal_id", "subscription_id", "as_of"]]
    e = t["HIT_LIMIT"].merge(ren, left_on="src", right_on="subscription_id")
    counts = e[e["event_date"] <= e["as_of"]].groupby("renewal_id").size()
    return counts.reindex(ren["renewal_id"], fill_value=0)


# row column -> Renewal column, for the rows a population template serves (count_key's hits is derived)
POP_COLUMNS = {"day": "as_of", "req": "agent_requests_28d", "amt": "overage_usd_28d", "pl": "plan_tier",
               "plan": "plan_tier", "rt": "route", "o": "outcome", "hits": "hits"}
FINE_COLUMNS = ("day", "req", "amt", "hits")


def _revealed(rows, ren, pinned=None) -> dict:
    """{renewal_id: outcome} of the renewals a population result serves one at a time: a row with k = 1
    whose columns (a date, a feature, a count, the cohort keys; for a template that pins a renewal by a
    parameter, that renewal) fit exactly one renewal."""
    out = {}
    for row in rows:
        fit = ren if pinned is None else ren.loc[[pinned]]
        if pinned is None and not set(row) & set(FINE_COLUMNS):
            continue                                        # a cohort cell of one is the tool layer's n<5 rule
        for col, value in row.items():
            if col in POP_COLUMNS and value is not None:
                target = fit[POP_COLUMNS[col]]
                fit = fit[(target == pd.Timestamp(value)) if col == "day" else (target == value)]
        if int(row.get("k", 0)) == 1 and len(fit) == 1:
            out[fit.index[0]] = row["o"]
    return out


def test_population_groupings_and_pins_single_out_masked_renewals(tiny_build):
    """Rule 7's refusals from this round are not style (verifier: C, C2, D1, and their neighbours): a
    WITH that groups by a date or a feature and hands it on as an aggregate alias, a map alias, a count
    per renewal as a key, one renewal chosen by SKIP, and a parameter beside a concatenation, a map
    alias or a simple CASE. Executed on tiny, each serves, row by row, the outcome of renewals that
    similar_top_k_visible masks for some source (the back door a second template would open)."""
    from lakehouse_graph import oracle, store

    bdir, _ = tiny_build
    t = oracle.load_tables(bdir)
    ren = t["Renewal"].set_index("renewal_id").assign(hits=_hits_by_renewal(t))
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    try:
        masked = _masked(conn, ren)
        targets = _targets(ren, masked)
        assert len(targets) == 3, targets
        found = {}
        for name in POPULATION_SINGLES:
            text = BAD_TEMPLATES[name][0]
            pins = bool(POP_PINNED.search(text) or re.search(r"\$p\b", text))
            got = {}
            for x in targets if pins else [None]:
                rows = _run(conn, text, _target_params(ren, x) if x else {})
                got |= _revealed(rows, ren, pinned=x)
            assert all(ren.at[rid, "outcome"] == outcome for rid, outcome in got.items()), (name, got)
            found[name] = set(got) & masked
    finally:
        conn.close()
        db.close()
    assert all(found.values()), {k: len(v) for k, v in found.items()}
    assert len(found["tool_population_with_group_by_as_of"]) >= 20         # the verifier counted 32 on tiny


# ---- rule 7, aggregates: decoding what an encoded aggregate serves (cells of MIN_CELL or more only)
MIN_CELL = 5          # the tool layer's small-cell rule (metrics.MIN_CELL): smaller rows would be suppressed
CELL_COLUMNS = {"pl": "plan_tier", "city": "city", "rt": "route", "o": "outcome"}


def _with_city(t) -> pd.DataFrame:
    """The Renewal frame by renewal_id, with its subscription's city (the population templates' cell key)."""
    ren = t["Renewal"].set_index("renewal_id")
    return ren.assign(city=ren["subscription_id"].map(t["Subscription"].set_index("subscription_id")["city"]))


def _cell(ren, row) -> pd.DataFrame:
    """The renewals of a population row's cell: the ones its cohort-key columns describe."""
    cell = ren
    for col, field in CELL_COLUMNS.items():
        if col in row:
            cell = cell[cell[field] == row[col]]
    return cell


def _rows_of_cells(rows) -> list:
    return [row for row in rows if int(row["k"]) >= MIN_CELL]


def _decode_extremes(rows, ren, cols=("hi", "lo", "enc")) -> dict:
    """max / min / percentile of agent_requests_28d * 10 + churned: the one renewal of the cell with that
    agent_requests_28d, and its churned."""
    got = {}
    for row in _rows_of_cells(rows):
        cell = _cell(ren, row)
        for col in cols:
            if row.get(col) is not None:
                req, churned = divmod(int(row[col]), 10)
                fit = cell[cell["agent_requests_28d"] == req]
                if len(fit) == 1:
                    got[fit.index[0]] = ("churned", churned)
    return got


def _decode_bitmask(rows, ren, col="mask") -> dict:
    """sum(churned * 2^(agent_requests_28d % 50)): every renewal of a cell whose bit positions are distinct."""
    got = {}
    for row in _rows_of_cells(rows):
        bits = _cell(ren, row)["agent_requests_28d"] % 50
        if row.get(col) is not None and bits.is_unique:
            got |= {rid: ("churned", (int(row[col]) >> int(b)) & 1) for rid, b in bits.items()}
    return got


def _decode_string(rows, ren, cols=("hi", "lo", "enc")) -> dict:
    """max / min of cast(as_of) + outcome: the one renewal of the cell with that as_of, and its outcome."""
    got = {}
    for row in _rows_of_cells(rows):
        cell = _cell(ren, row)
        for col in cols:
            if row.get(col) is not None:
                fit = cell[cell["as_of"] == pd.Timestamp(row[col][:10])]
                if len(fit) == 1:
                    got[fit.index[0]] = ("outcome", row[col][10:])
    return got


def _decode_by_outcome(rows, ren, cols=(("first_day", "as_of"), ("last_day", "as_of"), ("lo", "agent_requests_28d"),
                                        ("hi", "agent_requests_28d"))) -> dict:
    """Extremes grouped by outcome: the one renewal of the (plan, outcome) cell holding the extreme value."""
    got = {}
    for row in _rows_of_cells(rows):
        cell = _cell(ren, row)
        for col, field in cols:
            if "o" in row and row.get(col) is not None:
                value = pd.Timestamp(row[col]) if field == "as_of" else row[col]
                fit = cell[cell[field] == value]
                if len(fit) == 1:
                    got[fit.index[0]] = ("outcome", row["o"])
    return got


def _decode_packed(rows, ren, col="packed") -> dict:
    """lapses * 1000 + size of the overage-off-and-exposed sub-cell: a sub-cell of one is that renewal."""
    got = {}
    for row in _rows_of_cells(rows):
        lapses, size = divmod(int(row[col]), 1000)
        sub = _cell(ren, row)
        sub = sub[(sub["overage_toggled_off"] == 1) & (sub["incident_exposed_28d"] == 1)]
        if size == 1 and len(sub) == 1:
            got[sub.index[0]] = ("churned", lapses)
    return got


def _sub_cell(cell) -> pd.DataFrame:
    """The overage-off-and-exposed renewals of a cell (BOTH)."""
    return cell[(cell["overage_toggled_off"] == 1) & (cell["incident_exposed_28d"] == 1)]


def _decode_flags(rows, ren, one="one", lapsed="lapsed") -> dict:
    """sum(<sub-cell>) = 1 and sum(<sub-cell lapsed>) = 1: a sub-cell of one is that renewal, and its label."""
    got = {}
    for row in _rows_of_cells(rows):
        sub = _sub_cell(_cell(ren, row))
        if row.get(one) and row.get(lapsed) is not None and len(sub) == 1:
            got[sub.index[0]] = ("churned", int(bool(row[lapsed])))
    return got


def _decode_lapses_and_size(rows, ren, col="packed") -> dict:
    """lapses * 1000000 + size of a model cell in one number. The column is not a count, so the tool layer's
    n<5 rule, applied column by column, serves it whenever it is 5 or more (a cell with a lapse): a cell
    of one is that renewal, and its label."""
    got = {}
    model = ren[ren["route"] == "model"]
    flags = {"off": "overage_toggled_off", "inc": "incident_exposed_28d"}
    for row in rows:
        if row.get(col) is None or int(row[col]) < MIN_CELL:
            continue
        lapses, size = divmod(int(row[col]), 1000000)
        cell = _cell(model, row)
        for c, field in flags.items():
            if c in row:
                cell = cell[cell[field] == row[c]]
        if size == 1 and len(cell) == 1:
            got[cell.index[0]] = ("churned", lapses)
    return got


def _decode_repeated(rows, ren, plans: int) -> dict:
    """count(*) over a cell whose lapsed sub-cell renewals each match every Plan (the rest only 'pro'): the
    cell's size (a vetted count) plus (plans - 1) times their number; a sub-cell of one is that renewal."""
    got = {}
    for row in _rows_of_cells(rows):
        cell = _cell(ren, row)
        lapses, rest = divmod(int(row["k"]) - len(cell), plans - 1)
        sub = _sub_cell(cell)
        if rest == 0 and len(sub) == 1:
            got[sub.index[0]] = ("churned", lapses)
    return got


ENCODED_DECODERS = {"tool_population_encoded_extremes": _decode_extremes,
                    "tool_population_bitmask_sum": _decode_bitmask,
                    "tool_population_string_extremes": _decode_string,
                    "tool_population_extremes_by_outcome": _decode_by_outcome,
                    "tool_population_packed_columns": _decode_packed,
                    "tool_population_packed_in_case": _decode_packed,
                    "tool_population_packed_simple_case": _decode_packed,
                    "tool_population_flag_columns": _decode_flags,
                    "tool_population_cell_lapses_and_size": _decode_lapses_and_size,
                    "tool_population_repeated_rows": lambda rows, ren: _decode_repeated(rows, ren, 3)}


def _correct(got: dict, ren) -> set:
    """The decoded renewals whose decoded label is their true one."""
    return {rid for rid, (field, value) in got.items() if ren.at[rid, field] == value}


def test_aggregates_that_encode_a_label_really_decode_masked_renewals(tiny_build):
    """Rule 7's aggregate restriction is not style (verifier p1-hardening-2 round 2, the open major): each
    of these reads cohort keys only outside its aggregates, so the grouping rule alone accepted it, yet
    an extreme of a label beside a near-unique value, a sum weighted by a near-unique power of two, an
    extreme of a date-plus-outcome string, extremes grouped by outcome, and two cell statistics packed
    into one number give back single renewals' labels from rows of 5 or more renewals (rows the tool
    layer's n<5 rule would serve), among them renewals similar_top_k_visible masks for some source.
    Round 4 (p3-hardening verifier round 1): the same packing with the multiplier inside a CASE (searched
    or simple), the flag columns sum(<sub-cell>) = 1, a small cell's lapses and size in one number that is
    not a small count (served column by column), and count(*) over rows that repeat the lapsed renewals
    of a sub-cell (a MATCH (p:Plan) beside the renewal) decode the same way."""
    from lakehouse_graph import oracle, store

    bdir, _ = tiny_build
    tables = oracle.load_tables(bdir)
    assert len(tables["Plan"]) == 3                                             # _decode_repeated's multiplier
    ren = _with_city(tables)
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    found = {}
    try:
        masked = _masked(conn, ren)
        for name in POPULATION_ENCODED:
            assert queries.lint({name: BAD_TEMPLATES[name][0]}, contract_only=()), name
            got = ENCODED_DECODERS[name](_run(conn, BAD_TEMPLATES[name][0], {}), ren)
            assert got and _correct(got, ren) == set(got), (name, got)          # every decode is the truth
            found[name] = set(got) & masked
    finally:
        conn.close()
        db.close()
    assert all(found.values()), {k: len(v) for k, v in found.items()}
    assert len(found["tool_population_encoded_extremes"]) >= 5                 # the verifier decoded 12, 9 masked
    assert len(found["tool_population_packed_in_case"]) >= 2                   # the verifier decoded 3, 2 masked


def test_rule_7_vets_what_an_aggregate_computes(tiny_build):
    """Rule 7 is an allowlist of cell statistics: count / sum over *, a pattern variable, a cohort flag or
    key, a literal (0 or 1 in a sum), CASE over cohort keys (values 0 or 1 in a sum), or an alias of one
    (also of a cell of an earlier grouping), one statistic per column, each renewal counted once. The
    vetted forms are accepted and run on tiny; what the grammar has not vetted is refused, whether or not
    it leaks on its own (avg, a rate; a flag column; arithmetic or a comparison on an aggregate's alias in
    any clause, ORDER BY and a WHERE after a grouping included; rows a pattern repeats)."""
    from lakehouse_graph import store

    bdir, _ = tiny_build
    for name, (body, want) in REFUSED_CELL_STATISTICS.items():
        problems = queries.lint({name: POP + body + " LIMIT 10"}, contract_only=())
        assert any(want in p for p in problems), (name, problems)
    vetted = {name: POP + body + " LIMIT 10" for name, body in VETTED_CELL_STATISTICS.items()}
    assert queries.lint(vetted, contract_only=()) == []
    assert queries.population_templates(vetted, contract_only=()) == sorted(vetted)
    assert queries.lint({k: POP + v + " LIMIT 10" for k, v in VETTED_NOT_RUN.items()}, contract_only=()) == []
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    try:
        for name, text in vetted.items():
            assert _run(conn, text, {"plan": "pro"}), name
    finally:
        conn.close()
        db.close()
    # the rule's vocabulary: the engine's aggregates (CALL show_functions()), the vetted ones a subset
    assert set(queries.CELL_STATISTICS) < set(queries.ENGINE_AGGREGATES) and \
        set(queries.COHORT_FLAGS) < set(queries.COHORT_KEYS)
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    try:
        engine = {name.lower() for name, kind in store.rows(conn.execute("CALL show_functions() RETURN name, type"))
                  if kind == "AGGREGATE FUNCTION"}
    finally:
        conn.close()
        db.close()
    assert engine == set(queries.ENGINE_AGGREGATES), engine


CATALOG_NAMES = ("TEMPLATES", "TOOL_TEMPLATES", "CONTRACT_ONLY", "VETTED_TOOL_TEMPLATES")


def _template_uses(path) -> list[tuple]:
    """(how, template name or None, node, parent map) of every way one source file reaches a template:
    ``fetch(...)`` / ``x.fetch(...)`` (queries.fetch(conn, name, ...) or ctx.fetch(name, ...)), ``render(...)``
    / ``x.render(...)`` and ``TEMPLATES[...]``, with the name when it is a string literal (None: computed),
    and ``catalog`` for any other read of a catalog dict (CATALOG_NAMES: iterating it reaches every template)."""
    tree = ast.parse(path.read_text(), filename=str(path))
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else func.id if isinstance(func, ast.Name) else None
            if name not in ("fetch", "render"):
                continue
            module_call = isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and \
                func.value.id in ("queries", "q")
            pos = 1 if name == "fetch" and (module_call or isinstance(func, ast.Name)) else 0
            if name == "render" and not node.args:
                continue                                    # charts' svg.render(), envelope's render()
            arg = node.args[pos] if len(node.args) > pos else None
            literal = arg.value if isinstance(arg, ast.Constant) and isinstance(arg.value, str) else None
            out.append((name, literal, node, parents))
        elif isinstance(node, ast.Subscript) and ast.unparse(node.value).split(".")[-1] == "TEMPLATES":
            key = node.slice
            out.append(("TEMPLATES", key.value if isinstance(key, ast.Constant) and isinstance(key.value, str)
                        else None, node, parents))
        elif isinstance(node, (ast.Name, ast.Attribute)) and \
                (node.id if isinstance(node, ast.Name) else node.attr) in CATALOG_NAMES and \
                not isinstance(parents.get(node), ast.Subscript):
            out.append(("catalog", None, node, parents))
    return out


def _enclosing_function(node, parents) -> str | None:
    while node is not None and not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        node = parents.get(node)
    return node.name if node is not None else None


def test_population_templates_are_served_only_through_small_cell_suppression():
    """The test hook for rule 7's tool layer: every count a population template returns may hold fewer
    than 5 renewals (a hub hit by one subscription, a sub-cell), so no module under src/lakehouse_graph,
    at any depth (lineage/ included), serves such rows as they come. tools.py hands them straight to
    incident_table / pricing_table (graph_exposure: n<5 suppression, complementary over every published
    margin, then metrics.recoverable), and viz.py only sums them into the always-shown totals of a global
    event. A population template named in a fetch(), a render() or TEMPLATES[...] anywhere else fails
    here until its suppression is reviewed, and so does a template reached by a computed name or by
    reading the catalog itself (it could be any template), except in the two forwarding places that run
    their caller's literal name: ToolContext.fetch (context.py; every ctx.fetch call is checked where it
    is written) and the lineage catalog (lineage/: its own templates on the lineage database, none of
    them a population template of this catalog). scripts/ are not agent surfaces: the contract prints
    these totals in its report, and the check scripts compare them."""
    from lakehouse_graph.lineage import queries as lineage_queries

    population = set(queries.population_templates())
    assert population == {"exposure_incident_by_plan", "exposure_pricing_change", "first_renewal_after_by_plan",
                          "motif_limit_hit_then_overage_off", "routes"}
    assert not population & set(lineage_queries.TEMPLATES)
    root = REPO / "src/lakehouse_graph"
    served, forwarded = set(), set()
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root).as_posix()
        if rel == "queries.py":
            continue                                        # the catalog itself
        lineage = rel.startswith("lineage/")
        for how, name, node, parents in _template_uses(path):
            if name is None:
                where = _enclosing_function(node, parents)
                if (rel, how, where) == ("context.py", "fetch", "fetch") or lineage:
                    forwarded.add((rel, how))
                    continue
                raise AssertionError(f"{rel}:{node.lineno} reaches a template by a computed name or reads the "
                                     f"catalog ({how}): name the template literally, so this hook can see whether "
                                     f"it is a population template")
            if name not in population:
                continue
            served.add((rel, name))
            outer = parents.get(node)
            if rel == "tools.py" and how == "fetch":
                assert isinstance(outer, ast.Call) and isinstance(outer.func, ast.Name) and \
                    outer.func.id in ("incident_table", "pricing_table") and outer.args and outer.args[0] is node, \
                    f"tools.py serves {name} without incident_table / pricing_table"
            elif rel == "viz.py" and how == "fetch":
                gen = parents.get(outer)
                total = parents.get(gen)
                assert isinstance(outer, ast.comprehension) and isinstance(gen, ast.GeneratorExp) and \
                    isinstance(total, ast.Call) and getattr(total.func, "id", None) == "sum", \
                    f"viz.py uses {name} other than as a summed total"
                elt = gen.elt
                assert isinstance(elt, ast.Call) and getattr(elt.func, "id", None) == "int" and \
                    isinstance(elt.args[0], ast.Subscript) and elt.args[0].slice.value in ("exposed", "renewals"), \
                    f"viz.py sums a column of {name} that is not the event's total"
            else:
                raise AssertionError(f"{rel} reaches the population template {name} ({how}): route it through the "
                                     f"tool layer's small-cell suppression (tools.incident_table / pricing_table, "
                                     f"metrics.suppress) and extend this test")
    assert served == {("tools.py", "exposure_incident_by_plan"), ("tools.py", "exposure_pricing_change"),
                      ("viz.py", "exposure_incident_by_plan"), ("viz.py", "exposure_pricing_change")}, served
    assert ("context.py", "fetch") in forwarded and ("lineage/queries.py", "TEMPLATES") in forwarded, forwarded


def test_the_serving_hook_sees_every_way_to_reach_a_template(tmp_path):
    """The hook's reader (verifier p3-hardening round 1): it scans every depth, and it sees a population
    template reached through render(), TEMPLATES[...], a computed fetch name and the catalog dict, not
    only a literal fetch()."""
    probe = tmp_path / "probe.py"
    probe.write_text("from lakehouse_graph import queries\n"
                     "def a(conn):\n    return queries.fetch(conn, 'routes')\n"
                     "def b():\n    return queries.render('routes')\n"
                     "def c():\n    return queries.TEMPLATES['first_renewal_after_by_plan']\n"
                     "def d(conn, name):\n    return queries.fetch(conn, name)\n"
                     "def e(ctx):\n    return [ctx.fetch(n) for n in queries.TOOL_TEMPLATES]\n"
                     "def f(conn):\n    return conn.execute(queries.render('routes', ))\n")
    uses = {(how, name) for how, name, _node, _parents in _template_uses(probe)}
    assert uses >= {("fetch", "routes"), ("render", "routes"), ("TEMPLATES", "first_renewal_after_by_plan"),
                    ("fetch", None), ("catalog", None)}, uses
    nested = sorted((REPO / "src/lakehouse_graph").rglob("*.py"))
    assert any(p.parent.name == "lineage" for p in nested)          # the scan reaches sub-packages


def test_pinned_second_renewals_really_leak(tiny_build):
    """Rule 3's one-identity shapes are not style (verifier: A, A2, B, K_skip, K_feature, and the hub
    variants HUB_plan_*). With $renewal_id = the current renewal (its as_of is the latest, so its
    visibility rule shows nearly every outcome), each template pins a historical renewal h by $k /
    $limit (its id, its position or its feature value), by a literal or by a WITH ... SKIP, directly
    or through the source's Plan hub, or reads renewals that hang off another one, and serves
    neighbour outcomes observed after h's as_of: the ones similar_top_k_visible masks for h. The
    templates that name the renewal their rows describe (``via``) are measured against it: what they
    serve on its neighbourhood (SIMILAR_TO, a path, its nearest plan-mates by a feature, a window
    around its value) was observed after its as_of."""
    from lakehouse_graph import oracle, store

    bdir, _ = tiny_build
    ren = oracle.load_tables(bdir)["Renewal"].set_index("renewal_id")
    hist = "sub_00000:2026-07-27"            # route model; the current renewal's plan; the only agent_requests_28d 211
    assert (ren.at[hist, "route"], ren.at[hist, "plan_tier"]) == ("model", ren.at[SECOND, "plan_tier"])
    assert ren.index[ren["agent_requests_28d"] == 211].tolist() == [hist]
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    served = {}
    try:
        hidden = {r["renewal_id"] for r in queries.fetch(conn, "similar_top_k_visible", {"renewal_id": hist, "k": 10})
                  if not r["outcome_visible"]}
        for name in PINNED_SECOND:
            text = BAD_TEMPLATES[name][0]
            rows = _run(conn, text, {"renewal_id": SECOND, **_pins(ren, hist, text)})
            late = set()
            for row in rows:                    # the renewal a row describes: the pinned one, or the one it hangs off
                about = row.get("via") or row.get("source") or hist
                late |= {nid for nid, _ in _served(row) if ren.at[nid, "outcome_observed_on"] > ren.at[about, "as_of"]}
            served[name] = late if "via" in (rows[0] if rows else {}) else late & hidden
    finally:
        conn.close()
        db.close()
    assert hidden and all(served.values()), {k: sorted(v) for k, v in served.items()}
    assert served["tool_hub_plan_literal_pin"] == hidden            # all the tool masks for h (verifier: 4, 5, 10)


def test_a_renewal_pinned_through_the_pricing_hub_really_leaks(tiny_build):
    """The verifier's HUB_pricing_literal_pin: the source's FIRST_RENEWAL_AFTER pricing change is a hub
    too. On tiny, a historical renewal h that was a first renewal after cap-cut-2026-08 (pinned by its
    agent_requests_28d, unique) and a later renewal r of the same plan after the same cut: with r as
    $renewal_id the template serves h's neighbours whose outcomes were observed after h's as_of (and
    by r's), the ones similar_top_k_visible masks for h."""
    from lakehouse_graph import oracle, store

    bdir, _ = tiny_build
    t = oracle.load_tables(bdir)
    ren = t["Renewal"].set_index("renewal_id")
    fra = t["FIRST_RENEWAL_AFTER"]
    once = ren.groupby("agent_requests_28d")["plan_tier"].transform("size") == 1
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    found = {}
    try:
        for change, grp in sorted(fra.groupby("dst")):
            for h in sorted(set(grp["src"]) & set(ren.index[once])):
                later = [x for x in grp["src"] if ren.at[x, "as_of"] > ren.at[h, "as_of"]
                         and ren.at[x, "plan_tier"] == ren.at[h, "plan_tier"]]
                if not later:
                    continue
                r = max(later, key=lambda x: (ren.at[x, "as_of"], x))
                hidden = {row["renewal_id"] for row in queries.fetch(conn, "similar_top_k_visible",
                                                                     {"renewal_id": h, "k": 10})
                          if not row["outcome_visible"]}
                rows = _run(conn, BAD_TEMPLATES["tool_hub_pricing_literal_pin"][0],
                            {"renewal_id": r, "pin_requests": int(ren.at[h, "agent_requests_28d"])})
                late = {nid for row in rows for nid, _ in _served(row)} & hidden
                if late:
                    found[(change, h, r)] = late
    finally:
        conn.close()
        db.close()
    assert found, "no historical renewal served its masked neighbours through the pricing hub"
    assert all(ren.at[nid, "outcome_observed_on"] > ren.at[h, "as_of"] for (_, h, _), late in found.items()
               for nid in late)


RULE, EV_RULE, CUT_RULE = "n.outcome_observed_on <= r.as_of", "e.event_date <= r.as_of", "c.event_date <= r.as_of"
FUZZ_PREDICATES = {   # the rule itself, and ways of writing something that only looks like it
    "n": [RULE, RULE, RULE, f"({RULE})", f"(({RULE}) AND true)", RULE + " + INTERVAL('999 DAYS')", "NOT " + RULE,
          "n.outcome_observed_on >= r.as_of", "r.outcome_observed_on <= n.as_of", "true", "e.rank <= 5",
          "n.outcome_observed_on <= r.renewal_date", f"({RULE} OR true)", f"({RULE}) IS NOT NULL",
          f"CASE WHEN true AND {RULE} THEN true ELSE true END", f"n.plan_tier <> ' AND {RULE} AND '",
          f"NOT (NOT ({RULE}))", "n.outcome = 'renewed'", queries.visible_or_current("n", "r"),
          f"({queries.visible_or_current('n', 'r')})", f"{RULE} OR $today"],
    "e": [EV_RULE, EV_RULE, EV_RULE, f"({EV_RULE})", EV_RULE + " + INTERVAL('999 DAYS')", "NOT " + EV_RULE, "true",
          "e.event_date <= o.as_of", f"({EV_RULE} OR true)", f"CASE WHEN true AND {EV_RULE} THEN true ELSE true END",
          "e.event_date > r.as_of - INTERVAL('90 DAYS')", "e.event_date <= r.renewal_date", f"({EV_RULE}) IS NOT NULL",
          "x.event_date <= r.as_of", f"x.ticket_id <> ' AND {EV_RULE} AND '", "e.event_date >= r.as_of"],
    "c": [CUT_RULE, CUT_RULE, CUT_RULE, f"({CUT_RULE})", f"(({CUT_RULE}) AND true)",
          CUT_RULE + " + INTERVAL('90 DAYS')", "NOT " + CUT_RULE, "true", "c.event_date <= r.renewal_date",
          f"({CUT_RULE} OR true)", f"CASE WHEN true AND {CUT_RULE} THEN true ELSE true END",
          f"({CUT_RULE}) IS NOT NULL",
          "c.event_date >= r.as_of", "p.effective_date <= r.as_of", "c.event_date <= n.as_of",
          f"p.change_id <> ' AND {CUT_RULE} AND '"],
}
FUZZ_KINDS = (("n", 0.21), ("e", 0.15), ("c", 0.12), ("o", 0.18), ("p", 0.22), ("h", 0.12))


def _hub_predicates(var: str, date: str) -> list[str]:
    """For the "h" kind: the calendar cut on a hub node (``<var>.<date> <= r.as_of``) and lookalikes."""
    rule = f"{var}.{date} <= r.as_of"
    return [rule, rule, rule, f"({rule})", f"(({rule}) AND true)", rule + " + INTERVAL('90 DAYS')", "NOT " + rule,
            "true", f"{var}.{date} <= r.renewal_date", f"({rule} OR true)",
            f"CASE WHEN true AND {rule} THEN true ELSE true END", f"{var}.{date} > r.as_of", f"{var}.{date} >= r.as_of",
            f"{var}.{date} <= n.as_of", f"({rule}) IS NOT NULL", f"{var}.change_id <> ' AND {rule} AND '"]
# what a second identity reads as $renewal_id in the "o" kind: the current renewal, whose as_of is the latest
SECOND = "sub_maya:2026-10-07"
# "o" kind, second renewal pinned without a second identity parameter: by $k (a string compared with its
# id, or an integer compared with its agent_requests_28d), by a literal ('__rid__' = its id, 987654321 =
# its agent_requests_28d), disconnected from the source or hanging off it through the Plan hub
PIN_SECOND = ("lower(a.renewal_id) = lower($k)", "a.renewal_id + '' = $k", "a.renewal_id = $k",
              "a.agent_requests_28d = $k", "lower(a.renewal_id) = '__rid__'", "a.renewal_id = '__rid__'",
              "a.agent_requests_28d = 987654321", "a.renewal_id = $limit")
# "o" kind, fix round 2: the second renewal reached from the source through the Plan hub, its neighbours n
# joined through the same hub as well as through it, or related to it by value instead of an edge
HUB_A = "MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(p:Plan)<-[:ON_PLAN]-(a:Renewal)"
HUB_JOIN = "(n:Renewal)-[:ON_PLAN]->(p), (a)-[e:SIMILAR_TO]->(n)"
VALUE_RELATIONS = ("abs(n.agent_requests_28d - a.agent_requests_28d) <= 40",
                   "n.agent_requests_28d >= a.agent_requests_28d", "n.as_of <= a.as_of", "n.route = a.route",
                   "n.engagement_trend <= a.engagement_trend + 0.5")
VALUE_SORT = "\nWITH r, a, n ORDER BY abs(n.agent_requests_28d - a.agent_requests_28d), n.agent_requests_28d LIMIT 10"
# "p" kind: grouping keys (column -> expression), a non-cohort one singles renewals out
FINE_KEYS = {"day": "n.as_of", "req": "n.agent_requests_28d", "amt": "n.overage_usd_28d"}
COHORT_EXPRS = {"pl": "n.plan_tier", "rt": "n.route"}
POP_PINS = ("", "", "", "WHERE n.plan_tier = $plan", "WHERE $plan = n.plan_tier", "WHERE n.route = 'model'",
            "WHERE n.as_of = $day AND n.plan_tier = $plan", "WHERE cast(n.as_of, 'STRING') + n.plan_tier = $dayplan",
            "WHERE n.agent_requests_28d = $req", "WHERE $req = n.agent_requests_28d + 0",
            "WHERE n.agent_requests_28d IN [$req]", "WHERE n.agent_requests_28d - $req = 0",
            "WHERE CASE n.agent_requests_28d WHEN $req THEN true ELSE false END",
            "WITH n, {x: n.agent_requests_28d} AS m0\nWHERE m0.x = $req", "WHERE n.agent_requests_28d = 987654321")
POP_PINNED = re.compile(r"\$(day|dayplan|req|pos)\b|98765432[12]")   # a pin on a date / feature / position
# "p" kind, rule 7 aggregates: one aggregate column (enc) beside cohort keys, whole cells (no pin). The
# leaky ones encode a label beside a near-unique value, pack two cells, or take an extreme grouped by
# outcome (value: the decoder that reads it back); the vetted cell statistics carry nothing to decode.
POP_AGGREGATES = {
    f"max({ENC})": "extremes", f"min({ENC})": "extremes", f"percentiledisc({ENC}, 1.0)": "extremes",
    "sum(n.churned * pow(2.0, n.agent_requests_28d % 50))": "bitmask",
    "max(cast(n.as_of, 'STRING') + n.outcome)": "string", "min(cast(n.as_of, 'STRING') + n.outcome)": "string",
    "min(n.as_of)": "by_outcome_day", "max(n.agent_requests_28d)": "by_outcome_requests",
    f"sum(CASE WHEN {BOTH} THEN n.churned ELSE 0 END) * 1000 + sum(CASE WHEN {BOTH} THEN 1 ELSE 0 END)": "packed",
    f"sum(CASE WHEN {BOTH} AND n.churned = 1 THEN 1001 WHEN {BOTH} THEN 1 ELSE 0 END)": "packed",
    f"sum(CASE WHEN {BOTH} THEN CASE n.churned WHEN 1 THEN 1001 ELSE 1 END ELSE 0 END)": "packed",
    "sum(CASE WHEN n.churned = 1 THEN 1000001 ELSE 1 END)": "lapses_and_size",
    f"sum(CASE WHEN {BOTH} THEN 1 ELSE 0 END) = 1 AS one, sum(CASE WHEN {BOTH} THEN n.churned ELSE 0 END) = 1": "flags",
    "sum(n.churned)": None, "avg(n.churned)": None, "count(n.outcome)": None,
    "count(CASE WHEN n.churned = 1 THEN 1001 END)": None,
    "sum(CASE WHEN n.route = 'model' THEN n.churned ELSE 0 END)": None,
    "count(CASE WHEN n.outcome = 'renewed' THEN 1 END)": None,
    "sum(CASE n.plan_tier WHEN 'pro' THEN n.churned ELSE 0 END)": None,
}
AGGREGATE_MARK = " AS enc, count(*) AS k"


def _decode_aggregate(rows, ren, text) -> dict:
    """What a "p" template's aggregate column gives back, read by the decoder of its expression."""
    how = next((POP_AGGREGATES[e] for e in POP_AGGREGATES if f"{e}{AGGREGATE_MARK}" in text), None)
    if how is None:
        return {}
    return {"extremes": lambda: _decode_extremes(rows, ren, cols=("enc",)),
            "bitmask": lambda: _decode_bitmask(rows, ren, col="enc"),
            "string": lambda: _decode_string(rows, ren, cols=("enc",)),
            "by_outcome_day": lambda: _decode_by_outcome(rows, ren, cols=(("enc", "as_of"),)),
            "by_outcome_requests": lambda: _decode_by_outcome(rows, ren, cols=(("enc", "agent_requests_28d"),)),
            "packed": lambda: _decode_packed(rows, ren, col="enc"),
            "lapses_and_size": lambda: _decode_lapses_and_size(rows, ren, col="enc"),
            "flags": lambda: _decode_flags(rows, ren, one="one", lapsed="enc")}[how]()


# "p" kind, rule 7 rows: the lapsed renewals of the overage-off-and-exposed sub-cell match every Plan, the
# rest only 'pro', so count(*) adds (plans - 1) times their number to each (city, plan / route) cell; WITH
# DISTINCT n, s counts each renewal once again (accepted, nothing to decode)
REPEAT_DISTINCT = "WITH DISTINCT n, s\n"
REPEAT_MARK = f"MATCH (p:Plan)\nWHERE p.plan_tier = 'pro' OR ({BOTH} AND n.churned = 1)\n"


def _fuzz_population(rng) -> str:
    """One random population template ("p"): renewals grouped by cohort keys (plan, route, outcome) or
    by a date / feature, in one of the shapes rule 7 reads (RETURN keys, a WITH grouping, aggregate
    aliases over a group or over the renewal itself, a map alias, a CASE key, one renewal chosen by
    SKIP), optionally pinned by a parameter or a literal on a cohort key or on a date / feature; or
    grouped by cohort keys with one aggregate column that may encode a label (POP_AGGREGATES), or
    counted over rows that repeat some renewals (REPEAT_MARK), collapsed by WITH DISTINCT n or not."""
    pin = rng.choice(POP_PINS)
    keys = rng.sample(sorted({**FINE_KEYS, **COHORT_EXPRS}.items()), rng.choice([0, 1, 1, 2]))
    lead = "".join(f"{c}, " for c, _ in keys)
    agg = rng.choice(["min", "max", "sum"])
    q = "MATCH (n:Renewal)\n" + (pin + "\n" if pin else "")
    shape = rng.choice(["return", "return", "with_group", "with_alias", "with_bare_alias", "map_alias", "case", "skip",
                        "aggregate", "aggregate", "aggregate", "aggregate", "repeat", "repeat", "repeat_distinct"])
    if shape in ("repeat", "repeat_distinct"):    # rule 7: every aggregate counts each renewal once
        cohort = [("city", "s.city"), *rng.sample(sorted(COHORT_EXPRS.items()), rng.choice([1, 1, 2]))]
        return ("MATCH (n:Renewal)\nMATCH (s:Subscription)-[:HAS_RENEWAL]->(n)\n" + REPEAT_MARK +
                (REPEAT_DISTINCT if shape == "repeat_distinct" else "") + "RETURN " +
                "".join(f"{e} AS {c}, " for c, e in cohort) + "count(*) AS k\nORDER BY " +
                ", ".join(c for c, _ in cohort) + " LIMIT 1000")
    if shape == "aggregate":                      # rule 7: what an aggregate computes, over whole cells
        cohort = rng.sample(sorted(COHORT_EXPRS.items()), rng.choice([1, 1, 2]))
        by_outcome = rng.random() < 0.6
        keys_out = "".join(f"{c}, " for c, _ in cohort) + ("o, " if by_outcome else "")
        return ("MATCH (n:Renewal)\nRETURN " + "".join(f"{e} AS {c}, " for c, e in cohort) +
                ("n.outcome AS o, " if by_outcome else "") + rng.choice(sorted(POP_AGGREGATES)) + AGGREGATE_MARK +
                "\nORDER BY " + keys_out + "k LIMIT 1000")
    if shape == "return":
        q += "RETURN " + "".join(f"{e} AS {c}, " for c, e in keys) + "n.outcome AS o, count(*) AS k"
    elif shape == "with_group":
        q += "WITH " + "".join(f"{e} AS {c}, " for c, e in keys) + "n.outcome AS o, count(*) AS c\nRETURN " + lead + \
             "o, sum(c) AS k"
    elif shape == "with_alias":                   # grouped by the keys, which travel on as aggregate aliases
        q += "WITH " + "".join(f"{e} AS g{i}, " for i, (_, e) in enumerate(keys)) + "n.outcome AS o, " + \
             "".join(f"{agg}({e}) AS {c}, " for c, e in keys) + "count(*) AS c\nRETURN " + lead + "o, sum(c) AS k"
    elif shape == "with_bare_alias":              # one group per renewal, its values as aggregate aliases
        q += "WITH n, n.outcome AS o" + "".join(f", {agg}({e}) AS {c}" for c, e in keys) + "\nRETURN " + lead + \
             "o, count(*) AS k"
    elif shape == "map_alias":
        q += "WITH n, {" + ", ".join(f"{c}: {e}" for c, e in keys or [("pl", "n.plan_tier")]) + "} AS m\nRETURN " + \
             "".join(f"m.{c} AS {c}, " for c, _ in keys) + "n.outcome AS o, count(*) AS k"
    elif shape == "case":
        q += "RETURN " + "".join(f"CASE WHEN n.churned >= 0 THEN {e} END AS {c}, " for c, e in keys) + \
             "n.outcome AS o, count(*) AS k"
    else:                                         # one renewal chosen before the RETURN
        q += "WITH n ORDER BY n.agent_requests_28d SKIP " + rng.choice(["$pos", "987654322"]) + " LIMIT 1\n" \
             "RETURN n.outcome AS o, count(*) AS k"
        return q + "\nORDER BY o LIMIT 1000"
    return q + "\nORDER BY " + lead + "o LIMIT 1000"


def _fuzz_hub(rng) -> str:
    """One random calendar template ("h") for a renewal named by $renewal_id: a PricingChange or an
    Incident matched on its own, beside the source, in an OPTIONAL MATCH, through a cut or an
    exposure that is or is not bounded, next to a neighbour, or kept by a WITH; returning its date,
    when it ended, or an aggregate of them. A leak serves a calendar entry dated after the source's
    as_of (FIRST_RENEWAL_AFTER, the declared exception, is left out)."""
    cur = "MATCH (r:Renewal {renewal_id: $renewal_id})"
    ev = "MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal {renewal_id: $renewal_id})"
    if rng.random() < 0.6:
        var, node = "p", "(p:PricingChange)"
        preds = _hub_predicates("p", "effective_date") + [CUT_RULE] * 3
        shapes = [cur + f"\nMATCH {node}", cur + f", {node}", cur + f"\nOPTIONAL MATCH {node}",
                  cur + f"-[:ON_PLAN]->(pl:Plan), {node}\nOPTIONAL MATCH (p)-[c:CUT_CAP]->(pl)",
                  cur + f"-[:ON_PLAN]->(pl:Plan)<-[c:CUT_CAP]-{node}",
                  cur + f"-[:SIMILAR_TO]->(n:Renewal)\nMATCH {node}"]
        rets = ["p.change_id AS hub_id, p.effective_date AS event_date"] * 4 + [
            "p.change_id AS hub_id, p.cap_multiplier AS m, p.effective_date AS event_date",
            "max(p.effective_date) AS event_date, count(*) AS hub_id"]
    else:
        var, node = "i", "(i:Incident)"
        preds = _hub_predicates("i", "starts_on") + _hub_predicates("i", "ends_on") + [EV_RULE] * 3
        shapes = [cur + f"\nMATCH {node}", cur + f", {node}", cur + f"\nOPTIONAL MATCH {node}",
                  ev + f"\nMATCH (s)-[e:EXPOSED_TO]->{node}", ev + f"\nOPTIONAL MATCH (s)-[e:EXPOSED_TO]->{node}",
                  cur + f"-[:SIMILAR_TO]->(n:Renewal)\nMATCH {node}"]
        rets = ["i.incident_id AS hub_id, i.starts_on AS event_date"] * 3 + [
            "i.incident_id AS hub_id, i.ends_on AS event_date", "i.incident_id AS hub_id, i.ends_on AS event_date",
            "i.incident_id AS hub_id, i.days AS d, i.ends_on AS event_date",
            "max(i.ends_on) AS event_date, count(*) AS hub_id"]

    def cond():
        out = rng.choice(preds)
        for _ in range(rng.choice([0, 0, 1, 1, 2])):
            out = f"{out} {rng.choice(['AND', 'AND', 'AND', 'OR'])} {rng.choice(preds)}"
        return f"NOT ({out})" if rng.random() < 0.1 else out

    q = rng.choice(shapes) + (f"\nWHERE {cond()}" if rng.random() < 0.85 else "")
    if rng.random() < 0.2:
        key = f"{var}.{'effective_date' if var == 'p' else 'starts_on'}"
        q += f"\nWITH r, {var}" + rng.choice([f"\nORDER BY {key} DESC LIMIT 1", ""]) + \
            (f"\nWHERE {cond()}" if rng.random() < 0.5 else "")
    return q + "\nRETURN DISTINCT " + rng.choice(rets) + "\nORDER BY hub_id LIMIT 50"


def _fuzz_template(rng) -> str:
    """One random template: neighbours of a source ("n"), its tickets ("e"), the plan cuts it sees
    ("c"), neighbours of a second renewal the rows describe while $renewal_id is bound elsewhere
    ("o": a $rid source with $renewal_id in a second, plain or OPTIONAL MATCH, or a renewal pinned by
    $k / $limit / a literal / a WITH ... SKIP next to the $renewal_id source, disconnected or reached
    through its Plan hub, with its neighbours joined through the same hub as well, or related to the
    rows' renewals by value: a comparison or a sort key instead of an edge), a population
    grouping ("p") or the calendar a named renewal sees ("h"); filtered and returned in some random
    way. Most are leaks; some are safe; some are not even valid."""
    roll, kind = rng.random(), FUZZ_KINDS[-1][0]
    for name, weight in FUZZ_KINDS:
        if roll < weight:
            kind = name
            break
        roll -= weight
    if kind == "p":
        return _fuzz_population(rng)
    if kind == "h":
        return _fuzz_hub(rng)
    preds = FUZZ_PREDICATES["n" if kind == "o" else kind]

    def cond():
        out = rng.choice(preds)
        for _ in range(rng.choice([0, 0, 1, 1, 2])):
            out = f"{out} {rng.choice(['AND', 'AND', 'AND', 'OR', 'XOR'])} {rng.choice(preds)}"
            out = f"({out})" if rng.random() < 0.2 else out
        return f"NOT ({out})" if rng.random() < 0.1 else out

    def where(p=0.8):
        return f"\nWHERE {cond()}" if rng.random() < p else ""

    if kind == "c":
        cut = "MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(pl:Plan)<-[c:CUT_CAP]-(p:PricingChange)"
        q = rng.choice([
            lambda: cut + where(),
            lambda: "MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(pl:Plan)\n"
                    "OPTIONAL MATCH (pl)<-[c:CUT_CAP]-(p:PricingChange)" + where(),
            lambda: cut + where(0.3) + "\nOPTIONAL MATCH (p)-[:CUT_CAP]->(q:Plan)" + where(),
            lambda: cut + ", (r)-[:SIMILAR_TO]->(n:Renewal)" + where(),
            lambda: cut + where(0.3) + "\nWITH r, pl, c, p" + rng.choice(["\nORDER BY c.event_date DESC LIMIT 1", ""])
                    + where(),
            # the source or the cut shadowed by a struct
            lambda: cut + where(0.3) + f"\nWITH pl, c, p, {FAR} AS r" + where(),
            lambda: cut + where(0.3) + f"\nWITH r, pl, p, c.event_date AS d0, {{event_date: {NEVER}}} AS c" + where(),
        ])()
        rets = ["c.event_date AS event_date, p.change_id AS id"] * 4 + [
            "p.change_id AS id, c.*", "c.event_date AS event_date, p.change_id AS c", "c AS cut, p.change_id AS id"]
        if " AS d0" in q:
            rets = ["d0 AS event_date, p.change_id AS id"]
        return q + "\nRETURN DISTINCT " + rng.choice(rets) + "\nORDER BY 1, 2 LIMIT 50"
    if kind in ("n", "o"):
        src = "(r:Renewal {renewal_id: $renewal_id})"
        nbr = f"MATCH {src}-[e:SIMILAR_TO]->(n:Renewal)"
        # "o": the rows describe the neighbours of a $rid source while $renewal_id (the current
        # renewal in the test) is bound somewhere else: an OPTIONAL MATCH, a second pattern, a map
        second = [
            lambda: "MATCH (a:Renewal {renewal_id: $rid})-[e:SIMILAR_TO]->(n:Renewal)\n"
                    "OPTIONAL MATCH (r:Renewal {renewal_id: $renewal_id})\nWITH a, e, n, r" + where(),
            lambda: "MATCH (a:Renewal)-[e:SIMILAR_TO]->(n:Renewal)\nWHERE a.renewal_id = $rid\n"
                    "OPTIONAL MATCH (r:Renewal {renewal_id: $renewal_id})" + where(),
            lambda: "MATCH (a:Renewal {renewal_id: $rid})-[e:SIMILAR_TO]->(n:Renewal), "
                    "(r:Renewal {renewal_id: $renewal_id})" + where(),
            lambda: "MATCH (r:Renewal {renewal_id: $renewal_id})\n"
                    "OPTIONAL MATCH (a:Renewal {renewal_id: $rid})-[e:SIMILAR_TO]->(n:Renewal)" + where(),
            lambda: "MATCH (r:Renewal)-[e:SIMILAR_TO]->(n:Renewal)\nWHERE r.renewal_id = $rid\n"
                    "OPTIONAL MATCH (n {renewal_id: $renewal_id})-[:ON_PLAN]->(p:Plan)" + where(),
        ]
        # the second renewal pinned next to the $renewal_id source, by value, literal or position
        pinned = [
            lambda: rng.choice([f"MATCH {src}\nMATCH (a:Renewal)-[e:SIMILAR_TO]->(n:Renewal)",
                                "MATCH (r:Renewal {renewal_id: $renewal_id})-[:ON_PLAN]->(:Plan)<-[:ON_PLAN]-"
                                "(a:Renewal)-[e:SIMILAR_TO]->(n:Renewal)"])
                    + "\nWHERE " + rng.choice(PIN_SECOND) + rng.choice(["", "", f" AND {cond()}"]),
            lambda: f"MATCH {src}\nMATCH (a:Renewal)\nWITH r, a ORDER BY a.renewal_id SKIP "
                    + rng.choice(["$k", "987654322"]) + " LIMIT 1\nMATCH (a)-[e:SIMILAR_TO]->(n:Renewal)" + where(),
            # through the Plan hub, its neighbours joined through the same hub as well (fix round 2)
            lambda: HUB_A + "\nWHERE " + rng.choice(PIN_SECOND) + rng.choice(["\nWITH r, p, a", ""])
                    + f"\nMATCH {HUB_JOIN}" + where(0.5),
            lambda: HUB_A + f"\nMATCH {HUB_JOIN}\nWHERE " + rng.choice(PIN_SECOND)
                    + rng.choice(["", "", f" AND {cond()}"]),
        ]
        if kind == "o" and rng.random() < 0.25:      # related by value: a WHERE comparison or a sort key
            rel = rng.choice(VALUE_RELATIONS)
            q = HUB_A + "\nMATCH (n:Renewal)-[:ON_PLAN]->(p)\nWHERE " + rng.choice(PIN_SECOND) + \
                rng.choice([f" AND {rel}", ""]) + rng.choice(["", f" AND {RULE}", f" AND ({RULE} OR true)"])
            q += "" if f" AND {rel}" in q and rng.random() < 0.7 else VALUE_SORT
            return q + "\nRETURN " + rng.choice([
                "a.renewal_id AS via, n.renewal_id AS renewal_id, n.outcome AS outcome",
                "n.renewal_id AS renewal_id, n.outcome AS outcome",
                "n.renewal_id AS renewal_id, CASE WHEN n.outcome_observed_on <= r.as_of THEN n.outcome END AS outcome",
            ]) + "\nORDER BY renewal_id LIMIT 50"
        q = rng.choice((second if rng.random() < 0.45 else pinned + pinned[2:]) if kind == "o" else [
            lambda: nbr + where(),
            lambda: f"MATCH {src}\nOPTIONAL MATCH (r)-[e:SIMILAR_TO]->(n:Renewal)" + where(),
            lambda: nbr + where(0.3) + "\nOPTIONAL MATCH (n)-[:ON_PLAN]->(p:Plan)" + where(),
            lambda: nbr + where(0.3) + "\nWITH r, e, n" + rng.choice(["\nORDER BY e.rank LIMIT 5", ""]) + where(),
            lambda: nbr + where(0.3) + "\nMATCH (n)-[:ON_PLAN]->(p:Plan)" + where(),
            lambda: "MATCH (r:Renewal)-[e:SIMILAR_TO]->(n:Renewal)\nWHERE r.renewal_id = $renewal_id "
                    + rng.choice(["AND", "AND", "OR"]) + " " + cond(),
            lambda: nbr + where(0.5) + "\nWITH r, collect(n.outcome) AS outcomes\nMATCH (r)-[e:SIMILAR_TO]->(n:Renewal)"
                    + where() + "\nWITH r, e, n, outcomes[1] AS leaked",
            # names that do not mean what the bound says: the source or the neighbour shadowed by a struct
            lambda: nbr + where(0.3) + f"\nWITH n, e, {FAR} AS r" + where(),
            lambda: nbr + where(0.3) + "\nWITH r, e, n.renewal_id AS nid, n.outcome AS nout, "
                    f"{{outcome_observed_on: {NEVER}}} AS n" + where(),
            # a source map that restricts nothing
            lambda: "MATCH (r:Renewal)-[e:SIMILAR_TO]->(n:Renewal)\nOPTIONAL MATCH (n {renewal_id: $renewal_id})"
                    "-[:ON_PLAN]->(p:Plan)" + where(),
            # an UNWIND alias that shadows the source (the engine refuses this one itself)
            lambda: nbr + where(0.3) + f"\nUNWIND [{FAR}] AS r\nWITH r, e, n" + where(),
        ])()
        rets = ["n.renewal_id AS renewal_id, n.outcome AS outcome"] * 4 + [
            "n.renewal_id AS renewal_id, n.OUTCOME AS outcome", "n AS node, e.rank AS rank", "properties(n) AS node",
            "n.*", "e.rank AS rank, n.*", "n.renewal_id AS renewal_id, n.outcome AS outcome, e.rank AS e"]
        if "leaked" in q:
            rets = ["n.renewal_id AS renewal_id, leaked AS outcome"]
        elif " AS nout" in q:
            rets = ["nid AS renewal_id, nout AS outcome"]
        elif rng.random() < 0.4:
            q += f"\nWITH r, e, n, ({cond()}) AS visible"
            q += rng.choice(["", "", "", "", "\nWITH r, e, n, true AS visible",
                             "\nWITH r, e, n, visible OR true AS visible"])
            rets += ["n.renewal_id AS renewal_id, CASE WHEN visible THEN n.outcome END AS outcome"] * 5 + [
                "n.renewal_id AS renewal_id, CASE WHEN NOT visible THEN n.outcome END AS outcome",
                "n.renewal_id AS renewal_id, CASE WHEN visible THEN 'x' ELSE n.outcome END AS outcome",
                "n.renewal_id AS renewal_id, CASE WHEN visible THEN n.outcome END AS outcome, n.route AS route"]
        order = rng.choice(["e.rank", "e.rank, n.outcome"])
        return q + "\nRETURN " + rng.choice(rets) + "\nORDER BY " + order + " LIMIT 50"
    ev = "MATCH (s:Subscription)-[:HAS_RENEWAL]->(r:Renewal {renewal_id: $renewal_id})"
    q = rng.choice([
        lambda: ev + "\nMATCH (s)-[e:OPENED]->(x:Ticket)" + where(),
        lambda: ev + "\nOPTIONAL MATCH (s)-[e:OPENED]->(x:Ticket)" + where(),
        lambda: ev + "\nMATCH (s)-[e:OPENED]->(x:Ticket), (o:Renewal)" + where(),
        lambda: ev + "\nMATCH (s)-[e:OPENED]->(x:Ticket)" + where(0.3)
                + "\nOPTIONAL MATCH (s)-[b:HIT_LIMIT]->(y:LimitHit)" + where(),
        lambda: ev + "\nMATCH (x:Ticket)\nOPTIONAL MATCH (s)-[e:OPENED]->(x)" + where(),
        lambda: ev + "\nMATCH (s)-[e:OPENED]->(x:Ticket)" + where(0.3) + "\nWITH s, r, e, x"
                + rng.choice(["\nORDER BY e.event_date DESC LIMIT 3", ""]) + where(),
        # the renewal or the event edge shadowed by a struct
        lambda: ev + "\nMATCH (s)-[e:OPENED]->(x:Ticket)" + where(0.3) + f"\nWITH s, e, x, {FAR} AS r" + where(),
        lambda: ev + "\nMATCH (s)-[e:OPENED]->(x:Ticket)" + where(0.3)
                + f"\nWITH s, r, x, e.event_date AS d0, {{event_date: {NEVER}}} AS e" + where(),
    ])()
    rets = ["x.ticket_id AS id, e.event_date AS event_date"] * 4 + ["x.ticket_id AS id, x.event_date AS event_date",
                                                                   "x AS node", "x.ticket_id AS id, e.*",
                                                                   "x.ticket_id AS x, e.event_date AS event_date"]
    if " AS d0" in q:
        rets = ["x.ticket_id AS id, d0 AS event_date"]
    return q + "\nRETURN DISTINCT " + rng.choice(rets) + "\nORDER BY 1 LIMIT 200"


FUZZ_SHAPES = {   # the hardening shapes (kinds, marks), counted among the refused templates that really leak
    "dot_star": ("nec", (".*",)),
    "alias_shadow": ("nec", (" AS r\n", " AS nout", " AS d0", " AS e\n", " AS x\n", " AS c\n")),
    "optional_or_second_identity": ("o", ("OPTIONAL MATCH (n {", "$rid")),
    "cut_cap": ("c", ("CUT_CAP",)),
    "pinned_second": ("o", ("$k", "$limit", "__rid__", "98765432")),
    "hub_anchor": ("o", (HUB_JOIN,)),
    "value_relation": ("o", (*VALUE_RELATIONS, VALUE_SORT)),
    "population_grouping": ("p", ("\nWITH n.", "\nWITH n, n.outcome", "} AS m\n", "CASE WHEN n.churned")),
    "population_pin": ("p", ("$day", "$req", "$pos", "98765432")),
    "population_aggregate": ("p", (AGGREGATE_MARK,)),
    "population_repeat": ("p", (REPEAT_MARK,)),
    "calendar_hub": ("h", ("PricingChange", "Incident")),
}


def _fuzz_kind(text: str) -> str:
    if " AS hub_id" in text:
        return "h"
    if text.startswith("MATCH (n:Renewal)\n"):
        return "p"
    if "$rid" in text or "(a:Renewal" in text:
        return "o"
    return "c" if "CUT_CAP" in text else "e" if "HAS_RENEWAL" in text else "n"


def _singles(db, text, ren, targets, masked) -> int:
    """How many masked renewals a population template serves one at a time (see _revealed), or gives
    back through an aggregate column from rows of MIN_CELL or more (_decode_aggregate: decodes that
    are the truth only): for a template that pins a renewal by a date / feature / position, each
    target pinned in turn. One fresh Connection per statement text (see _leaks)."""
    from lakehouse_graph import store

    got = {}
    conn = store.connect(db)
    try:
        for x in targets if POP_PINNED.search(text) else [None]:
            rows = _run(conn, text, _target_params(ren, x) if x else {"plan": "pro"})
            got |= _revealed(rows, ren, pinned=x)
            if AGGREGATE_MARK in text:
                got |= dict.fromkeys(_correct(_decode_aggregate(rows, ren, text), ren), "decoded")
            if REPEAT_MARK in text and REPEAT_DISTINCT not in text:
                plans = ren["plan_tier"].nunique()
                got |= dict.fromkeys(_correct(_decode_repeated(rows, ren, plans), ren), "decoded")
    finally:
        conn.close()
    return len(set(got) & masked)


def fuzz_run(bdir, n: int, seed: int) -> dict:
    """Run ``n`` seeded random templates through the lint and the engine (tiny build ``bdir``).

    Returns counts: accepted / leaky_refused / invalid, per kind and per FUZZ_SHAPES entry, and
    ``holes`` = the texts the lint accepts that leak (must stay empty)."""
    import random

    from lakehouse_graph import oracle, store

    ren = _with_city(oracle.load_tables(bdir))          # the Renewal frame, with each renewal's city
    sources = ren.loc[[*ren.index[::6], SECOND]]
    rng = random.Random(seed)
    db = store.open_readonly(bdir / store.DB_FILE)
    out = {"accepted": 0, "leaky_refused": 0, "invalid": 0, "holes": [], "kinds": {},
           "shapes": dict.fromkeys(FUZZ_SHAPES, 0)}
    try:
        masked = _masked(db[1], ren)
        targets = _targets(ren, masked)
        for _ in range(n):
            text = _fuzz_template(rng)
            kind = _fuzz_kind(text)
            k = out["kinds"].setdefault(kind, {"accepted": 0, "leaky_refused": 0, "invalid": 0})
            ok = not queries.lint({"fuzz": text}, contract_only=())
            try:
                if kind == "p":                            # a population: renewals served one at a time
                    outcomes, events = _singles(db[0], text, ren, targets, masked), 0
                else:
                    outcomes, events, _rows = _leaks(db[0], sources, text, ren,
                                                     renewal_id=SECOND if kind == "o" else None)
            except RuntimeError:                           # the engine refuses it: not a template at all
                out["invalid"] += 1
                k["invalid"] += 1
                continue
            if ok:
                out["accepted"] += 1
                k["accepted"] += 1
                if outcomes + events:
                    out["holes"].append(text)
            elif outcomes + events:
                out["leaky_refused"] += 1
                k["leaky_refused"] += 1
                for shape, (kinds, marks) in FUZZ_SHAPES.items():
                    out["shapes"][shape] += kind in kinds and any(m in text for m in marks)
    finally:
        db[1].close()
        db[0].close()
    return out


def test_no_random_template_the_lint_accepts_leaks_on_tiny(tiny_build):
    """Differential check of the lint against the engine: of 1,500 random templates (seeded), every
    one the lint accepts is executed and serves no outcome, event or plan cut from after the as_of
    of the renewal its rows describe, and no population row that singles out a masked renewal. The
    generator makes leaks the lint must catch, too, and each hardening shape is exercised: n.* / e.*
    / c.* projections, WITH / UNWIND / RETURN aliases that shadow the source, the neighbour, the
    event edge or the cut, a $renewal_id bound only in an OPTIONAL MATCH or a second pattern while
    the rows describe a $rid source, a second renewal pinned by $k / $limit (as a string or an
    integer), a literal or a WITH ... SKIP, disconnected or through the Plan hub (its neighbours joined
    through that hub as well as through it), or related to the rows' renewals by value (a comparison or
    a sort key on a feature, a date or the route instead of an edge), CUT_CAP bounds that
    are weakened, misplaced or measured against a neighbour, population groupings by a date or a
    feature (RETURN keys, WITH groups, aggregate and map aliases, CASE keys, SKIP) or pinned by a
    parameter or literal beside an expression, an aggregate column that encodes a label beside a
    near-unique value, packs two cells (around the aggregates or through a CASE value of 1001 /
    1000001), is a flag column (sum(<sub-cell>) = 1) or takes an extreme grouped by outcome (decoded
    from rows of 5 or more renewals, _decode_aggregate), count(*) over rows that repeat a sub-cell's
    lapsed renewals (REPEAT_MARK, decoded against the cells' sizes; WITH DISTINCT n, s beside it is
    accepted), and the calendar a named renewal sees (a pricing change or
    an incident matched on its own, beside the source, optionally, through an unbounded cut or
    exposure, kept by a WITH; its date, when it ended, or an aggregate of them)."""
    bdir, _ = tiny_build
    got = fuzz_run(bdir, 1500, 20260930)
    assert got["holes"] == [], "the lint accepts templates that leak:\n\n" + "\n\n".join(got["holes"][:3])
    # measured (seed 20260930, fix round 4 generator): 137 accepted (n 11, e 9, c 12, p 89, h 16; o 0: a
    # second renewal is always refused), 848 refused that do leak (dot_star 52, alias_shadow 100, optional /
    # second identity 60, cut_cap 96, pinned second renewal 146, hub anchor 51, value relation 67, population
    # grouping 56, population pin 71, population aggregate 41, population repeat 45, calendar hub 99), 247 the
    # engine itself rejects. FUZZ_MEASURED_SEEDS: 16,000 more templates over seeds 1-4 (4,000 each), none
    # accepted leaks; 'o' accepted 0, population aggregate 91-118 and population repeat 95-132 refused leaks
    # per seed (earlier generators: 76,000 over seeds 1-7). The shape floor is 30: the 1,500-template run
    # exercises each shape 41 to 146 times.
    assert got["accepted"] >= 60 and got["leaky_refused"] >= 700 and got["invalid"] < 400, got
    assert all(k["accepted"] >= 5 for kind, k in got["kinds"].items() if kind != "o"), got["kinds"]
    assert all(k["leaky_refused"] >= 80 for k in got["kinds"].values()), got["kinds"]
    assert all(v >= 30 for v in got["shapes"].values()), got["shapes"]


def test_the_whole_catalog_fits_one_serving_connection(tiny_build):
    """Every template, in every rendering, runs on ONE read-only connection with the serving pool.

    The engine keeps each distinct parameterised statement prepared for the life of its Connection
    and each holds about 2 MB of the buffer pool (store.connect), so a catalog that outgrows the
    pool would fail on a long-lived server only after every template has been used once. This
    fails first, when such a template is added."""
    from lakehouse_graph import store

    bdir, _ = tiny_build
    p = {"renewal_id": "sub_maya:2026-10-07", "k": 3, "limit": 10, "today": False, "incident_id": "inc-002",
         "change_id": "cap-cut-2026-09"}
    db, conn = store.open_readonly(bdir / store.DB_FILE, buffer_pool_mb=store.SERVE_BUFFER_POOL_MB)
    ran = with_params = 0
    try:
        for _ in range(2):
            for name, text in queries.TEMPLATES.items():
                renderings = [{"label": x} for x in spec.NODE_SCHEMA] if "{label}" in text else \
                    [{"rel": x} for x in (spec.EDGE_SCHEMA if name == "count_rels" else spec.EVENT_RELATIONS)] \
                    if "{rel}" in text else [{}]
                for ids in renderings:
                    rows = queries.fetch(conn, name, p, contract=True, **ids)
                    assert isinstance(rows, list), name
                    ran += 1
                    with_params += "$" in text
        # and there is room left: as many ad-hoc parameterised statements again, on a connection of their own
        extra = store.connect(db)
        try:
            for i in range(10):
                assert store.rows(extra.execute(f"MATCH (r:Renewal {{renewal_id: $renewal_id}}) RETURN r.plan_tier AS "
                                                f"plan_{i} ORDER BY plan_{i} LIMIT 1", {"renewal_id": p["renewal_id"]}))
        finally:
            extra.close()
    finally:
        conn.close()
        db.close()
    assert ran == 2 * (len(queries.TEMPLATES) - 3 + len(spec.NODE_SCHEMA) + len(spec.EDGE_SCHEMA)
                       + len(spec.EVENT_RELATIONS)) and with_params >= 2 * 20


def test_lint_reads_tokens_clauses_and_patterns():
    """The pieces the leak lint stands on: case-folded tokens, clauses, conjuncts, MATCH patterns."""
    toks, bad = queries._tokenise("MATCH (N:Renewal) WHERE N.`x` = 'a // b' // c")
    assert bad == ["//", "`"] and [t.low for t in toks][:6] == ["match", "(", "n", ":", "renewal", ")"]
    assert queries._canon_text("N.Outcome_Observed_On<=R.AS_OF") == queries._canon_text(queries.visible("n", "r"))
    assert queries._canon_text("x = 'Model'") != queries._canon_text("x = 'model'")       # strings keep their case
    conj = queries._conjuncts(queries._tokenise("(a.x = 1) AND NOT b.y AND (c.z = 2 OR d.w = 3) AND (e.u AND f.v)")[0])
    assert conj == ["a . x = 1", "not b . y", "c . z = 2 or d . w = 3", "e . u", "f . v"]
    assert queries._conjuncts(queries._tokenise("a.x = 1 AND b.y = 2 OR c.z = 3")[0]) == \
        ["a . x = 1 and b . y = 2 or c . z = 3"]             # an OR at the top: nothing is required on its own
    for unbalanced in ("{end: 1}.end = 1 OR (a.x AND b.y)", "CASE WHEN a.x THEN 1 AND b.y", "a.x END AND b.y"):
        with pytest.raises(queries._NoParse):                # a keyword used as a name would re-nest the split
            queries._conjuncts(queries._tokenise(unbalanced)[0])
    assert queries._tokenise("WHERE x.y <> 'a\\' AND b'")[1] == ["\\"]   # \' ends a string for the engine, not the lint
    clauses, nested, lead = queries._clauses(queries._tokenise(
        "MATCH (a) OPTIONAL MATCH (a)-[e:OPENED]->(b) WHERE b.name STARTS WITH 'x' WITH a, count(e) AS n "
        "RETURN n ORDER BY n LIMIT 1")[0])
    assert [c.kind for c in clauses] == ["match", "optional match", "where", "with", "return", "order by", "limit"]
    assert (nested, lead) == ([], 0)
    paths, nodes, rels = queries._patterns(queries._tokenise(
        "p = (s:Subscription {city: 'Pune'})<-[e:OPENED|:BILLED]-(x), "
        "(a)-[f:SIMILAR_TO* WSHORTEST(dist)]->(b:{label})")[0])
    assert [t.low for t in paths] == ["p"] and [n.var for n in nodes] == ["s", "x", "a", "b"]
    assert nodes[0].keys == {"city"} and nodes[3].labels == {queries._ANY} and not nodes[0].source
    e, f = rels
    assert (e.types, e.direction, e.tail.var, e.head.var) == (("opened", "billed"), "in", "x", "s")
    assert e.what == "OPENED|BILLED"
    assert (f.var, f.recursive, f.lone, f.direction) == ("f", True, True, "out")
    for unreadable in ("(a)-[e:X]", "(a {x})", "(a)<-[e:X]->(b)", "(a) (b)", "shortest((a)-[e:X]->(b))"):
        with pytest.raises(queries._NoParse):
            queries._patterns(queries._tokenise(unreadable)[0])
    assert any("MATCH pattern the lint cannot read" in p for p in queries.lint(
        {"t": "MATCH (a:Renewal)<-[e:SIMILAR_TO]->(b:Renewal) RETURN a.renewal_id AS id ORDER BY id LIMIT 1"}, ()))


def test_lint_accepts_the_bounded_shapes_and_flags_an_unknown_contract_only_name():
    good = {k: queries.TEMPLATES[k] for k in queries.TOOL_TEMPLATES}
    assert queries.lint(good, contract_only=()) == []
    # the unbounded catalog templates fail as tool templates: that is why they are contract-only
    for name in queries.UNBOUNDED_NEIGHBOUR_OUTCOMES:
        assert queries.lint({name: queries.TEMPLATES[name]}, contract_only=()), name
    naive = [k for k in queries.CONTRACT_ONLY if queries.lint({k: queries.TEMPLATES[k]}, contract_only=())]
    assert len(naive) >= 10, naive                          # nearly every contract template is a planted leak
    assert queries.lint({}, contract_only=("gone",)) == ["gone: contract-only template is not in the catalog"]


def test_the_catalog_is_an_allowlist_of_vetted_tool_template_shapes():
    """lint() of the catalog also requires every tool template to be a vetted shape (its name and
    fingerprint in queries.VETTED_TOOL_TEMPLATES): layout and keyword case keep the fingerprint;
    any other edit, a new tool template, or a vetted name that is no longer served is refused,
    even when the template obeys every leak rule. Modules with their own catalogs opt in."""
    assert queries.lint() == []
    assert set(queries.VETTED_TOOL_TEMPLATES) == set(queries.TOOL_TEMPLATES)
    q = queries.TEMPLATES["renewal_header"]
    same = q.replace("\n", "  \n ").replace("MATCH", "match").replace("RETURN", "return").replace("LIMIT", "limit")
    assert same != q and queries.fingerprint(same) == queries.fingerprint(q)
    edited = q.replace("s.city AS city", "s.city AS city, r.cuts_so_far AS cuts")
    assert queries.fingerprint(edited) != queries.fingerprint(q)
    assert queries.lint({"renewal_header": edited}, contract_only=()) == []          # it obeys every leak rule ...
    problems = queries.lint({**queries.TEMPLATES, "renewal_header": edited}, vetted=queries.VETTED_TOOL_TEMPLATES)
    assert len(problems) == 1 and problems[0].startswith(
        "renewal_header: is not a vetted tool template shape (edited since it was vetted (98de4eeb63f68eed); "
        "fingerprint "), problems                                                    # ... and is still refused
    problems = queries.lint({**queries.TEMPLATES, "new_counts": GOOD_TEMPLATES["ok_population"]},
                            vetted=queries.VETTED_TOOL_TEMPLATES)
    assert len(problems) == 1 and "new_counts: is not a vetted tool template shape (a new tool template" in problems[0]
    gone = {k: v for k, v in queries.TEMPLATES.items() if k != "routes"}
    assert queries.lint(gone, vetted=queries.VETTED_TOOL_TEMPLATES) == [
        "routes: is vetted (VETTED_TOOL_TEMPLATES) but is not a tool template of the catalog"]
    assert queries.lint(GOOD_TEMPLATES, contract_only=()) == []                       # other catalogs: rules only


def test_fetch_refuses_contract_only_templates_unless_asked(tiny_build):
    from lakehouse_graph import store

    bdir, _ = tiny_build
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    p = {"renewal_id": "sub_maya:2026-10-07", "k": 3, "incident_id": "inc-002"}
    try:
        for name in queries.CONTRACT_ONLY:
            with pytest.raises(queries.ContractOnlyError, match="contract-only"):
                queries.fetch(conn, name, p)
        with pytest.raises(queries.ContractOnlyError, match="similar_top_k_visible"):
            queries.fetch(conn, "similar_top_k", p)
        assert len(queries.fetch(conn, "similar_top_k", p, contract=True)) == 3
        assert queries.fetch(conn, "contract_post_as_of_edges", contract=True, rel="HIT_LIMIT")[0]["edges"] > 0
        for name in ("similar_top_k_visible", "similar_nearest_lapses_known_by_as_of", "renewal_header",
                     "exposure_incident_by_plan", "routes"):
            assert queries.fetch(conn, name, p), name       # tool templates need no flag
    finally:
        conn.close()
        db.close()


def test_top_k_tool_variant_masks_outcomes_the_source_could_not_know(tiny_build):
    """similar_top_k_visible = similar_top_k with every neighbour outcome observed after the source's
    as_of masked; `today` widens it for a current source only (score_today / pending)."""
    from lakehouse_graph import oracle, store

    bdir, _ = tiny_build
    ren = oracle.load_tables(bdir)["Renewal"].set_index("renewal_id")
    current = ren.index[ren["route"].isin(["score_today", "pending"])].tolist()
    assert current == ["sub_maya:2026-10-07"]
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    masked = shown = 0
    try:
        for rid in ren.index:
            p = {"renewal_id": rid, "k": 10}
            raw = queries.fetch(conn, "similar_top_k", p, contract=True)
            for today in (False, True):
                got = queries.fetch(conn, "similar_top_k_visible", {**p, "today": today})
                assert [(r["rank"], r["renewal_id"], r["d2_q"], r["dist"], r["mutual"]) for r in got] == \
                       [(r["rank"], r["renewal_id"], r["d2_q"], r["dist"], r["mutual"]) for r in raw], rid
                assert "route" not in got[0]
                for g, r in zip(got, raw, strict=True):
                    known = pd.Timestamp(r["outcome_observed_on"]) <= ren.at[rid, "as_of"]
                    visible = known or (today and rid in current)
                    assert g["outcome_visible"] is visible, (rid, today, g)
                    if visible:
                        assert (g["outcome"], g["outcome_observed_on"]) == (r["outcome"], r["outcome_observed_on"])
                        shown += 1
                    else:                                   # nothing about the outcome gets through
                        assert (g["outcome"], g["outcome_observed_on"]) == (queries.NOT_YET_OBSERVED, None)
                        masked += 1
            default = queries.fetch(conn, "similar_top_k_visible", p)       # $today defaults to False
            assert default == queries.fetch(conn, "similar_top_k_visible", {**p, "today": False})
    finally:
        conn.close()
        db.close()
    assert masked > 100 and shown > 100                     # the rule both hides and serves on the fixture


def test_evidence_never_returns_billed_outcome_evidence(tiny_build):
    from lakehouse_graph import oracle, store

    bdir, _ = tiny_build
    t = oracle.load_tables(bdir)
    outcome = set(t["BILLED"].loc[t["BILLED"]["outcome_evidence"], "dst"])
    allowed = set(t["BILLED"].loc[~t["BILLED"]["outcome_evidence"], "dst"])
    assert len(outcome) == 161 and len(allowed) == 4
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    served = set()
    try:
        for rid in t["Renewal"]["renewal_id"]:
            rows = queries.evidence(conn, rid)
            billed = {r["target_id"] for r in rows if r["relation"] == "BILLED"}
            assert not billed & outcome, rid
            assert all(r["detail"] == "cancel_scheduled" for r in rows if r["relation"] == "BILLED"), rid
            assert all(r["known_by_as_of"] or r["declared_exception"] for r in rows), rid
            served |= billed
    finally:
        conn.close()
        db.close()
    assert served == allowed


def test_first_renewal_after_is_always_returned_flagged():
    for name in ("evidence_first_renewal_after", "exposure_pricing_change"):
        assert "known_by_as_of" in queries.TEMPLATES[name], name
    assert "declared_exception" in queries.TEMPLATES["evidence_first_renewal_after"]


def test_nearest_lapses_tool_variant_only_serves_outcomes_known_by_as_of(tiny_build):
    """similar_nearest_lapses_known_by_as_of = the contract template's rows minus the lapses that
    were observed after the source renewal's as_of (a neighbour's outcome is outcome evidence)."""
    from lakehouse_graph import oracle, store

    bdir, _ = tiny_build
    ren = oracle.load_tables(bdir)["Renewal"].set_index("renewal_id")
    db, conn = store.open_readonly(bdir / store.DB_FILE)
    served = hidden = 0
    try:
        for rid in [*ren.index[::10], "sub_maya:2026-10-07"]:
            p = {"renewal_id": rid, "k": 1000}
            every = queries.fetch(conn, "similar_nearest_lapses", p, contract=True)
            known = queries.fetch(conn, "similar_nearest_lapses_known_by_as_of", p)
            as_of = ren.at[rid, "as_of"]
            want = [(r["renewal_id"], r["path_dist"]) for r in every
                    if ren.at[r["renewal_id"], "outcome_observed_on"] <= as_of]
            assert [(r["renewal_id"], r["path_dist"]) for r in known] == want, rid
            assert all(pd.Timestamp(r["outcome_observed_on"]) <= as_of for r in known), rid
            assert all(r["route"] == ren.at[r["renewal_id"], "route"] for r in known), rid
            served, hidden = served + len(known), hidden + len(every) - len(known)
    finally:
        conn.close()
        db.close()
    assert served > 0 and hidden > 0  # the bound both serves and hides rows on the fixture


def test_render_only_accepts_spec_identifiers():
    assert "MATCH (x:Renewal)" in queries.render("count_nodes", label="Renewal")
    assert "[e:SIMILAR_TO]" in queries.render("count_rels", rel="SIMILAR_TO")
    for bad in ({"label": "Renewal) DETACH DELETE (n"}, {"rel": "NOPE"}, {"table": "Renewal"}):
        with pytest.raises(ValueError):
            queries.render("count_nodes" if "label" in bad or "table" in bad else "count_rels", **bad)


def test_every_ladybug_open_caps_pool_and_threads():
    opens = []
    for path in GRAPH_SOURCES:
        for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
            if isinstance(node, ast.Call) and getattr(node.func, "attr", getattr(node.func, "id", None)) == "Database":
                kws = {k.arg for k in node.keywords}
                opens.append((path.name, node.lineno))
                assert {"buffer_pool_size", "max_num_threads"} <= kws, \
                    f"{path.name}:{node.lineno} opens Ladybug without capping buffer_pool_size / max_num_threads"
    assert len(opens) >= 2, opens  # the loader and the read-only open


def test_no_extension_or_file_statements_anywhere():
    for path in GRAPH_SOURCES:
        for lineno, text in _string_constants(path):
            assert not FORBIDDEN_STATEMENT.search(text), f"{path.name}:{lineno}: {text[:80]!r}"
        # a plain grep must come back empty too (docstrings and comments included)
        assert not re.search(r"\bINSTALL\b|\bLOAD EXTENSION\b", path.read_text()), path.name


def test_loader_only_copies_from_parquet_into_a_fresh_db(tmp_path):
    from lakehouse_graph import store

    src = (REPO / "src/lakehouse_graph/store.py").read_text()
    assert src.count("COPY {name} FROM") == 1 and "CHECKPOINT" in src
    (tmp_path / "graph.lbdb").write_text("not a database")
    with pytest.raises(FileExistsError):
        store.load_ladybug(tmp_path, tmp_path / "graph.lbdb")
    with pytest.raises(FileNotFoundError):
        store.open_readonly(tmp_path / "missing.lbdb")
    with pytest.raises(ValueError):
        store._quoted(tmp_path / "it's")
