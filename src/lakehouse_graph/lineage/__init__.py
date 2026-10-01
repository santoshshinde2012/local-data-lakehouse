"""lakehouse_graph.lineage: the Tier-0 metadata / lineage graph (spec ``metadata-graph/0.1``).

A second, much smaller graph next to the business graph: it describes the pipeline itself
(datasets, columns, jobs, DAGs, Make targets, CI steps, contracts) and is extracted
offline from the repo's own code. It answers "where does this feature come from", "which
features can read data after T-7", "which checks guard this column" and "which columns does
nobody read".

  spec        node / edge schema, ids and ColumnRef rules, the bridge to the business graph
  scope_walk  sqlglot qualify + scope walk over the churn gold SQL (roles, row windows; it
              states exactly what counts as a time bound and stops on anything else)
  extract     ast / regex extraction of jobs, DAGs, Makefile, shell, CI, contracts, README
  graph       facts -> nodes and edges (LineageGraph)
  build       Parquet tables under <build_dir>/lineage/ + lineage.lbdb, lineage_build_id
  oracle      pure-Python BFS answers from the Parquet tables; ``--print-golden``
  queries     named Cypher templates (every one has ORDER BY and LIMIT)
  tools       lineage_trace / lineage_pit / lineage_guards / lineage_unused
  iceberg_facts  Tier 1 overlay (``--iceberg``): Snapshot / Ref / Run facts from the Iceberg catalog's
              ``.snapshots`` + ``.refs`` (read-only through lakehouse_graph.iceberg_source)
  openlineage Tier 2 overlay (``--openlineage``): Spark application runs, RAN_AS and PARENT from an
              OpenLineage JSONL file, action-level runs collapsed

Build it with scripts/build_lineage_local.py, check it with scripts/check_lineage_contract.py.
"""
from __future__ import annotations

from .spec import CONTRACT_VERSION, SPEC_VERSION, LineageExtractError

__all__ = ["CONTRACT_VERSION", "SPEC_VERSION", "LineageExtractError"]
