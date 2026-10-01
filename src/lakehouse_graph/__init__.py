"""lakehouse_graph: a derived, audited renewal graph on top of the churn gold table.

Import with ``PYTHONPATH=<repo>/src`` (``src/`` is not an installed package).
Python 3.12, dependencies pinned in requirements-graph.txt.

  spec      versions, schema, SIMILAR_TO rule, PIT windows, paths
  build     pandas builder: gold twin -> deterministic Parquet -> Ladybug
  manifest  business_build_id and provenance (manifest.json)
  store     Ladybug loader / read-only open, build lock, promote, gc
  queries   named Cypher templates (every one has ORDER BY and LIMIT)
  oracle    pandas golden answers and invariants; ``--print-golden``
"""
from __future__ import annotations

from .spec import CONTRACT_VERSION, GRAPH_SPEC_VERSION, SIMILAR_TO_SPEC_VERSION, SPEC_VERSIONS

__version__ = "0.1.0"
__all__ = ["CONTRACT_VERSION", "GRAPH_SPEC_VERSION", "SIMILAR_TO_SPEC_VERSION", "SPEC_VERSIONS", "__version__"]
