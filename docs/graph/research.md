# Graph on gold: research, decisions and sources

What was checked before building, what was decided, what was rejected and why. Facts were verified
between 2026-09-30 and 2026-10-01 (a multi-agent research pass with adversarial fact checks and hands-on
probes on the target Mac, an M1 Pro with 16 GB). Versions move fast here; re-check before relying on a
date-sensitive fact.

## Findings that shaped the design

1. **A graph adds explanation, not prediction, on this data.** The generator has no relationship between
   subscriptions. Neighbour features gave no lift (LR 0.7198 → 0.7198 temporally safe, 0.7197 on a random
   graph; only outcomes from after T-7 lift it to 0.7223). Communities rediscover the generator's
   segments (plan purity 1.0). PageRank on a kNN graph is in-degree in disguise (Spearman 0.90).
2. **Point-in-time correctness is the real lesson.** Bronze deliberately holds events after each
   renewal's T-7. A graph that keeps them and a contract that requires a naive traversal to be wrong
   (1,165 / 681 / 114 renewals) turn the leak into a failing test instead of prose.
3. **Typed tools over precomputed structures carry the agent value** (old-model evidence: 17/40 vs 0/40
   for text-to-SQL and 1/40 for SQL over the same precomputed tables). Mixing toolsets hurt (9/22
   graph-shaped). Small routed toolsets are a hypothesis the eval tests, not a rule.
4. **`read_only` is not a sandbox in the Kùzu family.** LadybugDB opened read-only still runs
   `LOAD FROM`, `COPY TO`, `EXPORT DATABASE`, `ATTACH` and `LOAD <extension>`, with no external-access
   switch (source-verified). A macOS `sandbox-exec` profile was proven to block those while `MATCH`
   works. Hence no raw query tool, vetted templates, and the OS sandbox.
5. **Iceberg reads must pin tags, not timestamps.** With this repo's Iceberg 1.6.1, `createOrReplace`
   keeps snapshots but cuts the parent chain, and `expire_snapshots` trims history; tags survive both.
   PyIceberg 0.12 can read the repo's JDBC catalog only with catalog name `lakehouse`,
   `init_catalog_tables=false`, no `schema_version` (v1 would ALTER the shared catalog), and an explicit
   `s3.region` next to the local endpoint (else it asks real AWS).
6. **The lineage of the gold SQL is now derivable.** The renewal model's gold SQL is runnable Spark SQL,
   so sqlglot plus a scope walk resolve all 30 output columns, including the filter, join and window roles
   `sqlglot.lineage()` misses. Building it found 7 real repo defects ([lineage.md](lineage.md#findings)).
7. **Laptop limits are real.** The host swaps with Docker down. Build, serve and lakehouse work are
   scheduled separately; the serving path needs no containers.
8. **Spark and pandas round differently.** At seed 42 two `accept_rate_change` cells differ by 1e-4 between
   Spark gold and the pandas twin: Spark rounds the decimal string half to even, numpy rounds `x * 1e4`
   ([lakehouse-twin.md](lakehouse-twin.md#why-gold-drifts-by-1e-4-in-two-cells)).

## Decisions

| # | Decision | Status |
|---|---|---|
| 1 | Build a deterministic renewal graph from the pandas gold twin; Parquet canonical, LadybugDB 0.21.1 as the embedded engine (exact pin + wheel hashes; rebuild, never migrate; pandas fallback where cheap) | built |
| 2 | SIMILAR_TO `similar_to/renewal-v1`: blocked by plan, 20 features, persisted scaler, quantised key `FLOOR(d2*1e9 + 0.5)` then `dst` | built, Spark parity exact |
| 3 | FIRST_RENEWAL_AFTER stays a declared, flagged exception (the gold rule is not changed) | built; the gold rule is the data owner's call |
| 4 | `renewals_completed` bound documented, not changed (0 rows affected at seed 42) | documented |
| 5 | The dangling LEAKY `cancel_at_period_end`: `check_repo_contracts.py` warns | built; the fix is the data owner's call |
| 6 | `check_gold_parity.py` coverage not changed (city, feature_as_of, renewal_date not compared) | noted |
| 7 | An independent `graph` CI job; the existing radar job untouched | **not added yet** |
| 8 | Commit `.mcp.json` and the skill; ship `graph_ask.sh` as an allowlist launcher; no repo-wide Claude settings deny-list | built |
| 9 | Lineage in the first release (Tier 0) | built |
| 10 | LadybugDB accepted despite a bus factor of about 1 | accepted |
| 11 | DuckDB 1.5.6 only in the eval's SE arm, never a product dependency (1.5.x EOL 2026-11-01) | eval not built |
| 12 | The Claude-arm eval budget is not spent without approval | not spent |
| 13 | `config/CATALOG.md` refresh | **not done yet** |
| 14 | A strict eval / parity preflight (Docker up, memory pressure, free swap), `FORCE=1` to override | **not built yet** |
| 15 | A "graph edition" generator (the only honest route to a network-effect lesson) is future work | not scheduled |
| 16 | Local model `qwen3:4b` (installed, Apache-2.0); `qwen3.5:4b` or the instruct sibling only with approval | researched; harness not built |
| 17 | The minimal Decimal + atomic-write fix in `src/jobs/churn/04_export_features.py` (a user file) | applied in this branch; review before merge |
| 18 | The gold rounding patch (Spark `bround` vs numpy) | proposed, not applied |

## Rejected options

| Option | Why not |
|---|---|
| Kùzu | archived 2025-10-10 at v0.11.3 (acquired); `.kuzu` files are not openable by its fork |
| Neo4j CE | GPLv3, a ~1.5 GB JVM server (Docker or a native install), the default new Browser is closed source; **not used** |
| Memgraph, FalkorDB / FalkorDBLite, ArangoDB 3.12+, SurrealDB, TuringDB, PuppyGraph, Kumo | not open source (BSL, SSPL or proprietary) |
| DuckPGQ | a research extension built for DuckDB 1.5.4 only, no property graph over views, slow variable-length paths |
| DuckDB as a product dependency | 1.5.x reaches end of life 2026-11-01, 2.0 ships 2026-10-21; eval-only instead |
| Apache AGE, ArcadeDB, Grafeo, Oxigraph | a sibling Postgres container with no algorithms; a JVM and many 2026 advisories; one committer with format breaks (watch list); RDF only |
| CozoDB, RyuGraph, NebulaGraph OSS, JanusGraph, TuGraph; PostgreSQL 19 SQL/PGQ | unmaintained or stalled; SQL/PGQ was reverted on 2026-09-07 |
| `mcp-server-ladybug` 0.1.3 | opens read-write, installs extensions from the network at start, one raw query tool, pins `mcp<2` |
| Ladybug vector (HNSW) and FTS extensions | not in the wheel, downloaded per version, segfaults measured; exact numpy kNN and a stdlib token index cost nothing at this scale |
| Text-to-SQL or raw Cypher for small models | a 3B model passed 0 of 40 text-to-SQL cases; Ladybug Cypher is not Neo4j Cypher |
| GraphRAG frameworks (MS GraphRAG, LightRAG, Graphiti, Mem0 graph memory, Cognee) | maintenance mode, removed, a Neo4j-only backend, needs ≥32B models, or explodes float columns into ~100k nodes; the gold data is already structured |
| OpenMetadata, DataHub, Atlas, Amundsen, Unity Catalog OSS, Marquez | 6 to 8 GB of RAM, archived, no open-source lineage, or stale amd64-only images |
| OpenLineage as the lineage source | with a JDBC Iceberg catalog it detects no datasets (issue #4677); runs and timing only |
| igraph, leidenalg | GPL-2.0+ / GPL-3.0+ (copyleft); NetworkX 3.7 (BSD) covers Louvain and Leiden |
| PageRank, node2vec, GNNs on the kNN graph; a retail order graph (19 orders); City / Plan hubs; contagion claims | gimmicks on this data |
| llama3.2:3b, phi4:14b, deepseek-r1:1.5b | Llama licence is not OSI; text tool calls (114 / 480); no tool support; no native calls |
| Claude Agent SDK, LangGraph + `langchain-mcp-adapters` | bundles the proprietary CLI under commercial terms; the adapters were archived 2026-09-16 |
| A repo-wide `.claude/settings.json` deny-list | would constrain the user's normal sessions; `graph_ask.sh` is an opt-in allowlist instead |
| Splink identity resolution across domains | the domains share no key (the hero is churn-only, the retail customer retail-only) |

## Dependencies and licences

The versions in this page are research-time (2026-10-01): Spark 3.5.3, Iceberg 1.6.1, SILO and a JDBC
catalog. The stack now runs Spark 4.1.3, Iceberg 1.11.0, the Lakekeeper REST catalog and RustFS (SILO is
an option), and Airflow 3.3.2 (current pins: the version table in the [README](../../README.md#versions)).
pyspark 3.5.3 remains only in the graph's local Spark harness (`requirements-graph-spark.txt`).
psycopg2 and SQLAlchemy were removed from the client lock (`requirements-graph-spark-client.txt`); they
remain in the spark harness lock for its SQLite / JDBC catalog code.

| Component | Version | Licence | Where |
|---|---|---|---|
| ladybug (LadybugDB) | 0.21.1 | MIT | core lock |
| pyarrow | 25.0.1 | Apache-2.0 | core lock |
| pandas | 3.0.6 | BSD-3-Clause | core lock |
| numpy | 2.5.3 | BSD-3-Clause (and others) | core lock |
| networkx | 3.7 | BSD-3-Clause | core lock |
| mcp (MCP Python SDK) | 2.2.0 | MIT | core lock |
| pydantic | 2.13.5 | MIT | core lock |
| sqlglot | 30.20.0 | MIT | core lock |
| pytest | 9.1.1 | MIT | core lock |
| pyspark | 3.5.3 | Apache-2.0 | spark lock (parity, local Iceberg) |
| pyiceberg | 0.12.0 | Apache-2.0 | spark and client locks |
| SQLAlchemy | 2.1.1 | MIT | spark lock (removed from the client lock) |
| psycopg2-binary | 2.9.13 | **LGPL-3.0 with exceptions** (flagged; pg8000, BSD-3, is the permissive alternative) | spark lock (removed from the client lock) |
| Cytoscape.js | 3.34.3 | MIT (vendored in `src/lakehouse_graph/vendor/`, sha256 pinned) | evidence views |
| Apache Spark / Iceberg runtime | 3.5.3 / 1.6.1 at research time; the stack now runs 4.1.3 / 1.11.0 | Apache-2.0 | the stack (3.5.3 only in the graph harness) |
| Silo (`pgsty/silo`) | the repo's pin | **AGPL-3.0** (flagged) | optional object store (`STORE=silo`); the default is RustFS 1.0.0 (Apache-2.0) |
| Apache Airflow | 2.10.4 at research time (**end of life 2026-04-22**); the stack now runs 3.3.2 | Apache-2.0 | optional DAG |
| Ollama / Qwen3 4B | 0.35.0 / `qwen3:4b` | MIT / Apache-2.0 | experimental open-source agent path |
| Claude Code and Claude models | 2.1.284 tested | **proprietary** | the Claude path |
| Neo4j | - | GPLv3 | **not used** |

Transitive versions are in `requirements-graph*.txt` (`uv pip compile --universal --generate-hashes`,
installed with `--require-hashes`). The `ladybug` PyPI name was reused from an unrelated 2014 package,
which is one reason the hashes are part of the contract.

## Honesty notes

1. The data is synthetic, and subscriptions have no relationships to each other. SIMILAR_TO means
   similar features, not connected.
2. The graph adds no predictive lift ([evaluation.md](evaluation.md#what-the-graph-does-not-add)).
3. Neighbours are narrative evidence, never a risk estimate.
4. Incident exposure shows no reliable effect; the pricing-cut association is planted by the generator.
5. The point-in-time leak is real and measurable, and the contract keeps it visible.
6. Gold-rule caveats: `first_renewal_after_pricing_change` reads cuts that took effect after T-7 (495
   flags); `renewals_completed` reads up to renewal_date (latent); `route` depends on the data end date.
7. Communities rediscover generator cohorts: labels, not structure.
8. Ladybug Cypher is not Neo4j Cypher: `on` and `Column` are reserved, `length(e)` on a weighted shortest
   path fails, `label()` differs, result order is undefined without ORDER BY.
9. The Claude path uses a proprietary client and model; its no-egress guarantee holds only inside
   `scripts/graph_ask.sh`.
10. Every measurement comes from one Mac under swap pressure. No Linux run exists yet.
11. The agent evidence so far is from the old churn model and a 3B model; the new eval is designed and
    not run.

## Sources

Fetched 2026-09-30 unless marked; versions as stated.

- LadybugDB: https://github.com/LadybugDB/ladybug · https://pypi.org/project/ladybug/ (0.21.1) ·
  https://docs.ladybugdb.com/concurrency/ · https://docs.ladybugdb.com/extensions/ ·
  https://github.com/LadybugDB/ladybug/issues/1007 · https://github.com/LadybugDB/mcp-server-ladybug
- Kùzu: https://github.com/kuzudb/kuzu · https://github.com/kuzudb/kuzu-mcp-server
- Neo4j: https://neo4j.com/docs/operations-manual/current/installation/requirements/ ·
  https://github.com/neo4j/mcp
- Source-available licences: https://github.com/memgraph/memgraph/blob/master/LICENSE ·
  https://raw.githubusercontent.com/FalkorDB/FalkorDB/master/LICENSE.txt
- DuckPGQ: https://github.com/cwida/duckpgq-extension · DuckDB release calendar:
  https://duckdb.org/release_calendar
- Other engines: https://github.com/GrafeoDB/grafeo · https://github.com/oxigraph/oxigraph ·
  https://incubator.apache.org/projects/graphar.html
- Community detection: https://github.com/igraph/python-igraph · https://github.com/vtraag/leidenalg
- PyIceberg 0.12.0: https://pypi.org/project/pyiceberg/ ·
  https://github.com/apache/iceberg-python/releases/tag/pyiceberg-0.12.0 · https://py.iceberg.apache.org/api/
- Apache Iceberg branching and tagging: https://iceberg.apache.org/docs/latest/branching/
- sqlglot 30.20: https://github.com/tobymao/sqlglot · https://sqlglot.com/sqlglot/lineage.html
- OpenLineage: https://github.com/OpenLineage/OpenLineage/issues/4677 ·
  https://github.com/OpenLineage/OpenLineage/pull/4754
- MCP specification 2026-07-28: https://modelcontextprotocol.io/specification/2026-07-28/basic/security_best_practices ·
  https://modelcontextprotocol.io/specification/2026-07-28/server/tools
- OWASP Top 10 for LLM Applications 2025: https://genai.owasp.org/llm-top-10/
- Simon Willison, "The lethal trifecta for AI agents" (2025-06-16):
  https://simonwillison.net/2025/Jun/16/the-lethal-trifecta/
- Anthropic engineering: "Writing effective tools for agents" (2025-09-11)
  https://www.anthropic.com/engineering/writing-tools-for-agents · "Demystifying evals for AI agents"
  (2026-01-09) https://www.anthropic.com/engineering/demystifying-evals-for-ai-agents
- Claude Code docs: https://code.claude.com/docs/en/mcp · https://code.claude.com/docs/en/cli-reference
- τ-bench (pass^k): https://arxiv.org/abs/2406.12045
- uv resolution: https://docs.astral.sh/uv/concepts/resolution/ · pip secure installs:
  https://pip.pypa.io/en/stable/topics/secure-installs/
- Feast point-in-time joins: https://docs.feast.dev/getting-started/concepts/point-in-time-joins
- Boring Semantic Layer (considered for metrics later): https://github.com/boringdata/boring-semantic-layer
