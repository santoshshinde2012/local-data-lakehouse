# Diagrams: palette and rules

Every Mermaid diagram in this repo uses the same palette and syntax. That covers the ```` ```mermaid ```` blocks in the Markdown pages, the end-to-end architecture in [`demo/README.md`](demo/README.md#architecture), and the generated diagrams in [`graph/results/mermaid/`](graph/results/mermaid/), which `src/lakehouse_graph/charts.py` writes. `tests/unit/test_mermaid_diagrams.py` (T0) checks every block against this page. [retention-radar](https://github.com/santoshshinde2012/retention-radar/blob/main/docs/diagrams.md) uses the same palette and rules, so the lakehouse-to-radar flow in both READMEs looks the same.

## Palette

Each layer has one colour. All text is dark (`#0F172A`) on a light fill, and every pair passes WCAG AA (text contrast 14.6:1 to 16.5:1). Edges are `#64748B`: 4.8:1 on white and 4.0:1 on GitHub's dark background. Subgraphs (the profiles and overlays) are white with a `#64748B` border, so their titles read the same in GitHub's light and dark modes.

| Layer | `classDef` | Fill | Border | Used for |
|---|---|---|---|---|
| Object storage | `storage` | `#DBEAFE` | `#1D4ED8` | RustFS, SILO, Iceberg tables on the store |
| Catalog | `catalog` | `#FEF3C7` | `#B45309` | Lakekeeper, its Postgres, lakehouse-init |
| Compute | `compute` | `#ECFCCB` | `#4D7C0F` | Spark, Trino, DuckDB, PyIceberg, Polars, the pandas twin |
| Orchestration | `orchestration` | `#FCE7F3` | `#BE185D` | Airflow, its socket proxy, the Docker graph overlay DAG |
| Graph layer | `graphlayer` | `#CCFBF1` | `#0F766E` | graph build, Parquet/LadybugDB, contracts, lineage, cohorts; ER entities |
| Consumers | `consumer` | `#FFEDD5` | `#C2410C` | retention-radar, MCP servers, agents |
| Data | `data` | `#F1F5F9` | `#475569` | files, exports, source code, lineage columns |

A dashed border (`style <id> stroke-dasharray:5 5`) marks an optional profile or overlay, or an experimental part.

## Rules

- The first line of every diagram is the shared `%%{init: …}%%` line: the `base` theme with the variables above (copy it from any diagram, or use `charts.MERMAID_INIT`). After it comes `flowchart LR` (or `flowchart TB`, or `erDiagram`), then the seven `classDef` lines exactly as in the table, and `class` statements.
- Lay diagrams out left to right. Each profile or overlay (light, full, trino, Airflow, graph) is a `subgraph`. Link a whole subgraph when the edge means the whole profile; Mermaid ignores a subgraph's `direction` once one of its nodes links outside it.
- Quote every label and every edge label (`A -->|"REST + vended credentials"| B`). The only HTML allowed is `<br/>`; GitHub strips the rest.
- `erDiagram` has no `classDef`: GitHub's renderer may not support it there, so entities take the graph-layer colours from the init line.
- Render after every change with mermaid-cli 12 (`npx -y @mermaid-js/mermaid-cli@12.0.0 -i <file> -o <file>.png -s 3 -b white`; for a Markdown page, `-i page.md` renders each block).
