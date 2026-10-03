# Contributing

Thanks for helping with local-data-lakehouse. Keep pull requests small and focused, and keep the
docs in step with the code in the same pull request.

## Set up

```bash
git clone https://github.com/santoshshinde2012/local-data-lakehouse.git
cd local-data-lakehouse
cp .env.example .env          # local-only credentials
make venv                     # .venv (Python 3.12) from the hash-locked requirements.txt
make graph-venv               # optional: .venv-graph for the graph layer
```

You need Docker with Compose v2 and [uv](https://docs.astral.sh/uv/). `make help` lists every target.

## Before you open a pull request

| Check | Command | Needs Docker |
|---|---|---|
| Unit and static checks, file names, links | `make test-t0` | no |
| Graph layer | `make graph-test` | no |
| Catalog contract | `make test-t1` | yes |
| Light-profile smoke | `make test-t2` | yes |
| Full-profile engine parity | `make test-t3` | yes |

CI runs the same tiers (`t0-unit`, `graph`, `t2-light`, and `t3-full` when the pull request has the
`full-stack` label). Record what you ran in the pull request body.

## File names

One convention for the whole repo, checked by `make docs-check` (`scripts/check_docs.py`), by
`tests/unit/test_file_naming.py` in T0, and by a CI step in `t0-unit`:

| Where | Rule | Examples |
|---|---|---|
| Repo root, standard files | Conventional uppercase names | `README.md`, `CONTRIBUTING.md`, `MIGRATION.md`, `RESULTS.md`, `LICENSE` |
| Any folder, directory index | `README.md` | `docs/graph/README.md` |
| Other `.md` and `.mmd` files | Lowercase kebab-case; dots only between parts | `docs/object-store.md`, `docs/demo/churn-e2e.excerpt.md`, `lineage-limit-hits-14d.mmd` |
| Everything under `docs/` and `results/` | Lowercase, no spaces (`a-z 0-9 . _ -`) | `docs/demo/img/airflow-lakehouse_churn.png` |
| Data files | Lowercase `snake_case` | `churn_user_features.csv`, `hero_inference_record.json` |
| Python | PEP 8: `snake_case.py` modules, tests `test_*.py` | `scripts/build_churn_gold_local.py` |
| Shell, SQL, YAML | Lowercase `snake_case` or kebab-case, as already used in that folder | `pipelines/radar_consume.sh` |

Tool-required names are the only exceptions (for example a Claude Code skill's `SKILL.md`). Rename
with `git mv` so history follows the file, then run `make docs-check`: it also fails on any relative
link or `#anchor` in a tracked Markdown file that no longer resolves.

## Diagrams

Mermaid diagrams follow [docs/diagrams.md](docs/diagrams.md): the repo palette, GitHub-safe syntax,
and a render check with mermaid-cli. `tests/unit/test_mermaid_diagrams.py` checks every Mermaid block
and `.mmd` file in T0.

## Results

Numbers in [RESULTS.md](RESULTS.md) and `docs/demo/*.excerpt.md` come from real runs. When you re-run
a step, replace the excerpt and the table row together and note the date and commit.

## Licence

By contributing you agree that your work is released under the [MIT licence](LICENSE).
