# Vendored third-party files

Used by `src/lakehouse_graph/viz.py` (the standalone evidence / lineage HTML views). Nothing here
is fetched at runtime: `viz.load_vendored()` reads the file and refuses it unless its sha256 is
the pinned one, and every generated HTML inlines it, so the page opens offline.

| File | What | Licence |
|---|---|---|
| `cytoscape.min.js` | `dist/cytoscape.min.js` from npm `cytoscape@3.34.3`, unmodified | MIT (notice inside the file and in `cytoscape.LICENSE`) |
| `cytoscape.LICENSE` | `LICENSE` from the same npm tarball, unmodified | MIT, (c) 2016-2026 The Cytoscape Consortium |

Pins (checked by `tests/graph/test_viz.py`; the same constants live in `viz.py`):

- npm tarball: https://registry.npmjs.org/cytoscape/-/cytoscape-3.34.3.tgz
- npm integrity: `sha512-yfYGhRcGAntq6YBD583j4n0Eg3jIxvWmZtz/5uz9UYkeIStSlMxuUja+ec5j3iBD8nv1rwaOAYMW09tBdkSeaQ==`
- bytes: 435,503
- sha256: `5f3b5b529546d5af1fc5628590af033b74511a5b6f789f5f4682845863228b91` (`viz.CYTOSCAPE_SHA256`)
- SRI: `sha384-qPKQxl9uMXOw7vSTUDAnpUilhLuulovw6P5Z4db4bqxW5VhumS7przEmHX0iM0Oc` (`viz.CYTOSCAPE_SRI`, used only by
  the opt-in `--cdn` mode; jsDelivr, unpkg and cdnjs serve byte-identical files)

Update (a dependency step, the only time the network is used): download the new npm tarball, check
its `dist.integrity` against the registry, copy `package/dist/cytoscape.min.js` and `package/LICENSE`
here unmodified, then change `CYTOSCAPE_VERSION`, `CYTOSCAPE_SHA256` and `CYTOSCAPE_SRI` in `viz.py`
and re-check `CYTOSCAPE_INJECTED_STYLE` (the text of the one `<style>` element the library injects;
`test_csp_hashes_cover_exactly_the_inline_code` fails if a release changes it). Run
`pytest tests/graph/test_viz.py`.
