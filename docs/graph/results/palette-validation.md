# Chart palette validation

Written by hand when the chart palette was chosen (2026-10-01); `make graph-evidence` does not
regenerate this file. Re-run it whenever `src/lakehouse_graph/charts.py` changes a colour.

The charts use the first four slots of a fixed categorical palette, in a fixed order, never cycled:
blue, orange, aqua, yellow (light `#2a78d6 #eb6834 #1baf7a #eda100`; dark
`#3987e5 #d95926 #199e70 #c98500`). Surfaces: light `#fcfcfb`, dark `#1a1a19`. Each set a chart uses
was run through the reference palette validator (`validate_palette.js` of the dataviz method: OKLCH
lightness band and chroma floor, CVD separation under protanopia / deuteranopia with Machado 2009 at
severity 1.0, a normal-vision floor, contrast against the surface), in both modes:

| Set | Used by | Pairs checked | Light | Dark |
|---|---|---|---|---|
| 2 slots (blue, orange) | leak surface, naive vs point in time, neighbour graph, lapse rates, latency, leakage | adjacent | pass | pass |
| 3 slots (+ aqua) | cohort dot plot (colour = plan) | all pairs | pass, contrast WARN on aqua (2.74:1) | pass |
| 4 slots (+ yellow) | inc-002 blast radius (stacked) | adjacent | pass, contrast WARN on aqua (2.74:1) and yellow (2.11:1) | pass |

The two light-mode contrast warnings oblige a relief channel: every chart ships its numbers as a
table next to the picture, segment values are printed inside the bars where they fit, and series are
also told apart by shape (dot, square, diamond) or by a direct label. Text never uses a series colour.

## Validator output

```text
### node validate_palette.js "#2a78d6,#eb6834" --mode light --pairs adjacent
Palette (light, surface #fcfcfb, categorical): 2 slots
  [PASS] Lightness band         all 2 inside L 0.43–0.77
  [PASS] Chroma floor           all 2 >= 0.1
  [PASS] CVD separation         worst adjacent #eb6834↔#2a78d6 ΔE 24.7 (protan) · tritan 32.7
  [PASS] Normal-vision floor    worst adjacent #eb6834↔#2a78d6 ΔE 33.6 (normal)
  [PASS] Contrast vs surface    all 2 >= 3:1
  → ALL CHECKS PASS
exit 0

### node validate_palette.js "#3987e5,#d95926" --mode dark --pairs adjacent
Palette (dark, surface #1a1a19, categorical): 2 slots
  [PASS] Lightness band         all 2 inside L 0.48–0.67
  [PASS] Chroma floor           all 2 >= 0.1
  [PASS] CVD separation         worst adjacent #d95926↔#3987e5 ΔE 26.8 (protan) · tritan 32.4
  [PASS] Normal-vision floor    worst adjacent #d95926↔#3987e5 ΔE 31.8 (normal)
  [PASS] Contrast vs surface    all 2 >= 3:1
  → ALL CHECKS PASS
exit 0

### node validate_palette.js "#2a78d6,#eb6834,#1baf7a" --mode light --pairs all
Palette (light, surface #fcfcfb, categorical): 3 slots
  [PASS] Lightness band         all 3 inside L 0.43–0.77
  [PASS] Chroma floor           all 3 >= 0.1
  [PASS] CVD separation         worst all-pairs #1baf7a↔#eb6834 ΔE 9.2 (deutan) · tritan 9.6
  [PASS] Normal-vision floor    worst all-pairs #1baf7a↔#2a78d6 ΔE 24.0 (normal)
  [WARN] Contrast vs surface    below 3:1 — relief required (visible labels or table view): [["#1baf7a",2.74]]
  → ALL CHECKS PASS
exit 0

### node validate_palette.js "#3987e5,#d95926,#199e70" --mode dark --pairs all
Palette (dark, surface #1a1a19, categorical): 3 slots
  [PASS] Lightness band         all 3 inside L 0.48–0.67
  [PASS] Chroma floor           all 3 >= 0.1
  [PASS] CVD separation         worst all-pairs #199e70↔#d95926 ΔE 9.4 (deutan) · tritan 4.0
  [PASS] Normal-vision floor    worst all-pairs #199e70↔#3987e5 ΔE 20.9 (normal)
  [PASS] Contrast vs surface    all 3 >= 3:1
  → ALL CHECKS PASS
exit 0

### node validate_palette.js "#2a78d6,#eb6834,#1baf7a,#eda100" --mode light --pairs adjacent
Palette (light, surface #fcfcfb, categorical): 4 slots
  [PASS] Lightness band         all 4 inside L 0.43–0.77
  [PASS] Chroma floor           all 4 >= 0.1
  [PASS] CVD separation         worst adjacent #eda100↔#1baf7a ΔE 9.1 (protan) · tritan 27.0
  [PASS] Normal-vision floor    worst adjacent #eda100↔#1baf7a ΔE 22.9 (normal)
  [WARN] Contrast vs surface    below 3:1 — relief required (visible labels or table view): [["#1baf7a",2.74],["#eda100",2.11]]
  → ALL CHECKS PASS
exit 0

### node validate_palette.js "#3987e5,#d95926,#199e70,#c98500" --mode dark --pairs adjacent
Palette (dark, surface #1a1a19, categorical): 4 slots
  [PASS] Lightness band         all 4 inside L 0.48–0.67
  [PASS] Chroma floor           all 4 >= 0.1
  [PASS] CVD separation         worst adjacent #c98500↔#199e70 ΔE 8.4 (protan) · tritan 24.4
  [PASS] Normal-vision floor    worst adjacent #c98500↔#199e70 ΔE 19.8 (normal)
  [PASS] Contrast vs surface    all 4 >= 3:1
  → ALL CHECKS PASS
exit 0
```

[Back to the results index](index.md)
