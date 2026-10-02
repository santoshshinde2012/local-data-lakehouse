# Churn parity (`make churn-parity`): Spark SQL gold vs the pandas twin

Captured on 2026-10-02 (IST) on a MacBook Pro (Apple M1 Pro, 16 GB, macOS 26.6.2; Docker Desktop 29.8.1, Compose 5.5.1, VM 10 CPUs / 7.65 GiB), branch `feat/local-first-stack-2026` at `7f5fc43`, in one run from empty volumes (`make purge` first). Real console output; trimmed only for noise (Spark INFO/WARN logs, docker build and container progress lines, pip notices). `<repo>` is the checkout, `~` the home directory; lines longer than 200 characters end in `…`. Index: [README.md](README.md).

Exit 0, 14.6 s.

```text
$ make churn-parity
.venv-graph-spark/bin/python scripts/check_gold_parity.py
Gold parity OK: 8001 renewals × 27 columns match (Spark SQL vs pandas) on <repo>/data/sample/churn
```

Exact within 1e-4: the two `accept_rate_change` cells that Spark's `bround` and numpy round differently ([../graph/lakehouse-twin.md](../graph/lakehouse-twin.md#why-gold-drifts-by-1e-4-in-two-cells)).
