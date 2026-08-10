# Ingestion / cloud packaging Bugbot rules

## ES usage / ranking queries

If `query_builder.py` or usage reporters filter URN fields, then:
- Require `.keyword` on URN/ID fields; Critical if missing.
- Prefer ES-side prefix filters over full event scans + client filter.

## Packaging

If `setup.py`, `pyproject.toml`, `requirements*.txt`, or a Dockerfile introduces
an editable (`-e`) or path dependency on `metadata-ingestion` for a published
or SaaS-deployed package, then:
- Blocking. Use a pinned released version.
- Title: "Pin published dependency"
- Label: packaging

If a license classifier changes, then:
- Flag for human confirmation that the classifier matches the package's actual
  license (policy, not auto-fix).

## Unbounded jobs

If usage/ranking/bootstrap jobs stream or read unbounded history by default, then:
- High OOM/cost risk; default off or bound lookback.
