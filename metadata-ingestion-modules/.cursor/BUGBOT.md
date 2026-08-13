# Ingestion / published-package Bugbot rules

## ES usage / ranking queries

If `query_builder.py` or usage reporters filter URN / ID fields, then:

- Require an exact keyword mapping: use `.keyword` only when that subfield
  exists; if the field is already mapped as `keyword`, query it directly (do not
  invent `urn.keyword` on an already-keyword field).
- Critical when an analyzed URN/ID field is queried as if it were exact.
- Prefer ES-side prefix/term filters over full event scans + client filter when
  an equivalent indexed exact field exists.

## Packaging

If `setup.py`, `pyproject.toml`, `requirements*.txt`, or a Dockerfile introduces
an editable (`-e`) or path dependency on `metadata-ingestion` for a published
package, then:

- Blocking. Use a pinned released version.
- Title: "Pin published dependency"
- Label: packaging

If a license classifier changes, then:

- Flag for human confirmation that the classifier matches the package's actual
  license (policy, not auto-fix).

## Unbounded jobs

If usage/ranking/bootstrap jobs stream or read unbounded history by default, then:

- High OOM/cost risk; default off or bound lookback.
