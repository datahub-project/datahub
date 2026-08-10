# Ingestion / cloud packaging Bugbot rules

If `setup.py`, `pyproject.toml`, `requirements*.txt`, or a Dockerfile introduces
an editable (`-e`) or path dependency on `metadata-ingestion` for a published
or SaaS-deployed package, then:
- Blocking. Use a pinned released version.
- Title: "Pin published dependency"
- Label: packaging

If a license classifier changes, then:
- Flag for human confirmation that the classifier matches the package's actual
  license (policy, not auto-fix).
