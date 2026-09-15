# DataHub Library Examples

This directory contains examples demonstrating how to use the DataHub Python SDK and metadata emission APIs.

## Structure

Each example is a standalone Python script that demonstrates a specific use case:

- **Create examples**: Show how to create new metadata entities
- **Update examples**: Show how to modify existing metadata
- **Query examples**: Show how to read and query metadata
- **Delete examples**: Show how to remove metadata

## Writing Examples

Each example is a plain, standalone script. Keep them simple enough to copy, paste and run:

- **No test-specific code.** Don't add injectable `client=` / `emitter=` parameters or other
  hooks that exist only for tests. Examples ship verbatim into the docs site, so anything you
  add is something every reader has to read past.
- **Read connection details from the environment**, so the same script works locally and in CI:

  ```python
  import os

  gms_server = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
  token = os.getenv("DATAHUB_GMS_TOKEN")
  ```

  `DataHubClient.from_env()` and `get_default_graph()` both do this for you.

- **Let failures raise.** Python exits non-zero on an unhandled exception, which is how the
  integration tests detect breakage. Don't wrap the whole script in `try`/`except` that
  swallows the error and exits 0 — a silent failure looks identical to success.
- **Exit non-zero when a lookup finds nothing.** `graph.get_aspect()` returns `None` for a
  missing entity or aspect rather than raising, so printing "not found" and falling off the
  end of the script exits 0 and makes a broken example indistinguishable from a working one:

  ```python
  props = graph.get_aspect(entity_urn=str(urn), aspect_type=TagPropertiesClass)
  if props is None:
      raise SystemExit(f"Tag not found: {urn}")

  print(f"Tag name: {props.name}")
  ```

  This is for the **primary** thing the example is about. Genuinely optional aspects —
  ownership, tags, terms an entity may legitimately not have — keep a quiet
  `if x is not None:` guard. (`client.entities.get()` already raises `ItemNotFoundError`,
  so SDK-based examples get this for free.)

- **Print what you looked up.** For read/query examples, print the values you fetched — it
  is what makes the example worth reading.
- **Use `print()`, not `logging`.** Every example in this directory prints. `import logging`
  plus `getLogger` plus `basicConfig` is three lines of scaffolding that has nothing to do
  with the DataHub API being demonstrated, and these files are inlined verbatim into the docs
  site. (Smoke tests use `logger.info()` instead, because their output interleaves under
  parallel execution — that rule is theirs, not ours.)

## Testing

Examples are covered by two layers.

### 1. Lint — every example, every PR

`./gradlew :metadata-ingestion:lint` runs both ruff and mypy over `examples/`. mypy catches
wrong constructor arity, misspelled imports, bad aspect field names, and `Optional` mistakes
without needing a DataHub instance:

```bash
./gradlew :metadata-ingestion:lint      # ruff + mypy
./gradlew :metadata-ingestion:lintFix   # auto-fix ruff findings
```

### 2. Integration — examples listed in the manifest

`smoke-test/tests/library_examples/` executes examples as scripts against a running DataHub
instance, in dependency order, and requires each to exit 0. See that directory's `README.md`
for how to add an example to `EXAMPLE_MANIFEST`.

```bash
cd smoke-test && source venv/bin/activate
pytest tests/library_examples/ -v
```

Lint is the safety net for all examples; the manifest is the safety net for the ones that
matter most. If you add a read/query example, add it to the manifest — a wrong endpoint or a
missing aspect is invisible to mypy and only shows up when the script actually runs, and
only fails the build if the example exits non-zero as described above.

## Guidelines

1. **Keep examples simple**: focus on demonstrating one concept clearly
2. **Use realistic data**: URNs, names, and values should look like real-world usage
3. **Add comments**: explain non-obvious choices or important details, not what the code
   already says
4. **Prefer a flat script**: top-to-bottom statements read better inlined into docs than a
   function you then have to call. Only factor something out when the example is genuinely
   about that function
5. **Fail loudly**: see the exit-code rule above — a missing entity should end the script,
   not print "not found" and exit 0
6. **Add read/query examples to the manifest**: `smoke-test/tests/library_examples/example_manifest.py`,
   after whichever CREATE example seeds the entity they read

## Example Categories

### Entity Creation

- `notebook_create.py` - Create a notebook entity
- `data_platform_create.py` - Create a custom data platform
- `glossary_term_create.py` - Create glossary terms

### Metadata Updates

- `dataset_add_term.py` - Add glossary terms to datasets
- `dataset_add_owner.py` - Add ownership information
- `notebook_add_tags.py` - Add tags to notebooks

### Querying Metadata

- `dataset_query_deprecation.py` - Check if a dataset is deprecated
- `search_with_query.py` - Search for entities
- `lineage_column_get.py` - Query column-level lineage

## Getting Help

- [DataHub Documentation](https://datahubproject.io/docs/)
- [Python SDK Reference](https://datahubproject.io/docs/python-sdk/)
- [Metadata Model](https://datahubproject.io/docs/metadata-model/)
- [GitHub Issues](https://github.com/datahub-project/datahub/issues)
