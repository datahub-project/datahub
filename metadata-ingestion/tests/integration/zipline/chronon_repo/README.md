# Zipline/Chronon test fixtures

This directory holds the **Chronon Python DSL** used to generate the compiled
thrift-as-JSON fixtures in `../production/`. Committing the DSL (not just its
output) keeps the fixtures reproducible and provably real: `compile.py` output
is what a Zipline user actually feeds the connector.

## Regenerating `../production/`

The compiler ships with `chronon-ai`, which is intentionally **not** a
dependency of `metadata-ingestion` (it is heavy and only needed to refresh
fixtures). Install it into a throwaway environment and run the compiler over
each config:

```bash
python -m venv /tmp/chronon && /tmp/chronon/bin/pip install chronon-ai
cd metadata-ingestion/tests/integration/zipline/chronon_repo
for conf in $(find group_bys joins staging_queries -name '*.py' ! -name '__init__.py'); do
  /tmp/chronon/bin/python -m ai.chronon.repo.compile \
    --chronon_root . --conf "$conf" --force-overwrite -y
done
cp -R production ../production && rm -rf production
```

Then regenerate the golden:

```bash
pytest tests/integration/zipline/test_zipline.py::test_zipline_ingest --update-golden-files
```

## Automated check

`test_zipline_compiles_from_real_chronon_build` performs this compile at test
time (guarded by `pytest.importorskip("ai.chronon")`) and asserts the connector
reproduces the golden — so the checked-in fixtures can never silently drift from
a genuine build. It is skipped automatically when `chronon-ai` is absent.
