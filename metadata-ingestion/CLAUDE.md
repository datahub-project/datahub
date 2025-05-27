# DataHub Metadata Ingestion - Claude Development Notes

## Python Environment Setup

**IMPORTANT**: Always use the local venv when working with metadata-ingestion Python code:

```bash
# Activate the metadata-ingestion venv
cd metadata-ingestion
source venv/bin/activate

# Run tests using the activated venv
python -m pytest tests/integration/iceberg/test_iceberg.py::test_multiprocessing_iceberg_ingest -v
```

## Notes

- Never run `python -m pytest` from the root datahub directory for metadata-ingestion tests
- Always cd into metadata-ingestion/ first and use the local venv
- This ensures proper dependency resolution and environment isolation
