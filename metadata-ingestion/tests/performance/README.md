# Performance Testing
This module provides a framework for performance testing our ingestion sources.

When running a performance test, make sure to output print statements and live logs:
```bash
pytest -s --log-cli-level=INFO -m performance tests/performance/<test_name>.py
```
