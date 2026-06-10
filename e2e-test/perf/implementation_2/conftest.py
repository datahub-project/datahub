"""
Session-level pytest fixtures for Grasshopper-based DataHub perf tests.
Grasshopper's complete_configuration fixture is provided automatically by the
locust-grasshopper pytest plugin registered on install.  This conftest:
  - Adds the implementation_2 root to sys.path so framework/ and journeys/ are importable
  - Exposes DataHub-specific env vars to Grasshopper's configuration chain
"""
import sys
from pathlib import Path

import pytest

# Allow `from framework.datahub_journey import ...` and `from journeys.search_journey import ...`
sys.path.insert(0, str(Path(__file__).parent))


@pytest.fixture(scope="session")
def extra_env_var_keys() -> list[str]:
    """Expose DataHub-specific env vars into Grasshopper's configuration hierarchy."""
    return ["DATAHUB_GMS_TOKEN", "DATAHUB_GMS_URL"]
