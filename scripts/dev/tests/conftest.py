"""Every dev-tool test gets an isolated HOME, registry and local stack root."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import datahub_dev
import datahub_dev_local as local


@pytest.fixture(autouse=True)
def isolated_dev_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(local, "_http_ok", lambda *args, **kwargs: False)
    directory = tmp_path / ".datahub/dev"
    monkeypatch.setattr(datahub_dev, "DATAHUB_DEV_DIR", directory)
    monkeypatch.setattr(datahub_dev, "INSTANCES_FILE", directory / "instances.json")
    monkeypatch.setattr(datahub_dev, "CONFIG_FILE", directory / "config.json")
    monkeypatch.setattr(local, "LOCAL_DATA_ROOT", tmp_path / ".datahub/local/stacks")
    monkeypatch.setattr(
        local, "SHARED_OPENSEARCH_DIR", tmp_path / ".datahub/local/opensearch-3"
    )


# These are standalone paid agent experiments, rather than pytest suites.
collect_ignore = ["test_agent_evals.py", "test_evals_operate.py", "test_evals_query.py"]
