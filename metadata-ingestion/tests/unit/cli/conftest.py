from pathlib import Path
from typing import Iterator

import pytest

from datahub.cli.config_utils import reset_profile_cache


@pytest.fixture(autouse=True)
def isolated_cli_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("DATAHUB_PROFILE", raising=False)
    reset_profile_cache()
    yield
    reset_profile_cache()
