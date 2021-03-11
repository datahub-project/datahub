import os
import sys
import time

import pytest

# See https://stackoverflow.com/a/33515264.
sys.path.append(os.path.join(os.path.dirname(__file__), "test_helpers"))

pytest_plugins = ["tests.integration.fixtures.sql_fixtures"]


@pytest.fixture
def mock_time(monkeypatch):
    def fake_time():
        return 1615443388.0975091

    monkeypatch.setattr(time, "time", fake_time)
    yield
