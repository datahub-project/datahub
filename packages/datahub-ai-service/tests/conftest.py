import pytest
from helpers import FakeGms


@pytest.fixture
def gms() -> FakeGms:
    return FakeGms()
