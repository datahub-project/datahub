import pathlib

import pytest

from datahub.sdk._entity import Entity
from tests.test_helpers import mce_helpers


def assert_entity_golden(
    pytestconfig: pytest.Config, entity: Entity, golden_path: pathlib.Path
) -> None:
    mce_helpers.check_goldens_stream(
        pytestconfig=pytestconfig,
        outputs=entity._as_mcps(),
        golden_path=golden_path,
        ignore_order=False,
    )
