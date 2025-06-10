import pathlib
from typing import Sequence

from datahub.sdk.entity import Entity
from datahub.testing import mce_helpers


def assert_entity_golden(
    entity: Entity,
    golden_path: pathlib.Path,
    ignore_paths: Sequence[str] = (),
) -> None:
    mce_helpers.check_goldens_stream(
        outputs=entity.as_mcps(),
        golden_path=golden_path,
        ignore_order=False,
        ignore_paths=ignore_paths,
    )
