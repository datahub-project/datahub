# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
