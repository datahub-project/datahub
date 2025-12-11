# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import pytest
from pydantic import ValidationError

from datahub_actions.plugin.action.metadata_change_sync.metadata_change_sync import (
    MetadataChangeSyncAction,
)
from tests.unit.test_helpers import pipeline_context


def test_create():
    # Create with gms server
    MetadataChangeSyncAction.create(
        {"gms_server": "https://demo.datahub.com/"}, pipeline_context
    )

    # Create with no gms_server
    with pytest.raises(AssertionError):
        MetadataChangeSyncAction.create({}, pipeline_context)

    # Create with invalid type config
    with pytest.raises(ValidationError, match="extra_headers"):
        MetadataChangeSyncAction.create(
            {
                "gms_server": "https://demo.datahub.com/",
                "extra_headers": ["test", "action"],
            },
            pipeline_context,
        )


def test_close():
    # Nothing to Test
    pass
