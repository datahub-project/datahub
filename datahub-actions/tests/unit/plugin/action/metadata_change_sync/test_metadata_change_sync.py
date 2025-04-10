import pytest
from pydantic import ValidationError

from datahub_actions.plugin.action.metadata_change_sync.metadata_change_sync import (
    MetadataChangeSyncAction,
)
from tests.unit.test_helpers import pipeline_context


def test_create():
    # Create with gms server
    MetadataChangeSyncAction.create(
        {"gms_server": "https://demo.datahubproject.io/"}, pipeline_context
    )

    # Create with no gms_server
    with pytest.raises(AssertionError):
        MetadataChangeSyncAction.create({}, pipeline_context)

    # Create with invalid type config
    with pytest.raises(ValidationError, match="extra_headers"):
        MetadataChangeSyncAction.create(
            {
                "gms_server": "https://demo.datahubproject.io/",
                "extra_headers": ["test", "action"],
            },
            pipeline_context,
        )


def test_close():
    # Nothing to Test
    pass
