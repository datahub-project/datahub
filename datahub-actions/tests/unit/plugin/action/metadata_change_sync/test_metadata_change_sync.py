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


def test_create_with_auth_provider():
    # A declarative auth config (OAuth token provider) must land on the
    # emitter's session so tokens refresh per request.
    from datahub.emitter.token_provider import TokenProviderAuth

    action = MetadataChangeSyncAction.create(
        {
            "gms_server": "https://destination.example.com/",
            "auth": {"type": "static", "config": {"token": "provider-tok"}},
        },
        pipeline_context,
    )
    assert isinstance(action.rest_emitter._session.auth, TokenProviderAuth)  # type: ignore[attr-defined]


def test_auth_and_token_mutually_exclusive():
    with pytest.raises(ValidationError, match="mutually exclusive"):
        MetadataChangeSyncAction.create(
            {
                "gms_server": "https://destination.example.com/",
                "gms_auth_token": "static-pat",
                "auth": {"type": "static", "config": {"token": "t"}},
            },
            pipeline_context,
        )
