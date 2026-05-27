from typing import cast
from unittest.mock import MagicMock

from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig
from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport
from datahub.ingestion.source.kinesis.kinesis_stream import KinesisStreamExtractor
from datahub.ingestion.source.kinesis.kinesis_tagging import (
    extract_owner_urns_from_tags,
)


def _make_stream_extractor(owner_tag_key: str = "owner") -> KinesisStreamExtractor:
    config = KinesisSourceConfig.model_validate(
        {"aws_config": {"aws_region": "us-east-1"}, "owner_tag_key": owner_tag_key}
    )
    return KinesisStreamExtractor(
        config=config,
        report=KinesisSourceReport(),
        session=MagicMock(),
        region_key=MagicMock(),
    )


class TestStreamTagsAndOwnership:
    def test_global_tags_from_aws_tags(self):
        ex = _make_stream_extractor()
        # boto3 stubs type _kinesis methods as plain Callables; cast back to
        # MagicMock to expose the return_value helper to mypy.
        kinesis_mock = cast(MagicMock, ex._kinesis)
        kinesis_mock.list_tags_for_stream.return_value = {
            "Tags": [
                {"Key": "owner", "Value": "data-team"},
                {"Key": "env", "Value": "prod"},
            ]
        }
        tags = ex.fetch_tags("events")
        assert {"Key": "owner", "Value": "data-team"} in tags
        assert {"Key": "env", "Value": "prod"} in tags

    def test_ownership_extracted_from_owner_tag(self):
        ex = _make_stream_extractor(owner_tag_key="owner")
        kinesis_mock = cast(MagicMock, ex._kinesis)
        kinesis_mock.list_tags_for_stream.return_value = {
            "Tags": [{"Key": "owner", "Value": "data-team"}]
        }
        owners = extract_owner_urns_from_tags(
            ex.fetch_tags("events"), ex.config.owner_tag_key
        )
        # Owner URN is always a corpuser, matching the dominant DataHub
        # connector pattern. DataHub's identity layer handles
        # group-membership mapping at the platform level.
        assert owners == ["urn:li:corpuser:data-team"]

    def test_no_owner_tag_returns_empty_owners(self):
        owners = extract_owner_urns_from_tags(
            [{"Key": "env", "Value": "prod"}], "owner"
        )
        assert owners == []

    def test_custom_owner_tag_key_honored(self):
        owners = extract_owner_urns_from_tags(
            [{"Key": "owner", "Value": "x"}, {"Key": "Team", "Value": "analytics"}],
            "Team",
        )
        # Should pick "Team" not "owner" because owner_tag_key="Team"
        assert owners == ["urn:li:corpuser:analytics"]


class TestExtractOwnerUrnsAlwaysCorpuser:
    """Owner URNs are always corpuser, regardless of whether the tag value
    looks like an email or a group slug.

    Earlier the connector tried to be clever (`@` in value → corpuser, else
    corpGroup), but no other DataHub source connector uses that heuristic —
    they all emit `make_user_urn(value)` directly and rely on DataHub's
    identity layer for group-membership mapping. Using the same pattern here
    keeps owner URNs consistent across sources when the same identifier
    appears in multiple platforms.
    """

    def test_email_owner_becomes_corpuser_urn(self):
        owners = extract_owner_urns_from_tags(
            [{"Key": "owner", "Value": "alice@acme.io"}], "owner"
        )
        assert owners == ["urn:li:corpuser:alice@acme.io"]

    def test_group_slug_owner_still_becomes_corpuser_urn(self):
        """A tag value like `data-team` (no `@`) was previously emitted as
        a corpGroup URN. Verify it now emits as corpuser to match the
        cross-connector convention.
        """
        owners = extract_owner_urns_from_tags(
            [{"Key": "owner", "Value": "data-team"}], "owner"
        )
        assert owners == ["urn:li:corpuser:data-team"]
