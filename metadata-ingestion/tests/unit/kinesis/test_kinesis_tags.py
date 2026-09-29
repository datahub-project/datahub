from typing import cast
from unittest.mock import MagicMock

from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig
from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport
from datahub.ingestion.source.kinesis.kinesis_stream import KinesisStreamExtractor
from datahub.ingestion.source.kinesis.kinesis_tagging import (
    build_global_tags_from_aws_tags,
)


def _make_stream_extractor() -> KinesisStreamExtractor:
    config = KinesisSourceConfig.model_validate(
        {"aws_config": {"aws_region": "us-east-1"}}
    )
    return KinesisStreamExtractor(
        config=config,
        report=KinesisSourceReport(),
        session=MagicMock(),
        region_key=MagicMock(),
    )


class TestStreamTags:
    def test_fetch_tags_returns_all_aws_tags(self):
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

    def test_global_tags_built_as_key_value_urns(self):
        global_tags = build_global_tags_from_aws_tags(
            [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "analytics"}]
        )
        assert global_tags is not None
        urns = {t.tag for t in global_tags.tags}
        assert "urn:li:tag:env:prod" in urns
        assert "urn:li:tag:team:analytics" in urns

    def test_no_tags_returns_none(self):
        assert build_global_tags_from_aws_tags([]) is None

    def test_empty_value_tag_emitted_as_key_only_urn(self):
        # AWS allows key-only tags (empty Value); they must NOT be silently
        # dropped — emit a key-only urn:li:tag:<Key> instead.
        global_tags = build_global_tags_from_aws_tags(
            [{"Key": "pii", "Value": ""}, {"Key": "tier", "Value": "gold"}]
        )
        assert global_tags is not None
        urns = {t.tag for t in global_tags.tags}
        assert "urn:li:tag:pii" in urns
        assert "urn:li:tag:tier:gold" in urns

    def test_tag_without_key_is_skipped(self):
        # A tag with no Key is unusable — skip it (can't form a tag URN).
        assert build_global_tags_from_aws_tags([{"Key": "", "Value": "x"}]) is None
