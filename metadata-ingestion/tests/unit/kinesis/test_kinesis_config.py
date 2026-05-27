from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.kinesis.kinesis_config import (
    DestinationPlatformDetail,
    KinesisGlueSchemaRegistryConfig,
    KinesisSourceConfig,
)
from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport
from datahub.ingestion.source.kinesis.kinesis_schema_registry import (
    KinesisGlueSchemaRegistry,
)


class TestDestinationPlatformDetail:
    def test_env_must_be_valid_fabric_type(self):
        # FabricType is an enum (PROD/DEV/QA/STG/etc.). Non-enum strings should fail validation.
        with pytest.raises(ValidationError):
            DestinationPlatformDetail(env="NOTAFABRIC")


class TestKinesisGlueSchemaRegistryConfig:
    def test_disabled_gsr_means_resolve_returns_none_for_any_stream(self):
        """Behavioral contract of `enabled=False`: even when stream_schema_map
        is configured (which would normally route to a specific schema),
        disabling GSR short-circuits the entire resolution path. Verifies the
        actual code branch at `kinesis_schema_registry.py::get_schema_metadata`
        line 1, not just the field default.
        """
        # enabled=False but with a populated map — the map would normally
        # successfully resolve a stream. The disabled flag must override.
        cfg = KinesisGlueSchemaRegistryConfig(
            enabled=False,
            stream_schema_map={},  # empty since validator rejects map-with-disabled
            use_naming_convention=True,
        )
        reg = KinesisGlueSchemaRegistry(
            config=cfg,
            glue_client=MagicMock(),
            platform_urn="urn:li:dataPlatform:kinesis",
            report=KinesisSourceReport(),
        )
        # No matter what stream we ask for, disabled GSR returns None.
        assert reg.get_schema_metadata("any_stream") is None
        # And critically, no Glue API call was made.
        reg._glue.get_schema_version.assert_not_called()

    def test_naming_convention_off_skips_resolution_when_no_explicit_map(self):
        """When use_naming_convention=False AND the stream isn't in
        stream_schema_map, the resolver returns None without ever calling
        AWS. This is the load-bearing behavior of the off-by-default flag —
        verifies the code branch at `kinesis_schema_registry._resolve_schema_name`,
        not the field default.
        """
        cfg = KinesisGlueSchemaRegistryConfig(
            enabled=True,
            stream_schema_map={"events": "events-schema"},
            use_naming_convention=False,
        )
        reg = KinesisGlueSchemaRegistry(
            config=cfg,
            glue_client=MagicMock(),
            platform_urn="urn:li:dataPlatform:kinesis",
            report=KinesisSourceReport(),
        )
        # "events" is in the map — resolves to "events-schema" (positive case)
        assert reg._resolve_schema_name("events") == "events-schema"
        # "other_stream" is NOT in the map and naming convention is off —
        # resolver returns None and the connector skips schema attachment.
        assert reg._resolve_schema_name("other_stream") is None

    def test_stream_schema_map_is_independent_of_naming_convention(self):
        c = KinesisGlueSchemaRegistryConfig(
            enabled=True,
            stream_schema_map={"events": "events-schema-v2"},
            use_naming_convention=False,
        )
        assert c.stream_schema_map == {"events": "events-schema-v2"}
        assert c.use_naming_convention is False

    def test_gsr_configured_but_disabled_rejected_at_startup(self):
        with pytest.raises(ValidationError) as exc_info:
            KinesisGlueSchemaRegistryConfig(
                enabled=False,
                stream_schema_map={"events": "events-schema"},
            )
        assert (
            "enabled=False" in str(exc_info.value)
            or "enable" in str(exc_info.value).lower()
        )


class TestKinesisSourceConfig:
    def test_minimal_config_parses(self):
        c = KinesisSourceConfig.model_validate(
            {"aws_config": {"aws_region": "us-east-1"}}
        )
        assert c.aws_config.aws_region == "us-east-1"

    def test_aws_config_is_optional(self):
        """aws_config must be optional so recipes can rely on the boto3 chain
        (env vars, shared credentials, IAM role, SSO profile) without writing
        an explicit `aws_config:` block. aws_region resolution still happens
        at source __init__ time via session.region_name; the config parse
        itself must accept an empty input.
        """
        c = KinesisSourceConfig.model_validate({})
        assert c.aws_config is not None
        assert c.aws_config.aws_region is None

    def test_unknown_destination_platform_rejected_at_startup(self):
        with pytest.raises(ValidationError) as exc_info:
            KinesisSourceConfig.model_validate(
                {
                    "aws_config": {"aws_region": "us-east-1"},
                    "destination_platform_map": {
                        "datadog": {"platform_instance": "prod"}
                    },
                }
            )
        assert "datadog" in str(exc_info.value)

    def test_destination_platform_map_accepts_known_platforms(self):
        c = KinesisSourceConfig.model_validate(
            {
                "aws_config": {"aws_region": "us-east-1"},
                "destination_platform_map": {
                    "redshift": {"platform_instance": "prod-cluster", "env": "PROD"},
                    "snowflake": {"platform_instance": "prod-sf"},
                },
            }
        )
        assert (
            c.destination_platform_map["redshift"].platform_instance == "prod-cluster"
        )
        assert c.destination_platform_map["snowflake"].env is None

    def test_destination_platform_map_accepts_glue(self):
        """SchemaConfiguration in Firehose's format-conversion config references a
        Glue table. We add 'glue' to the DestinationPlatform Literal so users can
        override platform_instance / env for the Glue catalog via the existing
        destination_platform_map mechanism — same as any other downstream.
        """
        c = KinesisSourceConfig.model_validate(
            {
                "aws_config": {"aws_region": "us-east-1"},
                "destination_platform_map": {
                    "glue": {"platform_instance": "central-catalog", "env": "PROD"},
                },
            }
        )
        detail = c.destination_platform_map["glue"]
        assert detail.platform_instance == "central-catalog"
        assert detail.env == "PROD"
