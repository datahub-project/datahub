import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import (
    SnowflakeLineageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeStage,
    SnowflakeStageType,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_stages import (
    SnowflakeStagesExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    SubTypesClass,
)


def _make_config(**kwargs: Any) -> SnowflakeV2Config:
    return SnowflakeV2Config(
        account_id="test_account",
        username="user",
        password="pass",  # type: ignore
        include_stages=True,
        **kwargs,
    )


def _make_internal_stage(name: str = "int_stage") -> SnowflakeStage:
    return SnowflakeStage(
        name=name,
        created=datetime(2024, 1, 1),
        owner="ADMIN",
        database_name="TEST_DB",
        schema_name="PUBLIC",
        comment="Internal stage for loading",
        stage_type=SnowflakeStageType.INTERNAL,
    )


def _make_external_stage(
    name: str = "ext_stage",
    url: str = "s3://my-bucket/data/",
) -> SnowflakeStage:
    return SnowflakeStage(
        name=name,
        created=datetime(2024, 1, 1),
        owner="ADMIN",
        database_name="TEST_DB",
        schema_name="PUBLIC",
        comment="External S3 stage",
        stage_type=SnowflakeStageType.EXTERNAL,
        url=url,
        cloud="aws",
        region="us-east-1",
        storage_integration="S3_INT",
    )


def _collect_workunits(
    stages: List[SnowflakeStage],
    config: Optional[SnowflakeV2Config] = None,
) -> tuple:
    """Returns (workunits, extractor) so tests can inspect the stage_lookup."""
    config = config or _make_config()
    report = SnowflakeV2Report()
    identifiers = SnowflakeIdentifierBuilder(
        identifier_config=config, structured_reporter=report
    )
    data_dict = MagicMock()
    data_dict.get_stages_for_schema.return_value = stages

    schema_key = identifiers.gen_schema_key("TEST_DB", "PUBLIC")

    extractor = SnowflakeStagesExtractor(
        config=config,
        report=report,
        data_dictionary=data_dict,
        identifiers=identifiers,
    )
    wus = list(extractor.get_workunits("TEST_DB", "PUBLIC", schema_key))
    return wus, extractor, report


class TestSnowflakeStagesExtractor:
    def test_no_stages_emits_nothing(self) -> None:
        wus, extractor, report = _collect_workunits([])
        assert len(wus) == 0
        assert report.stages_scanned == 0
        assert len(extractor.stage_lookup) == 0

    def test_internal_stage_emits_container_and_dataset(self) -> None:
        stage = _make_internal_stage()
        wus, extractor, report = _collect_workunits([stage])

        assert report.stages_scanned == 1
        assert len(extractor.stage_lookup) == 1

        # Should have container MCPs + dataset MCPs
        # Container: containerProperties, subTypes, container (parent), status, dataPlatformInstance ~ 5 MCPs
        # Dataset: datasetProperties, subTypes, status, container (parent) ~ 4 MCPs
        assert len(wus) >= 5

        # Check container subtype
        container_subtypes = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, SubTypesClass)
            and "Snowflake Stage" in wu.metadata.aspect.typeNames
        ]
        assert len(container_subtypes) == 1

        # Check dataset was emitted (internal stage placeholder)
        dataset_props = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DatasetPropertiesClass)
        ]
        assert len(dataset_props) == 1
        assert (
            dataset_props[0].description == "Internal stage data managed by Snowflake"
        )
        assert dataset_props[0].customProperties["stage_type"] == "INTERNAL"
        assert dataset_props[0].customProperties["stage_name"] == "int_stage"

        # Check dataset subtype
        dataset_subtypes = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, SubTypesClass)
            and "Snowflake Stage Data" in wu.metadata.aspect.typeNames
        ]
        assert len(dataset_subtypes) == 1

        # Lookup entry should have dataset_urn
        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.INT_STAGE")
        assert entry is not None
        assert entry.dataset_urn is not None
        assert "int_stage" in entry.dataset_urn

    def test_external_stage_emits_container_only(self) -> None:
        stage = _make_external_stage()
        wus, extractor, report = _collect_workunits([stage])

        assert report.stages_scanned == 1

        # Should have container MCPs but NO dataset MCPs
        dataset_props = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DatasetPropertiesClass)
        ]
        assert len(dataset_props) == 0

        # Lookup entry should have a resolved S3 dataset_urn
        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT_STAGE")
        assert entry is not None
        assert entry.dataset_urn is not None
        assert "s3" in entry.dataset_urn
        assert entry.stage.url == "s3://my-bucket/data/"

    def test_external_stage_container_has_url_in_properties(self) -> None:
        stage = _make_external_stage()
        wus, _, _ = _collect_workunits([stage])

        container_props = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, ContainerPropertiesClass)
        ]
        assert len(container_props) == 1
        props = container_props[0]
        assert props.customProperties is not None
        assert props.customProperties["stage_type"] == "EXTERNAL"
        assert props.customProperties["url"] == "s3://my-bucket/data/"
        assert props.customProperties["cloud"] == "aws"
        assert props.customProperties["region"] == "us-east-1"
        assert props.customProperties["storage_integration"] == "S3_INT"

    def test_mixed_stages(self) -> None:
        internal = _make_internal_stage("int_stg")
        external = _make_external_stage("ext_stg")
        wus, extractor, report = _collect_workunits([internal, external])

        assert report.stages_scanned == 2
        assert len(extractor.stage_lookup) == 2

        int_entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.INT_STG")
        ext_entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT_STG")
        assert int_entry is not None and int_entry.dataset_urn is not None
        assert ext_entry is not None and ext_entry.dataset_urn is not None
        # Internal resolves to snowflake platform, external to s3
        assert "snowflake" in int_entry.dataset_urn
        assert "s3" in ext_entry.dataset_urn

    def test_stage_pattern_filtering(self) -> None:
        config = _make_config()
        config.stage_pattern.deny = [".*INT.*"]
        report = SnowflakeV2Report()
        identifiers = SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        )
        data_dict = MagicMock()
        data_dict.get_stages_for_schema.return_value = [
            _make_internal_stage("int_stage"),
            _make_external_stage("ext_stage"),
        ]
        schema_key = identifiers.gen_schema_key("TEST_DB", "PUBLIC")

        extractor = SnowflakeStagesExtractor(
            config=config,
            report=report,
            data_dictionary=data_dict,
            identifiers=identifiers,
        )
        list(extractor.get_workunits("TEST_DB", "PUBLIC", schema_key))

        # Only external stage should be emitted
        assert report.stages_scanned == 1
        assert extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.INT_STAGE") is None
        assert extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT_STAGE") is not None

    def test_lookup_is_case_insensitive(self) -> None:
        stage = _make_internal_stage("My_Stage")
        _, extractor, _ = _collect_workunits([stage])

        # Lookup should work regardless of case
        assert extractor.get_stage_lookup_entry("test_db.public.my_stage") is not None
        assert extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.MY_STAGE") is not None


class TestS3UpstreamPlatformInstance:
    """
    S3 upstreams emitted by Snowflake must carry the platform instance that the *S3*
    recipe was ingested with. If they don't, the Snowflake-side URN
    (`s3,bucket/path,PROD`) and the S3-side URN (`s3,product.bucket/path,PROD`) never
    join, and cross-platform lineage silently breaks with no error reported.
    """

    def test_s3_instance_is_not_snowflakes_own_platform_instance(self) -> None:
        # The two are independent: `platform_instance` names this Snowflake account,
        # `platform_instance_map["s3"]` names the S3 recipe that owns the bucket.
        config = _make_config(
            platform_instance="snowflake_prod",
            platform_instance_map={"s3": "product"},
        )
        _, extractor, _ = _collect_workunits([_make_external_stage()], config)

        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT_STAGE")
        assert entry is not None
        assert entry.dataset_urn == (
            "urn:li:dataset:(urn:li:dataPlatform:s3,product.my-bucket/data,PROD)"
        )

    def test_external_stage_urn_applies_platform_instance(self) -> None:
        config = _make_config(platform_instance_map={"s3": "product"})
        _, extractor, _ = _collect_workunits([_make_external_stage()], config)

        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT_STAGE")
        assert entry is not None
        assert entry.dataset_urn == (
            "urn:li:dataset:(urn:li:dataPlatform:s3,product.my-bucket/data,PROD)"
        )

    def test_gcs_and_abs_stage_urns_apply_platform_instance(self) -> None:
        config = _make_config(
            platform_instance_map={"gcs": "gcs_inst", "abs": "abs_inst"}
        )
        stages = [
            _make_external_stage("gcs_stage", url="gcs://my-bucket/data"),
            _make_external_stage(
                "abs_stage",
                url="azure://myacct.blob.core.windows.net/my-container/data",
            ),
        ]
        _, extractor, _ = _collect_workunits(stages, config)

        gcs_entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.GCS_STAGE")
        abs_entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.ABS_STAGE")
        assert gcs_entry is not None and abs_entry is not None
        assert gcs_entry.dataset_urn == (
            "urn:li:dataset:(urn:li:dataPlatform:gcs,gcs_inst.my-bucket/data,PROD)"
        )
        assert abs_entry.dataset_urn == (
            "urn:li:dataset:(urn:li:dataPlatform:abs,abs_inst.my-container/data,PROD)"
        )

    def test_unsupported_stage_scheme_warns(self) -> None:
        # Previously logger.debug -- invisible at default level, so a user with 500
        # unsupported stages saw stages_scanned=500 and silently zero lineage.
        _, _, report = _collect_workunits(
            [_make_external_stage("hdfs_stage", url="hdfs://namenode/data")]
        )

        assert len(report.warnings) == 1

    def test_copy_history_lineage_applies_platform_instance(self) -> None:
        config = _make_config(platform_instance_map={"s3": "product"})
        identifiers = SnowflakeIdentifierBuilder(
            identifier_config=config,
            structured_reporter=SnowflakeV2Report(),
        )

        mapping = SnowflakeLineageExtractor._process_external_lineage_result_row(
            db_row={
                "DOWNSTREAM_TABLE_NAME": "DB.SCHEMA.TABLE",
                "UPSTREAM_LOCATIONS": json.dumps(["s3://my-bucket/data"]),
            },
            discovered_tables=None,
            identifiers=identifiers,
        )

        assert mapping is not None
        assert mapping.upstream_urn == (
            "urn:li:dataset:(urn:li:dataPlatform:s3,product.my-bucket/data,PROD)"
        )

    def test_copy_history_lineage_covers_gcs_and_abs(self) -> None:
        # All three storage platforms go through the shared helper, so COPY history
        # gets the same scheme coverage as stage lineage rather than S3 only.
        config = _make_config(
            platform_instance_map={"gcs": "gcs_inst", "abs": "abs_inst"}
        )
        identifiers = SnowflakeIdentifierBuilder(
            identifier_config=config,
            structured_reporter=SnowflakeV2Report(),
        )

        def upstream_for(location: str) -> Optional[str]:
            mapping = SnowflakeLineageExtractor._process_external_lineage_result_row(
                db_row={
                    "DOWNSTREAM_TABLE_NAME": "DB.SCHEMA.TABLE",
                    "UPSTREAM_LOCATIONS": json.dumps([location]),
                },
                discovered_tables=None,
                identifiers=identifiers,
            )
            return mapping.upstream_urn if mapping else None

        assert upstream_for("gcs://my-bucket/data") == (
            "urn:li:dataset:(urn:li:dataPlatform:gcs,gcs_inst.my-bucket/data,PROD)"
        )
        assert upstream_for(
            "azure://myacct.blob.core.windows.net/my-container/data"
        ) == (
            "urn:li:dataset:(urn:li:dataPlatform:abs,abs_inst.my-container/data,PROD)"
        )
        assert upstream_for("hdfs://namenode/data") is None

    def test_external_upstreams_apply_platform_instance(self) -> None:
        config = _make_config(platform_instance_map={"s3": "product"})
        extractor = SnowflakeLineageExtractor(
            config=config,
            report=SnowflakeV2Report(),
            connection=MagicMock(),
            filters=MagicMock(),
            identifiers=SnowflakeIdentifierBuilder(
                identifier_config=config, structured_reporter=SnowflakeV2Report()
            ),
            redundant_run_skip_handler=None,
            sql_aggregator=MagicMock(),
        )

        upstreams = extractor.get_external_upstreams({"s3://my-bucket/data"})

        assert len(upstreams) == 1
        assert upstreams[0].dataset == (
            "urn:li:dataset:(urn:li:dataPlatform:s3,product.my-bucket/data,PROD)"
        )
        assert upstreams[0].type == DatasetLineageTypeClass.COPY

    def test_external_table_ddl_lineage_applies_platform_instance(self) -> None:
        config = _make_config(
            platform_instance_map={"s3": "product", "gcs": "gcs_inst"}
        )
        report = SnowflakeV2Report()
        schema_gen = MagicMock()
        schema_gen.config = config
        schema_gen.report = report
        schema_gen.identifiers = SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        )
        schema_gen.connection.query.return_value = [
            {
                "name": "EXT_TABLE",
                "schema_name": "TEST_SCHEMA",
                "database_name": "TEST_DB",
                "location": "s3://my-bucket/data",
            },
            {
                "name": "GCS_TABLE",
                "schema_name": "TEST_SCHEMA",
                "database_name": "TEST_DB",
                "location": "gcs://other-bucket/data",
            },
            {
                "name": "HDFS_TABLE",
                "schema_name": "TEST_SCHEMA",
                "database_name": "TEST_DB",
                "location": "hdfs://namenode/data",
            },
        ]

        mappings = list(
            SnowflakeSchemaGenerator._external_tables_ddl_lineage(
                schema_gen,
                [
                    "test_db.test_schema.ext_table",
                    "test_db.test_schema.gcs_table",
                    "test_db.test_schema.hdfs_table",
                ],
            )
        )

        assert [m.upstream_urn for m in mappings] == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,product.my-bucket/data,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:gcs,gcs_inst.other-bucket/data,PROD)",
        ]
        # Counts edges emitted, not rows seen: the hdfs:// row produces no edge, so
        # counting it would report lineage that does not exist.
        assert report.num_external_table_edges_scanned == 2

    def test_external_table_ddl_lineage_skips_undiscovered_table(self) -> None:
        config = _make_config(platform_instance_map={"s3": "product"})
        report = SnowflakeV2Report()
        schema_gen = MagicMock()
        schema_gen.config = config
        schema_gen.report = report
        schema_gen.identifiers = SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        )
        schema_gen.connection.query.return_value = [
            {
                "name": "NOT_DISCOVERED",
                "schema_name": "TEST_SCHEMA",
                "database_name": "TEST_DB",
                "location": "s3://my-bucket/data",
            }
        ]

        mappings = list(
            SnowflakeSchemaGenerator._external_tables_ddl_lineage(
                schema_gen, ["test_db.test_schema.ext_table"]
            )
        )

        # The row's key is not in discovered_tables, so it's filtered before the S3
        # check ever runs -- no lineage, and it isn't counted as a scanned edge either.
        assert mappings == []
        assert report.num_external_table_edges_scanned == 0


class TestPlatformInstanceMapKeyValidation:
    """
    `lineage_platform_instance` looks keys up by exact platform name, so an unread key
    would be accepted and then ignored -- zero lineage, no error. The keys are validated
    at startup instead.
    """

    @pytest.mark.parametrize(
        "platform_instance_map",
        [
            {"s3": "product"},
            {"s3": "product", "gcs": "gcs_inst", "abs": "abs_inst"},
            {},
            None,
        ],
    )
    def test_accepts_platforms_snowflake_reads(
        self, platform_instance_map: Optional[Dict[str, str]]
    ) -> None:
        config = _make_config(platform_instance_map=platform_instance_map)
        assert config.platform_instance_map == platform_instance_map

    @pytest.mark.parametrize(
        "bad_key",
        [
            "S3",  # right platform, wrong case
            "aws",  # cloud provider rather than DataHub platform name
            "gs",  # GCS's own URI scheme rather than the platform name
            "hive",  # a real platform, but not one Snowflake reads
        ],
    )
    def test_rejects_keys_snowflake_never_reads(self, bad_key: str) -> None:
        with pytest.raises(
            ValidationError, match="are not read by the Snowflake source"
        ):
            _make_config(platform_instance_map={bad_key: "product"})

    def test_error_names_the_offending_key(self) -> None:
        with pytest.raises(ValidationError, match=r"\['aws'\]"):
            _make_config(platform_instance_map={"s3": "ok", "aws": "typo"})
