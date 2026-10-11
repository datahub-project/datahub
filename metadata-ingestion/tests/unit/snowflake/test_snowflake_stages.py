from datetime import datetime
from typing import List
from unittest.mock import MagicMock

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeStage,
    SnowflakeStageType,
)
from datahub.ingestion.source.snowflake.snowflake_stages import (
    SnowflakeStagesExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    DatasetPropertiesClass,
    SubTypesClass,
)


def _make_config() -> SnowflakeV2Config:
    return SnowflakeV2Config(
        account_id="test_account",
        username="user",
        password="pass",  # type: ignore
        include_stages=True,
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
) -> tuple:
    """Returns (workunits, extractor) so tests can inspect the stage_lookup."""
    config = _make_config()
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
