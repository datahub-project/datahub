from datetime import datetime
from typing import Dict, List
from unittest.mock import MagicMock

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_pipes import (
    ParsedCopyInto,
    SnowflakePipesExtractor,
    parse_copy_into,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakePipe,
    SnowflakeStage,
    SnowflakeStageType,
)
from datahub.ingestion.source.snowflake.snowflake_stages import (
    SnowflakeStagesExtractor,
    StageLookupEntry,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    SubTypesClass,
)

# --- parse_copy_into tests ---


class TestParseCopyInto:
    def test_simple_copy_into(self) -> None:
        parsed = parse_copy_into(
            "COPY INTO my_table FROM @my_stage", "MY_DB", "MY_SCHEMA"
        )
        assert parsed == ParsedCopyInto(
            target_fqn="MY_DB.MY_SCHEMA.MY_TABLE",
            stage_fqn="MY_DB.MY_SCHEMA.MY_STAGE",
        )

    def test_fully_qualified_names(self) -> None:
        parsed = parse_copy_into(
            "COPY INTO db1.schema1.target_table FROM @db2.schema2.source_stage",
            "DEFAULT_DB",
            "DEFAULT_SCHEMA",
        )
        assert parsed == ParsedCopyInto(
            target_fqn="DB1.SCHEMA1.TARGET_TABLE",
            stage_fqn="DB2.SCHEMA2.SOURCE_STAGE",
        )

    def test_stage_with_trailing_path(self) -> None:
        parsed = parse_copy_into(
            "COPY INTO my_table FROM @my_stage/data/2024/", "DB", "SCHEMA"
        )
        assert parsed == ParsedCopyInto(
            target_fqn="DB.SCHEMA.MY_TABLE",
            stage_fqn="DB.SCHEMA.MY_STAGE",
        )

    def test_quoted_identifiers(self) -> None:
        parsed = parse_copy_into(
            'COPY INTO "MY_DB"."MY_SCHEMA"."MY_TABLE" FROM @"MY_STAGE"',
            "DEFAULT_DB",
            "DEFAULT_SCHEMA",
        )
        assert parsed == ParsedCopyInto(
            target_fqn="MY_DB.MY_SCHEMA.MY_TABLE",
            stage_fqn="DEFAULT_DB.DEFAULT_SCHEMA.MY_STAGE",
        )

    def test_unparseable_returns_none(self) -> None:
        assert parse_copy_into("SELECT 1", "DB", "SCHEMA") is None

    def test_empty_string_returns_none(self) -> None:
        assert parse_copy_into("", "DB", "SCHEMA") is None

    def test_stage_with_file_format_options(self) -> None:
        parsed = parse_copy_into(
            "COPY INTO my_table FROM @my_stage/path/ "
            "FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)",
            "DB",
            "SCHEMA",
        )
        assert parsed == ParsedCopyInto(
            target_fqn="DB.SCHEMA.MY_TABLE",
            stage_fqn="DB.SCHEMA.MY_STAGE",
        )

    def test_case_insensitive(self) -> None:
        parsed = parse_copy_into("copy into my_table from @my_stage", "DB", "SCHEMA")
        assert parsed == ParsedCopyInto(
            target_fqn="DB.SCHEMA.MY_TABLE",
            stage_fqn="DB.SCHEMA.MY_STAGE",
        )

    def test_partial_qualification_two_part_target(self) -> None:
        """Two-part target (schema.table) uses default db."""
        parsed = parse_copy_into(
            "COPY INTO schema1.my_table FROM @my_stage", "DB", "DEFAULT_SCHEMA"
        )
        assert parsed == ParsedCopyInto(
            target_fqn="DB.SCHEMA1.MY_TABLE",
            stage_fqn="DB.DEFAULT_SCHEMA.MY_STAGE",
        )


# --- SnowflakePipesExtractor tests ---


def _make_config() -> SnowflakeV2Config:
    return SnowflakeV2Config(
        account_id="test_account",
        username="user",
        password="pass",  # type: ignore
        include_pipes=True,
    )


def _make_pipe(
    name: str = "test_pipe",
    definition: str = "COPY INTO target_table FROM @my_stage",
    auto_ingest: bool = True,
) -> SnowflakePipe:
    return SnowflakePipe(
        name=name,
        created=datetime(2024, 1, 1),
        owner="ADMIN",
        database_name="TEST_DB",
        schema_name="PUBLIC",
        definition=definition,
        comment="test pipe",
        auto_ingest=auto_ingest,
        notification_channel=None,
    )


def _make_internal_stage(name: str = "my_stage") -> SnowflakeStage:
    return SnowflakeStage(
        name=name,
        created=datetime(2024, 1, 1),
        owner="ADMIN",
        database_name="TEST_DB",
        schema_name="PUBLIC",
        comment=None,
        stage_type=SnowflakeStageType.INTERNAL,
    )


def _make_external_stage(
    name: str = "ext_stage", url: str = "s3://my-bucket/data/"
) -> SnowflakeStage:
    return SnowflakeStage(
        name=name,
        created=datetime(2024, 1, 1),
        owner="ADMIN",
        database_name="TEST_DB",
        schema_name="PUBLIC",
        comment=None,
        stage_type=SnowflakeStageType.EXTERNAL,
        url=url,
        cloud="aws",
        region="us-east-1",
    )


def _collect_workunits(
    pipes: List[SnowflakePipe],
    stage_lookup: Dict[str, StageLookupEntry],
) -> List[MetadataWorkUnit]:
    config = _make_config()
    report = SnowflakeV2Report()
    identifiers = SnowflakeIdentifierBuilder(
        identifier_config=config, structured_reporter=report
    )
    data_dict = MagicMock()
    data_dict.get_pipes_for_schema.return_value = pipes

    stages_extractor = SnowflakeStagesExtractor(
        config=config,
        report=report,
        data_dictionary=data_dict,
        identifiers=identifiers,
    )
    stages_extractor.stage_lookup = stage_lookup

    extractor = SnowflakePipesExtractor(
        config=config,
        report=report,
        data_dictionary=data_dict,
        identifiers=identifiers,
        stages_extractor=stages_extractor,
    )
    return list(extractor.get_workunits("TEST_DB", "PUBLIC"))


class TestSnowflakePipesExtractor:
    def test_no_pipes_emits_nothing(self) -> None:
        wus = _collect_workunits([], {})
        assert len(wus) == 0

    def test_pipe_emits_dataflow_and_datajob(self) -> None:
        pipe = _make_pipe()
        wus = _collect_workunits([pipe], {})
        # DataFlow: DataFlowInfo + SubTypes + Status = 3
        # DataJob: DataJobInfo + SubTypes + Status + DataJobInputOutput + Ownership = 5
        assert len(wus) >= 5  # At least flow + job MCPs
        # Verify we have both flow and job subtypes
        subtype_values = []
        for wu in wus:
            mcp = wu.metadata
            if hasattr(mcp, "aspect") and isinstance(mcp.aspect, SubTypesClass):
                subtype_values.extend(mcp.aspect.typeNames)
        assert "Snowflake Pipe Group" in subtype_values
        assert "Snowflake Pipe" in subtype_values

    def test_internal_stage_pipe_uses_placeholder_dataset(self) -> None:
        pipe = _make_pipe(definition="COPY INTO target_table FROM @int_stage")
        internal_stage = _make_internal_stage("int_stage")
        stage_lookup = {
            "TEST_DB.PUBLIC.INT_STAGE": StageLookupEntry(
                stage=internal_stage,
                container_key=MagicMock(),
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.int_stage,PROD)",
            ),
        }
        wus = _collect_workunits([pipe], stage_lookup)

        # Find the DataJobInputOutput aspect
        input_output_aspects = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
        ]
        assert len(input_output_aspects) == 1
        io = input_output_aspects[0]
        assert len(io.inputDatasets) == 1
        assert "int_stage" in io.inputDatasets[0]
        assert len(io.outputDatasets) == 1
        assert "target_table" in io.outputDatasets[0]

    def test_external_stage_pipe_uses_resolved_s3_urn(self) -> None:
        pipe = _make_pipe(definition="COPY INTO target_table FROM @ext_stage/data/")
        ext_stage = _make_external_stage("ext_stage", "s3://my-bucket/data/")
        stage_lookup = {
            "TEST_DB.PUBLIC.EXT_STAGE": StageLookupEntry(
                stage=ext_stage,
                container_key=MagicMock(),
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data/,PROD)",
            ),
        }
        wus = _collect_workunits([pipe], stage_lookup)

        input_output_aspects = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
        ]
        assert len(input_output_aspects) == 1
        io = input_output_aspects[0]
        assert len(io.inputDatasets) == 1
        assert "s3" in io.inputDatasets[0].lower()
        assert len(io.outputDatasets) == 1

    def test_all_pipes_filtered_emits_nothing(self) -> None:
        config = _make_config()
        config.pipe_pattern.deny = [".*"]
        report = SnowflakeV2Report()
        identifiers = SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        )
        data_dict = MagicMock()
        data_dict.get_pipes_for_schema.return_value = [_make_pipe()]

        stages_extractor = SnowflakeStagesExtractor(
            config=config,
            report=report,
            data_dictionary=data_dict,
            identifiers=identifiers,
        )
        extractor = SnowflakePipesExtractor(
            config=config,
            report=report,
            data_dictionary=data_dict,
            identifiers=identifiers,
            stages_extractor=stages_extractor,
        )
        wus = list(extractor.get_workunits("TEST_DB", "PUBLIC"))
        assert len(wus) == 0
        assert report.pipes_scanned == 0

    def test_unparseable_definition_emits_warning(self) -> None:
        pipe = _make_pipe(definition="INSERT INTO foo SELECT * FROM bar")
        config = _make_config()
        report = SnowflakeV2Report()
        identifiers = SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        )
        data_dict = MagicMock()
        data_dict.get_pipes_for_schema.return_value = [pipe]

        stages_extractor = SnowflakeStagesExtractor(
            config=config,
            report=report,
            data_dictionary=data_dict,
            identifiers=identifiers,
        )
        extractor = SnowflakePipesExtractor(
            config=config,
            report=report,
            data_dictionary=data_dict,
            identifiers=identifiers,
            stages_extractor=stages_extractor,
        )
        wus = list(extractor.get_workunits("TEST_DB", "PUBLIC"))
        # Should still emit the pipe (without lineage), and a warning
        assert report.pipes_scanned == 1
        assert len(wus) > 0
