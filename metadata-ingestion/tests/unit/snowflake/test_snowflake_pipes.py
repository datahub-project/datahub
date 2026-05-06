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
    DataJobInfoClass,
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
            stage_fqns=("MY_DB.MY_SCHEMA.MY_STAGE",),
        )

    def test_fully_qualified_names(self) -> None:
        parsed = parse_copy_into(
            "COPY INTO db1.schema1.target_table FROM @db2.schema2.source_stage",
            "DEFAULT_DB",
            "DEFAULT_SCHEMA",
        )
        assert parsed == ParsedCopyInto(
            target_fqn="DB1.SCHEMA1.TARGET_TABLE",
            stage_fqns=("DB2.SCHEMA2.SOURCE_STAGE",),
        )

    def test_stage_with_trailing_path(self) -> None:
        parsed = parse_copy_into(
            "COPY INTO my_table FROM @my_stage/data/2024/", "DB", "SCHEMA"
        )
        assert parsed == ParsedCopyInto(
            target_fqn="DB.SCHEMA.MY_TABLE",
            stage_fqns=("DB.SCHEMA.MY_STAGE",),
        )

    def test_quoted_identifiers(self) -> None:
        parsed = parse_copy_into(
            'COPY INTO "MY_DB"."MY_SCHEMA"."MY_TABLE" FROM @"MY_STAGE"',
            "DEFAULT_DB",
            "DEFAULT_SCHEMA",
        )
        assert parsed == ParsedCopyInto(
            target_fqn="MY_DB.MY_SCHEMA.MY_TABLE",
            stage_fqns=("DEFAULT_DB.DEFAULT_SCHEMA.MY_STAGE",),
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
            stage_fqns=("DB.SCHEMA.MY_STAGE",),
        )

    def test_case_insensitive(self) -> None:
        parsed = parse_copy_into("copy into my_table from @my_stage", "DB", "SCHEMA")
        assert parsed == ParsedCopyInto(
            target_fqn="DB.SCHEMA.MY_TABLE",
            stage_fqns=("DB.SCHEMA.MY_STAGE",),
        )

    def test_partial_qualification_two_part_target(self) -> None:
        """Two-part target (schema.table) uses default db."""
        parsed = parse_copy_into(
            "COPY INTO schema1.my_table FROM @my_stage", "DB", "DEFAULT_SCHEMA"
        )
        assert parsed == ParsedCopyInto(
            target_fqn="DB.SCHEMA1.MY_TABLE",
            stage_fqns=("DB.DEFAULT_SCHEMA.MY_STAGE",),
        )

    def test_target_with_column_list(self) -> None:
        """Column list wraps target in a Schema; we must unwrap to Table."""
        parsed = parse_copy_into(
            "COPY INTO my_table(col_a, col_b) FROM @my_stage", "DB", "SCHEMA"
        )
        assert parsed == ParsedCopyInto(
            target_fqn="DB.SCHEMA.MY_TABLE",
            stage_fqns=("DB.SCHEMA.MY_STAGE",),
        )

    def test_from_subquery(self) -> None:
        """FROM (SELECT ... FROM @stage) — stage is nested inside a Subquery."""
        parsed = parse_copy_into(
            "COPY INTO my_table FROM (SELECT $1 FROM @my_stage)",
            "DB",
            "SCHEMA",
        )
        assert parsed == ParsedCopyInto(
            target_fqn="DB.SCHEMA.MY_TABLE",
            stage_fqns=("DB.SCHEMA.MY_STAGE",),
        )

    def test_column_list_and_subquery(self) -> None:
        """Real-world Snowpipe shape: column list on target plus FROM subquery."""
        parsed = parse_copy_into(
            "COPY INTO my_table(col_a, col_b) "
            "FROM (SELECT $1 AS col_a, $2 AS col_b FROM @my_stage t)",
            "DB",
            "SCHEMA",
        )
        assert parsed == ParsedCopyInto(
            target_fqn="DB.SCHEMA.MY_TABLE",
            stage_fqns=("DB.SCHEMA.MY_STAGE",),
        )

    def test_subquery_with_qualified_stage_and_path(self) -> None:
        parsed = parse_copy_into(
            "COPY INTO my_table(col_a) "
            "FROM (SELECT $1 AS col_a FROM @other_db.other_schema.my_stage/p/q t)",
            "DB",
            "SCHEMA",
        )
        assert parsed == ParsedCopyInto(
            target_fqn="DB.SCHEMA.MY_TABLE",
            stage_fqns=("OTHER_DB.OTHER_SCHEMA.MY_STAGE",),
        )

    def test_subquery_without_stage_returns_none(self) -> None:
        """Subquery selecting from a regular table (not a stage) yields no lineage."""
        parsed = parse_copy_into(
            "COPY INTO my_table FROM (SELECT a, b FROM other_table)",
            "DB",
            "SCHEMA",
        )
        assert parsed is None

    def test_subquery_union_all_collects_multiple_stages(self) -> None:
        """COPY INTO with UNION ALL across stages yields all stages, in source order."""
        parsed = parse_copy_into(
            "COPY INTO my_table FROM ("
            " SELECT $1 FROM @stage_us"
            " UNION ALL"
            " SELECT $1 FROM @stage_eu"
            ")",
            "DB",
            "SCHEMA",
        )
        assert parsed == ParsedCopyInto(
            target_fqn="DB.SCHEMA.MY_TABLE",
            stage_fqns=("DB.SCHEMA.STAGE_US", "DB.SCHEMA.STAGE_EU"),
        )

    def test_repeated_stage_in_subquery_dedups(self) -> None:
        """A stage referenced twice in the same subquery is captured only once."""
        parsed = parse_copy_into(
            "COPY INTO my_table FROM ("
            " SELECT $1 FROM @my_stage WHERE 1=1"
            " UNION ALL"
            " SELECT $1 FROM @my_stage WHERE 2=2"
            ")",
            "DB",
            "SCHEMA",
        )
        assert parsed == ParsedCopyInto(
            target_fqn="DB.SCHEMA.MY_TABLE",
            stage_fqns=("DB.SCHEMA.MY_STAGE",),
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

    def test_pipe_with_union_all_stages_emits_lineage_for_each(self) -> None:
        """A pipe whose COPY unions two stages should yield two input datasets
        and a comma-joined ``stage_name`` custom property."""
        pipe = _make_pipe(
            definition=(
                "COPY INTO target_table FROM ("
                " SELECT $1 FROM @stage_us"
                " UNION ALL"
                " SELECT $1 FROM @stage_eu"
                ")"
            ),
        )
        stage_us = _make_external_stage("stage_us", "s3://bucket-us/data/")
        stage_eu = _make_external_stage("stage_eu", "s3://bucket-eu/data/")
        stage_lookup = {
            "TEST_DB.PUBLIC.STAGE_US": StageLookupEntry(
                stage=stage_us,
                container_key=MagicMock(),
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:s3,bucket-us/data/,PROD)",
            ),
            "TEST_DB.PUBLIC.STAGE_EU": StageLookupEntry(
                stage=stage_eu,
                container_key=MagicMock(),
                dataset_urn="urn:li:dataset:(urn:li:dataPlatform:s3,bucket-eu/data/,PROD)",
            ),
        }
        wus = _collect_workunits([pipe], stage_lookup)

        ios = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
        ]
        assert len(ios) == 1
        assert len(ios[0].inputDatasets) == 2
        assert any("bucket-us" in u for u in ios[0].inputDatasets)
        assert any("bucket-eu" in u for u in ios[0].inputDatasets)

        job_infos = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInfoClass)
        ]
        assert len(job_infos) == 1
        props = job_infos[0].customProperties
        assert props["stage_name"] == "TEST_DB.PUBLIC.STAGE_US, TEST_DB.PUBLIC.STAGE_EU"
        # Both stages are EXTERNAL; the de-duplicated stage_type should not repeat.
        assert props["stage_type"] == "EXTERNAL"

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

    def test_malformed_copy_into_emits_warning(self) -> None:
        """Pipe body that starts with COPY but is unparseable should warn."""
        pipe = _make_pipe(definition="COPY INTO ((( malformed")
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
        assert report.pipes_scanned == 1
        assert len(wus) > 0
        messages = [w.message for w in report.warnings]
        assert any("COPY INTO" in m for m in messages), (
            f"Expected a COPY INTO parse warning; got: {messages}"
        )
        contexts = [str(w.context) for w in report.warnings]
        assert any("test_pipe" in c for c in contexts)

    def test_non_copy_definition_skipped_silently(self) -> None:
        """Pipe body that is not a COPY INTO at all should NOT emit a warning."""
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
        assert report.pipes_scanned == 1
        assert len(wus) > 0
        # No COPY INTO parse warning — pipe body wasn't COPY INTO at all.
        messages = [w.message for w in report.warnings]
        assert not any("COPY INTO" in m for m in messages), (
            f"Did not expect a COPY INTO parse warning; got: {messages}"
        )

    def test_unresolvable_stage_ref_emits_specific_warning(self) -> None:
        """Stage reference with too many dotted parts should yield a
        normalization warning, distinct from a generic parse warning."""
        # Four-part stage reference is rejected by `_stage_reference_to_fqn`.
        pipe = _make_pipe(definition="COPY INTO target_table FROM @a.b.c.d/path/")
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
        assert report.pipes_scanned == 1
        assert len(wus) > 0
        messages = [w.message for w in report.warnings]
        # Should be the normalization warning, not the generic parse warning.
        assert any("could not be normalized" in m for m in messages), (
            f"Expected a stage normalization warning; got: {messages}"
        )
        contexts = [str(w.context) for w in report.warnings]
        # Raw reference (with @ and path) should appear in context.
        assert any("@a.b.c.d" in c for c in contexts)
