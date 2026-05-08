from datetime import datetime
from typing import List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeStage,
    SnowflakeStageType,
)
from datahub.ingestion.source.snowflake.snowflake_stages import (
    SnowflakeStagesExtractor,
    _dataset_path_is_rooted_at,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    DatasetPropertiesClass,
    SubTypesClass,
)
from datahub.utilities.global_warning_util import (
    clear_global_warnings,
    get_global_warnings,
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
) -> Tuple[List[MetadataWorkUnit], SnowflakeStagesExtractor, SnowflakeV2Report]:
    """Returns (workunits, extractor, report) so tests can inspect the stage_lookup."""
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

        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.INT_STAGE")
        assert entry is not None
        assert len(entry.dataset_urns) == 1
        assert "int_stage" in entry.dataset_urns[0]

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

        # Default fallback: path-based URN (no graph resolution).
        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT_STAGE")
        assert entry is not None
        assert len(entry.dataset_urns) == 1
        assert "s3" in entry.dataset_urns[0]
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
        assert int_entry is not None and int_entry.dataset_urns
        assert ext_entry is not None and ext_entry.dataset_urns
        assert "snowflake" in int_entry.dataset_urns[0]
        assert "s3" in ext_entry.dataset_urns[0]

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


def _run_with_graph(
    stages: List[SnowflakeStage],
    graph: MagicMock,
    *,
    resolve_via_graph: bool = True,
    platform_instance: Optional[str] = None,
) -> SnowflakeStagesExtractor:
    config = _make_config()
    config.resolve_external_stage_lineage_via_graph = resolve_via_graph
    config.external_stage_platform_instance = platform_instance
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
        graph=graph,
    )
    list(extractor.get_workunits("TEST_DB", "PUBLIC", schema_key))
    return extractor


class TestGraphResolvedExternalStageLineage:
    def test_returns_matching_dataset_urns(self) -> None:
        stage = _make_external_stage("ext", "s3://my-bucket/folder/")
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter(
            [
                "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/folder/table_a,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/folder/table_b,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/folder,PROD)",
                # `folder_other` shares a character prefix with `folder` but is a sibling
                # path, not a child — must be excluded.
                "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/folder_other,PROD)",
            ]
        )

        extractor = _run_with_graph([stage], graph)
        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT")
        assert entry is not None
        assert len(entry.dataset_urns) == 3
        assert all("my-bucket/folder" in u for u in entry.dataset_urns)
        assert not any("folder_other" in u for u in entry.dataset_urns)
        assert extractor.report.external_stage_lineage_resolved == 1
        assert extractor.report.external_stage_lineage_unresolved == 0

    def test_no_matches_skips_emission(self) -> None:
        stage = _make_external_stage("ext", "s3://my-bucket/folder/")
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter([])

        extractor = _run_with_graph([stage], graph)
        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT")
        assert entry is not None
        assert entry.dataset_urns == []
        assert extractor.report.external_stage_lineage_resolved == 0
        assert extractor.report.external_stage_lineage_unresolved == 1

    def test_disabled_flag_falls_back_to_path_urn(self) -> None:
        stage = _make_external_stage("ext", "s3://my-bucket/folder/")
        graph = MagicMock()
        extractor = _run_with_graph([stage], graph, resolve_via_graph=False)

        graph.get_urns_by_filter.assert_not_called()
        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT")
        assert entry is not None
        assert len(entry.dataset_urns) == 1
        assert "s3" in entry.dataset_urns[0]

    def test_graph_query_failure_warns_and_skips(self) -> None:
        stage = _make_external_stage("ext", "s3://my-bucket/folder/")
        graph = MagicMock()
        graph.get_urns_by_filter.side_effect = RuntimeError("boom")

        extractor = _run_with_graph([stage], graph)
        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT")
        assert entry is not None
        assert entry.dataset_urns == []
        assert extractor.report.external_stage_lineage_unresolved == 1

    def test_graph_query_failure_does_not_poison_cache(self) -> None:
        # First stage hits a transient graph error; second stage with the same URL
        # should retry the graph (not reuse the failed cached result).
        stage_a = _make_external_stage("a", "s3://my-bucket/folder/")
        stage_b = _make_external_stage("b", "s3://my-bucket/folder/")
        graph = MagicMock()
        graph.get_urns_by_filter.side_effect = [
            RuntimeError("transient blip"),
            iter(["urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/folder/t,PROD)"]),
        ]

        extractor = _run_with_graph([stage_a, stage_b], graph)
        a = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.A")
        b = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.B")
        assert a is not None and a.dataset_urns == []
        assert b is not None and len(b.dataset_urns) == 1
        assert graph.get_urns_by_filter.call_count == 2
        # Direct cache inspection: only the successful result should be cached.
        # Verifies the contract independently of call_count.
        assert extractor._stage_url_cache == {
            "s3://my-bucket/folder/": (
                "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/folder/t,PROD)",
            )
        }

    def test_platform_instance_is_passed_to_graph(self) -> None:
        stage = _make_external_stage("ext", "s3://my-bucket/folder/")
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter([])

        _run_with_graph([stage], graph, platform_instance="prod_s3")
        kwargs = graph.get_urns_by_filter.call_args.kwargs
        assert kwargs["platform_instance"] == "prod_s3"
        urn_prefix = kwargs["extraFilters"][0]["values"][0]
        assert "prod_s3.my-bucket/folder" in urn_prefix

    def test_gcs_stage_resolves_via_graph(self) -> None:
        stage = _make_external_stage("ext", "gcs://my-bucket/folder/")
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter(
            [
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/folder/table_a,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/folder_other,PROD)",
            ]
        )

        extractor = _run_with_graph([stage], graph)
        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT")
        assert entry is not None
        assert entry.dataset_urns == [
            "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/folder/table_a,PROD)"
        ]
        assert graph.get_urns_by_filter.call_args.kwargs["platform"] == "gcs"

    def test_azure_stage_resolves_via_graph(self) -> None:
        # Verifies that the storage-account host (`account.blob.core.windows.net`) is
        # stripped before querying, so the path matches what the ABS source emits.
        stage = _make_external_stage(
            "ext", "azure://account.blob.core.windows.net/container/folder/"
        )
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter(
            [
                "urn:li:dataset:(urn:li:dataPlatform:abs,container/folder/table_a,PROD)",
            ]
        )

        extractor = _run_with_graph([stage], graph)
        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT")
        assert entry is not None
        assert entry.dataset_urns == [
            "urn:li:dataset:(urn:li:dataPlatform:abs,container/folder/table_a,PROD)"
        ]
        kwargs = graph.get_urns_by_filter.call_args.kwargs
        assert kwargs["platform"] == "abs"
        assert "container/folder" in kwargs["extraFilters"][0]["values"][0]

    def test_url_results_are_cached_within_run(self) -> None:
        stage_a = _make_external_stage("a", "s3://bucket/x/")
        stage_b = _make_external_stage("b", "s3://bucket/x/")
        graph = MagicMock()
        graph.get_urns_by_filter.return_value = iter(
            ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/x/t,PROD)"]
        )

        extractor = _run_with_graph([stage_a, stage_b], graph)
        assert graph.get_urns_by_filter.call_count == 1
        a = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.A")
        b = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.B")
        assert a is not None and b is not None
        assert a.dataset_urns == b.dataset_urns

    def test_unsupported_scheme_is_skipped(self) -> None:
        stage = _make_external_stage("ext", "ftp://host/path/")
        graph = MagicMock()
        extractor = _run_with_graph([stage], graph)
        graph.get_urns_by_filter.assert_not_called()
        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT")
        assert entry is not None and entry.dataset_urns == []

    def test_empty_path_skips_graph_and_reports_unresolved(self) -> None:
        # Degenerate input: `s3://` strips to empty path. The guard prevents
        # constructing a malformed URN prefix that would over-match in the graph.
        stage = _make_external_stage("ext", "s3://")
        graph = MagicMock()
        extractor = _run_with_graph([stage], graph)
        graph.get_urns_by_filter.assert_not_called()
        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT")
        assert entry is not None and entry.dataset_urns == []
        assert extractor.report.external_stage_lineage_unresolved == 1

    def test_internal_stage_does_not_query_graph(self) -> None:
        # Internal stages resolve to a synthetic Snowflake URN, never the graph.
        stage = _make_internal_stage("int_stg")
        graph = MagicMock()
        _run_with_graph([stage], graph)
        graph.get_urns_by_filter.assert_not_called()

    def test_malformed_stage_url_warns_and_skips(self) -> None:
        # azure:// host that doesn't match ABS_PREFIXES_REGEX → strip_abs_prefix raises.
        stage = _make_external_stage("ext", "azure://corrupted-host/path/")
        graph = MagicMock()
        extractor = _run_with_graph([stage], graph)
        entry = extractor.get_stage_lookup_entry("TEST_DB.PUBLIC.EXT")
        assert entry is not None and entry.dataset_urns == []
        # The malformed URL should be reported; production must not crash extraction.
        assert any(
            "Failed to parse external stage URL" in str(w.title)
            for w in extractor.report.warnings
        )


_S3_BUCKET_FOLDER_PREFIX = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder"
_GCS_BUCKET_FOLDER_PREFIX = "urn:li:dataset:(urn:li:dataPlatform:gcs,bucket/folder"


@pytest.mark.parametrize(
    "dataset_urn, urn_prefix, expected",
    [
        # Child folder under the stage path → match.
        (
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder/t,PROD)",
            _S3_BUCKET_FOLDER_PREFIX,
            True,
        ),
        # Exact-path dataset (next char is `,`) → match.
        (
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder,PROD)",
            _S3_BUCKET_FOLDER_PREFIX,
            True,
        ),
        # `bucket/folder_other` shares characters but is a sibling → reject.
        (
            "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/folder_other,PROD)",
            _S3_BUCKET_FOLDER_PREFIX,
            False,
        ),
        # GCS dataset under an s3 prefix → reject (different platform).
        (
            "urn:li:dataset:(urn:li:dataPlatform:gcs,bucket/folder/t,PROD)",
            _S3_BUCKET_FOLDER_PREFIX,
            False,
        ),
        # GCS dataset under a gcs prefix → match (parametrize covers all platforms).
        (
            "urn:li:dataset:(urn:li:dataPlatform:gcs,bucket/folder/t,PROD)",
            _GCS_BUCKET_FOLDER_PREFIX,
            True,
        ),
        # Malformed URN missing the dataPlatform segment → reject.
        ("urn:li:something-else:foo", _S3_BUCKET_FOLDER_PREFIX, False),
    ],
)
def test_dataset_path_is_rooted_at(
    dataset_urn: str, urn_prefix: str, expected: bool
) -> None:
    assert _dataset_path_is_rooted_at(dataset_urn, urn_prefix) is expected


class TestExternalStageConfigValidators:
    @staticmethod
    def _make_config(**overrides: object) -> SnowflakeV2Config:
        params: dict = dict(account_id="test_account", username="user", password="pass")
        params.update(overrides)
        return SnowflakeV2Config(**params)  # type: ignore[arg-type]

    def test_blank_platform_instance_normalizes_to_none(self) -> None:
        # Blank/whitespace strings would silently produce malformed URN prefixes
        # like `urn:li:dataset:(urn:li:dataPlatform:s3,.<path>` and yield zero
        # matches. The validator must normalize them to None.
        for blank in ["", "   ", "\n\t"]:
            cfg = self._make_config(
                resolve_external_stage_lineage_via_graph=True,
                external_stage_platform_instance=blank,
            )
            assert cfg.external_stage_platform_instance is None

    def test_platform_instance_is_stripped(self) -> None:
        cfg = self._make_config(
            resolve_external_stage_lineage_via_graph=True,
            external_stage_platform_instance="  prod_s3  ",
        )
        assert cfg.external_stage_platform_instance == "prod_s3"

    def test_platform_instance_preserved_when_already_clean(self) -> None:
        cfg = self._make_config(
            resolve_external_stage_lineage_via_graph=True,
            external_stage_platform_instance="prod_s3",
        )
        assert cfg.external_stage_platform_instance == "prod_s3"

    def test_platform_instance_without_resolve_flag_warns(self) -> None:
        clear_global_warnings()
        try:
            self._make_config(
                resolve_external_stage_lineage_via_graph=False,
                external_stage_platform_instance="prod_s3",
            )
            warnings = list(get_global_warnings())
        finally:
            clear_global_warnings()
        assert any(
            "external_stage_platform_instance" in w
            and "resolve_external_stage_lineage_via_graph" in w
            for w in warnings
        )

    def test_no_warning_when_both_set_correctly(self) -> None:
        clear_global_warnings()
        try:
            self._make_config(
                resolve_external_stage_lineage_via_graph=True,
                external_stage_platform_instance="prod_s3",
            )
            warnings = list(get_global_warnings())
        finally:
            clear_global_warnings()
        assert not any("external_stage_platform_instance" in w for w in warnings)
