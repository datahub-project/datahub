"""Behavioural tests for BigQuerySchemaGenerator: project/dataset orchestration,
lightweight table discovery, schema field generation and dataset workunits."""

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.bigquery.table import TableListItem

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery import BigqueryV2Source
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryDataset,
    BigqueryProject,
    BigqueryTable,
    BigqueryTableConstraint,
    BigqueryTableSnapshot,
    BigqueryView,
    PartitionInfo,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema_gen import (
    BigQuerySchemaGenerator,
)
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    DatasetPropertiesClass,
    DomainsClass,
    GlobalTagsClass,
    PlatformResourceInfoClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
)

PROJECT = "test-project"
DATASET = "test_dataset"
NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)


def make_schema_gen(**config_overrides: Any) -> BigQuerySchemaGenerator:
    with (
        patch.object(BigQueryV2Config, "get_bigquery_client"),
        patch.object(BigQueryV2Config, "get_projects_client"),
    ):
        config = BigQueryV2Config.model_validate(
            {"project_id": PROJECT, **config_overrides}
        )
        source = BigqueryV2Source(config=config, ctx=PipelineContext(run_id="test"))
    schema_gen = source.bq_schema_extractor
    # The real BigQuerySchemaApi wraps a MagicMock client; replace it wholesale so
    # each test states exactly what the API returns.
    schema_gen.schema_api = MagicMock()
    return schema_gen


def mock_api(schema_gen: BigQuerySchemaGenerator) -> MagicMock:
    assert isinstance(schema_gen.schema_api, MagicMock)
    return schema_gen.schema_api


def make_table(name: str, **kwargs: Any) -> BigqueryTable:
    return BigqueryTable(
        name=name,
        comment=None,
        created=NOW,
        last_altered=NOW,
        size_in_bytes=None,
        rows_count=None,
        **kwargs,
    )


def make_column(
    name: str,
    data_type: str = "STRING",
    ordinal_position: int = 1,
    field_path: Optional[str] = None,
    **kwargs: Any,
) -> BigqueryColumn:
    return BigqueryColumn(
        name=name,
        ordinal_position=ordinal_position,
        is_nullable=True,
        data_type=data_type,
        comment=kwargs.pop("comment", None),
        field_path=field_path or name,
        is_partition_column=kwargs.pop("is_partition_column", False),
        cluster_column_position=kwargs.pop("cluster_column_position", None),
        **kwargs,
    )


def table_list_item(table_id: str, table_type: Optional[str] = None) -> TableListItem:
    resource: Dict[str, Any] = {
        "tableReference": {
            "projectId": PROJECT,
            "datasetId": DATASET,
            "tableId": table_id,
        }
    }
    if table_type:
        resource["type"] = table_type
    return TableListItem(resource)


def aspects_of(wus: Iterable[MetadataWorkUnit], aspect_type: type) -> List[Any]:
    out = []
    for wu in wus:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
            wu.metadata.aspect, aspect_type
        ):
            out.append(wu.metadata.aspect)
    return out


def urns_with(wus: Iterable[MetadataWorkUnit], aspect_type: type) -> List[str]:
    return [
        wu.metadata.entityUrn  # type: ignore[misc]
        for wu in wus
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, aspect_type)
    ]


class TestProjectProcessing:
    def test_dataset_listing_failure_is_reported_and_project_skipped(self) -> None:
        schema_gen = make_schema_gen()
        mock_api(schema_gen).get_datasets_for_project_id.side_effect = Exception(
            "project has not enabled BigQuery."
        )

        wus = list(schema_gen.get_project_workunits(BigqueryProject(PROJECT, PROJECT)))

        assert wus == []
        assert len(schema_gen.report.failures) == 1

    def test_empty_project_is_dropped_when_exclude_empty_projects(self) -> None:
        schema_gen = make_schema_gen(exclude_empty_projects=True)
        mock_api(schema_gen).get_datasets_for_project_id.return_value = []

        wus = list(schema_gen.get_project_workunits(BigqueryProject(PROJECT, PROJECT)))

        assert wus == []
        assert PROJECT in schema_gen.report.filtered
        assert not schema_gen.report.warnings

    def test_empty_project_still_gets_container_and_warning_by_default(self) -> None:
        schema_gen = make_schema_gen()
        mock_api(schema_gen).get_datasets_for_project_id.return_value = []

        wus = list(schema_gen.get_project_workunits(BigqueryProject(PROJECT, PROJECT)))

        assert aspects_of(wus, ContainerPropertiesClass)
        assert len(schema_gen.report.warnings) == 1

    def test_profiler_runs_after_dataset_processing_when_profiling_enabled(
        self,
    ) -> None:
        schema_gen = make_schema_gen(
            profiling={"enabled": True},
            include_views=False,
            include_table_snapshots=False,
        )
        dataset = BigqueryDataset(name=DATASET, location="US")
        mock_api(schema_gen).get_datasets_for_project_id.return_value = [dataset]
        mock_api(schema_gen).get_columns_for_dataset.return_value = {}
        mock_api(schema_gen).get_table_constraints_for_dataset.return_value = {}
        table = make_table("t1")
        schema_gen.get_tables_for_dataset = MagicMock(return_value=[table])  # type: ignore[method-assign]
        profile_wu = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:bigquery,p.d.t,PROD)",
            aspect=StatusClass(removed=False),
        ).as_workunit()
        schema_gen.profiler = MagicMock()
        schema_gen.profiler.get_workunits.return_value = [profile_wu]

        wus = list(schema_gen.get_project_workunits(BigqueryProject(PROJECT, PROJECT)))

        assert profile_wu in wus
        schema_gen.profiler.get_workunits.assert_called_once()
        kwargs = schema_gen.profiler.get_workunits.call_args.kwargs
        assert kwargs["project_id"] == PROJECT
        assert kwargs["tables"] == {DATASET: [table]}
        assert schema_gen.report.num_project_datasets_to_scan[PROJECT] == 1
        assert "US" in schema_gen.discovered_locations

    def test_denied_dataset_is_dropped_and_failing_dataset_does_not_stop_others(
        self,
    ) -> None:
        schema_gen = make_schema_gen(
            dataset_pattern={"deny": [".*denied.*"]},
            include_views=False,
            include_table_snapshots=False,
            include_table_constraints=False,
        )
        mock_api(schema_gen).get_columns_for_dataset.return_value = {}
        broken = BigqueryDataset(name="broken")
        good = BigqueryDataset(name="good")
        denied = BigqueryDataset(name="denied_ds")

        def tables_for(
            project_id: str, dataset: BigqueryDataset
        ) -> List[BigqueryTable]:
            if dataset.name == "broken":
                raise RuntimeError("tables.list forbidden")
            return [make_table("t1")]

        schema_gen.get_tables_for_dataset = MagicMock(side_effect=tables_for)  # type: ignore[method-assign]

        wus = list(
            schema_gen._process_project_datasets(
                BigqueryProject(PROJECT, PROJECT, datasets=[broken, good, denied]), {}
            )
        )

        assert "denied_ds.*" in schema_gen.report.filtered
        assert len(schema_gen.report.failures) == 1
        assert f"{PROJECT}.broken" in str(schema_gen.report.failures[0].context)
        assert any(urn.endswith("good.t1,PROD)") for urn in urns_with(wus, StatusClass))


class TestProcessSchema:
    def test_full_schema_path_emits_tables_views_and_snapshots(self) -> None:
        schema_gen = make_schema_gen(
            rate_limit=True,
            include_table_constraints=True,
            capture_dataset_label_as_tag=True,
        )
        dataset = BigqueryDataset(
            name=DATASET,
            location="EU",
            labels={"env": "prod"},
            comment="dataset docs",
            created=NOW,
            last_altered=NOW,
        )
        pk = BigqueryTableConstraint(
            name="pk",
            project_id=PROJECT,
            dataset_name=DATASET,
            table_name="t1",
            type="PRIMARY KEY",
            field_path="id",
        )
        mock_api(schema_gen).get_columns_for_dataset.return_value = {
            "t1": [make_column("id", "INT64")],
            "v1": [make_column("id", "INT64")],
            "s1": [make_column("id", "INT64")],
        }
        mock_api(schema_gen).get_table_constraints_for_dataset.return_value = {
            "t1": [pk]
        }
        schema_gen.get_tables_for_dataset = MagicMock(return_value=[make_table("t1")])  # type: ignore[method-assign]
        mock_api(schema_gen).get_views_for_dataset.return_value = [
            BigqueryView(
                name="v1",
                comment=None,
                created=NOW,
                last_altered=NOW,
                view_definition="SELECT 1",
            )
        ]
        mock_api(schema_gen).get_snapshots_for_dataset.return_value = [
            BigqueryTableSnapshot(
                name="s1",
                comment=None,
                created=NOW,
                last_altered=NOW,
                size_in_bytes=10,
                rows_count=5,
                ddl="CREATE SNAPSHOT TABLE s1",
                snapshot_time=NOW,
                base_table_identifier=BigqueryTableIdentifier(PROJECT, DATASET, "t1"),
            )
        ]

        wus = list(schema_gen._process_schema(PROJECT, dataset, {}, {}, {}))

        # Rate limiter is threaded through to the columns query.
        columns_kwargs = mock_api(schema_gen).get_columns_for_dataset.call_args.kwargs
        assert columns_kwargs["rate_limiter"] is not None

        container_props = aspects_of(wus, ContainerPropertiesClass)
        dataset_container = next(c for c in container_props if c.name == DATASET)
        assert dataset_container.customProperties["location"] == "EU"
        assert dataset_container.description == "dataset docs"
        container_tags = [
            t.tag for a in aspects_of(wus, GlobalTagsClass) for t in a.tags
        ]
        assert "urn:li:tag:env:prod" in container_tags
        assert aspects_of(wus, PlatformResourceInfoClass)

        status_urns = urns_with(wus, StatusClass)
        for name in ("t1", "v1", "s1"):
            assert any(urn.endswith(f"{DATASET}.{name},PROD)") for urn in status_urns)
        assert len(schema_gen.table_refs) == 3

        schema_for_t1 = next(
            s
            for s in aspects_of(wus, SchemaMetadataClass)
            if s.schemaName.endswith(".t1")
        )
        assert schema_for_t1.fields[0].isPartOfKey is True

        snapshot_ref = next(ref for ref in schema_gen.table_refs if ref.endswith("s1"))
        assert snapshot_ref in schema_gen.snapshots_by_ref
        snapshot_props = next(
            p
            for p in aspects_of(wus, DatasetPropertiesClass)
            if p.qualifiedName and p.qualifiedName.endswith(".s1")
        )
        assert (
            snapshot_props.customProperties["snapshot_ddl"]
            == "CREATE SNAPSHOT TABLE s1"
        )
        assert snapshot_props.customProperties["rows_count"] == "5"
        assert snapshot_props.customProperties["size_in_bytes"] == "10"

    def test_biglake_dataset_skips_constraints_and_region_autodetect(self) -> None:
        schema_gen = make_schema_gen(
            include_table_constraints=True,
            include_tables=False,
            include_views=False,
            include_table_snapshots=False,
        )
        mock_api(schema_gen).get_columns_for_dataset.return_value = {}
        dataset = BigqueryDataset(name=DATASET, location="aws-us-east-1")

        list(schema_gen._process_schema(PROJECT, dataset, {}, {}, {}))

        mock_api(schema_gen).get_table_constraints_for_dataset.assert_not_called()
        assert schema_gen.discovered_locations == set()
        assert schema_gen.report.num_biglake_datasets_skipped_for_region_autodetect == 1

    def test_snapshot_denied_by_pattern_is_dropped(self) -> None:
        schema_gen = make_schema_gen(table_snapshot_pattern={"deny": [".*old.*"]})
        snapshot = BigqueryTableSnapshot(
            name="old_snap",
            comment=None,
            created=NOW,
            last_altered=NOW,
            size_in_bytes=None,
            rows_count=None,
        )

        wus = list(schema_gen._process_snapshot(snapshot, [], PROJECT, DATASET))

        assert wus == []
        assert schema_gen.report.snapshots_scanned == 1
        assert f"{PROJECT}.{DATASET}.old_snap" in schema_gen.report.filtered

    def test_partition_columns_attached_only_when_every_field_resolves(self) -> None:
        schema_gen = make_schema_gen()
        cols = [make_column("a", "DATE", 1), make_column("b", "STRING", 2)]

        full = make_table("full", partition_info=PartitionInfo(fields=("b", "a")))
        list(schema_gen._process_table(full, cols, PROJECT, DATASET))
        assert full.partition_info is not None
        assert full.partition_info.columns is not None
        assert [c.name for c in full.partition_info.columns] == ["b", "a"]

        partial = make_table(
            "partial", partition_info=PartitionInfo(fields=("a", "zz"))
        )
        list(schema_gen._process_table(partial, cols, PROJECT, DATASET))
        assert partial.partition_info is not None
        assert partial.partition_info.columns is None


class TestLightweightDiscovery:
    def test_schema_off_keeps_only_newest_shard_in_table_refs(self) -> None:
        schema_gen = make_schema_gen(
            include_schema_metadata=False, include_table_lineage=True
        )
        mock_api(schema_gen).list_tables.return_value = [
            table_list_item("events_20220101"),
            table_list_item("events_20220103"),
            table_list_item("events_20220102"),
            table_list_item("plain"),
        ]

        wus = list(
            schema_gen._process_schema(
                PROJECT, BigqueryDataset(name=DATASET), {}, {}, {}
            )
        )

        assert wus == []
        assert schema_gen.report.num_sharded_tables_scanned == 3
        assert schema_gen.report.num_sharded_tables_deduped == 2
        assert sorted(schema_gen.table_refs) == [
            f"projects/{PROJECT}/datasets/{DATASET}/tables/events",
            f"projects/{PROJECT}/datasets/{DATASET}/tables/plain",
        ]
        mock_api(schema_gen).get_columns_for_dataset.assert_not_called()


class TestCoreTableDetails:
    def test_views_temp_and_denied_tables_are_filtered(self) -> None:
        schema_gen = make_schema_gen(
            include_views=False,
            table_pattern={"deny": [".*secret.*"]},
        )
        mock_api(schema_gen).list_tables.return_value = [
            table_list_item("kept"),
            table_list_item("v_plain", "VIEW"),
            table_list_item("v_20240101", "VIEW"),
            table_list_item("secret_20240101"),
            table_list_item("secret_plain"),
        ]

        items = schema_gen.get_core_table_details(DATASET, PROJECT, "_tmp")

        assert list(items) == ["kept"]
        dropped = set(schema_gen.report.filtered)
        assert f"{PROJECT}.{DATASET}.v_plain" in dropped
        assert f"{PROJECT}.{DATASET}.v_20240101" in dropped
        assert f"{PROJECT}.{DATASET}.secret" in dropped
        assert f"{PROJECT}.{DATASET}.secret_plain" in dropped

    def test_sharded_views_are_kept_when_views_are_included(self) -> None:
        schema_gen = make_schema_gen(include_views=True)
        mock_api(schema_gen).list_tables.return_value = [
            table_list_item("v_20240101", "VIEW"),
            table_list_item("v_20240102", "VIEW"),
        ]

        items = schema_gen.get_core_table_details(DATASET, PROJECT, "_tmp")

        assert list(items) == ["v_20240102"]

    def test_tables_are_fetched_in_batches(self) -> None:
        schema_gen = make_schema_gen(
            number_of_datasets_process_in_batch=2,
            profiling={"enabled": True},
        )
        mock_api(schema_gen).list_tables.return_value = [
            table_list_item(f"t{i}") for i in range(5)
        ]
        mock_api(schema_gen).get_tables_for_dataset.side_effect = lambda *a, **k: [
            make_table(name) for name in a[2]
        ]

        tables = list(
            schema_gen.get_tables_for_dataset(PROJECT, BigqueryDataset(name=DATASET))
        )

        assert [t.name for t in tables] == [f"t{i}" for i in range(5)]
        calls = mock_api(schema_gen).get_tables_for_dataset.call_args_list
        # Profiling means partition stats are requested, and its batch size (200)
        # dwarfs the 5 tables, so everything arrives in a single call.
        assert len(calls) == 1
        assert calls[0].kwargs["with_partitions"] is True
        assert f"{PROJECT}.{DATASET}" in schema_gen.report.metadata_extraction_sec

    def test_small_batch_size_without_partitions_splits_requests(self) -> None:
        schema_gen = make_schema_gen(number_of_datasets_process_in_batch=2)
        mock_api(schema_gen).list_tables.return_value = [
            table_list_item(f"t{i}") for i in range(5)
        ]
        batch_sizes: List[int] = []

        def fetch(*args: Any, **kwargs: Any) -> List[BigqueryTable]:
            # The batch dict is reused and cleared between calls, so snapshot its size.
            batch_sizes.append(len(args[2]))
            return [make_table(name) for name in args[2]]

        mock_api(schema_gen).get_tables_for_dataset.side_effect = fetch

        tables = list(
            schema_gen.get_tables_for_dataset(PROJECT, BigqueryDataset(name=DATASET))
        )

        assert len(tables) == 5
        assert batch_sizes == [2, 2, 1]
        calls = mock_api(schema_gen).get_tables_for_dataset.call_args_list
        assert all(c.kwargs["with_partitions"] is False for c in calls)


class TestDatasetWorkunits:
    def test_external_table_gets_subtype_properties_and_gcs_lineage_registration(
        self,
    ) -> None:
        schema_gen = make_schema_gen()
        table = make_table(
            "ext",
            external=True,
            table_type="EXTERNAL",
            ddl="CREATE EXTERNAL TABLE `p.d.ext` OPTIONS(uris=['gs://b/x/*'])",
            external_source_format="PARQUET",
            external_source_uris=["gs://b/x/*", "gs://b/y/*"],
            external_compression="GZIP",
            external_max_bad_records=0,
            partition_info=PartitionInfo(fields=("dt",)),
        )

        wus = list(schema_gen.gen_table_dataset_workunits(table, [], PROJECT, DATASET))

        props = aspects_of(wus, DatasetPropertiesClass)[0].customProperties
        assert props["external_source_format"] == "PARQUET"
        assert props["external_source_uris"] == "gs://b/x/*, gs://b/y/*"
        assert props["external_compression"] == "GZIP"
        assert props["external_max_bad_records"] == "0"
        assert "partition_info" in props
        sub_types = aspects_of(wus, SubTypesClass)[0].typeNames
        assert sub_types[0] == DatasetSubTypes.EXTERNAL_TABLE
        assert len(schema_gen.external_tables) == 1

    def test_domain_assignment_is_emitted_when_configured(self) -> None:
        schema_gen = make_schema_gen(
            domain={"urn:li:domain:finance": {"allow": [f"{PROJECT}\\..*"]}}
        )

        wus = list(
            schema_gen.gen_table_dataset_workunits(
                make_table("t1"), [], PROJECT, DATASET
            )
        )

        domains = aspects_of(wus, DomainsClass)
        assert domains and domains[0].domains == ["urn:li:domain:finance"]

    def test_view_labels_become_tags_backed_by_platform_resources(self) -> None:
        schema_gen = make_schema_gen(capture_view_label_as_tag=True)
        view = BigqueryView(
            name="v1",
            comment=None,
            created=NOW,
            last_altered=NOW,
            view_definition="SELECT 1",
            labels={"team": "data", "flag": ""},
        )

        wus = list(schema_gen.gen_view_dataset_workunits(view, [], PROJECT, DATASET))

        tags = [t.tag for a in aspects_of(wus, GlobalTagsClass) for t in a.tags]
        assert sorted(tags) == ["urn:li:tag:flag", "urn:li:tag:team:data"]
        assert len(aspects_of(wus, PlatformResourceInfoClass)) == 2

    def test_label_platform_resource_conflict_keeps_original_tag(self) -> None:
        schema_gen = make_schema_gen(capture_table_label_as_tag=True)
        schema_gen.platform_resource_helper = MagicMock()
        schema_gen.platform_resource_helper.generate_label_platform_resource.side_effect = ValueError(
            "urn mismatch"
        )
        table = make_table("t1", labels={"team": "data"})

        wus = list(schema_gen.gen_table_dataset_workunits(table, [], PROJECT, DATASET))

        tags = [t.tag for a in aspects_of(wus, GlobalTagsClass) for t in a.tags]
        assert tags == ["urn:li:tag:team:data"]
        assert not aspects_of(wus, PlatformResourceInfoClass)

    def test_encoded_tag_labels_are_decoded(self) -> None:
        schema_gen = make_schema_gen()
        # Encoding used when syncing DataHub tags into BigQuery labels.
        assert (
            schema_gen.modified_base32decode(
                "urn_li_encoded_tag_jbswy3dpeblw64tmmq______"
            )
            == "Hello World"
        )
        assert schema_gen.modified_base32decode("plain_label") == "plain_label"


class TestSchemaMetadata:
    def test_nested_struct_columns_get_descriptions_and_flat_columns_get_tags(
        self,
    ) -> None:
        schema_gen = make_schema_gen()
        columns = [
            make_column(
                "address",
                "STRUCT<city STRING, zip INT64>",
                ordinal_position=1,
                comment="where they live",
            ),
            make_column(
                "address",
                "STRUCT<city STRING, zip INT64>",
                ordinal_position=1,
                field_path="address.city",
                comment="the city",
            ),
            make_column(
                "id",
                "INT64",
                ordinal_position=2,
                cluster_column_position=1,
                policy_tags=["pii"],
                is_partition_column=True,
            ),
        ]
        pk = BigqueryTableConstraint(
            name="pk",
            project_id=PROJECT,
            dataset_name=DATASET,
            table_name="t1",
            type="PRIMARY KEY",
            field_path="id",
        )

        fields = schema_gen.gen_schema_fields(columns, [pk])

        by_path = {f.fieldPath: f for f in fields}
        struct_field = next(f for p, f in by_path.items() if p.endswith(".address"))
        city_field = next(f for p, f in by_path.items() if p.endswith(".city"))
        zip_field = next(f for p, f in by_path.items() if p.endswith(".zip"))
        assert struct_field.description == "where they live"
        assert city_field.description == "the city"
        assert zip_field.description is None
        id_field = by_path["id"]
        assert id_field.isPartOfKey is True
        assert id_field.isPartitioningKey is True
        assert id_field.globalTags is not None
        assert [t.tag for t in id_field.globalTags.tags] == [
            "urn:li:tag:CLUSTERING_COLUMN_1",
            "urn:li:tag:pii",
        ]

    def test_foreign_keys_are_grouped_per_referenced_table_and_schema_feeds_resolver(
        self,
    ) -> None:
        schema_gen = make_schema_gen(lineage_use_sql_parser=True)

        def fk(column: str, ref_column: str, ref_table: str) -> BigqueryTableConstraint:
            return BigqueryTableConstraint(
                name=f"fk_{column}",
                project_id=PROJECT,
                dataset_name=DATASET,
                table_name="orders",
                type="FOREIGN KEY",
                field_path=column,
                referenced_project_id=PROJECT,
                referenced_dataset=DATASET,
                referenced_table_name=ref_table,
                referenced_column_name=ref_column,
            )

        table = make_table(
            "orders",
            constraints=[
                fk("customer_id", "id", "customers"),
                fk("customer_region", "region", "customers"),
                fk("product_id", "id", "products"),
            ],
        )
        identifier = BigqueryTableIdentifier(PROJECT, DATASET, "orders")
        dataset_urn = schema_gen.identifiers.gen_dataset_urn(PROJECT, DATASET, "orders")

        wu = schema_gen.gen_schema_metadata(
            dataset_urn, table, [make_column("customer_id", "INT64")], identifier
        )

        schema = wu.get_aspect_of_type(SchemaMetadataClass)
        assert schema is not None and schema.foreignKeys is not None
        by_name = {fk.name: fk for fk in schema.foreignKeys}
        assert set(by_name) == {
            f"{PROJECT}.{DATASET}.customers",
            f"{PROJECT}.{DATASET}.products",
        }
        assert len(by_name[f"{PROJECT}.{DATASET}.customers"].sourceFields) == 2
        assert schema_gen.sql_parser_schema_resolver.has_urn(dataset_urn)

    def test_schema_is_registered_for_linked_copy_lineage_without_sql_parser(
        self,
    ) -> None:
        schema_gen = make_schema_gen(lineage_use_sql_parser=False)
        schema_gen.sharing_handler = MagicMock()
        schema_gen.sharing_handler.needs_schema_for_copy_lineage.return_value = True
        identifier = BigqueryTableIdentifier(PROJECT, DATASET, "t1")
        dataset_urn = schema_gen.identifiers.gen_dataset_urn(PROJECT, DATASET, "t1")

        schema_gen.gen_schema_metadata(dataset_urn, make_table("t1"), [], identifier)

        assert schema_gen.sql_parser_schema_resolver.has_urn(dataset_urn)


@pytest.mark.parametrize(
    "key,value,expected",
    [("team", "data", "urn:li:tag:team:data"), ("flag", "", "urn:li:tag:flag")],
)
def test_make_tag_urn_from_label(key: str, value: str, expected: str) -> None:
    assert make_schema_gen().make_tag_urn_from_label(key, value) == expected
