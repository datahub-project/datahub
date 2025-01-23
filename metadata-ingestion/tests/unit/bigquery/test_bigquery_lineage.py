import datetime
from typing import Dict, List, Optional, Set

import pytest

import datahub.metadata.schema_classes as models
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigQueryTableRef,
    QueryEvent,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryV2Config,
    GcsLineageProviderConfig,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.common import BigQueryIdentifierBuilder
from datahub.ingestion.source.bigquery_v2.lineage import (
    BigqueryLineageExtractor,
    LineageEdge,
)
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.sql_parsing.schema_resolver import SchemaResolver


@pytest.fixture
def lineage_entries() -> List[QueryEvent]:
    return [
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query="""
                INSERT INTO `my_project.my_dataset.my_table`
                SELECT first.a, second.b FROM `my_project.my_dataset.my_source_table1` first
                LEFT JOIN `my_project.my_dataset.my_source_table2` second ON first.id = second.id
            """,
            statementType="INSERT",
            project_id="proj_12344",
            end_time=None,
            referencedTables=[
                BigQueryTableRef.from_string_name(
                    "projects/my_project/datasets/my_dataset/tables/my_source_table1"
                ),
                BigQueryTableRef.from_string_name(
                    "projects/my_project/datasets/my_dataset/tables/my_source_table2"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/my_project/datasets/my_dataset/tables/my_table"
            ),
        ),
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query="testQuery",
            statementType="SELECT",
            project_id="proj_12344",
            end_time=datetime.datetime.fromtimestamp(
                1617295943.17321, tz=datetime.timezone.utc
            ),
            referencedTables=[
                BigQueryTableRef.from_string_name(
                    "projects/my_project/datasets/my_dataset/tables/my_source_table3"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/my_project/datasets/my_dataset/tables/my_table"
            ),
        ),
        QueryEvent(
            timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            actor_email="bla@bla.com",
            query="testQuery",
            statementType="SELECT",
            project_id="proj_12344",
            referencedViews=[
                BigQueryTableRef.from_string_name(
                    "projects/my_project/datasets/my_dataset/tables/my_source_view1"
                ),
            ],
            destinationTable=BigQueryTableRef.from_string_name(
                "projects/my_project/datasets/my_dataset/tables/my_table"
            ),
        ),
    ]


def test_lineage_with_timestamps(lineage_entries: List[QueryEvent]) -> None:
    config = BigQueryV2Config()
    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )

    bq_table = BigQueryTableRef.from_string_name(
        "projects/my_project/datasets/my_dataset/tables/my_table"
    )

    lineage_map: Dict[str, Set[LineageEdge]] = extractor._create_lineage_map(
        iter(lineage_entries)
    )

    upstream_lineage = extractor.get_lineage_for_table(
        bq_table=bq_table,
        bq_table_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        lineage_metadata=lineage_map,
    )
    assert upstream_lineage
    assert len(upstream_lineage.upstreams) == 4


def test_column_level_lineage(lineage_entries: List[QueryEvent]) -> None:
    config = BigQueryV2Config(extract_column_lineage=True, incremental_lineage=False)
    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )

    bq_table = BigQueryTableRef.from_string_name(
        "projects/my_project/datasets/my_dataset/tables/my_table"
    )

    lineage_map: Dict[str, Set[LineageEdge]] = extractor._create_lineage_map(
        lineage_entries[:1],
    )

    upstream_lineage = extractor.get_lineage_for_table(
        bq_table=bq_table,
        bq_table_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        lineage_metadata=lineage_map,
    )
    assert upstream_lineage
    assert len(upstream_lineage.upstreams) == 2
    assert (
        upstream_lineage.fineGrainedLineages
        and len(upstream_lineage.fineGrainedLineages) == 2
    )


def test_lineage_for_external_bq_table(mock_datahub_graph_instance):
    pipeline_context = PipelineContext(run_id="bq_gcs_lineage")
    pipeline_context.graph = mock_datahub_graph_instance

    def fake_schema_metadata(entity_urn: str) -> models.SchemaMetadataClass:
        return models.SchemaMetadataClass(
            schemaName="sample_schema",
            platform="urn:li:dataPlatform:gcs",  # important <- platform must be an urn
            version=0,
            hash="",
            platformSchema=models.OtherSchemaClass(
                rawSchema="__insert raw schema here__"
            ),
            fields=[
                models.SchemaFieldClass(
                    fieldPath="age",
                    type=models.SchemaFieldDataTypeClass(type=models.NumberTypeClass()),
                    nativeDataType="int",
                ),
                models.SchemaFieldClass(
                    fieldPath="firstname",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                ),
                models.SchemaFieldClass(
                    fieldPath="lastname",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR(100)",
                ),
            ],
        )

    pipeline_context.graph.get_schema_metadata = fake_schema_metadata  # type: ignore
    path_specs: List[PathSpec] = [
        PathSpec(include="gs://bigquery_data/{table}/*.parquet"),
        PathSpec(include="gs://bigquery_data/customer3/{table}/*.parquet"),
    ]
    gcs_lineage_config: GcsLineageProviderConfig = GcsLineageProviderConfig(
        path_specs=path_specs
    )

    config = BigQueryV2Config(
        include_table_lineage=True,
        include_column_lineage_with_gcs=True,
        gcs_lineage_config=gcs_lineage_config,
    )

    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )

    upstream_lineage = extractor.get_lineage_for_external_table(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        source_uris=[
            "gs://bigquery_data/customer1/*.parquet",
            "gs://bigquery_data/customer2/*.parquet",
            "gs://bigquery_data/customer3/my_table/*.parquet",
        ],
        graph=pipeline_context.graph,
    )

    expected_schema_field_urns = [
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer1,PROD),age)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer1,PROD),firstname)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer1,PROD),lastname)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer2,PROD),age)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer2,PROD),firstname)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer2,PROD),lastname)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer3/my_table,PROD),age)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer3/my_table,PROD),firstname)",
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer3/my_table,PROD),lastname)",
    ]
    assert upstream_lineage
    assert len(upstream_lineage.upstreams) == 3
    assert (
        upstream_lineage.fineGrainedLineages
        and len(upstream_lineage.fineGrainedLineages) == 9
    )
    # Extracting column URNs from upstream_lineage.upstreams
    actual_schema_field_urns = [
        fine_grained_lineage.upstreams[0]
        if fine_grained_lineage.upstreams is not None
        else []
        for fine_grained_lineage in upstream_lineage.fineGrainedLineages
    ]
    assert all(urn in expected_schema_field_urns for urn in actual_schema_field_urns), (
        "Some expected column URNs are missing from fine grained lineage."
    )


def test_lineage_for_external_bq_table_no_column_lineage(mock_datahub_graph_instance):
    pipeline_context = PipelineContext(run_id="bq_gcs_lineage")
    pipeline_context.graph = mock_datahub_graph_instance

    def fake_schema_metadata(entity_urn: str) -> Optional[models.SchemaMetadataClass]:
        return None

    pipeline_context.graph.get_schema_metadata = fake_schema_metadata  # type: ignore
    path_specs: List[PathSpec] = [
        PathSpec(include="gs://bigquery_data/{table}/*.parquet"),
        PathSpec(include="gs://bigquery_data/customer3/{table}/*.parquet"),
    ]
    gcs_lineage_config: GcsLineageProviderConfig = GcsLineageProviderConfig(
        path_specs=path_specs
    )

    config = BigQueryV2Config(
        include_table_lineage=True,
        include_column_lineage_with_gcs=True,
        gcs_lineage_config=gcs_lineage_config,
    )

    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )

    upstream_lineage = extractor.get_lineage_for_external_table(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        source_uris=[
            "gs://bigquery_data/customer1/*.parquet",
            "gs://bigquery_data/customer2/*.parquet",
            "gs://bigquery_data/customer3/my_table/*.parquet",
        ],
        graph=pipeline_context.graph,
    )

    expected_dataset_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer2,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,bigquery_data/customer3/my_table,PROD)",
    ]
    assert upstream_lineage
    assert len(upstream_lineage.upstreams) == 3
    # Extracting dataset URNs from upstream_lineage.upstreams
    actual_dataset_urns = [upstream.dataset for upstream in upstream_lineage.upstreams]
    assert all(urn in actual_dataset_urns for urn in expected_dataset_urns), (
        "Some expected dataset URNs are missing from upstream lineage."
    )
    assert upstream_lineage.fineGrainedLineages is None


def test_lineage_for_external_table_with_non_gcs_uri(mock_datahub_graph_instance):
    pipeline_context = PipelineContext(run_id="non_gcs_lineage")
    pipeline_context.graph = mock_datahub_graph_instance

    config = BigQueryV2Config(
        include_table_lineage=True,
        include_column_lineage_with_gcs=False,  # Column lineage disabled for simplicity
    )
    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )

    upstream_lineage = extractor.get_lineage_for_external_table(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        source_uris=[
            "https://some_non_gcs_path/customer1/file.csv",
            "https://another_path/file.txt",
        ],
        graph=pipeline_context.graph,
    )

    assert upstream_lineage is None


def test_lineage_for_external_table_path_not_matching_specs(
    mock_datahub_graph_instance,
):
    pipeline_context = PipelineContext(run_id="path_not_matching_lineage")
    pipeline_context.graph = mock_datahub_graph_instance

    path_specs: List[PathSpec] = [
        PathSpec(include="gs://different_data/db2/db3/{table}/*.parquet"),
    ]
    gcs_lineage_config: GcsLineageProviderConfig = GcsLineageProviderConfig(
        path_specs=path_specs, ignore_non_path_spec_path=True
    )
    config = BigQueryV2Config(
        include_table_lineage=True,
        include_column_lineage_with_gcs=False,
        gcs_lineage_config=gcs_lineage_config,
    )

    report = BigQueryV2Report()
    extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
        config,
        report,
        schema_resolver=SchemaResolver(platform="bigquery"),
        identifiers=BigQueryIdentifierBuilder(config, report),
    )

    upstream_lineage = extractor.get_lineage_for_external_table(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,my_project.my_dataset.my_table,PROD)",
        source_uris=[
            "gs://bigquery_data/customer1/*.parquet",
            "gs://bigquery_data/customer2/*.parquet",
        ],
        graph=pipeline_context.graph,
    )

    assert upstream_lineage is None
