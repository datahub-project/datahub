from unittest import mock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_common import DBTColumn, DBTNode
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource


def _make_glue_source() -> DBTCoreSource:
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-glue")
    ctx.graph = None
    return DBTCoreSource(
        DBTCoreConfig(
            manifest_path="temp/",
            catalog_path="temp/",
            target_platform="glue",
            target_platform_instance="prod-usw2-glue",
            include_database_name=False,
            enable_meta_mapping=False,
        ),
        ctx,
    )


def _make_glue_nodes(
    upstream_dbt_name: str,
    model_dbt_name: str,
    compiled_sql: str,
) -> tuple:
    upstream = DBTNode(
        database=None,  # stripped by include_database_name=False
        schema="angi_snowplow_lake_loader_events_prod",
        name="events",
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter=None,
        dbt_name=upstream_dbt_name,
        dbt_file_path=None,
        dbt_package_name="project",
        node_type="source",
        max_loaded_at=None,
        materialization=None,
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
        columns=[
            DBTColumn(
                name="event_id", comment="", description="", index=0, data_type="string"
            )
        ],
    )
    model = DBTNode(
        database=None,
        schema="analytics",
        name="my_model",
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="trino",
        dbt_name=model_dbt_name,
        dbt_file_path=None,
        dbt_package_name="project",
        node_type="model",
        max_loaded_at=None,
        materialization="table",
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
        upstream_nodes=[upstream_dbt_name],
        compiled_code=compiled_sql,
    )
    return upstream, model


def test_glue_cll_catalog_prefix_in_sql_resolves_to_registered_node() -> None:
    """
    Regression: compiled SQL has "iceberg_prod_usw2"."schema"."table" (3-part) but
    the dbt source node is registered without the catalog (include_database_name=False).
    SchemaResolver strips the catalog on lookup so upstream_cll is populated.
    """
    upstream_dbt_name = "source.project.angi_snowplow.events"
    model_dbt_name = "model.project.my_model"
    upstream, model = _make_glue_nodes(
        upstream_dbt_name,
        model_dbt_name,
        compiled_sql=(
            "SELECT event_id "
            'FROM "iceberg_prod_usw2"."angi_snowplow_lake_loader_events_prod"."events"'
        ),
    )

    source = _make_glue_source()
    source._infer_schemas_and_update_cll(
        {upstream_dbt_name: upstream, model_dbt_name: model}
    )

    assert len(model.upstream_cll) == 1
    cll = model.upstream_cll[0]
    assert cll.upstream_dbt_name == upstream_dbt_name
    assert cll.upstream_col == "event_id"
    assert cll.downstream_col == "event_id"


def test_glue_cll_without_catalog_prefix_still_works() -> None:
    """Control: 2-part SQL reference also produces correct upstream_cll."""
    upstream_dbt_name = "source.project.angi_snowplow.events"
    model_dbt_name = "model.project.my_model"
    upstream, model = _make_glue_nodes(
        upstream_dbt_name,
        model_dbt_name,
        compiled_sql=(
            'SELECT event_id FROM "angi_snowplow_lake_loader_events_prod"."events"'
        ),
    )

    source = _make_glue_source()
    source._infer_schemas_and_update_cll(
        {upstream_dbt_name: upstream, model_dbt_name: model}
    )

    assert len(model.upstream_cll) == 1
    assert model.upstream_cll[0].upstream_dbt_name == upstream_dbt_name


def test_glue_cll_v2_fieldpath_schema_from_graph_resolves() -> None:
    """When the upstream schema is fetched from the graph (e.g. a Glue dataset),
    its fieldPaths are v2-encoded (``[version=2.0].[type=string].event_id``).
    `_to_schema_info` must strip the v2 wrapper so the SQL parser can match the
    bare column names; otherwise upstream_cll comes back empty."""
    from datahub.metadata.schema_classes import (
        SchemaFieldClass,
        SchemaFieldDataTypeClass,
        SchemaMetadataClass,
        StringTypeClass,
    )

    upstream_dbt_name = "source.project.angi_snowplow.events"
    model_dbt_name = "model.project.my_model"
    upstream, model = _make_glue_nodes(
        upstream_dbt_name,
        model_dbt_name,
        compiled_sql=(
            "SELECT event_id "
            'FROM "iceberg_prod_usw2"."angi_snowplow_lake_loader_events_prod"."events"'
        ),
    )
    upstream.columns = []  # force the schema to come from the graph, not the catalog

    v2_schema = SchemaMetadataClass(
        schemaName="events",
        platform="urn:li:dataPlatform:glue",
        version=0,
        hash="",
        platformSchema=None,  # type: ignore[arg-type]
        fields=[
            SchemaFieldClass(
                fieldPath="[version=2.0].[type=string].event_id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            )
        ],
    )

    source = _make_glue_source()
    source.ctx.graph = mock.MagicMock()
    source.ctx.graph.get_aspect.return_value = v2_schema

    source._infer_schemas_and_update_cll(
        {upstream_dbt_name: upstream, model_dbt_name: model}
    )

    assert len(model.upstream_cll) == 1
    assert model.upstream_cll[0].upstream_col == "event_id"
    assert model.upstream_cll[0].downstream_col == "event_id"
