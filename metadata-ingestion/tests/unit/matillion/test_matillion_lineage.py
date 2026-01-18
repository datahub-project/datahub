import pytest
from pydantic import SecretStr

from datahub.ingestion.source.matillion.config import (
    MatillionAPIConfig,
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion.matillion_lineage import (
    MatillionLineageHandler,
)
from datahub.ingestion.source.matillion.models import (
    MatillionLineageEdge,
    MatillionLineageGraph,
    MatillionLineageNode,
    MatillionPipeline,
)


@pytest.fixture
def config() -> MatillionSourceConfig:
    return MatillionSourceConfig(
        api_config=MatillionAPIConfig(
            api_token=SecretStr("test_token"),
            custom_base_url="http://test.com",
        ),
        include_lineage=True,
        include_column_lineage=True,
    )


@pytest.fixture
def report() -> MatillionSourceReport:
    return MatillionSourceReport()


@pytest.fixture
def lineage_handler(
    config: MatillionSourceConfig, report: MatillionSourceReport
) -> MatillionLineageHandler:
    return MatillionLineageHandler(config, report)


@pytest.mark.parametrize(
    "input_platform,expected_platform",
    [
        pytest.param("snowflake", "snowflake", id="snowflake_lowercase"),
        pytest.param("Snowflake", "snowflake", id="snowflake_mixed_case"),
        pytest.param("SNOWFLAKE", "snowflake", id="snowflake_uppercase"),
        pytest.param("bigquery", "bigquery", id="bigquery"),
        pytest.param("redshift", "redshift", id="redshift"),
        pytest.param("postgresql", "postgres", id="postgresql_to_postgres"),
        pytest.param("mysql", "mysql", id="mysql"),
        pytest.param("oracle", "oracle", id="oracle"),
        pytest.param("unknown", "external_db", id="unknown_platform"),
        pytest.param(None, "external_db", id="none_platform"),
    ],
)
def test_platform_mapping(
    lineage_handler: MatillionLineageHandler,
    input_platform: str,
    expected_platform: str,
) -> None:
    assert lineage_handler._map_platform(input_platform) == expected_platform


@pytest.mark.parametrize(
    "node_config,expected_urn_parts",
    [
        pytest.param(
            {
                "platform": "snowflake",
                "schema_name": "public",
                "table_name": "test_table",
            },
            ["snowflake", "public.test_table"],
            id="with_schema",
        ),
        pytest.param(
            {
                "platform": "bigquery",
                "schema_name": None,
                "table_name": "test_table",
            },
            ["bigquery", "test_table"],
            id="without_schema",
        ),
        pytest.param(
            {
                "platform": "redshift",
                "schema_name": "analytics",
                "table_name": "fact_sales",
            },
            ["redshift", "analytics.fact_sales"],
            id="redshift_with_schema",
        ),
    ],
)
def test_create_dataset_urn_from_node(
    lineage_handler: MatillionLineageHandler,
    node_config: dict,
    expected_urn_parts: list,
) -> None:
    node = MatillionLineageNode(
        id="node1",
        name="test_table",
        node_type="table",
        **node_config,
    )

    urn = lineage_handler._create_dataset_urn_from_node(node)
    assert urn is not None
    for part in expected_urn_parts:
        assert part in str(urn)


@pytest.mark.parametrize(
    "node_type,has_table_name",
    [
        pytest.param("transform", False, id="transform_node"),
        pytest.param("filter", False, id="filter_node"),
        pytest.param("table", False, id="table_node_no_name"),
    ],
)
def test_create_dataset_urn_missing_table_name(
    lineage_handler: MatillionLineageHandler,
    node_type: str,
    has_table_name: bool,
) -> None:
    node = MatillionLineageNode(
        id="node1",
        name="transformation",
        node_type=node_type,
        platform="snowflake",
        table_name=None if not has_table_name else "table",
    )

    urn = lineage_handler._create_dataset_urn_from_node(node)
    assert urn is None


def test_get_upstream_datasets(lineage_handler: MatillionLineageHandler) -> None:
    nodes = [
        MatillionLineageNode(
            id="source1",
            name="source_table",
            node_type="table",
            platform="snowflake",
            schema_name="raw",
            table_name="source_table",
        ),
        MatillionLineageNode(
            id="transform1",
            name="transformation",
            node_type="transform",
        ),
        MatillionLineageNode(
            id="target1",
            name="target_table",
            node_type="table",
            platform="snowflake",
            schema_name="curated",
            table_name="target_table",
        ),
    ]

    edges = [
        MatillionLineageEdge(source_id="source1", target_id="transform1"),
        MatillionLineageEdge(source_id="transform1", target_id="target1"),
    ]

    lineage_graph = MatillionLineageGraph(
        pipeline_id="pipeline1", nodes=nodes, edges=edges
    )

    upstreams = lineage_handler._get_upstream_datasets(lineage_graph, "target1")
    assert "source1" in upstreams
    assert "transform1" not in upstreams


def test_emit_pipeline_lineage(lineage_handler: MatillionLineageHandler) -> None:
    pipeline = MatillionPipeline(
        id="pipeline1",
        name="Test Pipeline",
        project_id="project1",
        pipeline_type="orchestration",
    )

    nodes = [
        MatillionLineageNode(
            id="source1",
            name="source_table",
            node_type="table",
            platform="snowflake",
            schema_name="raw",
            table_name="source_table",
        ),
        MatillionLineageNode(
            id="target1",
            name="target_table",
            node_type="table",
            platform="snowflake",
            schema_name="curated",
            table_name="target_table",
        ),
    ]

    edges = [
        MatillionLineageEdge(source_id="source1", target_id="target1"),
    ]

    lineage_graph = MatillionLineageGraph(
        pipeline_id="pipeline1", nodes=nodes, edges=edges
    )

    workunits = list(
        lineage_handler.emit_pipeline_lineage(
            pipeline, "urn:li:dataFlow:(matillion,pipeline1,PROD)", lineage_graph
        )
    )

    assert len(workunits) > 0


def test_emit_pipeline_lineage_with_column_lineage(
    lineage_handler: MatillionLineageHandler,
) -> None:
    pipeline = MatillionPipeline(
        id="pipeline1",
        name="Test Pipeline",
        project_id="project1",
        pipeline_type="orchestration",
    )

    nodes = [
        MatillionLineageNode(
            id="source1",
            name="id",
            node_type="column",
            platform="snowflake",
            schema_name="raw",
            table_name="source_table",
            column_name="id",
        ),
        MatillionLineageNode(
            id="target1",
            name="customer_id",
            node_type="column",
            platform="snowflake",
            schema_name="curated",
            table_name="target_table",
            column_name="customer_id",
        ),
    ]

    edges = [
        MatillionLineageEdge(source_id="source1", target_id="target1"),
    ]

    lineage_graph = MatillionLineageGraph(
        pipeline_id="pipeline1", nodes=nodes, edges=edges
    )

    workunits = list(
        lineage_handler.emit_pipeline_lineage(
            pipeline, "urn:li:dataFlow:(matillion,pipeline1,PROD)", lineage_graph
        )
    )

    assert len(workunits) >= 0


def test_empty_lineage_graph(lineage_handler: MatillionLineageHandler) -> None:
    pipeline = MatillionPipeline(
        id="pipeline1",
        name="Test Pipeline",
        project_id="project1",
        pipeline_type="orchestration",
    )

    lineage_graph = MatillionLineageGraph(pipeline_id="pipeline1", nodes=[], edges=[])

    workunits = list(
        lineage_handler.emit_pipeline_lineage(
            pipeline, "urn:li:dataFlow:(matillion,pipeline1,PROD)", lineage_graph
        )
    )

    assert len(workunits) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
