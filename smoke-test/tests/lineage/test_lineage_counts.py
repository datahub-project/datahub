import logging
from random import randint
from typing import Dict, Iterator, List, Optional

import pytest

import datahub.metadata.schema_classes as models
from datahub.configuration.common import GraphError
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from tests.utilities.domains import Domain
from tests.utils import delete_urns, wait_for_writes_to_sync

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

COUNTS_QUERY = """
query($input: SearchAcrossLineageCountsInput!) {
  searchAcrossLineageCounts(input: $input) {
    total
  }
}
"""

# The three kinds of schema field a lineage graph can point at, distinguished by the flags:
#   MATERIALIZED   -- has aspects of its own, so the entity index can return it
#   DECLARED_ONLY  -- no aspects, but its parent's schemaMetadata lists it, so it does exist
#   NONEXISTENT    -- only a graph edge; its parent's schemaMetadata does not list it
MATERIALIZED = "materialized_col"
DECLARED_ONLY = "declared_only_col"
NONEXISTENT = "nonexistent_col"


def _schema(*field_paths: str) -> models.SchemaMetadataClass:
    return models.SchemaMetadataClass(
        schemaName="counts",
        platform=make_data_platform_urn("snowflake"),
        version=0,
        hash="",
        platformSchema=models.OtherSchemaClass(rawSchema=""),
        fields=[
            models.SchemaFieldClass(
                fieldPath=field_path,
                type=models.SchemaFieldDataTypeClass(models.StringTypeClass()),
                nativeDataType="VARCHAR",
            )
            for field_path in field_paths
        ],
    )


def _fine_grained(
    upstream_field: str, downstream_field: str
) -> models.FineGrainedLineageClass:
    return models.FineGrainedLineageClass(
        upstreamType=models.FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        downstreamType=models.FineGrainedLineageDownstreamTypeClass.FIELD,
        upstreams=[upstream_field],
        downstreams=[downstream_field],
    )


@pytest.fixture(scope="module")
def lineage_counts_data(graph_client: DataHubGraph) -> Iterator[Dict[str, str]]:
    """Three downstream columns, each isolating one thing the counts query has to get right.

    col_a has one upstream of each materialization kind; col_b's only upstream sits under a
    dataset that was never emitted at all; col_c has upstreams on two different parents.
    """
    suffix = randint(10, 100000)
    upstream = make_dataset_urn("snowflake", f"counts_upstream_{suffix}")
    sibling = make_dataset_urn("dbt", f"counts_sibling_{suffix}")
    unknown_parent = make_dataset_urn("snowflake", f"counts_unknown_{suffix}")
    downstream = make_dataset_urn("snowflake", f"counts_downstream_{suffix}")

    col_a = make_schema_field_urn(downstream, "col_a")
    col_b = make_schema_field_urn(downstream, "col_b")
    col_c = make_schema_field_urn(downstream, "col_c")

    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=upstream, aspect=_schema(MATERIALIZED, DECLARED_ONLY)
        ),
        MetadataChangeProposalWrapper(entityUrn=sibling, aspect=_schema(DECLARED_ONLY)),
        MetadataChangeProposalWrapper(
            entityUrn=downstream, aspect=_schema("col_a", "col_b", "col_c")
        ),
        # Gives the schema field aspects of its own, which is what puts it in the entity index.
        # The rest of the columns below have none, so only the graph knows about them.
        MetadataChangeProposalWrapper(
            entityUrn=make_schema_field_urn(upstream, MATERIALIZED),
            aspect=models.DocumentationClass(
                documentations=[
                    models.DocumentationAssociationClass(
                        documentation="Materialized column"
                    )
                ]
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=downstream,
            aspect=models.UpstreamLineageClass(
                upstreams=[
                    models.UpstreamClass(
                        dataset=upstream,
                        type=models.DatasetLineageTypeClass.TRANSFORMED,
                    )
                ],
                fineGrainedLineages=[
                    _fine_grained(make_schema_field_urn(upstream, MATERIALIZED), col_a),
                    _fine_grained(
                        make_schema_field_urn(upstream, DECLARED_ONLY), col_a
                    ),
                    _fine_grained(make_schema_field_urn(upstream, NONEXISTENT), col_a),
                    _fine_grained(
                        make_schema_field_urn(unknown_parent, DECLARED_ONLY), col_b
                    ),
                    _fine_grained(
                        make_schema_field_urn(upstream, DECLARED_ONLY), col_c
                    ),
                    _fine_grained(make_schema_field_urn(sibling, DECLARED_ONLY), col_c),
                ],
            ),
        ),
    ]
    for mcp in mcps:
        graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()

    yield {
        "upstream": upstream,
        "sibling": sibling,
        "unknown_parent": unknown_parent,
        "col_a": col_a,
        "col_b": col_b,
        "col_c": col_c,
    }

    delete_urns(
        graph_client,
        [
            upstream,
            sibling,
            unknown_parent,
            downstream,
            make_schema_field_urn(upstream, MATERIALIZED),
        ],
    )


def count_upstream_columns(
    graph_client: DataHubGraph,
    urn: str,
    include_ghost_entities: Optional[bool] = None,
    validate_schema_fields: Optional[str] = None,
    or_filters: Optional[List[dict]] = None,
    lineage_flags: Optional[dict] = None,
    include_soft_deleted: Optional[bool] = None,
) -> int:
    query_input: Dict[str, object] = {
        "urn": urn,
        "direction": "UPSTREAM",
        "types": ["SCHEMA_FIELD"],
    }
    if include_ghost_entities is not None:
        query_input["includeGhostEntities"] = include_ghost_entities
    if validate_schema_fields is not None:
        query_input["validateSchemaFields"] = validate_schema_fields
    if or_filters is not None:
        query_input["orFilters"] = or_filters
    if lineage_flags is not None:
        query_input["lineageFlags"] = lineage_flags
    if include_soft_deleted is not None:
        query_input["includeSoftDeleted"] = include_soft_deleted

    result = graph_client.execute_graphql(
        COUNTS_QUERY, variables={"input": query_input}
    )
    return result["searchAcrossLineageCounts"]["total"]


def test_counts_by_materialization(graph_client, lineage_counts_data):
    """The flags select how far past the entity index a count is willing to look."""
    col_a = lineage_counts_data["col_a"]

    # Everything the graph points at
    assert (
        count_upstream_columns(
            graph_client,
            col_a,
            include_ghost_entities=True,
            validate_schema_fields="NONE",
        )
        == 3
    ), f"expected {MATERIALIZED}, {DECLARED_ONLY} and {NONEXISTENT}"

    # Everything that exists, whether or not it was materialized: validation reads the parent's
    # schemaMetadata, which lists the first two and not the third
    assert (
        count_upstream_columns(
            graph_client,
            col_a,
            include_ghost_entities=True,
            validate_schema_fields="ALWAYS",
        )
        == 2
    ), f"expected {MATERIALIZED} and {DECLARED_ONLY}"

    # Only what the entity index can return
    assert (
        count_upstream_columns(graph_client, col_a, include_ghost_entities=False) == 1
    ), f"expected {MATERIALIZED} alone"

    # And the count above really is keyed on materialization: col_c's upstreams both exist per
    # their parents' schemaMetadata, but neither was materialized, so the index sees none of them
    assert (
        count_upstream_columns(
            graph_client,
            lineage_counts_data["col_c"],
            include_ghost_entities=True,
            validate_schema_fields="ALWAYS",
        )
        == 2
    )
    assert (
        count_upstream_columns(
            graph_client, lineage_counts_data["col_c"], include_ghost_entities=False
        )
        == 0
    )


def test_validation_drops_columns_whose_parent_has_no_schema(
    graph_client, lineage_counts_data
):
    col_b = lineage_counts_data["col_b"]

    assert (
        count_upstream_columns(
            graph_client,
            col_b,
            include_ghost_entities=True,
            validate_schema_fields="NONE",
        )
        == 1
    )
    # Its parent was never emitted, so there is no schemaMetadata to confirm it against
    assert (
        count_upstream_columns(
            graph_client,
            col_b,
            include_ghost_entities=True,
            validate_schema_fields="ALWAYS",
        )
        == 0
    )


def test_counts_filter_by_parent(graph_client, lineage_counts_data):
    col_c = lineage_counts_data["col_c"]

    def parent_filter(parent: str, negated: bool) -> List[dict]:
        return [
            {
                "and": [
                    {
                        "field": "parent",
                        "condition": "EQUAL",
                        "values": [parent],
                        "negated": negated,
                    }
                ]
            }
        ]

    def count(or_filters: Optional[List[dict]]) -> int:
        return count_upstream_columns(
            graph_client,
            col_c,
            include_ghost_entities=True,
            validate_schema_fields="NONE",
            or_filters=or_filters,
        )

    # A schema field nests its parent in its own urn, so the graph-only path can honor this
    # without the entity index -- this is how columns folded into a sibling node are excluded
    assert count(None) == 2
    assert count(parent_filter(lineage_counts_data["sibling"], negated=True)) == 1
    assert count(parent_filter(lineage_counts_data["sibling"], negated=False)) == 1


def test_counts_filter_by_platform(graph_client, lineage_counts_data):
    col_c = lineage_counts_data["col_c"]

    def platform_filter(platform: str) -> List[dict]:
        return [
            {
                "and": [
                    {
                        "field": "platform",
                        "condition": "EQUAL",
                        "values": [make_data_platform_urn(platform)],
                    }
                ]
            }
        ]

    # A schema field takes its parent's platform, so a platform filter narrows a column count
    assert (
        count_upstream_columns(
            graph_client,
            col_c,
            include_ghost_entities=True,
            validate_schema_fields="NONE",
            or_filters=platform_filter("dbt"),
        )
        == 1
    )
    assert (
        count_upstream_columns(
            graph_client,
            col_c,
            include_ghost_entities=True,
            validate_schema_fields="NONE",
            or_filters=platform_filter("bigquery"),
        )
        == 0
    )


def test_counts_reject_unservable_graph_query(graph_client, lineage_counts_data):
    # The graph-only path can only answer filters derivable from a urn, and a request that asks
    # for it without meeting that has to fail rather than quietly fall back to the entity index
    unsupported_filter = [
        {
            "and": [
                {
                    "field": "description",
                    "condition": "CONTAIN",
                    "values": ["anything"],
                }
            ]
        }
    ]
    with pytest.raises(GraphError):
        count_upstream_columns(
            graph_client,
            lineage_counts_data["col_a"],
            include_ghost_entities=True,
            or_filters=unsupported_filter,
        )


DBT_PLATFORM = make_data_platform_urn("dbt")

# The hops the lineage graph walks through rather than drawing, mirroring the frontend's
# generateIgnoreAsHops. The SCHEMA_FIELD entry is the one that needs a schema field's platform to
# be read off its parent, since a schema field urn carries no platform of its own.
IGNORE_AS_HOPS: List[dict] = [
    {"entityType": "DATASET", "platforms": [DBT_PLATFORM]},
    {"entityType": "SCHEMA_FIELD", "platforms": [DBT_PLATFORM]},
    {"entityType": "DATA_PROCESS_INSTANCE"},
    {"entityType": "DATA_JOB"},
]


def related_column_filters(merged_urns: Optional[List[str]] = None) -> List[dict]:
    """The filters the lineage graph's column counts send, from buildRelatedColumnFilters."""
    and_criteria: List[dict] = [
        {"field": "degree", "values": ["1"]},
        {
            "field": "parent",
            "values": [DBT_PLATFORM],
            "condition": "CONTAIN",
            "negated": True,
        },
    ]
    if merged_urns:
        and_criteria.append({"field": "parent", "values": merged_urns, "negated": True})
    return [{"and": and_criteria}]


@pytest.fixture(scope="module")
def hops_data(graph_client: DataHubGraph) -> Iterator[Dict[str, str]]:
    """A column chain running through a dbt model, which the graph walks through as a hop.

    source.src_col -> dbt_model.dbt_col -> target.tgt_col
    """
    suffix = randint(10, 100000)
    source = make_dataset_urn("snowflake", f"hops_source_{suffix}")
    dbt_model = make_dataset_urn("dbt", f"hops_dbt_{suffix}")
    target = make_dataset_urn("snowflake", f"hops_target_{suffix}")

    mcps = [
        MetadataChangeProposalWrapper(entityUrn=source, aspect=_schema("src_col")),
        MetadataChangeProposalWrapper(entityUrn=dbt_model, aspect=_schema("dbt_col")),
        MetadataChangeProposalWrapper(entityUrn=target, aspect=_schema("tgt_col")),
        MetadataChangeProposalWrapper(
            entityUrn=dbt_model,
            aspect=models.UpstreamLineageClass(
                upstreams=[
                    models.UpstreamClass(
                        dataset=source, type=models.DatasetLineageTypeClass.TRANSFORMED
                    )
                ],
                fineGrainedLineages=[
                    _fine_grained(
                        make_schema_field_urn(source, "src_col"),
                        make_schema_field_urn(dbt_model, "dbt_col"),
                    )
                ],
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=target,
            aspect=models.UpstreamLineageClass(
                upstreams=[
                    models.UpstreamClass(
                        dataset=dbt_model,
                        type=models.DatasetLineageTypeClass.TRANSFORMED,
                    )
                ],
                fineGrainedLineages=[
                    _fine_grained(
                        make_schema_field_urn(dbt_model, "dbt_col"),
                        make_schema_field_urn(target, "tgt_col"),
                    )
                ],
            ),
        ),
    ]
    for mcp in mcps:
        graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()

    yield {
        "source": source,
        "dbt_model": dbt_model,
        "target": target,
        "tgt_col": make_schema_field_urn(target, "tgt_col"),
        "src_col": make_schema_field_urn(source, "src_col"),
        "dbt_col": make_schema_field_urn(dbt_model, "dbt_col"),
    }

    delete_urns(graph_client, [source, dbt_model, target])


def test_counts_ignore_as_hops_for_schema_fields(graph_client, hops_data):
    """The exact parameters the lineage graph's column counts send, all together."""
    tgt_col = hops_data["tgt_col"]

    def count(or_filters: List[dict], ignore_as_hops: Optional[List[dict]]) -> int:
        return count_upstream_columns(
            graph_client,
            tgt_col,
            include_ghost_entities=True,
            or_filters=or_filters,
            lineage_flags=(
                {"ignoreAsHops": ignore_as_hops} if ignore_as_hops is not None else None
            ),
        )

    # Without the dbt column being walked through, the only degree-1 upstream is that column --
    # which the parent filter then excludes, leaving nothing to count
    assert count(related_column_filters(), None) == 0

    # Treating it as a hop puts the snowflake column behind it at degree 1 instead
    assert count(related_column_filters(), IGNORE_AS_HOPS) == 1

    # Siblings are drawn folded into the node they belong to, so their columns are excluded the
    # same way -- here that removes the one remaining upstream
    assert count(related_column_filters([hops_data["source"]]), IGNORE_AS_HOPS) == 0


def test_counts_with_lineage_time_window(graph_client, hops_data):
    # A time window is served off the graph like any other lineage flag, and a window wide enough
    # to contain everything leaves the count alone
    assert (
        count_upstream_columns(
            graph_client,
            hops_data["tgt_col"],
            include_ghost_entities=True,
            or_filters=related_column_filters(),
            lineage_flags={
                "ignoreAsHops": IGNORE_AS_HOPS,
                "startTimeMillis": 0,
                "endTimeMillis": 99999999999999,
            },
        )
        == 1
    )


def test_include_soft_deleted_is_ignored_off_the_graph(graph_client, hops_data):
    # Documented behavior: a count read off the graph never consults the entity index, so the
    # soft-delete state held there cannot narrow it either way
    counts = {
        count_upstream_columns(
            graph_client,
            hops_data["tgt_col"],
            include_ghost_entities=True,
            or_filters=related_column_filters(),
            lineage_flags={"ignoreAsHops": IGNORE_AS_HOPS},
            include_soft_deleted=soft_deleted,
        )
        for soft_deleted in (True, False)
    }
    assert counts == {1}
