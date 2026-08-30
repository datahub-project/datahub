import subprocess
import sys
import textwrap
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, cast
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import GoogleAPIError, PermissionDenied

from datahub.emitter.mce_builder import schema_field_urn_to_key
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
    BigQueryTableRef,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryIdentifierConfig,
    BigQueryV2Config,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryDataset,
)
from datahub.ingestion.source.bigquery_v2.bigquery_sharing import (
    REASON_SERVICE_DISABLED,
    BigQuerySharingHandler,
    LinkedDatasetInfo,
)
from datahub.ingestion.source.bigquery_v2.common import BigQueryIdentifierBuilder
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

CONSUMER_PROJECT = "consumer-project"
PUBLISHER_PROJECT = "publisher-project"
PUBLISHER_PROJECT_NUMBER = "123456789012"
LINKED_DATASET = "linked_ds"
SOURCE_DATASET = "source_ds"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_column(
    name: str,
    field_path: Optional[str] = None,
    ordinal_position: int = 1,
    data_type: Optional[str] = "STRING",
) -> BigqueryColumn:
    return BigqueryColumn(
        name=name,
        ordinal_position=ordinal_position,
        field_path=field_path or name,
        is_nullable=True,
        # data_type is None for nested STRUCT leaf rows at runtime (queries.py's
        # `CASE WHEN CONTAINS_SUBSTR(field_path, ".") THEN NULL ELSE c.data_type END`),
        # even though BigqueryColumn.data_type is typed as `str`, not `Optional[str]`.
        data_type=data_type,  # type: ignore[arg-type]
        comment=None,
        is_partition_column=False,
        cluster_column_position=None,
    )


def _linked_dataset_properties(
    source_project_number: str = PUBLISHER_PROJECT_NUMBER,
    source_dataset_id: str = SOURCE_DATASET,
    link_state: Optional[str] = "LINKED",
    include_source: bool = True,
) -> Dict[str, Any]:
    properties: Dict[str, Any] = {"type": "LINKED"}
    if include_source:
        properties["linkedDatasetSource"] = {
            "sourceDataset": {
                "projectId": source_project_number,
                "datasetId": source_dataset_id,
            }
        }
    if link_state is not None:
        properties["linkedDatasetMetadata"] = {"linkState": link_state}
    return properties


def _make_get_dataset_response(
    source_project_number: str = PUBLISHER_PROJECT_NUMBER,
    source_dataset_id: str = SOURCE_DATASET,
    link_state: Optional[str] = "LINKED",
    include_source: bool = True,
) -> MagicMock:
    # REST resource object, not an INFORMATION_SCHEMA row - attributes assigned
    # directly onto `_properties`, matching the precedent at
    # test_bigquery_source.py:578-586 for `location`.
    response = MagicMock()
    response._properties = _linked_dataset_properties(
        source_project_number=source_project_number,
        source_dataset_id=source_dataset_id,
        link_state=link_state,
        include_source=include_source,
    )
    return response


def _resolvable_client(get_dataset_response: Optional[MagicMock] = None) -> MagicMock:
    """A bq client that resolves any linked dataset's publisher via tier 1 alone."""
    client = MagicMock()
    client.get_dataset.return_value = (
        get_dataset_response
        if get_dataset_response is not None
        else _make_get_dataset_response()
    )
    client.list_projects.return_value = [
        SimpleNamespace(
            project_id=PUBLISHER_PROJECT,
            numeric_id=PUBLISHER_PROJECT_NUMBER,
            friendly_name="",
        )
    ]
    return client


# The handler takes a BigQueryV2Config; the tests pass a lightweight SimpleNamespace
# stand-in carrying just the attributes the handler reads, cast to that type. A real
# BigQueryV2Config is a pydantic model that rejects the per-test attribute assignment
# (e.g. monkeypatching get_sharing_client) that several tests rely on.
# `client`/`projects_client` mirror BigQuerySchemaApi's own parameter names.
def _make_handler(
    client: Optional[MagicMock] = None,
    projects_client: Optional[MagicMock] = None,
    report: Optional[BigQueryV2Report] = None,
    include_table_lineage: bool = True,
    convert_column_urns_to_lowercase: bool = False,
) -> BigQuerySharingHandler:
    report = report if report is not None else BigQueryV2Report()
    config = SimpleNamespace(
        include_linked_dataset_lineage=True,
        extract_subscriptions_from_analytics_hub=False,
        include_table_lineage=include_table_lineage,
        convert_column_urns_to_lowercase=convert_column_urns_to_lowercase,
    )
    identifiers = BigQueryIdentifierBuilder(BigQueryIdentifierConfig(), report)
    return BigQuerySharingHandler(
        cast(BigQueryV2Config, config),
        report,
        identifiers=identifiers,
        client=client if client is not None else MagicMock(),
        projects_client=(
            projects_client if projects_client is not None else MagicMock()
        ),
    )


def _info(
    handler: BigQuerySharingHandler, dataset: str = LINKED_DATASET
) -> LinkedDatasetInfo:
    info = handler.get_info(CONSUMER_PROJECT, dataset)
    assert info is not None
    return info


def _upstream_lineage(
    workunits: List[MetadataWorkUnit],
) -> Optional[UpstreamLineageClass]:
    for wu in workunits:
        if isinstance(wu.metadata.aspect, UpstreamLineageClass):  # type: ignore[union-attr]
            return wu.metadata.aspect  # type: ignore[union-attr]
    return None


def _make_aggregator(resolver: SchemaResolver) -> SqlParsingAggregator:
    """A lineage aggregator built like BigqueryLineageExtractor's (lineage.py:247)."""
    return SqlParsingAggregator(
        platform="bigquery",
        platform_instance=None,
        env="PROD",
        schema_resolver=resolver,
        eager_graph_load=False,
        generate_lineage=True,
        generate_queries=True,
        generate_usage_statistics=False,
        generate_query_usage_statistics=False,
        generate_operations=False,
    )


def _register_and_flush(
    handler: BigQuerySharingHandler,
    schema_by_urn: Dict[str, Dict[str, str]],
    table_refs: Set[str],
) -> List[MetadataWorkUnit]:
    """Register the given linked table_refs as COPY mappings on an aggregator whose
    resolver holds the given consumer schemas, then flush. An empty schema_by_urn is the
    lineage_use_sql_parser-off shape, where no identity column lineage can be built.
    """
    resolver = SchemaResolver(platform="bigquery", env="PROD")
    for urn, schema_info in schema_by_urn.items():
        resolver.add_raw_schema_info(urn, schema_info)
    aggregator = _make_aggregator(resolver)
    handler.register_known_lineage(aggregator, set(table_refs))
    return list(auto_workunit(aggregator.gen_metadata()))


def _lineage_workunits(
    handler: BigQuerySharingHandler,
    project_id: str,
    dataset_name: str,
    entity_name: str,
    columns: List[BigqueryColumn],
) -> List[MetadataWorkUnit]:
    """Drive the production path for a single linked entity: seed its consumer schema the
    way the schema generator seeds the shared resolver (simple top-level column name,
    lowercased when convert_column_urns_to_lowercase is on), then register + flush via
    _register_and_flush. Covers the nested-column collapse and the casing.
    """
    consumer_urn = handler.identifiers.gen_dataset_urn(
        project_id, dataset_name, entity_name
    )
    lowercase = handler.config.convert_column_urns_to_lowercase
    schema_info: Dict[str, str] = {}
    for column in columns:
        name = column.name.lower() if lowercase else column.name
        schema_info.setdefault(name, column.data_type or "STRING")
    table_ref = str(
        BigQueryTableRef(
            BigqueryTableIdentifier(project_id, dataset_name, entity_name)
        ).get_sanitized_table_ref()
    )
    return _register_and_flush(handler, {consumer_urn: schema_info}, {table_ref})


def _field_path_of(schema_field_urn: str) -> str:
    key = schema_field_urn_to_key(schema_field_urn)
    assert key is not None
    return key.fieldPath


def _first_downstream(fgl: FineGrainedLineageClass) -> str:
    assert fgl.downstreams
    return fgl.downstreams[0]


def _first_upstream(fgl: FineGrainedLineageClass) -> str:
    assert fgl.upstreams
    return fgl.upstreams[0]


# ---------------------------------------------------------------------------
# Nested-column duplication
# ---------------------------------------------------------------------------


def test_nested_columns_dedupe_to_one_edge_each() -> None:
    # COLUMN_FIELD_PATHS returns one row per leaf field path,
    # so a STRUCT column is represented by several BigqueryColumn entries that share
    # one `.name` but differ in `.field_path`. A table with `id` plus a 3-field
    # `person` struct yields 5 such rows for 2 real columns. Without deduping by
    # name, fineGrainedLineages would carry 3 duplicate `person` entries instead of 1.
    handler = _make_handler(client=_resolvable_client())
    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    columns = [
        _make_column("id"),
        _make_column(
            "person", field_path="person.name", ordinal_position=2, data_type=None
        ),
        _make_column(
            "person", field_path="person.age", ordinal_position=2, data_type=None
        ),
        _make_column(
            "person",
            field_path="person.address.zip",
            ordinal_position=2,
            data_type=None,
        ),
    ]

    workunits = _lineage_workunits(
        handler, CONSUMER_PROJECT, LINKED_DATASET, "nested_cols_table", columns
    )

    upstream_lineage = _upstream_lineage(workunits)
    assert upstream_lineage is not None
    assert upstream_lineage.fineGrainedLineages is not None
    downstream_field_names = sorted(
        _field_path_of(_first_downstream(fgl))
        for fgl in upstream_lineage.fineGrainedLineages
    )
    assert downstream_field_names == ["id", "person"]


def test_copy_edge_without_resolver_schema_has_no_cll() -> None:
    # lineage_use_sql_parser=False opts out of schema-resolved lineage, so the shared
    # resolver never learns the consumer's columns. The table-level COPY edge is still
    # a known mapping and must emit; there is just no schema to build identity CLL from,
    # so fineGrainedLineages is empty. It must degrade, not error.
    handler = _make_handler(client=_resolvable_client())
    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    table_ref = str(
        BigQueryTableRef(
            BigqueryTableIdentifier(CONSUMER_PROJECT, LINKED_DATASET, "plain_table")
        ).get_sanitized_table_ref()
    )
    workunits = _register_and_flush(handler, {}, {table_ref})

    upstream_lineage = _upstream_lineage(workunits)
    assert upstream_lineage is not None
    publisher_urn = handler.identifiers.gen_dataset_urn(
        PUBLISHER_PROJECT, SOURCE_DATASET, "plain_table"
    )
    assert [u.dataset for u in upstream_lineage.upstreams] == [publisher_urn]
    assert not upstream_lineage.fineGrainedLineages


def test_snapshot_in_a_linked_dataset_gets_the_copy_edge() -> None:
    # A linked snapshot mirrors the publisher's same-named snapshot exactly like a linked
    # table or view, so it gets the same COPY edge.
    handler = _make_handler(client=_resolvable_client())
    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    def _ref(name: str) -> str:
        return str(
            BigQueryTableRef(
                BigqueryTableIdentifier(CONSUMER_PROJECT, LINKED_DATASET, name)
            ).get_sanitized_table_ref()
        )

    schema_by_urn = {
        handler.identifiers.gen_dataset_urn(CONSUMER_PROJECT, LINKED_DATASET, name): {
            "id": "INT64"
        }
        for name in ("plain_table", "a_snapshot")
    }
    workunits = _register_and_flush(
        handler, schema_by_urn, {_ref("plain_table"), _ref("a_snapshot")}
    )

    entity_urns = {
        wu.metadata.entityUrn  # type: ignore[union-attr]
        for wu in workunits
        if isinstance(wu.metadata.aspect, UpstreamLineageClass)  # type: ignore[union-attr]
    }
    for name in ("plain_table", "a_snapshot"):
        assert (
            handler.identifiers.gen_dataset_urn(CONSUMER_PROJECT, LINKED_DATASET, name)
            in entity_urns
        )
    assert handler.report.num_linked_dataset_lineage_emitted == 2


# ---------------------------------------------------------------------------
# Half-lowercased URNs
# ---------------------------------------------------------------------------


def test_column_name_is_lowercased_before_the_field_urn_is_built() -> None:
    # convert_column_urns_to_lowercase lowers the column
    # name itself before make_schema_field_urn is called, mirroring the existing
    # gen_foreign_keys pattern (bigquery_schema_gen.py:919-930). Applying it only to
    # the dataset half produces a schema field URN that is lowercase on one side and
    # mixed-case on the other, which resolves to nothing.
    #
    # project/dataset/table identifiers below are already all-lowercase
    # so the assertion below isolates the column-name half specifically.
    handler = _make_handler(
        client=_resolvable_client(), convert_column_urns_to_lowercase=True
    )
    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    workunits = _lineage_workunits(
        handler,
        CONSUMER_PROJECT,
        LINKED_DATASET,
        "plain_table",
        [_make_column("UserId")],
    )

    upstream_lineage = _upstream_lineage(workunits)
    assert upstream_lineage is not None
    assert upstream_lineage.fineGrainedLineages
    fgl = upstream_lineage.fineGrainedLineages[0]
    downstream_urn = _first_downstream(fgl)
    upstream_urn = _first_upstream(fgl)
    # Assert on the field-path half specifically. A whole-URN lowercase check cannot
    # pass on any implementation: the env segment is `PROD`, uppercase by
    # construction, as the shipped bigquery_lowercase_columns golden also shows.
    # Lowercasing only the dataset half - the bug this guards - leaves the field path
    # as `UserId` and fails here.
    assert _field_path_of(downstream_urn) == "userid"
    assert _field_path_of(upstream_urn) == "userid"


# ---------------------------------------------------------------------------
# Resolution failure -> zero aspects emitted, plus a structured warning
# ---------------------------------------------------------------------------


def test_populate_for_project_missing_source_emits_nothing_and_warns() -> None:
    # An absent linkedDatasetSource means the publisher reference cannot be built at
    # all - a URN assembled from an unresolved reference would match nothing and
    # create a phantom entity, so nothing must be emitted, and the gap must be
    # visible in the report.
    client = MagicMock()
    client.get_dataset.return_value = _make_get_dataset_response(include_source=False)
    report = BigQueryV2Report()
    handler = _make_handler(client=client, report=report)

    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    assert len(report.warnings) > 0

    workunits = _lineage_workunits(
        handler, CONSUMER_PROJECT, LINKED_DATASET, "plain_table", [_make_column("id")]
    )
    assert workunits == []


# ---------------------------------------------------------------------------
# linkState absent -> lineage still emitted (the regression that would kill the
# feature silently and without warning)
# ---------------------------------------------------------------------------


def test_populate_for_project_missing_link_state_still_emits_lineage() -> None:
    # A missing
    # linkedDatasetMetadata is not evidence the link is dead - it could be a region,
    # an API version, or a link created moments ago - and must not suppress lineage.
    # Gating on `link_state != LINKED` instead of `link_state not in (None, LINKED)`
    # passes every other test in this file while breaking this one.
    handler = _make_handler(
        client=_resolvable_client(
            get_dataset_response=_make_get_dataset_response(link_state=None)
        )
    )
    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    workunits = _lineage_workunits(
        handler, CONSUMER_PROJECT, LINKED_DATASET, "plain_table", [_make_column("id")]
    )
    assert _upstream_lineage(workunits) is not None
    assert handler.report.num_linked_datasets_missing_link_state == 1


# ---------------------------------------------------------------------------
# linkState explicitly not LINKED -> no lineage
# ---------------------------------------------------------------------------


def test_populate_for_project_dead_link_state_suppresses_lineage() -> None:
    # Synthetic input: no live route to this state is known (revoking a share
    # deletes the linked dataset outright within seconds, so a present-but-dead link
    # has never been observed), but the gate is kept as insurance in case that
    # changes.
    handler = _make_handler(
        client=_resolvable_client(
            get_dataset_response=_make_get_dataset_response(link_state="NOT_LINKED")
        )
    )
    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    workunits = _lineage_workunits(
        handler, CONSUMER_PROJECT, LINKED_DATASET, "plain_table", [_make_column("id")]
    )
    assert workunits == []
    assert handler.report.num_linked_datasets_not_linked == 1


# ---------------------------------------------------------------------------
# A project with no linked datasets -> zero get_dataset calls
# ---------------------------------------------------------------------------


def test_populate_for_project_no_linked_datasets_makes_no_api_calls() -> None:
    client = MagicMock()
    handler = _make_handler(client=client)

    datasets = [
        BigqueryDataset(name="plain_ds", type=None),
        BigqueryDataset(name="another_ds", type="DEFAULT"),
    ]
    handler.populate_for_project(CONSUMER_PROJECT, datasets)

    client.get_dataset.assert_not_called()
    client.list_projects.assert_not_called()


def test_warn_all_types_missing() -> None:
    # BigQuery normally returns Dataset.type for every dataset. If a client or API
    # change stops returning it, every dataset reads as non-linked and the feature
    # silently does nothing -- warn once so that is distinguishable from a project that
    # simply has no linked datasets. Asserted against report.warning calls (deterministic)
    # rather than caplog, which is prone to cross-block record contamination.
    title = "Linked datasets enabled but no dataset types returned"

    handler = _make_handler()
    with patch.object(
        handler.report, "warning", wraps=handler.report.warning
    ) as warn_spy:
        # Two projects, both with every type missing: the warning must be one-time.
        handler.populate_for_project(
            CONSUMER_PROJECT,
            [BigqueryDataset(name="a"), BigqueryDataset(name="b")],
        )
        handler.populate_for_project("another-project", [BigqueryDataset(name="c")])
    fired = [c for c in warn_spy.call_args_list if c.kwargs.get("title") == title]
    assert len(fired) == 1

    # A project with at least one typed dataset is a normal "no linked datasets here"
    # case, not a missing-type failure: no warning.
    typed_handler = _make_handler()
    with patch.object(
        typed_handler.report, "warning", wraps=typed_handler.report.warning
    ) as typed_spy:
        typed_handler.populate_for_project(
            CONSUMER_PROJECT,
            [BigqueryDataset(name="a", type="DEFAULT"), BigqueryDataset(name="b")],
        )
    assert not [c for c in typed_spy.call_args_list if c.kwargs.get("title") == title]


# ---------------------------------------------------------------------------
# 50 datasets / 2 linked -> exactly 2 get_dataset calls
# ---------------------------------------------------------------------------


def test_populate_for_project_get_dataset_call_count_matches_linked_count() -> None:
    # get_dataset() call count must equal the number of LINKED datasets, independent
    # of total dataset count in the project - a 2,000-dataset project with 3 shares
    # should cost exactly 3 calls, not 2,000.
    client = _resolvable_client()
    handler = _make_handler(client=client)

    datasets = [BigqueryDataset(name=f"plain_ds_{i}", type=None) for i in range(48)]
    datasets.append(BigqueryDataset(name="linked_a", type="LINKED"))
    datasets.append(BigqueryDataset(name="linked_b", type="LINKED"))
    assert len(datasets) == 50

    handler.populate_for_project(CONSUMER_PROJECT, datasets)

    assert client.get_dataset.call_count == 2


# ---------------------------------------------------------------------------
# Publisher already in scope -> zero Resource Manager calls
# ---------------------------------------------------------------------------


def test_populate_for_project_publisher_in_scope_skips_resource_manager() -> None:
    # Tier 1 (bq_client.list_projects()) already maps the publisher's project number
    # to an id, so tier 2 (resourcemanager.projects.get) must not be called - it is a
    # fallback for publishers outside this run's ingestion scope only.
    projects_client = MagicMock()
    handler = _make_handler(
        client=_resolvable_client(), projects_client=projects_client
    )

    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    projects_client.get_project.assert_not_called()


# ---------------------------------------------------------------------------
# An exception on one dataset does not abort the others
# ---------------------------------------------------------------------------


def test_populate_for_project_one_dataset_failure_does_not_abort_others() -> None:
    good_response = _make_get_dataset_response()

    def _get_dataset(dataset_ref: str) -> MagicMock:
        if "broken" in dataset_ref:
            raise Exception("500 Internal error")
        return good_response

    client = MagicMock()
    client.get_dataset.side_effect = _get_dataset
    client.list_projects.return_value = [
        SimpleNamespace(
            project_id=PUBLISHER_PROJECT,
            numeric_id=PUBLISHER_PROJECT_NUMBER,
            friendly_name="",
        )
    ]
    report = BigQueryV2Report()
    handler = _make_handler(client=client, report=report)

    datasets = [
        BigqueryDataset(name="broken_linked", type="LINKED"),
        BigqueryDataset(name="good_linked", type="LINKED"),
    ]
    handler.populate_for_project(CONSUMER_PROJECT, datasets)  # must not raise

    assert len(report.warnings) > 0

    good_workunits = _lineage_workunits(
        handler, CONSUMER_PROJECT, "good_linked", "plain_table", [_make_column("id")]
    )
    assert _upstream_lineage(good_workunits) is not None


# ---------------------------------------------------------------------------
# Optional enrichment from the BigQuery Sharing API
# ---------------------------------------------------------------------------


def _make_subscription(
    consumer_dataset: str = LINKED_DATASET,
    listing: str = "projects/123456789012/locations/us/dataExchanges/ex/listings/my_listing",
    state_name: str = "STATE_ACTIVE",
    resource_type: Any = None,
) -> SimpleNamespace:
    from google.cloud import bigquery_analyticshub_v1 as ah

    return SimpleNamespace(
        listing=listing,
        # Documented as the display name of the SUBSCRIBER's project, not the
        # publisher's organization. Present so the fixture matches the real proto;
        # not surfaced as a property.
        organization_display_name="consumer-project",
        state=SimpleNamespace(name=state_name),
        resource_type=(
            resource_type
            if resource_type is not None
            else ah.SharedResourceType.BIGQUERY_DATASET
        ),
        destination_dataset=SimpleNamespace(
            dataset_reference=SimpleNamespace(
                project_id=CONSUMER_PROJECT, dataset_id=consumer_dataset
            )
        ),
        # Present on the real object and carrying project NUMBERS, so
        # matching on this instead of destination_dataset finds nothing.
        linked_resources=[
            SimpleNamespace(linked_dataset="projects/760763464100/datasets/linked_ds")
        ],
    )


def _enriched_handler(
    subscriptions: List[Any],
    dataset_name: str = LINKED_DATASET,
) -> BigQuerySharingHandler:
    handler = _make_handler(client=_resolvable_client())
    handler.config.extract_subscriptions_from_analytics_hub = True
    sharing_client = MagicMock()
    sharing_client.list_subscriptions.return_value = subscriptions
    handler._sharing_client = sharing_client
    handler.populate_for_project(
        CONSUMER_PROJECT,
        [BigqueryDataset(name=dataset_name, type="LINKED", location="US")],
    )
    return handler


def test_list_subscriptions_called_once_per_location_with_a_bounded_timeout() -> None:
    # The API is location-scoped, so the parent string decides whether anything is
    # found at all -- a wrong location returns an empty list, which is
    # indistinguishable from "no subscriptions" and warns about nothing. The location
    # is lowercased, absent locations fall back to US, and the timeout is explicit
    # because the generated client leaves it unbounded.
    handler = _make_handler(client=_resolvable_client())
    handler.config.extract_subscriptions_from_analytics_hub = True
    sharing_client = MagicMock()
    sharing_client.list_subscriptions.return_value = []
    handler._sharing_client = sharing_client

    handler.populate_for_project(
        CONSUMER_PROJECT,
        [
            BigqueryDataset(name="in_eu", type="LINKED", location="EU"),
            BigqueryDataset(name="no_location", type="LINKED", location=None),
        ],
    )

    calls = sharing_client.list_subscriptions.call_args_list
    assert sorted(call.kwargs["parent"] for call in calls) == [
        f"projects/{CONSUMER_PROJECT}/locations/eu",
        f"projects/{CONSUMER_PROJECT}/locations/us",
    ]
    assert {call.kwargs["timeout"] for call in calls} == {600.0}


def test_sharing_client_construction_failure_warns_rather_than_raising() -> None:
    # Constructing the client resolves credentials and opens a channel, so it fails on
    # more than a missing package. Nothing between here and get_workunits_internal
    # catches, so raising would end the whole run for an optional property.
    handler = _make_handler(client=_resolvable_client())
    handler.config.extract_subscriptions_from_analytics_hub = True
    handler.config.get_sharing_client = MagicMock(  # type: ignore[method-assign]
        side_effect=ValueError("could not resolve credentials")
    )

    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    assert [w.title for w in handler.report.warnings] == [
        "BigQuery Sharing client could not be created"
    ]
    # Lineage is unaffected: the publisher still resolved.
    assert _info(handler).publisher is not None


def test_sharing_properties_absent_when_flag_off() -> None:
    # The default. No Analytics Hub client is built at all, because constructing one makes a
    # real outbound call even against a mocked credential.
    handler = _make_handler(client=_resolvable_client())
    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    props = _info(handler).to_extra_properties()
    assert "listing_id" not in props and "subscription_state" not in props
    assert handler._sharing_client is None


def test_sharing_properties_populated_when_flag_on() -> None:
    handler = _enriched_handler([_make_subscription()])

    props = _info(handler).to_extra_properties()
    assert props["listing_id"] == "my_listing"
    # STATE_ is a protobuf artefact and is stripped before the value is emitted.
    assert props["subscription_state"] == "ACTIVE"


def test_sharing_enrichment_preserves_lineage_fields() -> None:
    # Enrichment replaces the entry; the publisher resolved without the sharing API
    # must survive that, so turning the flag on leaves the lineage intact.
    handler = _enriched_handler([_make_subscription()])

    info = _info(handler)
    assert info.publisher is not None
    assert info.publisher.project_id == PUBLISHER_PROJECT
    assert info.is_live_link


def test_sharing_enrichment_ignores_non_bigquery_subscriptions() -> None:
    from google.cloud import bigquery_analyticshub_v1 as ah

    handler = _enriched_handler(
        [_make_subscription(resource_type=ah.SharedResourceType.PUBSUB_TOPIC)]
    )

    props = _info(handler).to_extra_properties()
    assert "listing_id" not in props


def test_sharing_permission_denied_warns_and_keeps_lineage() -> None:
    # A missing optional permission must not fail a run that already produced its
    # main output. The lineage is complete without the sharing API.
    from google.api_core.exceptions import PermissionDenied

    report = BigQueryV2Report()
    handler = _make_handler(client=_resolvable_client(), report=report)
    handler.config.extract_subscriptions_from_analytics_hub = True
    sharing_client = MagicMock()
    sharing_client.list_subscriptions.side_effect = PermissionDenied(
        "analyticshub.subscriptions.list denied"
    )
    handler._sharing_client = sharing_client

    handler.populate_for_project(
        CONSUMER_PROJECT,
        [BigqueryDataset(name=LINKED_DATASET, type="LINKED", location="US")],
    )

    assert [w.title for w in report.warnings] == [
        "Missing permission to list BigQuery Sharing subscriptions"
    ]
    assert len(report.failures) == 0
    workunits = _lineage_workunits(
        handler, CONSUMER_PROJECT, LINKED_DATASET, "plain_table", [_make_column("id")]
    )
    assert _upstream_lineage(workunits) is not None


def test_publisher_resolved_via_resource_manager_when_not_in_bigquery_scope() -> None:
    # The fallback. When the ingestion account holds no BigQuery role on the
    # publisher's project, list_projects() cannot see it and Resource Manager is the
    # only route. Needs `resourcemanager.projects.get` on that project.
    client = MagicMock()
    client.get_dataset.return_value = _make_get_dataset_response()
    client.list_projects.return_value = []  # publisher invisible in BigQuery

    projects_client = MagicMock()
    projects_client.get_project.return_value = SimpleNamespace(
        project_id=PUBLISHER_PROJECT
    )

    handler = _make_handler(client=client, projects_client=projects_client)
    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    projects_client.get_project.assert_called_once_with(
        name=f"projects/{PUBLISHER_PROJECT_NUMBER}"
    )
    info = _info(handler)
    assert info.publisher is not None
    assert info.publisher.project_id == PUBLISHER_PROJECT
    assert info.is_live_link


def test_publisher_resolution_failure_is_cached_not_retried() -> None:
    # A publisher resolvable by neither route must cost one lookup for the run, not
    # one per linked dataset. Negative results are cached alongside positive ones.
    from google.api_core.exceptions import NotFound

    client = MagicMock()
    client.get_dataset.return_value = _make_get_dataset_response()
    client.list_projects.return_value = []
    projects_client = MagicMock()
    projects_client.get_project.side_effect = NotFound("no such project")

    report = BigQueryV2Report()
    handler = _make_handler(
        client=client, projects_client=projects_client, report=report
    )
    handler.populate_for_project(
        CONSUMER_PROJECT,
        [
            BigqueryDataset(name="linked_a", type="LINKED"),
            BigqueryDataset(name="linked_b", type="LINKED"),
        ],
    )

    assert projects_client.get_project.call_count == 1
    assert len(report.warnings) > 0
    # Nothing emitted: a URN built from an unresolved project number would match
    # nothing and create a phantom entity.
    assert (
        _lineage_workunits(
            handler, CONSUMER_PROJECT, "linked_a", "plain_table", [_make_column("id")]
        )
        == []
    )


def test_negative_cache_returns_none_when_project_map_never_built() -> None:
    # Both tiers fail, then the cached None stops the second dataset retrying either.
    client = MagicMock()
    client.get_dataset.return_value = _make_get_dataset_response()
    client.list_projects.side_effect = GoogleAPIError("cannot list")
    projects_client = MagicMock()
    projects_client.get_project.side_effect = PermissionDenied("denied")

    report = BigQueryV2Report()
    handler = _make_handler(
        client=client, projects_client=projects_client, report=report
    )
    handler.populate_for_project(
        CONSUMER_PROJECT,
        [
            BigqueryDataset(name="linked_a", type="LINKED"),
            BigqueryDataset(name="linked_b", type="LINKED"),
        ],
    )

    assert client.list_projects.call_count == 1
    assert projects_client.get_project.call_count == 1


def test_publisher_recovers_next_project_but_earlier_entry_stays_sealed() -> None:
    # A transient outage seals project P's dataset unresolved; project Q's build then
    # succeeds, but P's entry is not backfilled mid-run (it heals next run).
    client = MagicMock()
    client.get_dataset.return_value = _make_get_dataset_response()
    client.list_projects.side_effect = [
        GoogleAPIError("list outage"),  # project P: tier-1 build fails
        [  # project Q: build succeeds, now carrying the publisher
            SimpleNamespace(
                project_id=PUBLISHER_PROJECT,
                numeric_id=PUBLISHER_PROJECT_NUMBER,
                friendly_name="",
            )
        ],
    ]
    projects_client = MagicMock()
    projects_client.get_project.side_effect = PermissionDenied("no RM access")

    handler = _make_handler(client=client, projects_client=projects_client)
    handler.populate_for_project(
        "consumer-p", [BigqueryDataset(name="ds_p", type="LINKED", location="US")]
    )
    handler.populate_for_project(
        "consumer-q", [BigqueryDataset(name="ds_q", type="LINKED", location="US")]
    )

    q_info = handler.get_info("consumer-q", "ds_q")
    p_info = handler.get_info("consumer-p", "ds_p")
    assert q_info is not None and q_info.publisher is not None  # Q recovered
    assert (
        p_info is not None and p_info.publisher is None
    )  # P still sealed, not backfilled
    assert handler.report.warnings  # P's failure was surfaced


def test_project_number_map_is_built_once_per_run() -> None:
    client = _resolvable_client()
    handler = _make_handler(client=client)
    handler.populate_for_project(
        "consumer-a", [BigqueryDataset(name="linked_a", type="LINKED", location="US")]
    )
    handler.populate_for_project(
        "consumer-b", [BigqueryDataset(name="linked_b", type="LINKED", location="US")]
    )

    assert client.list_projects.call_count == 1


def test_malformed_linked_dataset_payload_warns_rather_than_aborting() -> None:
    # A truthy non-mapping in the linked payload must warn, not raise: this parse runs
    # before the per-dataset fan-out, so an uncaught error would abort every project.
    client = MagicMock()
    malformed = MagicMock()
    malformed._properties = {"linkedDatasetSource": "not-a-mapping"}
    client.get_dataset.return_value = malformed

    handler = _make_handler(client=client)
    handler.populate_for_project(
        CONSUMER_PROJECT,
        [BigqueryDataset(name="linked_a", type="LINKED", location="US")],
    )

    assert handler.get_info(CONSUMER_PROJECT, "linked_a") is None
    assert handler.report.warnings


def test_subscription_keys_on_the_destination_project_not_the_loop() -> None:
    # A subscription's destination dataset can sit in a different project, so the listing
    # must land on that dataset, not a same-named one in the project being scanned.
    from google.cloud import bigquery_analyticshub_v1 as ah

    handler = _make_handler()
    handler._lookup[("proj-a", "shared")] = LinkedDatasetInfo()
    handler._lookup[("proj-b", "shared")] = LinkedDatasetInfo()

    subscription = SimpleNamespace(
        listing="projects/p/locations/us/dataExchanges/e/listings/L",
        state=SimpleNamespace(name="STATE_ACTIVE"),
        resource_type=ah.SharedResourceType.BIGQUERY_DATASET,
        destination_dataset=SimpleNamespace(
            dataset_reference=SimpleNamespace(project_id="proj-b", dataset_id="shared")
        ),
        linked_resources=[],
    )
    handler._apply_subscription("proj-a", subscription)  # type: ignore[arg-type]

    dest = handler.get_info("proj-b", "shared")
    scanned = handler.get_info("proj-a", "shared")
    assert dest is not None and dest.listing == "L"
    assert dest.subscription_state == "ACTIVE"
    assert scanned is not None and scanned.listing is None


def test_transient_resource_manager_failure_is_not_cached() -> None:
    # A non-permission error may be transient, so it must not be cached as a permanent
    # None; a rerun retries. Tier 2 is hit once, not once per dataset.
    from google.api_core.exceptions import DeadlineExceeded

    client = MagicMock()
    client.get_dataset.return_value = _make_get_dataset_response()
    client.list_projects.return_value = []  # tier 1 empty, so tier 2 is used
    projects_client = MagicMock()
    projects_client.get_project.side_effect = DeadlineExceeded("deadline")

    handler = _make_handler(client=client, projects_client=projects_client)
    handler.populate_for_project(
        CONSUMER_PROJECT,
        [
            BigqueryDataset(name="ds1", type="LINKED", location="US"),
            BigqueryDataset(name="ds2", type="LINKED", location="US"),
        ],
    )

    assert projects_client.get_project.call_count == 1  # not re-hit per dataset
    assert PUBLISHER_PROJECT_NUMBER not in handler._resolved_publisher_ids  # not cached
    ds1 = handler.get_info(CONSUMER_PROJECT, "ds1")
    assert ds1 is not None and ds1.publisher is None
    assert [w.title for w in handler.report.warnings] == [
        "Publisher project resolution failed, possibly transiently"
    ]


def test_two_consumers_of_one_publisher_each_carry_their_own_copy_edge() -> None:
    # Two datasets in the same project can link the same published dataset. Each
    # consumer carries its own COPY edge and nothing is written onto the publisher, so
    # separate recipes ingesting different consumers cannot overwrite each other.
    handler = _make_handler(client=_resolvable_client())
    handler.populate_for_project(
        CONSUMER_PROJECT,
        [
            BigqueryDataset(name="linked_a", type="LINKED"),
            BigqueryDataset(name="linked_b", type="LINKED"),
        ],
    )
    consumer_urns = {
        handler.identifiers.gen_dataset_urn(CONSUMER_PROJECT, dataset, "plain_table")
        for dataset in ("linked_a", "linked_b")
    }
    table_refs = {
        str(
            BigQueryTableRef(
                BigqueryTableIdentifier(CONSUMER_PROJECT, dataset, "plain_table")
            ).get_sanitized_table_ref()
        )
        for dataset in ("linked_a", "linked_b")
    }
    workunits = _register_and_flush(
        handler, {urn: {"id": "INT64"} for urn in consumer_urns}, table_refs
    )

    publisher_urn = handler.identifiers.gen_dataset_urn(
        PUBLISHER_PROJECT, SOURCE_DATASET, "plain_table"
    )
    edges = [
        wu
        for wu in workunits
        if isinstance(wu.metadata.aspect, UpstreamLineageClass)  # type: ignore[union-attr]
    ]
    assert {wu.metadata.entityUrn for wu in edges} == consumer_urns  # type: ignore[union-attr]
    for wu in edges:
        aspect = wu.metadata.aspect  # type: ignore[union-attr]
        assert isinstance(aspect, UpstreamLineageClass)
        assert [upstream.dataset for upstream in aspect.upstreams] == [publisher_urn]
        assert [upstream.type for upstream in aspect.upstreams] == [
            DatasetLineageTypeClass.COPY
        ]


def test_sharing_denied_classified_by_reason_then_by_message() -> None:
    # `reason` is only populated when grpcio-status is importable, which is a
    # google-api-core extra rather than a hard requirement here. Both signals must
    # therefore work, and the structured one must win when the message disagrees.
    def _titles(exc: PermissionDenied) -> List[Optional[str]]:
        report = BigQueryV2Report()
        handler = _make_handler(report=report)
        handler._report_sharing_denied(CONSUMER_PROJECT, "us", exc)
        return [w.title for w in report.warnings]

    # no ErrorInfo -> fall back to the message
    assert _titles(PermissionDenied("SERVICE_DISABLED")) == [
        "BigQuery Sharing API not enabled"
    ]
    assert _titles(PermissionDenied("caller lacks permission")) == [
        "Missing permission to list BigQuery Sharing subscriptions"
    ]

    # The real disabled-API text: without grpcio-status there is no ErrorInfo, and the bare
    # SERVICE_DISABLED token is not in the message, so the phrase match is the only signal.
    assert _titles(
        PermissionDenied(
            "Analytics Hub API has not been used in project 1234 before or it is disabled"
        )
    ) == ["BigQuery Sharing API not enabled"]

    # ErrorInfo present -> it decides, even against a misleading message
    disabled = PermissionDenied("caller lacks permission")
    disabled._error_info = SimpleNamespace(reason=REASON_SERVICE_DISABLED)
    assert _titles(disabled) == ["BigQuery Sharing API not enabled"]

    denied = PermissionDenied("SERVICE_DISABLED appears in this text")
    denied._error_info = SimpleNamespace(reason="IAM_PERMISSION_DENIED")
    assert _titles(denied) == [
        "Missing permission to list BigQuery Sharing subscriptions"
    ]


def test_detected_reconciles_with_resolved_plus_unresolved() -> None:
    # Publisher resolution is cached, so counting unresolved inside it would tally
    # publishers rather than datasets: two linked datasets sharing one bad publisher
    # would report 2 detected but only 1 unresolved.
    client = MagicMock()
    client.get_dataset.return_value = _make_get_dataset_response()
    client.list_projects.return_value = []
    projects_client = MagicMock()
    projects_client.get_project.side_effect = PermissionDenied("denied")

    report = BigQueryV2Report()
    handler = _make_handler(
        client=client, projects_client=projects_client, report=report
    )
    handler.populate_for_project(
        CONSUMER_PROJECT,
        [
            BigqueryDataset(name="linked_a", type="LINKED"),
            BigqueryDataset(name="linked_b", type="LINKED"),
        ],
    )

    detected = report.num_linked_datasets_detected[CONSUMER_PROJECT]
    assert detected == 2
    assert (
        report.num_linked_datasets_resolved + report.num_linked_datasets_unresolved
        == detected
    )
    # The warning stays deduplicated by the resolution cache.
    assert len(report.warnings) == 1


def test_sharing_api_error_warns_and_keeps_lineage() -> None:
    # A transient listing failure is not PermissionDenied and must not be mistaken for
    # one: the sharing properties are dropped, lineage is untouched.
    handler = _make_handler(client=_resolvable_client())
    handler.config.extract_subscriptions_from_analytics_hub = True
    sharing_client = MagicMock()
    sharing_client.list_subscriptions.side_effect = GoogleAPIError("transient")
    handler._sharing_client = sharing_client
    handler.populate_for_project(
        CONSUMER_PROJECT,
        [BigqueryDataset(name=LINKED_DATASET, type="LINKED", location="US")],
    )

    info = _info(handler)
    assert info.listing is None and info.subscription_state is None
    assert info.publisher is not None  # lineage survives
    assert [w.title for w in handler.report.warnings] == [
        "Could not read BigQuery Sharing subscriptions"
    ]


def test_malformed_subscription_does_not_abort_the_project() -> None:
    # An unknown enum value surfaces as a raw int, so `subscription.state.name` raises
    # AttributeError; enrichment is optional, so it must absorb that and keep lineage.
    from google.cloud import bigquery_analyticshub_v1 as ah

    handler = _make_handler(client=_resolvable_client())
    handler.config.extract_subscriptions_from_analytics_hub = True
    sharing_client = MagicMock()
    sharing_client.list_subscriptions.return_value = [
        SimpleNamespace(
            listing="projects/p/locations/us/dataExchanges/e/listings/l",
            state=5,  # unknown enum value comes back as a raw int -> `.name` raises
            resource_type=ah.SharedResourceType.BIGQUERY_DATASET,
            destination_dataset=SimpleNamespace(
                dataset_reference=SimpleNamespace(
                    project_id=CONSUMER_PROJECT, dataset_id=LINKED_DATASET
                )
            ),
            linked_resources=[],
        )
    ]
    handler._sharing_client = sharing_client

    handler.populate_for_project(
        CONSUMER_PROJECT,
        [BigqueryDataset(name=LINKED_DATASET, type="LINKED", location="US")],
    )

    info = _info(handler)
    assert info.publisher is not None  # lineage survives; the project was not aborted
    assert handler.report.warnings  # the failure was surfaced


def test_one_location_failing_does_not_drop_the_others() -> None:
    # list_subscriptions is per-location and can fail with more than GoogleAPIError (a
    # credential refresh raises RefreshError); one failing must not stop the rest.
    from google.auth.exceptions import RefreshError
    from google.cloud import bigquery_analyticshub_v1 as ah

    handler = _make_handler(client=_resolvable_client())
    handler.config.extract_subscriptions_from_analytics_hub = True
    sharing_client = MagicMock()

    def _list(parent: str, timeout: float) -> List[Any]:
        if parent.endswith("/locations/eu"):
            raise RefreshError("token refresh failed")
        return [
            SimpleNamespace(
                listing="projects/p/locations/us/dataExchanges/e/listings/l",
                state=SimpleNamespace(name="STATE_ACTIVE"),
                resource_type=ah.SharedResourceType.BIGQUERY_DATASET,
                destination_dataset=SimpleNamespace(
                    dataset_reference=SimpleNamespace(
                        project_id=CONSUMER_PROJECT, dataset_id="us_linked"
                    )
                ),
                linked_resources=[],
            )
        ]

    sharing_client.list_subscriptions.side_effect = _list
    handler._sharing_client = sharing_client

    handler.populate_for_project(
        CONSUMER_PROJECT,
        [
            BigqueryDataset(name="eu_linked", type="LINKED", location="EU"),
            BigqueryDataset(name="us_linked", type="LINKED", location="US"),
        ],
    )

    us_info = handler.get_info(CONSUMER_PROJECT, "us_linked")
    assert us_info is not None and us_info.listing is not None
    assert handler.report.warnings  # eu's failure was surfaced


def test_subscription_for_undetected_dataset_is_counted_not_applied() -> None:
    # The sharing API reports a subscription for a dataset detection never saw: a
    # filtered dataset, or the two disagreeing. It must not create an entry.
    handler = _enriched_handler([_make_subscription(consumer_dataset="some_other_ds")])

    assert handler.report.num_sharing_subscriptions_unmatched == 1
    assert handler.get_info(CONSUMER_PROJECT, "some_other_ds") is None
    # the genuinely linked dataset keeps its layer-1 fields
    assert _info(handler).publisher is not None


def test_tier1_failure_falls_through_to_resource_manager() -> None:
    # list_projects raising must not abort resolution; tier 2 still runs.
    client = MagicMock()
    client.get_dataset.return_value = _make_get_dataset_response()
    client.list_projects.side_effect = GoogleAPIError("cannot list")
    projects_client = MagicMock()
    projects_client.get_project.return_value = SimpleNamespace(
        project_id=PUBLISHER_PROJECT
    )

    report = BigQueryV2Report()
    handler = _make_handler(
        client=client, projects_client=projects_client, report=report
    )
    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    info = _info(handler)
    assert info.publisher is not None
    assert info.publisher.project_id == PUBLISHER_PROJECT
    assert report.num_publisher_lookups_from_project_list == 0
    assert report.num_publisher_lookups_from_resource_manager == 1


def test_bigquery_source_imports_without_the_analyticshub_package() -> None:
    # google-cloud-bigquery-analyticshub ships only with the `bigquery` extra, so a
    # bigquery-slim install does not have it. Importing it at module scope anywhere
    # reachable from bigquery.py makes `type: bigquery` unloadable on that install.
    #
    # Runs in a subprocess: blocking a module and re-importing a package tree mutates
    # interpreter-global state, and this suite runs in random order.
    script = textwrap.dedent(
        """
        import sys, importlib, importlib.abc
        blocked = "google.cloud.bigquery_analyticshub_v1"

        class _Blocker(importlib.abc.MetaPathFinder):
            def find_spec(self, name, path=None, target=None):
                if name == blocked or name.startswith(blocked + "."):
                    raise ImportError("blocked: " + name)
                return None

        sys.meta_path.insert(0, _Blocker())
        importlib.import_module("datahub.ingestion.source.bigquery_v2.bigquery")
        print("OK")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=300
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert "OK" in result.stdout


def test_missing_analyticshub_package_warns_instead_of_raising() -> None:
    # bigquery-slim has no google-cloud-bigquery-analyticshub, so this flag can be set
    # on an install with no client to build. The run continues and the lineage, which
    # never touches that package, is already emitted.
    handler = _make_handler(client=_resolvable_client())
    handler.config.extract_subscriptions_from_analytics_hub = True
    handler.config.get_sharing_client = MagicMock(  # type: ignore[method-assign]
        side_effect=ModuleNotFoundError(
            "No module named 'google.cloud.bigquery_analyticshub_v1'"
        )
    )
    handler.populate_for_project(
        CONSUMER_PROJECT,
        [BigqueryDataset(name=LINKED_DATASET, type="LINKED", location="US")],
    )

    assert [w.title for w in handler.report.warnings] == [
        "BigQuery Sharing client unavailable"
    ]
    info = _info(handler)
    assert info.publisher is not None
    assert info.listing is None and info.subscription_state is None


def test_tier1_non_api_error_still_falls_through_to_resource_manager() -> None:
    # list_projects can fail with more than GoogleAPIError: credential refresh raises
    # its own types, and a malformed response raises ValueError. Any of them must
    # degrade to tier 2 rather than end the project.
    client = MagicMock()
    client.get_dataset.return_value = _make_get_dataset_response()
    client.list_projects.side_effect = ValueError("malformed response")
    projects_client = MagicMock()
    projects_client.get_project.return_value = SimpleNamespace(
        project_id=PUBLISHER_PROJECT
    )

    report = BigQueryV2Report()
    handler = _make_handler(
        client=client, projects_client=projects_client, report=report
    )
    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    info = _info(handler)
    assert info.publisher is not None
    assert info.publisher.project_id == PUBLISHER_PROJECT
    assert report.num_publisher_lookups_from_resource_manager == 1


def test_tier2_non_api_error_warns_rather_than_ending_the_run() -> None:
    # A credential refresh in tier 2 raises a non-GoogleAPIError (here a ValueError), not a
    # permission denial, so _from_resource_manager treats it as transient: warns, not raises.
    client = MagicMock()
    client.get_dataset.return_value = _make_get_dataset_response()
    client.list_projects.return_value = []  # publisher invisible in BigQuery
    projects_client = MagicMock()
    projects_client.get_project.side_effect = ValueError("credential refresh failed")

    report = BigQueryV2Report()
    handler = _make_handler(
        client=client, projects_client=projects_client, report=report
    )
    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    assert [w.title for w in report.warnings] == [
        "Publisher project resolution failed, possibly transiently"
    ]
    assert _info(handler).publisher is None


def test_project_list_failure_is_reported_and_not_cached_as_partial() -> None:
    # list_projects paginates lazily, so a failure part-way through must not leave a
    # half-filled map authoritative for the rest of the run: every publisher past the
    # failure would silently become a tier-2 lookup needing a permission it should not
    # need. The failure also has to reach the report, because the tier-2 warning that
    # follows blames the publisher's IAM when the cause is the consumer's own project
    # list.
    def _one_page_then_fail():
        yield SimpleNamespace(
            project_id="some-other-project", numeric_id="999", friendly_name=""
        )
        raise GoogleAPIError("quota exceeded mid-pagination")

    client = MagicMock()
    client.get_dataset.return_value = _make_get_dataset_response()
    # A fresh generator per call, so a retry can succeed where the first attempt failed.
    client.list_projects.side_effect = lambda **kwargs: _one_page_then_fail()
    projects_client = MagicMock()
    projects_client.get_project.return_value = SimpleNamespace(
        project_id=PUBLISHER_PROJECT
    )

    report = BigQueryV2Report()
    handler = _make_handler(
        client=client, projects_client=projects_client, report=report
    )
    handler.populate_for_project(
        CONSUMER_PROJECT,
        [
            BigqueryDataset(name="linked_a", type="LINKED"),
            BigqueryDataset(name="linked_b", type="LINKED"),
        ],
    )

    assert "Could not list projects to resolve publisher project numbers" in [
        w.title for w in report.warnings
    ]
    # Nothing was cached, so the map is still unset and a later publisher number would
    # re-enumerate rather than be denied by a partial answer.
    assert handler._project_number_map is None
    # Both datasets still resolve, via tier 2.
    assert _info(handler, "linked_a").publisher is not None
    assert _info(handler, "linked_b").publisher is not None
    # One tier-2 lookup, not two: _resolved_publisher_ids caches per project number and
    # bounds how often the failed enumeration is retried.
    assert report.num_publisher_lookups_from_resource_manager == 1
    assert client.list_projects.call_count == 1
    # The client leaves the per-request timeout at None, so passing one is the only
    # thing bounding a hung connection. Asserted by presence rather than by value so
    # the bound can be retuned without editing this test.
    assert client.list_projects.call_args.kwargs.get("timeout") is not None


def test_last_segment_handles_absent_and_trailing_slash() -> None:
    from datahub.ingestion.source.bigquery_v2.bigquery_sharing import _last_segment

    assert _last_segment(None) is None
    assert _last_segment("") is None
    assert _last_segment("projects/p/locations/us/listings/") is None
    assert _last_segment("projects/p/locations/us/listings/my_listing") == "my_listing"


def test_subscription_without_a_destination_dataset_is_counted_not_dropped() -> None:
    # resource_type is BIGQUERY_DATASET but the destination reference is absent, so
    # there is no dataset name to match on. It is still counted both ways, so the
    # subscription leaves a trace instead of disappearing without any record.
    handler = _enriched_handler([_make_subscription(consumer_dataset="")])

    assert handler.report.num_sharing_subscriptions_scanned == 1
    assert handler.report.num_sharing_subscriptions_unmatched == 1
    assert _info(handler).listing is None


def test_dead_link_registers_no_lineage() -> None:
    # A dead link (link_state present but not LINKED) is skipped: register_known_lineage
    # emits no COPY edge for it, because parsed lineage for a stale mirror is no better
    # than none.
    handler = _make_handler(
        client=_resolvable_client(_make_get_dataset_response(link_state="UNLINKED"))
    )
    handler.populate_for_project(
        CONSUMER_PROJECT, [BigqueryDataset(name=LINKED_DATASET, type="LINKED")]
    )

    assert not _info(handler).is_live_link
    assert (
        _lineage_workunits(
            handler, CONSUMER_PROJECT, LINKED_DATASET, "t", [_make_column("id")]
        )
        == []
    )
