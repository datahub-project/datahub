from unittest.mock import MagicMock, patch

from datahub.configuration.common import ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.models import (
    InformixColumn,
    InformixForeignKey,
    InformixTable,
)
from datahub.ingestion.source.informix.source import InformixSource
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    OwnershipTypeClass,
    SchemaMetadataClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset


class _FakeClient:
    def get_tables(self):
        return [
            InformixTable(name="customers", owner="informix", is_view=False, nrows=42),
            InformixTable(name="active", owner="informix", is_view=True),
        ]

    def get_columns(self, table):
        return [InformixColumn(name="id", coltype=258, length=4, colno=1, is_pk=True)]

    def get_foreign_keys(self, table):
        return []

    def get_view_definition(self, table):
        return None

    def close(self):
        pass


class _TwoTableClient:
    def get_tables(self):
        return [
            InformixTable(name="customers", owner="informix", is_view=False),
            InformixTable(name="orders", owner="informix", is_view=False),
        ]

    def get_columns(self, table):
        return [InformixColumn(name="id", coltype=258, length=4, colno=1, is_pk=True)]

    def get_foreign_keys(self, table):
        return []

    def get_view_definition(self, table):
        return None

    def close(self):
        pass


class _PartialFailureClient:
    def get_tables(self):
        return [
            InformixTable(name="customers", owner="informix", is_view=False),
            InformixTable(name="orders", owner="informix", is_view=False),
        ]

    def get_columns(self, table):
        if table.name == "orders":
            raise RuntimeError("boom")
        return [InformixColumn(name="id", coltype=258, length=4, colno=1, is_pk=True)]

    def get_foreign_keys(self, table):
        return []

    def get_view_definition(self, table):
        return None

    def close(self):
        pass


class _TotalFailureClient(_TwoTableClient):
    def get_columns(self, table):
        raise RuntimeError("catalog unreadable")


class _FkClient:
    def get_tables(self):
        return [
            InformixTable(name="customers", owner="informix", is_view=False),
            InformixTable(name="orders", owner="informix", is_view=False),
        ]

    def get_columns(self, table):
        if table.name == "orders":
            return [
                InformixColumn(name="id", coltype=258, length=4, colno=1, is_pk=True),
                InformixColumn(
                    name="customer_id", coltype=2, length=4, colno=2, is_pk=False
                ),
            ]
        return [InformixColumn(name="id", coltype=258, length=4, colno=1, is_pk=True)]

    def get_foreign_keys(self, table):
        if table.name == "orders":
            return [
                InformixForeignKey(
                    name="fk_orders_customer",
                    child_columns=["customer_id"],
                    parent_table="customers",
                    parent_owner="informix",
                    parent_columns=["id"],
                )
            ]
        return []

    def get_view_definition(self, table):
        return None

    def close(self):
        pass


_VIEW_SQL = (
    'create view "informix".customer_orders (customer_id,customer_name,order_id,amount) as '
    "select x0.id ,x0.name ,x1.order_id ,x1.amount from "
    '("informix".customers x0 join "informix".orders x1 on (x0.id = x1.customer_id ) )'
)


class _ViewLineageClient:
    def get_tables(self):
        return [
            InformixTable(name="customers", owner="informix", is_view=False),
            InformixTable(name="orders", owner="informix", is_view=False),
            InformixTable(name="customer_orders", owner="informix", is_view=True),
        ]

    def get_columns(self, table):
        if table.name == "customers":
            return [
                InformixColumn(name="id", coltype=258, length=4, colno=1, is_pk=True),
                InformixColumn(name="name", coltype=13, length=50, colno=2),
                InformixColumn(name="email", coltype=13, length=50, colno=3),
            ]
        if table.name == "orders":
            return [
                InformixColumn(
                    name="order_id", coltype=258, length=4, colno=1, is_pk=True
                ),
                InformixColumn(name="customer_id", coltype=2, length=4, colno=2),
                InformixColumn(name="amount", coltype=5, length=8, colno=3),
            ]
        return [
            InformixColumn(name="customer_id", coltype=2, length=4, colno=1),
            InformixColumn(name="customer_name", coltype=13, length=50, colno=2),
            InformixColumn(name="order_id", coltype=2, length=4, colno=3),
            InformixColumn(name="amount", coltype=5, length=8, colno=4),
        ]

    def get_foreign_keys(self, table):
        return []

    def get_view_definition(self, table):
        return _VIEW_SQL if table.name == "customer_orders" else None

    def close(self):
        pass


def test_source_emits_containers_and_datasets():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_FakeClient()
    )
    entities = list(source.get_workunits_internal())

    datasets = [e for e in entities if isinstance(e, Dataset)]
    containers = [e for e in entities if isinstance(e, Container)]
    names = sorted(d.urn.name for d in datasets)
    assert names == ["testdb.informix.active", "testdb.informix.customers"]
    # one database container + one schema(owner) container
    assert len(containers) == 2

    db_key = source._database_key()
    schema_key = source._schema_key("informix")
    schema_container = next(
        c for c in containers if c.subtype == DatasetContainerSubTypes.SCHEMA
    )
    assert schema_container.parent_container == db_key.as_urn_typed()
    for dataset in datasets:
        assert dataset.parent_container == schema_key.as_urn_typed()

    view_dataset = next(d for d in datasets if d.display_name == "active")
    table_dataset = next(d for d in datasets if d.display_name == "customers")
    assert view_dataset.subtype == DatasetSubTypes.VIEW
    assert table_dataset.subtype == DatasetSubTypes.TABLE
    assert view_dataset.subtype != table_dataset.subtype


def test_source_emits_row_count_profile_for_tables_only():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_FakeClient()
    )
    entities = list(source.get_workunits_internal())

    profiles = [
        e.metadata.aspect
        for e in entities
        if isinstance(e, MetadataWorkUnit)
        and isinstance(e.metadata, MetadataChangeProposalWrapper)
        and isinstance(e.metadata.aspect, DatasetProfileClass)
    ]
    assert len(profiles) == 1
    assert profiles[0].rowCount == 42


def test_source_does_not_emit_row_count_for_views_even_with_nrows():
    class _ViewWithNrowsClient(_FakeClient):
        def get_tables(self):
            return [
                InformixTable(
                    name="customers", owner="informix", is_view=False, nrows=42
                ),
                InformixTable(name="active", owner="informix", is_view=True, nrows=99),
            ]

    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_ViewWithNrowsClient()
    )
    entities = list(source.get_workunits_internal())

    profiles = [
        e.metadata.aspect
        for e in entities
        if isinstance(e, MetadataWorkUnit)
        and isinstance(e.metadata, MetadataChangeProposalWrapper)
        and isinstance(e.metadata.aspect, DatasetProfileClass)
    ]
    assert len(profiles) == 1
    assert profiles[0].rowCount == 42


def test_source_suppresses_row_count_when_disabled():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb", "include_row_counts": False}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_FakeClient()
    )
    entities = list(source.get_workunits_internal())

    profiles = [
        e.metadata.aspect
        for e in entities
        if isinstance(e, MetadataWorkUnit)
        and isinstance(e.metadata, MetadataChangeProposalWrapper)
        and isinstance(e.metadata.aspect, DatasetProfileClass)
    ]
    assert not profiles


def test_source_isolates_per_table_failures():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_PartialFailureClient()
    )
    entities = list(source.get_workunits_internal())

    datasets = [e for e in entities if isinstance(e, Dataset)]
    names = sorted(d.urn.name for d in datasets)
    assert names == ["testdb.informix.customers"]
    assert len(source.report.warnings) == 1


def test_source_fails_when_every_selected_object_fails():
    # A systemic catalog problem must not finish as a successful run that emitted
    # only containers -- per-object warnings are escalated to one run failure.
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_TotalFailureClient()
    )
    entities = list(source.get_workunits_internal())

    assert [e for e in entities if isinstance(e, Dataset)] == []
    assert len(source.report.failures) == 1
    assert source.report.tables_scanned == 0
    assert source.report.objects_selected == 2
    # The escalation adds a run failure; it must not swallow the per-object
    # warnings saying which objects failed and why. The report collapses repeats
    # of one title into a single entry, so both objects show up as contexts.
    per_object = [
        w for w in source.report.warnings if "Failed to ingest table" in str(w.title)
    ]
    assert len(per_object) == 1
    assert len(per_object[0].context) == 2


class _NoViewDefinitionClient(_ViewLineageClient):
    def get_tables(self):
        return super().get_tables() + [
            InformixTable(name="active_customers", owner="informix", is_view=True),
        ]

    def get_view_definition(self, table):
        return None


def test_source_warns_when_no_view_definition_could_be_read():
    # sysviews returning empty for every view only bumped a counter, so a
    # permissions problem scoped to sysviews produced no diagnostic at all --
    # the pass 1 escalation does not cover it, the datasets still ingest fine.
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_NoViewDefinitionClient()
    )
    entities = list(source.get_workunits_internal())

    assert [e for e in entities if isinstance(e, Dataset)]
    assert source.report.views_without_definition == 2
    assert len(source.report.failures) == 0
    assert any(
        "No view definitions could be read" in str(w.title)
        for w in source.report.warnings
    )


def test_source_does_not_warn_when_some_view_definitions_are_read():
    # One view has stored SQL and one does not, which is an ordinary catalog
    # state rather than a systemic read failure.
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_ViewLineageClient()
    )
    list(source.get_workunits_internal())

    assert not any(
        "No view definitions could be read" in str(w.title)
        for w in source.report.warnings
    )


def test_source_does_not_fail_when_everything_is_filtered_out():
    # Nothing was selected, so there is no failure to escalate.
    config = InformixSourceConfig.parse_obj(
        {
            "server": "informix",
            "database": "testdb",
            "table_pattern": {"deny": [".*"]},
        }
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_TotalFailureClient()
    )
    list(source.get_workunits_internal())

    assert len(source.report.failures) == 0


def test_source_does_not_fail_when_some_objects_succeed():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_PartialFailureClient()
    )
    list(source.get_workunits_internal())

    assert len(source.report.failures) == 0
    assert source.report.tables_scanned == 1


def test_source_applies_table_pattern_deny():
    config = InformixSourceConfig.parse_obj(
        {
            "server": "informix",
            "database": "testdb",
            # table_pattern matches the full database.owner.table identifier.
            "table_pattern": {"deny": [".*orders"]},
        }
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_TwoTableClient()
    )
    entities = list(source.get_workunits_internal())

    datasets = [e for e in entities if isinstance(e, Dataset)]
    names = sorted(d.urn.name for d in datasets)
    assert names == ["testdb.informix.customers"]
    assert list(source.report.filtered) == ["testdb.informix.orders"]


def test_source_attaches_foreign_keys_to_schema():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(PipelineContext(run_id="test"), config, client=_FkClient())
    entities = list(source.get_workunits_internal())

    datasets = [e for e in entities if isinstance(e, Dataset)]
    orders = next(d for d in datasets if d.display_name == "orders")
    schema_metadata = orders._get_aspect(SchemaMetadataClass)
    assert schema_metadata is not None
    assert schema_metadata.foreignKeys is not None
    assert len(schema_metadata.foreignKeys) == 1
    fk = schema_metadata.foreignKeys[0]
    assert fk.name == "fk_orders_customer"
    assert fk.foreignDataset.endswith("testdb.informix.customers,PROD)")

    customers = next(d for d in datasets if d.display_name == "customers")
    customers_schema = customers._get_aspect(SchemaMetadataClass)
    assert customers_schema is not None
    assert not customers_schema.foreignKeys


def test_source_emits_view_lineage():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_ViewLineageClient()
    )
    entities = list(source.get_workunits_internal())

    view_dataset = next(
        e
        for e in entities
        if isinstance(e, Dataset) and e.display_name == "customer_orders"
    )
    lineage_mcps = [
        e.metadata
        for e in entities
        if isinstance(e, MetadataWorkUnit)
        and isinstance(e.metadata, MetadataChangeProposalWrapper)
        and isinstance(e.metadata.aspect, UpstreamLineageClass)
    ]
    assert len(lineage_mcps) == 1
    mcp = lineage_mcps[0]
    assert mcp.entityUrn == view_dataset.urn.urn()
    upstream_lineage = mcp.aspect
    assert isinstance(upstream_lineage, UpstreamLineageClass)
    upstream_names = sorted(
        u.dataset.split(",")[-2] for u in upstream_lineage.upstreams
    )
    assert upstream_names == ["testdb.informix.customers", "testdb.informix.orders"]
    assert upstream_lineage.fineGrainedLineages
    assert source.report.views_with_lineage == 1


def test_source_applies_schema_pattern_deny():
    config = InformixSourceConfig.parse_obj(
        {
            "server": "informix",
            "database": "testdb",
            "schema_pattern": {"deny": ["informix"]},
        }
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_TwoTableClient()
    )
    entities = list(source.get_workunits_internal())

    assert not [e for e in entities if isinstance(e, Dataset)]
    assert len(source.report.filtered) == 2


def test_source_applies_view_pattern_deny():
    config = InformixSourceConfig.parse_obj(
        {
            "server": "informix",
            "database": "testdb",
            "view_pattern": {"deny": [".*active"]},
        }
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_FakeClient()
    )
    entities = list(source.get_workunits_internal())

    names = sorted(d.urn.name for d in entities if isinstance(d, Dataset))
    assert names == ["testdb.informix.customers"]
    assert list(source.report.filtered) == ["testdb.informix.active"]


def test_source_respects_include_tables_false():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb", "include_tables": False}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_FakeClient()
    )
    entities = list(source.get_workunits_internal())

    names = sorted(d.urn.name for d in entities if isinstance(d, Dataset))
    assert names == ["testdb.informix.active"]


def test_source_respects_include_views_false():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb", "include_views": False}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_FakeClient()
    )
    entities = list(source.get_workunits_internal())

    names = sorted(d.urn.name for d in entities if isinstance(d, Dataset))
    assert names == ["testdb.informix.customers"]


def test_source_skips_foreign_keys_when_disabled():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb", "include_foreign_keys": False}
    )
    source = InformixSource(PipelineContext(run_id="test"), config, client=_FkClient())
    entities = list(source.get_workunits_internal())

    orders = next(
        d for d in entities if isinstance(d, Dataset) and d.display_name == "orders"
    )
    schema_metadata = orders._get_aspect(SchemaMetadataClass)
    assert schema_metadata is not None
    assert not schema_metadata.foreignKeys


class _CompositeFkClient(_FkClient):
    def get_columns(self, table):
        if table.name == "orders":
            return [
                InformixColumn(name="region", coltype=2, length=4, colno=1),
                InformixColumn(name="customer_id", coltype=2, length=4, colno=2),
            ]
        return [InformixColumn(name="id", coltype=258, length=4, colno=1, is_pk=True)]

    def get_foreign_keys(self, table):
        if table.name == "orders":
            return [
                InformixForeignKey(
                    name="fk_orders_customer",
                    child_columns=["region", "customer_id"],
                    parent_table="customers",
                    parent_owner="informix",
                    parent_columns=["region", "id"],
                )
            ]
        return []


def test_source_warns_on_composite_foreign_key():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_CompositeFkClient()
    )
    list(source.get_workunits_internal())

    assert any("Composite foreign key" in str(w.title) for w in source.report.warnings)


def test_source_emits_owner_from_systables_owner():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_FakeClient()
    )
    entities = list(source.get_workunits_internal())

    customers = next(
        d for d in entities if isinstance(d, Dataset) and d.display_name == "customers"
    )
    assert customers.owners is not None
    assert [o.owner for o in customers.owners] == ["urn:li:corpuser:informix"]
    assert [o.type for o in customers.owners] == [OwnershipTypeClass.DATAOWNER]

    # the schema container is owned by the same user it is named after
    schema_container = next(
        c for c in entities if isinstance(c, Container) and c.display_name == "informix"
    )
    assert schema_container.owners is not None
    assert [o.owner for o in schema_container.owners] == ["urn:li:corpuser:informix"]


def test_source_suppresses_ownership_when_disabled():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb", "include_ownership": False}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_FakeClient()
    )
    entities = list(source.get_workunits_internal())

    assert all(
        e.owners is None for e in entities if isinstance(e, (Dataset, Container))
    )


def test_source_assigns_domain_from_pattern():
    config = InformixSourceConfig.parse_obj(
        {
            "server": "informix",
            "database": "testdb",
            "domain": {"urn:li:domain:sales": {"allow": [".*customers"]}},
        }
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_FakeClient()
    )
    entities = list(source.get_workunits_internal())

    customers = next(
        d for d in entities if isinstance(d, Dataset) and d.display_name == "customers"
    )
    active = next(
        d for d in entities if isinstance(d, Dataset) and d.display_name == "active"
    )
    assert customers.domain is not None
    assert str(customers.domain) == "urn:li:domain:sales"
    # the view does not match the pattern, so it gets no domain
    assert active.domain is None


def test_source_respects_include_view_lineage_false():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb", "include_view_lineage": False}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_ViewLineageClient()
    )
    entities = list(source.get_workunits_internal())

    lineage_mcps = [
        e
        for e in entities
        if isinstance(e, MetadataWorkUnit)
        and isinstance(e.metadata, MetadataChangeProposalWrapper)
        and isinstance(e.metadata.aspect, UpstreamLineageClass)
    ]
    assert not lineage_mcps
    assert source.report.views_with_lineage == 0

    view_props = [
        e.metadata.aspect
        for e in entities
        if isinstance(e, MetadataWorkUnit)
        and isinstance(e.metadata, MetadataChangeProposalWrapper)
        and isinstance(e.metadata.aspect, ViewPropertiesClass)
    ]
    assert len(view_props) == 1
    assert view_props[0].viewLogic == _VIEW_SQL
    assert view_props[0].viewLanguage == "SQL"
    assert view_props[0].materialized is False


def test_source_emits_view_properties_with_lineage():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_ViewLineageClient()
    )
    entities = list(source.get_workunits_internal())

    view_props = [
        e.metadata.aspect
        for e in entities
        if isinstance(e, MetadataWorkUnit)
        and isinstance(e.metadata, MetadataChangeProposalWrapper)
        and isinstance(e.metadata.aspect, ViewPropertiesClass)
    ]
    assert len(view_props) == 1
    assert view_props[0].viewLogic == _VIEW_SQL

    lineage_mcps = [
        e
        for e in entities
        if isinstance(e, MetadataWorkUnit)
        and isinstance(e.metadata, MetadataChangeProposalWrapper)
        and isinstance(e.metadata.aspect, UpstreamLineageClass)
    ]
    assert lineage_mcps
    assert source.report.views_with_lineage == 1


def test_test_connection_sanitizes_query_failure_reason():
    client = MagicMock()
    client.get_tables.side_effect = RuntimeError(
        "Failed using jdbc:informix-sqli://host/db:INFORMIXSERVER=s;user=u;password=secret"
    )
    with patch(
        "datahub.ingestion.source.informix.source.InformixClient", return_value=client
    ):
        report = InformixSource.test_connection(
            {
                "server": "informix",
                "database": "testdb",
                "host_port": "host:9088",
                "password": "secret",
            }
        )
    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    reason = str(report.basic_connectivity.failure_reason)
    assert "secret" not in reason
    assert "password=" not in reason
    assert "informix" in reason


def test_test_connection_reports_schema_capability_on_success():
    client = MagicMock()
    client.get_tables.return_value = [
        InformixTable(name="customers", owner="informix", is_view=False)
    ]
    with patch(
        "datahub.ingestion.source.informix.source.InformixClient", return_value=client
    ):
        report = InformixSource.test_connection(
            {"server": "informix", "database": "testdb"}
        )
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    assert SourceCapability.SCHEMA_METADATA in report.capability_report
    assert report.capability_report[SourceCapability.SCHEMA_METADATA].capable
    client.close.assert_called_once()


class _UnparseableViewClient(_ViewLineageClient):
    def get_view_definition(self, table):
        # Return SQL that cannot be parsed so the lineage pass hits a table_error.
        return "not valid sql at all ((" if table.name == "customer_orders" else None


def test_source_counts_unparseable_view_lineage():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_UnparseableViewClient()
    )
    entities = list(source.get_workunits_internal())

    # datasets still emit; the parse failure is surfaced (counted + warned),
    # not silently swallowed.
    assert any(isinstance(e, Dataset) for e in entities)
    assert source.report.views_with_lineage == 0
    assert source.report.view_lineage_failures == 1
    assert any("view lineage" in str(w.title).lower() for w in source.report.warnings)


def test_stale_entity_removal_processor_wired_when_stateful_enabled():
    config = InformixSourceConfig.parse_obj(
        {
            "server": "informix",
            "database": "testdb",
            "stateful_ingestion": {"enabled": True, "remove_stale_metadata": True},
        }
    )
    ctx = PipelineContext(
        run_id="test", pipeline_name="test_pipeline", graph=MagicMock()
    )
    source = InformixSource(ctx, config, client=_FakeClient())
    processor_owners = [
        type(getattr(p, "__self__", None)).__name__
        for p in source.get_workunit_processors()
    ]
    assert "AutoStaleEntityRemovalProcessor" in processor_owners


class _MismatchedFkClient(_FkClient):
    def get_foreign_keys(self, table):
        if table.name == "orders":
            return [
                # Bypass the model invariant so we can exercise the source's
                # defensive length check (production client already filters).
                InformixForeignKey.model_construct(
                    name="fk_mismatched",
                    child_columns=["region", "customer_id"],
                    parent_table="customers",
                    parent_owner="informix",
                    parent_columns=["id"],
                ),
                InformixForeignKey(
                    name="fk_ok",
                    child_columns=["customer_id"],
                    parent_table="customers",
                    parent_owner="informix",
                    parent_columns=["id"],
                ),
            ]
        return []


def test_source_skips_foreign_key_with_mismatched_column_counts():
    config = InformixSourceConfig.parse_obj(
        {"server": "informix", "database": "testdb"}
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_MismatchedFkClient()
    )
    entities = list(source.get_workunits_internal())

    orders = next(
        d for d in entities if isinstance(d, Dataset) and d.display_name == "orders"
    )
    schema_metadata = orders._get_aspect(SchemaMetadataClass)
    assert schema_metadata is not None
    # The usable constraint still lands; only the ambiguous one is dropped.
    assert [fk.name for fk in schema_metadata.foreignKeys or []] == ["fk_ok"]
    assert source.report.foreign_keys_dropped_mismatched == 1
    assert any(
        "mismatched column counts" in str(w.title) for w in source.report.warnings
    )


class _MixedCaseFkClient:
    def get_tables(self):
        return [
            InformixTable(name="Customers", owner="Informix", is_view=False),
            InformixTable(name="Orders", owner="Informix", is_view=False),
        ]

    def get_columns(self, table):
        if table.name == "Orders":
            return [
                InformixColumn(name="id", coltype=258, length=4, colno=1, is_pk=True),
                InformixColumn(name="customer_id", coltype=2, length=4, colno=2),
            ]
        return [InformixColumn(name="id", coltype=258, length=4, colno=1, is_pk=True)]

    def get_foreign_keys(self, table):
        if table.name == "Orders":
            return [
                InformixForeignKey(
                    name="fk_orders_customer",
                    child_columns=["customer_id"],
                    parent_table="Customers",
                    parent_owner="Informix",
                    parent_columns=["id"],
                )
            ]
        return []

    def get_view_definition(self, table):
        return None

    def close(self):
        pass


def test_convert_urns_to_lowercase_applies_to_dataset_and_foreign_key_urns():
    # The dataset path and build_foreign_key_constraints each take the flag
    # separately, so they can drift; assert both sides come out lowercased.
    config = InformixSourceConfig.parse_obj(
        {
            "server": "informix",
            "database": "TestDB",
            "convert_urns_to_lowercase": True,
        }
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_MixedCaseFkClient()
    )
    entities = list(source.get_workunits_internal())

    orders = next(
        d for d in entities if isinstance(d, Dataset) and d.display_name == "Orders"
    )
    assert "testdb.informix.orders" in orders.urn.urn()

    schema_metadata = orders._get_aspect(SchemaMetadataClass)
    assert schema_metadata is not None
    fk = (schema_metadata.foreignKeys or [])[0]
    assert "testdb.informix.customers" in fk.foreignDataset
    assert all("testdb.informix.orders" in f for f in fk.sourceFields)
    assert all("testdb.informix.customers" in f for f in fk.foreignFields)


def test_test_connection_reports_failure_reason_without_raising():
    with patch(
        "datahub.ingestion.source.informix.source.InformixClient",
        side_effect=ConfigurationError("cannot reach server"),
    ):
        report = InformixSource.test_connection(
            {"server": "informix", "database": "testdb"}
        )
    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert "cannot reach server" in str(report.basic_connectivity.failure_reason)


def test_test_connection_closes_client_on_query_failure():
    client = MagicMock()
    client.get_tables.side_effect = RuntimeError("catalog unreadable")
    with patch(
        "datahub.ingestion.source.informix.source.InformixClient", return_value=client
    ):
        report = InformixSource.test_connection(
            {"server": "informix", "database": "testdb"}
        )
    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    client.close.assert_called_once()
