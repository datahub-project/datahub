from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
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
from datahub.metadata.schema_classes import DatasetProfileClass, SchemaMetadataClass
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

    def close(self):
        pass


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


def test_source_applies_table_pattern_deny():
    config = InformixSourceConfig.parse_obj(
        {
            "server": "informix",
            "database": "testdb",
            "table_pattern": {"deny": ["orders"]},
        }
    )
    source = InformixSource(
        PipelineContext(run_id="test"), config, client=_TwoTableClient()
    )
    entities = list(source.get_workunits_internal())

    datasets = [e for e in entities if isinstance(e, Dataset)]
    names = sorted(d.urn.name for d in datasets)
    assert names == ["testdb.informix.customers"]
    assert source.report.filtered == 1


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
