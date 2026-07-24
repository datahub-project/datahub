from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.models import InformixColumn, InformixTable
from datahub.ingestion.source.informix.source import InformixSource
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset


class _FakeClient:
    def get_tables(self):
        return [
            InformixTable(name="customers", owner="informix", is_view=False),
            InformixTable(name="active", owner="informix", is_view=True),
        ]

    def get_columns(self, table):
        return [InformixColumn(name="id", coltype=258, length=4, colno=1, is_pk=True)]

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
