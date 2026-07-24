from datahub.ingestion.api.common import PipelineContext
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
