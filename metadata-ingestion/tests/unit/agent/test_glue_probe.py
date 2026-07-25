from typing import Any

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.source.aws.glue_probe import list_glue_children
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


class _DbPaginator:
    def __init__(self, dbs):
        self._dbs = dbs

    def paginate(self):
        return iter([{"DatabaseList": [{"Name": d} for d in self._dbs]}])


class _TablesPaginator:
    def __init__(self, tables):
        self._tables = tables

    def paginate(self, DatabaseName=None):
        return iter(
            [{"TableList": [{"Name": t} for t in self._tables.get(DatabaseName, [])]}]
        )


class _GlueClient:
    def __init__(self, dbs, tables):
        self._dbs = dbs
        self._tables = tables

    def get_paginator(self, name):
        if name == "get_databases":
            return _DbPaginator(self._dbs)
        return _TablesPaginator(self._tables)


# A real pydantic config (not a plain SimpleNamespace) so resolve_pattern_field can
# introspect model_fields for database_pattern/table_pattern, which the probe now
# resolves by convention rather than declaring explicitly.
class _Config(ConfigModel):
    glue_client: Any
    database_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


def _config():
    return _Config(
        glue_client=_GlueClient(
            ["analytics", "staging"], {"analytics": ["orders", "tmp_scratch"]}
        ),
        database_pattern=AllowDenyPattern(allow=[".*"]),
        table_pattern=AllowDenyPattern(allow=[".*"], deny=["^tmp_.*"]),
    )


def test_glue_lists_databases_with_pattern_verdict():
    result = list_glue_children(_config(), [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["analytics"].kind == DatasetContainerSubTypes.DATABASE
    assert by_name["analytics"].pattern_field == "database_pattern"
    assert by_name["analytics"].included is True


def test_glue_lists_tables_reusing_table_pattern():
    result = list_glue_children(_config(), ["analytics"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].included is True
    # The connector's own table_pattern deny (^tmp_) is reused for the verdict.
    assert by_name["tmp_scratch"].included is False
    assert by_name["tmp_scratch"].excluded_by == "table_pattern"
