from types import SimpleNamespace

from datahub.configuration.common import AllowDenyPattern
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


def _config():
    return SimpleNamespace(
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


def test_glue_table_pattern_matches_the_fully_qualified_name():
    # glue.py's _gen_table_wu matches table_pattern against
    # "<database>.<table>" (e.g. "analytics.tmp_scratch"), not the bare table
    # name — so a deny anchored to the bare name ("^tmp_.*") never matches and
    # does NOT exclude the table; this is what real ingestion does too.
    result = list_glue_children(_config(), ["analytics"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].included is True
    assert by_name["tmp_scratch"].included is True

    # A deny anchored to the fully qualified name does exclude it.
    fqn_config = _config()
    fqn_config.table_pattern = AllowDenyPattern(
        allow=[".*"], deny=[r"^analytics\.tmp_scratch$"]
    )
    result = list_glue_children(fqn_config, ["analytics"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["tmp_scratch"].included is False
    assert by_name["tmp_scratch"].excluded_by == "table_pattern"
    assert by_name["orders"].included is True
