from types import SimpleNamespace
from typing import Any

import pytest

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.agent.probe import probe_hierarchy
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.dynamodb.dynamodb_probe import list_dynamodb_children


class _Paginator:
    def __init__(self, names):
        self._names = names

    def paginate(self):
        return iter([{"TableNames": self._names}])


class _DynClient:
    def __init__(self, names):
        self._names = names
        self.meta = SimpleNamespace(region_name="us-east-1")

    def get_paginator(self, name):
        return _Paginator(self._names)


# A real pydantic config (not a plain SimpleNamespace) so resolve_pattern_field can
# introspect model_fields for table_pattern, which the probe now resolves by
# convention rather than declaring explicitly.
class _DynConfig(ConfigModel):
    dynamodb_client: Any
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


def test_dynamodb_lists_region_qualified_tables():
    config = _DynConfig(
        dynamodb_client=_DynClient(["orders", "tmp_scratch"]),
        table_pattern=AllowDenyPattern(allow=[".*"], deny=[".*tmp_.*"]),
    )
    result = list_dynamodb_children(config, [], 100)
    by_name = {n.name: n for n in result.nodes}
    # Region-qualified to match the name the connector's table_pattern sees.
    assert by_name["us-east-1.orders"].kind == DatasetSubTypes.TABLE
    assert by_name["us-east-1.orders"].included is True
    assert by_name["us-east-1.tmp_scratch"].included is False
    assert by_name["us-east-1.tmp_scratch"].excluded_by == "table_pattern"


def test_snowflake_summary_inherits_probe():
    # SnowflakeSummaryConfig extends SnowflakeConnectionConfig → probe inherited.
    pytest.importorskip("snowflake.connector")
    assert probe_hierarchy("snowflake-summary")


def test_snowflake_queries_delegates_probe():
    pytest.importorskip("snowflake.connector")
    assert probe_hierarchy("snowflake-queries")


def test_bigquery_queries_delegates_probe():
    pytest.importorskip("google.cloud.bigquery")
    assert probe_hierarchy("bigquery-queries")
