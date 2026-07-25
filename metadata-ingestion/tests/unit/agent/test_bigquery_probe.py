from types import SimpleNamespace

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.models import ProbeLeafKind
from datahub.ingestion.source.bigquery_v2.bigquery_probe import list_bigquery_children
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


class _Named:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _FakeBqClient:
    def __init__(self) -> None:
        self.closed = False

    def list_projects(self):
        return [_Named(project_id="acryl-staging"), _Named(project_id="other-proj")]

    def list_datasets(self, project):
        return [_Named(dataset_id="smoke_test_db"), _Named(dataset_id="tmp_db")]

    def list_tables(self, path):
        return [
            _Named(table_id="orders", table_type="TABLE"),
            _Named(table_id="v_orders", table_type="VIEW"),
            _Named(table_id="mv_orders", table_type="MATERIALIZED_VIEW"),
        ]

    def get_table(self, path):
        return _Named(schema=[_Named(name="id"), _Named(name="amount")])

    def close(self):
        self.closed = True


@pytest.fixture
def bq():
    client = _FakeBqClient()
    config = SimpleNamespace(
        get_bigquery_client=lambda: client,
        project_ids=[],
        project_id_pattern=AllowDenyPattern(allow=[".*"], deny=["^other-proj$"]),
        dataset_pattern=AllowDenyPattern(allow=[".*"], deny=["^tmp_.*"]),
        match_fully_qualified_names=False,
        table_pattern=AllowDenyPattern(allow=[".*"]),
    )
    return SimpleNamespace(config=config, client=client)


def test_projects_apply_project_gate(bq):
    result = list_bigquery_children(bq.config, [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["acryl-staging"].kind == DatasetContainerSubTypes.BIGQUERY_PROJECT
    assert by_name["acryl-staging"].pattern_field == "project_id_pattern"
    assert by_name["acryl-staging"].included is True
    assert by_name["other-proj"].included is False
    assert by_name["other-proj"].excluded_by == "project_id_pattern"
    assert bq.client.closed is True


def test_explicit_project_ids_override_the_pattern(bq):
    bq.config.project_ids = ["other-proj"]
    by_name = {n.name: n for n in list_bigquery_children(bq.config, [], 100).nodes}
    assert by_name["other-proj"].included is True
    assert by_name["acryl-staging"].included is False


def test_datasets_are_project_qualified_and_filtered(bq):
    result = list_bigquery_children(bq.config, ["acryl-staging"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["smoke_test_db"].kind == DatasetContainerSubTypes.BIGQUERY_DATASET
    assert by_name["smoke_test_db"].fqn == "acryl-staging.smoke_test_db"
    assert by_name["smoke_test_db"].included is True
    assert by_name["tmp_db"].included is False
    assert by_name["tmp_db"].excluded_by == "dataset_pattern"


def test_tables_and_views_split_by_table_type(bq):
    result = list_bigquery_children(bq.config, ["acryl-staging", "smoke_test_db"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].pattern_field == "table_pattern"
    assert by_name["orders"].fqn == "acryl-staging.smoke_test_db.orders"
    assert by_name["v_orders"].kind == DatasetSubTypes.VIEW
    assert by_name["v_orders"].pattern_field == "view_pattern"
    assert by_name["mv_orders"].kind == DatasetSubTypes.VIEW


def test_table_pattern_matches_fully_qualified_name(bq):
    bq.config.table_pattern = AllowDenyPattern(allow=[".*"], deny=[".*\\.orders$"])
    by_name = {
        n.name: n
        for n in list_bigquery_children(
            bq.config, ["acryl-staging", "smoke_test_db"], 100
        ).nodes
    }
    assert by_name["orders"].included is False
    assert by_name["orders"].excluded_by == "table_pattern"


def test_excluded_view_reports_table_pattern_as_the_reason(bq):
    # Quirk: unlike Snowflake (which splits table_pattern/view_pattern), BigQuery's
    # classify_table always names "table_pattern" as excluded_by, even for a view —
    # only the node's own pattern_field stays "view_pattern". Pinned here so a
    # declarative rewrite can't silently "fix" this into two distinct reasons.
    bq.config.table_pattern = AllowDenyPattern(allow=[".*"], deny=[".*\\.v_orders$"])
    by_name = {
        n.name: n
        for n in list_bigquery_children(
            bq.config, ["acryl-staging", "smoke_test_db"], 100
        ).nodes
    }
    assert by_name["v_orders"].kind == DatasetSubTypes.VIEW
    assert by_name["v_orders"].included is False
    assert by_name["v_orders"].excluded_by == "table_pattern"
    assert by_name["v_orders"].pattern_field == "view_pattern"


def test_columns_are_fully_qualified_leaves(bq):
    result = list_bigquery_children(
        bq.config, ["acryl-staging", "smoke_test_db", "orders"], 100
    )
    assert [n.name for n in result.nodes] == ["id", "amount"]
    assert all(n.kind == ProbeLeafKind.COLUMN for n in result.nodes)
    assert result.nodes[0].fqn == "acryl-staging.smoke_test_db.orders.id"
