from types import SimpleNamespace

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.sql.sql_probe import list_two_tier_children


class _FakeInspector:
    def get_table_names(self, schema=None):
        # Hive's SQLAlchemy connector reports views inside get_table_names too;
        # see hive_source.py's comment on _process_view.
        return ["orders", "v_orders"]

    def get_view_names(self, schema=None):
        return ["v_orders"]


class _FakeEngine:
    def __init__(self) -> None:
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


@pytest.fixture
def two_tier(monkeypatch):
    import sqlalchemy

    engine = _FakeEngine()
    monkeypatch.setattr(sqlalchemy, "create_engine", lambda url, **kw: engine)
    monkeypatch.setattr(sqlalchemy, "inspect", lambda eng: _FakeInspector())
    config = SimpleNamespace(
        get_sql_alchemy_url=lambda: "hive://host/",
        options={},
        default_schemas=lambda: [],
        database_pattern=AllowDenyPattern(allow=[".*"]),
        table_pattern=AllowDenyPattern(allow=[".*"], deny=["^v_.*"]),
        view_pattern=AllowDenyPattern(allow=[".*"]),
    )
    return SimpleNamespace(config=config, engine=engine)


def test_hive_shaped_table_listing_still_classifies_the_view(two_tier):
    # Hive's get_table_names includes views alongside tables (TWO_TIER_PROBE). A
    # name reported by both listings must come back as a view, not a table, even
    # though the table listing is the first source in the level's LevelSource list.
    result = list_two_tier_children(two_tier.config, ["mydb"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["v_orders"].kind == DatasetSubTypes.VIEW
    assert by_name["v_orders"].pattern_field == "view_pattern"
    # table_pattern denies ^v_.*; if v_orders were judged against it (the bug),
    # it would be excluded. Judged against view_pattern (which allows all), it
    # must be included.
    assert by_name["v_orders"].included is True
    assert by_name["v_orders"].excluded_by is None
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].pattern_field == "table_pattern"
    assert by_name["orders"].included is True
    assert two_tier.engine.disposed is True
