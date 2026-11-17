"""`include_tables` and `include_views` against real ingestion.

`SQLCommonConfig` marks both with `Enables`, so `probe filter` judges every
listed table (view) excluded once its switch is off. Nothing but ingestion
checks that the switch still gates what `SQLAlchemySource` emits; this runs
the generic `sqlalchemy` source on a sqlite file and compares.
"""

from pathlib import Path
from typing import Callable, Dict, Optional, Set

import pytest
from sqlalchemy import create_engine

from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.schema_classes import SubTypesClass
from datahub.metadata.urns import DatasetUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

_SOURCE_TYPE = "sqlalchemy"
# sqlite has no schemas of its own: the Inspector lists the attached database,
# "main", as the one schema, and ingestion names a table "main.<table>", the
# qualified name a listing fanned out over `containers` is compared on.
_TABLES = {"main.orders", "main.customers"}
_VIEWS = {"main.recent_orders", "main.big_customers"}


def _sqlite_fixture(path: Path) -> str:
    engine = create_engine(f"sqlite:///{path}")
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE orders (id INTEGER, amount INTEGER)")
        conn.exec_driver_sql("CREATE TABLE customers (id INTEGER, name TEXT)")
        conn.exec_driver_sql(
            "CREATE VIEW recent_orders AS SELECT id FROM orders WHERE id > 10"
        )
        conn.exec_driver_sql(
            "CREATE VIEW big_customers AS SELECT name FROM customers WHERE id < 5"
        )
    engine.dispose()
    return f"sqlite:///{path}"


def _datasets_of(sub_type: str) -> Callable[[EmittedIndex], Set[str]]:
    def emitted(index: EmittedIndex) -> Set[str]:
        return {
            DatasetUrn.from_string(urn).name
            for urn in index.urns("dataset", with_aspect=SubTypesClass)
            if any(
                isinstance(aspect, SubTypesClass) and sub_type in aspect.typeNames
                for aspect in index.aspects[urn]
            )
        }

    return emitted


def _listing(command: str, sub_type: str, *, switched_off: bool) -> ParityListing:
    return ParityListing(
        command,
        command,
        emitted=_datasets_of(sub_type),
        fan_out=FanOut("containers", "schema"),
        expect_empty=switched_off,
    )


@pytest.mark.parametrize(
    "switch",
    [None, "include_tables", "include_views"],
    ids=["defaults", "include_tables_off", "include_views_off"],
)
def test_switching_a_kind_off_matches_what_ingestion_emits(
    tmp_path: Path, switch: Optional[str]
) -> None:
    recipe: Dict[str, object] = {
        "platform": "sqlite",
        "connect_uri": _sqlite_fixture(tmp_path / "fixture.db"),
    }
    if switch is not None:
        recipe[switch] = False

    report = assert_probe_parity(
        _SOURCE_TYPE,
        recipe,
        pipeline_ingestion(_SOURCE_TYPE, tmp_path),
        [
            _listing(
                "tables",
                DatasetSubTypes.TABLE,
                switched_off=switch == "include_tables",
            ),
            _listing(
                "views", DatasetSubTypes.VIEW, switched_off=switch == "include_views"
            ),
        ],
    )

    for label, names, field in (
        ("tables", _TABLES, "include_tables"),
        ("views", _VIEWS, "include_views"),
    ):
        if switch == field:
            assert report.kinds[label].emitted == frozenset()
            assert report.excluded_by(label) == {name: field for name in names}
        else:
            assert report.kinds[label].included == names
