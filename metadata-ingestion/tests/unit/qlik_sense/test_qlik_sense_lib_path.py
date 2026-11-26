from typing import List, Optional

import pytest

from datahub.ingestion.source.qlik_sense.lib_path import (
    build_lib_path,
    lib_path_segments,
    named_space,
    parse_lib_connection,
    path_has_label,
    path_key,
)


@pytest.mark.parametrize(
    "connection, space, name",
    [
        ("Lake", "", "Lake"),
        ("Team:Lake", "Team", "Lake"),
        ("[Team: Sales]:Lake", "Team: Sales", "Lake"),
        ("[Team: Sales]", "", "Team: Sales"),
        ("[Team]:[Lake]", "Team", "Lake"),
    ],
)
def test_lib_connection_splits_space_and_name(
    connection: str, space: str, name: str
) -> None:
    parsed = parse_lib_connection(connection)
    assert (parsed.space, parsed.name) == (space, name)


def test_named_space_is_unknown_when_the_separator_is_ambiguous() -> None:
    # Without brackets this may be space `team: sales` or connection `sales:lake`.
    assert named_space("team: sales:lake") is None
    assert named_space("[Team: Sales]:Lake") == "Team: Sales"
    assert named_space("Lake") is None


@pytest.mark.parametrize(
    "path, segments",
    [
        ("lib://Lake/raw/orders.qvd", ["", "Lake", "raw", "orders.qvd"]),
        ("lib://Team:Lake/orders.qvd", ["Team", "Lake", "orders.qvd"]),
        (
            "lib://[Team: Sales]:Lake/raw\\orders.qvd",
            ["[Team: Sales]", "Lake", "raw", "orders.qvd"],
        ),
        ("lib://Lake/$(vFolder)/orders.qvd", None),
        ("lib://team: sales:lake/orders.qvd", None),
    ],
)
def test_lib_path_segments_round_trip(path: str, segments: Optional[List[str]]) -> None:
    assert lib_path_segments(path) == segments
    if segments is not None:
        assert lib_path_segments(build_lib_path(segments)) == segments


def test_path_key_ignores_case_and_optional_brackets() -> None:
    assert path_key("lib://[Team]:Lake/Orders.qvd") == path_key(
        "lib://team:lake/orders.qvd"
    )
    assert path_has_label("lib://Lake/Raw/Orders.qvd", "raw/orders.qvd")
    assert not path_has_label("lib://Lake/Raw/Orders.qvd", "orders.qvd")
