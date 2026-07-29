import pytest
from sqlalchemy.engine.url import make_url

from datahub.ingestion.source.sql.clickhouse_connection import (
    CLICKHOUSE_CLIENT_NAME,
    with_client_identity,
)


@pytest.mark.parametrize(
    "uri",
    [
        "clickhouse://user:password@host:8123/db",
        "clickhouse+http://user:password@host:8123/db",
    ],
)
def test_http_url_gets_user_agent_header(uri: str) -> None:
    url = with_client_identity(make_url(uri))
    assert url.query.get("header__User-Agent") == CLICKHOUSE_CLIENT_NAME
    assert "client_name" not in url.query


@pytest.mark.parametrize("scheme", ["clickhouse+native", "clickhouse+asynch"])
def test_native_url_gets_client_name(scheme: str) -> None:
    url = with_client_identity(
        make_url(f"{scheme}://user:password@host:9000/db?secure=true")
    )
    assert url.query.get("client_name") == CLICKHOUSE_CLIENT_NAME
    assert "header__User-Agent" not in url.query
    assert url.query.get("secure") == "true"


def test_user_supplied_http_ua_is_preserved() -> None:
    url = with_client_identity(
        make_url("clickhouse://user:password@host:8123/db?header__User-Agent=mycorp")
    )
    assert url.query.get("header__User-Agent") == "mycorp"


def test_user_supplied_http_ua_is_preserved_case_insensitively() -> None:
    # HTTP header names are case-insensitive, so a lowercase override must still win.
    url = with_client_identity(
        make_url("clickhouse://user:password@host:8123/db?header__user-agent=mycorp")
    )
    assert url.query.get("header__user-agent") == "mycorp"
    assert "header__User-Agent" not in url.query


def test_user_supplied_native_client_name_is_preserved() -> None:
    url = with_client_identity(
        make_url("clickhouse+native://user:password@host:9000/db?client_name=mycorp")
    )
    assert url.query.get("client_name") == "mycorp"


def test_unknown_driver_is_unchanged() -> None:
    original = make_url("postgresql://user:password@host:5432/db")
    assert with_client_identity(original) == original
