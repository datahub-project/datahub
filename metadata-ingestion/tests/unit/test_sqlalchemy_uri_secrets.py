from typing import Dict

import pytest

from datahub.ingestion.source.sql.sqlalchemy_uri import sqlalchemy_uri_secrets


@pytest.mark.parametrize(
    "url, expected",
    [
        pytest.param(
            "postgresql://my_user@my-host/my_db?sslmode=require",
            {},
            id="no_password",
        ),
        pytest.param(
            "postgresql://my_user:Str0ng%21Uri%23Secret@my-host/my_db",
            {
                "sqlalchemy_uri.userinfo_password.verbatim": "Str0ng%21Uri%23Secret",
                "sqlalchemy_uri.userinfo_password": "Str0ng!Uri#Secret",
            },
            id="userinfo",
        ),
        pytest.param(
            "postgresql://my_user:abc@secret@my-host/my_db",
            {"sqlalchemy_uri.userinfo_password.verbatim": "abc@secret"},
            id="userinfo_with_unencoded_at_skips_the_misparsed_prefix",
        ),
        pytest.param(
            "postgresql://my_user:my-secret@my-host:bad/my_db?password=my-query-secret",
            {
                "sqlalchemy_uri.userinfo_password.verbatim": "my-secret",
                "sqlalchemy_uri.query.password.0.verbatim": "my-query-secret",
            },
            id="invalid_uri_keeps_the_as_written_forms",
        ),
        pytest.param(
            "postgresql://my_user:my-uri-secret@my-host/my_db?password=pa%2fss%3Aword",
            {
                "sqlalchemy_uri.userinfo_password.verbatim": "my-uri-secret",
                "sqlalchemy_uri.userinfo_password": "my-uri-secret",
                "sqlalchemy_uri.query.password.0.verbatim": "pa%2fss%3Aword",
                "sqlalchemy_uri.query.password.0": "pa/ss:word",
            },
            id="userinfo_and_query_param_as_written_and_decoded",
        ),
        pytest.param(
            "postgresql://my_user@my-host/my_db?PWD=my-first&pwd=my-second",
            {
                "sqlalchemy_uri.query.PWD.0.verbatim": "my-first",
                "sqlalchemy_uri.query.pwd.0.verbatim": "my-second",
                "sqlalchemy_uri.query.PWD.0": "my-first",
                "sqlalchemy_uri.query.pwd.0": "my-second",
            },
            id="query_param_names_are_case_insensitive",
        ),
        pytest.param(
            "postgresql://my_user@my-host/my_db?password=my-first&password=my-second",
            {
                "sqlalchemy_uri.query.password.0.verbatim": "my-first",
                "sqlalchemy_uri.query.password.1.verbatim": "my-second",
                "sqlalchemy_uri.query.password.0": "my-first",
                "sqlalchemy_uri.query.password.1": "my-second",
            },
            id="repeated_query_param",
        ),
        pytest.param(
            "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BMy+Driver%7D%3BPWD%3Dpa%2Bss%2525x",
            {
                "sqlalchemy_uri.query.odbc_connect.0.verbatim": "pa%2Bss%2525x",
                "sqlalchemy_uri.query.odbc_connect.0": "pa+ss%25x",
            },
            id="odbc_connect_pwd_decoded_once",
        ),
        pytest.param(
            "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BMy+Driver%7D%3BPWD%3D%7Bmy%3Bodbc-secret%7D",
            {
                "sqlalchemy_uri.query.odbc_connect.0.verbatim": "my%3Bodbc-secret",
                "sqlalchemy_uri.query.odbc_connect.0": "my;odbc-secret",
            },
            id="odbc_connect_braced_pwd",
        ),
        pytest.param(
            "mssql+pyodbc:///?odbc_connect=DRIVER={My Driver};PASSWORD=my-odbc-secret;",
            {
                "sqlalchemy_uri.query.odbc_connect.0.verbatim": "my-odbc-secret",
                "sqlalchemy_uri.query.odbc_connect.0": "my-odbc-secret",
            },
            id="odbc_connect_unencoded",
        ),
        pytest.param("not a url", {}, id="unparseable"),
    ],
)
def test_sqlalchemy_uri_secrets(url: str, expected: Dict[str, str]) -> None:
    assert sqlalchemy_uri_secrets(url) == expected
