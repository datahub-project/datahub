from typing import Dict, List

import pytest

from datahub.ingestion.source.sql.sqlalchemy_uri import (
    _ODBC_PASSWORD_AS_WRITTEN,
    sqlalchemy_uri_secrets,
)


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
        pytest.param(
            "snowflake://my_user@my-account/my_db?authenticator=oauth&token=my-oauth-token",
            {
                "sqlalchemy_uri.query.token.0.verbatim": "my-oauth-token",
                "sqlalchemy_uri.query.token.0": "my-oauth-token",
            },
            id="oauth_token_query_param",
        ),
        pytest.param(
            "snowflake://my_user@my-account/my_db?private_key=Str0ng%2FKey%3D%3D",
            {
                "sqlalchemy_uri.query.private_key.0.verbatim": "Str0ng%2FKey%3D%3D",
                "sqlalchemy_uri.query.private_key.0": "Str0ng/Key==",
            },
            id="private_key_query_param",
        ),
        pytest.param(
            "bigquery://my-project?credentials_base64=bXktc2VjcmV0LWpzb24%3D",
            {
                "sqlalchemy_uri.query.credentials_base64.0.verbatim": "bXktc2VjcmV0LWpzb24%3D",
                "sqlalchemy_uri.query.credentials_base64.0": "bXktc2VjcmV0LWpzb24=",
            },
            id="credentials_base64_query_param",
        ),
        pytest.param("not a url", {}, id="unparseable"),
    ],
)
def test_sqlalchemy_uri_secrets(url: str, expected: Dict[str, str]) -> None:
    assert sqlalchemy_uri_secrets(url) == expected


@pytest.mark.parametrize(
    "connect_string, expected",
    [
        pytest.param("SERVER%3Dh%3BPWD%3Dsecret", ["secret"], id="encoded_separators"),
        pytest.param("SERVER=h;PWD=secret", ["secret"], id="literal_separators"),
        pytest.param("SERVER=h%3BPWD=secret", ["secret"], id="mixed_separators"),
        pytest.param("PWD%3D%7Bmy%3Bsecret%7D", ["my%3Bsecret"], id="encoded_braces"),
        pytest.param("PWD={my;secret}", ["my;secret"], id="literal_braces"),
        pytest.param("PASSWORD%3Dsecret", ["secret"], id="password_alias"),
        pytest.param("pwd%3Dsecret", ["secret"], id="lowercase_key"),
        pytest.param("PWD+%3D+secret", ["secret"], id="plus_as_space"),
        pytest.param("PWD%20%3D%20secret", ["secret"], id="percent_20_as_space"),
        pytest.param("PWD%3Dsecret%3BUID%3Du", ["secret"], id="at_start"),
        pytest.param("MYPWD%3Dnotasecret", [], id="key_suffix_is_not_a_match"),
        pytest.param("PWD%3Dse=cret%3BUID%3Du", ["se=cret"], id="value_with_equals"),
        pytest.param(
            "PWD%3Dfirst%3BUID%3Du%3BPWD%3Dsecond",
            ["first", "second"],
            id="two_passwords",
        ),
        pytest.param("PWD%3Dsecret%3B", ["secret"], id="trailing_separator"),
        pytest.param("SERVER%3Dh%3BPWD%3D", [""], id="empty_value"),
        pytest.param(
            "PWD%3D%7Bunterminated",
            ["%7Bunterminated"],
            id="unterminated_brace_falls_back_to_the_plain_value",
        ),
    ],
)
def test_odbc_password_as_written_grammar(
    connect_string: str, expected: List[str]
) -> None:
    assert [
        match.group("braced") or match.group("plain")
        for match in _ODBC_PASSWORD_AS_WRITTEN.finditer(connect_string)
    ] == expected


def test_empty_odbc_password_is_not_registered() -> None:
    url = "mssql+pyodbc:///?odbc_connect=SERVER%3Dmy-host%3BPWD%3D"

    assert sqlalchemy_uri_secrets(url) == {}
