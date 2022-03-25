from datahub.utilities import config_clean


def test_remove_suffix():
    assert (
        config_clean.remove_suffix(
            "xaaabcdef.snowflakecomputing.com", ".snowflakecomputing.com"
        )
        == "xaaabcdef"
    )

    assert (
        config_clean.remove_suffix("xaaabcdef", ".snowflakecomputing.com")
        == "xaaabcdef"
    )


def test_url_without_slash_suffix():
    assert (
        config_clean.remove_trailing_slashes("http://example.com")
        == "http://example.com"
    )


def test_url_with_suffix():
    assert (
        config_clean.remove_trailing_slashes("http://example.com/")
        == "http://example.com"
    )


def test_url_with_multiple_slashes():
    assert (
        config_clean.remove_trailing_slashes("http://example.com/a/b/c")
        == "http://example.com/a/b/c"
    )
    assert (
        config_clean.remove_trailing_slashes("http://example.com/a/b/c/")
        == "http://example.com/a/b/c"
    )
    assert (
        config_clean.remove_trailing_slashes("http://example.com/a/b/c///")
        == "http://example.com/a/b/c"
    )


def test_remove_protocol_https():
    assert config_clean.remove_protocol("https://localhost:3000") == "localhost:3000"


def test_remove_protocol_http():
    assert config_clean.remove_protocol("http://www.example.com") == "www.example.com"


def test_remove_protocol_http_accidental_multiple():
    assert (
        config_clean.remove_protocol("http://http://www.example.com")
        == "www.example.com"
    )
