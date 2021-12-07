from datahub.utilities import config_clean


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
