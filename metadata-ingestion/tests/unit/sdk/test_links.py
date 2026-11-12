import urllib.parse

import datahub.ingestion.graph.links as links
from datahub.testing.doctest import assert_doctest


def test_links() -> None:
    assert_doctest(links)


def test_make_url_for_api_urn_uses_apis_route() -> None:
    api_urn = "urn:li:api:my-api-id"
    assert links.make_url_for_urn("https://demo.datahub.com", api_urn) == (
        f"https://demo.datahub.com/apis/{urllib.parse.quote(api_urn, safe='')}/"
    )
