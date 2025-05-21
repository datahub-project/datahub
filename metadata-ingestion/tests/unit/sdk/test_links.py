import datahub.ingestion.graph.links as links
from datahub.testing.doctest import assert_doctest


def test_links() -> None:
    assert_doctest(links)
