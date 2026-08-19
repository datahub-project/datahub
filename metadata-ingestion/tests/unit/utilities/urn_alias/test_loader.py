from typing import Optional
from unittest import mock

import pytest

from datahub.utilities.urn_alias.index import CatalogSlice, UrnAliasIndex
from datahub.utilities.urn_alias.loader import load_urn_alias_index

_LOWER = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
_UPPER = "urn:li:dataset:(urn:li:dataPlatform:bigquery,PROJECT.DATASET.TABLE,PROD)"
_OTHER = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.orders,PROD)"

_BIGQUERY_PROD = CatalogSlice(platform="bigquery", platform_instance=None, env="PROD")


def _graph(*urns: str, fails: bool = False) -> mock.MagicMock:
    """A server holding `urns`, whose scroll optionally dies part way through."""
    graph = mock.MagicMock()

    def scroll(**kwargs: object) -> object:
        yield from urns
        if fails:
            raise RuntimeError("boom")

    graph.get_urns_by_filter.side_effect = scroll
    return graph


def _load(graph: mock.MagicMock) -> Optional[UrnAliasIndex]:
    return load_urn_alias_index(
        graph=graph, platform="bigquery", platform_instance=None, env="PROD"
    )


def test_a_completed_scroll_makes_a_miss_inside_it_an_answer() -> None:
    index = _load(_graph(_LOWER))

    assert index is not None
    assert index.catalog_slice == _BIGQUERY_PROD
    assert index.covers(_OTHER)


def test_a_scroll_that_fails_part_way_yields_no_index() -> None:
    # Its rows are real, but their keys would claim to hold every casing of the name.
    assert _load(_graph(_LOWER, fails=True)) is None


def test_one_index_is_shared_by_every_consumer_of_a_region() -> None:
    graph = _graph(_LOWER)

    assert _load(graph) is _load(graph)
    assert graph.get_urns_by_filter.call_count == 1


def test_every_casing_of_a_name_is_found_under_one_key() -> None:
    index = _load(_graph(_LOWER, _UPPER))

    assert index is not None
    assert sorted(index.matches(_LOWER)) == sorted([_LOWER, _UPPER])


def test_an_empty_region_still_loads_and_says_so(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The known hole while platform_instance is still a scroll filter — see
    # docs/dev_guides/lineage_urn_casing.md.
    index = load_urn_alias_index(
        graph=_graph(), platform="bigquery", platform_instance="prod_wh", env="PROD"
    )

    assert index is not None
    assert index.knows(_LOWER) is False
    assert any("is empty" in r.message for r in caplog.records)
