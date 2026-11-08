import pytest
from looker_sdk.sdk.api40.models import (
    DashboardElement,
    LookWithQuery,
    Query,
    ResultMakerWithIdVisConfigAndDynamicFields,
)

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker.looker_config import LookerDashboardSourceConfig
from datahub.ingestion.source.looker.looker_selection import element_has_query
from datahub.ingestion.source.looker.looker_source import LookerDashboardSource
from tests.unit.looker.looker_probe_fixtures import fake_looker, recipe

_QUERY = Query(model="sales", view="orders")


# The one rule ingestion cannot call the predicate for without restructuring
# _get_looker_dashboard_element, whose other branches record reachable explores.
@pytest.mark.parametrize(
    "element",
    [
        DashboardElement(id="1", query=_QUERY),
        DashboardElement(id="2", look=LookWithQuery(query=_QUERY)),
        DashboardElement(id="3", look=LookWithQuery()),
        DashboardElement(
            id="4",
            result_maker=ResultMakerWithIdVisConfigAndDynamicFields(
                query=_QUERY, filterables=[]
            ),
        ),
        DashboardElement(id="5"),
    ],
    ids=["query", "look-with-query", "look-without-query", "result-maker", "none"],
)
def test_element_has_query_is_ingestions_element_read(
    element: DashboardElement,
) -> None:
    with fake_looker():
        source = LookerDashboardSource(
            LookerDashboardSourceConfig.model_validate(recipe()),
            PipelineContext(run_id="looker-selection"),
        )
        ingested = source._get_looker_dashboard_element(element)
    assert element_has_query(element) == (ingested is not None)
