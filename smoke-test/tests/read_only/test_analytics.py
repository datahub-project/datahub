import pytest

from tests.utilities.domains import Domain
from tests.utilities.metadata_operations import (
    get_analytics_charts,
    get_highlights,
    get_metadata_analytics_charts,
)

pytestmark = pytest.mark.domain(Domain.PLATFORM)


@pytest.mark.read_only
def test_highlights_is_accessible(auth_session):
    res_data = get_highlights(auth_session)
    assert res_data is not None, f"Received data was {res_data}"
    # getHighlights catches every exception and returns an empty list, so a
    # not-None check passes even on total failure. The two active-user cards are
    # added unconditionally, so anything shorter means the resolver threw.
    assert len(res_data) >= 2, (
        f"Expected at least the two active-user highlights, received {res_data}"
    )


@pytest.mark.read_only
def test_analytics_chart_is_accessible(auth_session):
    res_data = get_analytics_charts(auth_session)
    assert res_data is not None, f"Received data was {res_data}"


@pytest.mark.read_only
def test_metadata_analytics_chart_is_accessible(auth_session):
    res_data = get_metadata_analytics_charts(auth_session)
    assert res_data is not None, f"Received data was {res_data}"
