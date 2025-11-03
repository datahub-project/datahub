import pytest

from tests.utilities.metadata_operations import (
    get_analytics_charts,
    get_highlights,
    get_metadata_analytics_charts,
)


@pytest.mark.read_only
def test_highlights_is_accessible(auth_session):
    res_data = get_highlights(auth_session)
    assert res_data is not None, f"Received data was {res_data}"


@pytest.mark.read_only
def test_analytics_chart_is_accessible(auth_session):
    res_data = get_analytics_charts(auth_session)
    assert res_data is not None, f"Received data was {res_data}"


@pytest.mark.read_only
def test_metadata_analytics_chart_is_accessible(auth_session):
    res_data = get_metadata_analytics_charts(auth_session)
    assert res_data is not None, f"Received data was {res_data}"
