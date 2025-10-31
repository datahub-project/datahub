import logging

import pytest

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_cypress_suite1


def test_analytics_charts_have_data(auth_session, analytics_events_loaded):
    """Test that analytics charts return data after backfilling."""

    # Query to get analytics charts using correct schema
    json = {
        "query": """query getAnalyticsCharts {
            getAnalyticsCharts {
                groupId
                title
                charts {
                    ... on TimeSeriesChart {
                        title
                        lines {
                            name
                            data {
                                x
                                y
                            }
                        }
                    }
                    ... on BarChart {
                        title
                        bars {
                            name
                            segments {
                                label
                                value
                            }
                        }
                    }
                    ... on TableChart {
                        title
                        rows {
                            values
                        }
                    }
                }
            }
        }"""
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    if "errors" in res_data:
        logger.error(f"GraphQL errors: {res_data['errors']}")
    assert res_data["data"], f"No data in response: {res_data}"
    assert res_data["data"]["getAnalyticsCharts"]

    chart_groups = res_data["data"]["getAnalyticsCharts"]
    assert len(chart_groups) > 0, "Should have at least one chart group"

    logger.info(f"Found {len(chart_groups)} chart groups")

    # Flatten all charts from all groups
    all_charts = []
    for group in chart_groups:
        logger.info(f"Group: {group['title']}")
        all_charts.extend(group["charts"])

    # Track which charts have data
    charts_with_data = []
    charts_without_data = []

    for chart in all_charts:
        title = chart.get("title", "Unknown")
        has_data = False

        # Check TimeSeriesChart
        if "lines" in chart:
            for line in chart["lines"]:
                if len(line.get("data", [])) > 0:
                    has_data = True
                    break

        # Check BarChart
        if "bars" in chart:
            if len(chart["bars"]) > 0:
                has_data = True

        if has_data:
            charts_with_data.append(title)
        else:
            charts_without_data.append(title)

    logger.info(f"Charts with data: {charts_with_data}")
    logger.info(f"Charts without data: {charts_without_data}")

    # Assert that we have data in key charts that are shown by default
    # Note: "Top Viewed Datasets" and similar entity-specific charts only show
    # when filtering by domain or search term in the UI
    required_charts = [
        "Weekly Active Users",
        "Number of Searches",
        "Actions By Entity Type (Past Week)",
    ]

    # MAU is optional because it only queries data from previous months (not current month)
    # and can be empty if test runs early in the month with backfilled data from current month
    optional_charts = [
        "Monthly Active Users",
    ]

    # Verify required charts have data
    for expected_chart in required_charts:
        assert expected_chart in charts_with_data, (
            f"Expected '{expected_chart}' to have data"
        )

    # Log status of optional charts
    for chart in optional_charts:
        if chart in charts_with_data:
            logger.info(f"✓ Optional chart '{chart}' has data")
        else:
            logger.warning(
                f"⚠ Optional chart '{chart}' has no data (this may happen depending on when in the month the test runs)"
            )

    # Verify we loaded analytics data successfully
    # Require at least 3 charts (the required ones) to have data
    assert len(charts_with_data) >= 3, (
        f"Expected at least 3 charts with data, got {len(charts_with_data)}"
    )
    logger.info(
        f"✅ Analytics backfill successful! {len(charts_with_data)} charts have data"
    )


def test_weekly_active_users_chart(auth_session, analytics_events_loaded):
    """Test Weekly Active Users chart specifically."""
    json = {
        "query": """query getAnalyticsCharts {
            getAnalyticsCharts {
                groupId
                title
                charts {
                    ... on TimeSeriesChart {
                        title
                        lines {
                            name
                            data {
                                x
                                y
                            }
                        }
                    }
                }
            }
        }"""
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    # Flatten all charts from all groups
    chart_groups = res_data["data"]["getAnalyticsCharts"]
    charts = []
    for group in chart_groups:
        charts.extend(group["charts"])
    wau_chart = next((c for c in charts if c["title"] == "Weekly Active Users"), None)

    assert wau_chart is not None, "Weekly Active Users chart not found"
    assert "lines" in wau_chart
    assert len(wau_chart["lines"]) > 0
    assert len(wau_chart["lines"][0]["data"]) > 0, "WAU chart should have data points"

    # Check that we have reasonable values (> 0)
    data_points = wau_chart["lines"][0]["data"]

    # Log data points for debugging
    logger.info(f"WAU chart has {len(data_points)} data points")
    active_weeks = [point for point in data_points if point["y"] > 0]
    logger.info(f"Weeks with active users: {len(active_weeks)}/{len(data_points)}")
    if active_weeks:
        logger.info(f"Sample active week: {active_weeks[0]}")
    else:
        logger.error(f"No active users found. Sample data points: {data_points[:3]}")

    assert any(point["y"] > 0 for point in data_points), (
        f"WAU should have users. Found {len(data_points)} data points but all have y=0. "
        f"This may indicate events are missing the 'browserId' field."
    )


def test_top_searches_chart(auth_session, analytics_events_loaded):
    """Test Top Searches chart has data."""
    json = {
        "query": """query getAnalyticsCharts {
            getAnalyticsCharts {
                groupId
                title
                charts {
                    ... on BarChart {
                        title
                        bars {
                            name
                            segments {
                                value
                            }
                        }
                    }
                    ... on TableChart {
                        title
                        rows {
                            values
                        }
                    }
                }
            }
        }"""
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    # Flatten all charts from all groups
    chart_groups = res_data["data"]["getAnalyticsCharts"]
    charts = []
    for group in chart_groups:
        charts.extend(group["charts"])

    # Find Top Searches chart (could be "Top Searches (Past Week)")
    top_searches_chart = next(
        (c for c in charts if "Top Searches" in c.get("title", "")), None
    )

    if top_searches_chart:
        # Top Searches is a TableChart, not a BarChart
        if "rows" in top_searches_chart:
            assert len(top_searches_chart["rows"]) > 0, "Top Searches should have rows"

            # Verify we have search queries from our backfill
            search_names = [row["values"][0] for row in top_searches_chart["rows"]]
            logger.info(f"Top searches: {search_names}")

            # Our backfill uses queries like "customer", "revenue", "user", etc.
            expected_queries = ["customer", "revenue", "user", "transaction", "product"]
            has_expected = any(q in search_names for q in expected_queries)
            assert has_expected, (
                f"Expected to find queries from backfill in: {search_names}"
            )


def test_top_viewed_datasets_chart(auth_session, analytics_events_loaded):
    """Test Top Viewed Datasets chart has data."""
    json = {
        "query": """query getAnalyticsCharts {
            getAnalyticsCharts {
                groupId
                title
                charts {
                    ... on TableChart {
                        title
                        rows {
                            values
                        }
                    }
                }
            }
        }"""
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    # Flatten all charts from all groups
    chart_groups = res_data["data"]["getAnalyticsCharts"]
    charts = []
    for group in chart_groups:
        charts.extend(group["charts"])

    # Find Top Viewed Datasets chart
    top_datasets_chart = next(
        (c for c in charts if "Top Viewed Datasets" in c.get("title", "")), None
    )

    if top_datasets_chart:
        # Top Viewed Datasets is a TableChart
        assert "rows" in top_datasets_chart
        assert len(top_datasets_chart["rows"]) > 0, (
            "Top Viewed Datasets should have rows"
        )

        dataset_names = [row["values"][0] for row in top_datasets_chart["rows"]]
        logger.info(f"Top viewed datasets: {dataset_names}")

        # Should have dataset names with views
        assert len(dataset_names) > 0, "Should have viewed datasets"


def test_tab_views_by_entity_type_chart(auth_session, analytics_events_loaded):
    """Test Tab Views By Entity Type chart has data."""
    json = {
        "query": """query getAnalyticsCharts {
            getAnalyticsCharts {
                groupId
                title
                charts {
                    ... on BarChart {
                        title
                        bars {
                            name
                            segments {
                                label
                                value
                            }
                        }
                    }
                }
            }
        }"""
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    # Flatten all charts from all groups
    chart_groups = res_data["data"]["getAnalyticsCharts"]
    charts = []
    for group in chart_groups:
        charts.extend(group["charts"])

    # Find Tab Views By Entity Type chart
    tab_views_chart = next(
        (c for c in charts if "Tab Views By Entity Type" in c.get("title", "")), None
    )

    if tab_views_chart:
        assert "bars" in tab_views_chart
        assert len(tab_views_chart["bars"]) > 0, "Tab Views should have bars"

        entity_types = [bar["name"] for bar in tab_views_chart["bars"]]
        logger.info(f"Entity types with tab views: {entity_types}")

        # Our backfill creates events for dataset, dashboard, and chart
        expected_types = ["DATASET", "DASHBOARD", "CHART"]
        found_types = [et for et in expected_types if et in entity_types]

        assert len(found_types) > 0, (
            f"Expected to find {expected_types} in {entity_types}"
        )

        # Verify we have view counts
        total_tab_views = sum(
            sum(seg["value"] for seg in bar["segments"])
            for bar in tab_views_chart["bars"]
        )
        assert total_tab_views > 0, "Should have tab view counts"
