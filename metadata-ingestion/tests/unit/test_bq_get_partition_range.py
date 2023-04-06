import datetime

from datahub.ingestion.source.bigquery_v2.profiler import BigqueryProfiler


def test_get_partition_range_from_partition_id():
    # yearly partition check
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2022", datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)
    ) == (
        datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc),
        datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
    )
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2022", datetime.datetime(2022, 3, 12, tzinfo=datetime.timezone.utc)
    ) == (
        datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc),
        datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
    )
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2022", datetime.datetime(2021, 5, 2, tzinfo=datetime.timezone.utc)
    ) == (
        datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc),
        datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc),
    )
    assert BigqueryProfiler.get_partition_range_from_partition_id("2022", None) == (
        datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc),
        datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
    )
    # monthly partition check
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "202202", datetime.datetime(2022, 2, 1, tzinfo=datetime.timezone.utc)
    ) == (
        datetime.datetime(2022, 2, 1, tzinfo=datetime.timezone.utc),
        datetime.datetime(2022, 3, 1, tzinfo=datetime.timezone.utc),
    )
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "202202", datetime.datetime(2022, 2, 3, tzinfo=datetime.timezone.utc)
    ) == (
        datetime.datetime(2022, 2, 1, tzinfo=datetime.timezone.utc),
        datetime.datetime(2022, 3, 1, tzinfo=datetime.timezone.utc),
    )
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "202202", datetime.datetime(2021, 12, 13, tzinfo=datetime.timezone.utc)
    ) == (
        datetime.datetime(2021, 12, 1, tzinfo=datetime.timezone.utc),
        datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc),
    )
    assert BigqueryProfiler.get_partition_range_from_partition_id("202202", None) == (
        datetime.datetime(2022, 2, 1, tzinfo=datetime.timezone.utc),
        datetime.datetime(2022, 3, 1, tzinfo=datetime.timezone.utc),
    )
    # daily partition check
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "20220205", datetime.datetime(2022, 2, 5, tzinfo=datetime.timezone.utc)
    ) == (
        datetime.datetime(2022, 2, 5, tzinfo=datetime.timezone.utc),
        datetime.datetime(2022, 2, 6, tzinfo=datetime.timezone.utc),
    )
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "20220205", datetime.datetime(2022, 2, 3, tzinfo=datetime.timezone.utc)
    ) == (
        datetime.datetime(2022, 2, 3, tzinfo=datetime.timezone.utc),
        datetime.datetime(2022, 2, 4, tzinfo=datetime.timezone.utc),
    )
    assert BigqueryProfiler.get_partition_range_from_partition_id("20220205", None) == (
        datetime.datetime(2022, 2, 5, tzinfo=datetime.timezone.utc),
        datetime.datetime(2022, 2, 6, tzinfo=datetime.timezone.utc),
    )
    # hourly partition check
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2022020509", datetime.datetime(2022, 2, 5, 9, tzinfo=datetime.timezone.utc)
    ) == (
        datetime.datetime(2022, 2, 5, 9, tzinfo=datetime.timezone.utc),
        datetime.datetime(2022, 2, 5, 10, tzinfo=datetime.timezone.utc),
    )
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2022020509", datetime.datetime(2022, 2, 3, 1, tzinfo=datetime.timezone.utc)
    ) == (
        datetime.datetime(2022, 2, 3, 1, tzinfo=datetime.timezone.utc),
        datetime.datetime(2022, 2, 3, 2, tzinfo=datetime.timezone.utc),
    )
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2022020509", None
    ) == (
        datetime.datetime(2022, 2, 5, 9, tzinfo=datetime.timezone.utc),
        datetime.datetime(2022, 2, 5, 10, tzinfo=datetime.timezone.utc),
    )
