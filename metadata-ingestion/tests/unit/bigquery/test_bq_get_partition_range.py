import datetime

from datahub.ingestion.source.bigquery_v2.profiler import BigqueryProfiler


def test_get_partition_range_from_partition_id():
    # yearly partition check
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2022", datetime.datetime(2022, 1, 1)
    ) == (datetime.datetime(2022, 1, 1), datetime.datetime(2023, 1, 1))
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2022", datetime.datetime(2022, 3, 12)
    ) == (datetime.datetime(2022, 1, 1), datetime.datetime(2023, 1, 1))
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2022", datetime.datetime(2021, 5, 2)
    ) == (datetime.datetime(2021, 1, 1), datetime.datetime(2022, 1, 1))
    assert BigqueryProfiler.get_partition_range_from_partition_id("2022", None) == (
        datetime.datetime(2022, 1, 1),
        datetime.datetime(2023, 1, 1),
    )
    # monthly partition check
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "202202", datetime.datetime(2022, 2, 1)
    ) == (datetime.datetime(2022, 2, 1), datetime.datetime(2022, 3, 1))
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "202202", datetime.datetime(2022, 2, 3)
    ) == (datetime.datetime(2022, 2, 1), datetime.datetime(2022, 3, 1))
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "202202", datetime.datetime(2021, 12, 13)
    ) == (datetime.datetime(2021, 12, 1), datetime.datetime(2022, 1, 1))
    assert BigqueryProfiler.get_partition_range_from_partition_id("202202", None) == (
        datetime.datetime(2022, 2, 1),
        datetime.datetime(2022, 3, 1),
    )
    # daily partition check
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "20220205", datetime.datetime(2022, 2, 5)
    ) == (datetime.datetime(2022, 2, 5), datetime.datetime(2022, 2, 6))
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "20220205", datetime.datetime(2022, 2, 3)
    ) == (datetime.datetime(2022, 2, 3), datetime.datetime(2022, 2, 4))
    assert BigqueryProfiler.get_partition_range_from_partition_id("20220205", None) == (
        datetime.datetime(2022, 2, 5),
        datetime.datetime(2022, 2, 6),
    )
    # hourly partition check
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2022020509", datetime.datetime(2022, 2, 5, 9)
    ) == (datetime.datetime(2022, 2, 5, 9), datetime.datetime(2022, 2, 5, 10))
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2022020509", datetime.datetime(2022, 2, 3, 1)
    ) == (datetime.datetime(2022, 2, 3, 1), datetime.datetime(2022, 2, 3, 2))
    assert BigqueryProfiler.get_partition_range_from_partition_id(
        "2022020509", None
    ) == (
        datetime.datetime(2022, 2, 5, 9),
        datetime.datetime(2022, 2, 5, 10),
    )
