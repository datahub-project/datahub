from datetime import timedelta

from datahub.ingestion.source.bigquery_v2.profiling.partition_discovery.date_utils import (
    DateUtils,
)


def test_is_date_like_column_recognized_and_unrecognized():
    assert DateUtils.is_date_like_column("event_date") is True
    assert DateUtils.is_date_like_column("EVENT_DATE") is True
    assert DateUtils.is_date_like_column("region") is False


def test_is_date_type_column():
    assert DateUtils.is_date_type_column("TIMESTAMP") is True
    assert DateUtils.is_date_type_column("timestamp") is True
    assert DateUtils.is_date_type_column("STRING") is False
    assert DateUtils.is_date_type_column("") is False


def test_get_column_ordering_strategy_date_vs_non_date():
    # A date-typed / date-named column orders by the column value; anything else falls
    # back to picking the most frequent value.
    assert (
        DateUtils.get_column_ordering_strategy("event_date", "DATE")
        == "`event_date` DESC"
    )
    assert (
        DateUtils.get_column_ordering_strategy("some_col", "TIMESTAMP")
        == "`some_col` DESC"
    )
    assert (
        DateUtils.get_column_ordering_strategy("region", "STRING")
        == "record_count DESC"
    )


def test_strategic_candidate_dates_are_midnight_calendar_pair():
    candidates = DateUtils.get_strategic_candidate_dates()
    assert [label for _, label in candidates] == ["today", "yesterday"]
    today, yesterday = candidates[0][0], candidates[1][0]
    # Both candidates are midnight-normalized and exactly one calendar day apart.
    for moment in (today, yesterday):
        assert (moment.hour, moment.minute, moment.second, moment.microsecond) == (
            0,
            0,
            0,
            0,
        )
    assert today - yesterday == timedelta(days=1)
