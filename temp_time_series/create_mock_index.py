#!/usr/bin/env python3
import os
from datetime import datetime
from typing import Optional, Generator, Tuple

# import hashlib

HOUR_IN_MS = 3600000
DAY_IN_MS = 86400000
START_DAY_IN_MS = int(datetime.now().timestamp() * 1000) - 5 * DAY_IN_MS

CounterType = Optional[int]
NameType = Optional[str]
IndexRowType = Tuple[
    NameType,
    CounterType,
    CounterType,
    NameType,
    CounterType,
    CounterType,
    CounterType,
    CounterType,
    CounterType,
    CounterType,
]


def day(n: int) -> int:
    return START_DAY_IN_MS + n * DAY_IN_MS


class MockIndexGenerator:
    INDEX_NAME = "mock_dataset_stats_aspect_v1"

    INDEX_FIELD_NAMES = [
        "urn",
        "rowCount",
        "columnCount",
        "columnStats.key",
        "columnStats.numNull",
        "eventTimestampMillis",
        "eventGranularity",
        "partitionSpec.parition",
        "partitionSpec.timeWindow.startTimeMillis",
        "partitionSpec.timeWindow.granulatiry",
    ]

    INDEX_FIELD_TYPES = [
        "keyword",
        "long",
        "long",
        "keyword",
        "long",
        "date",
        "long",
        "keyword",
        "date",
        "long",
    ]

    def __init__(self, start_days_in_ms, num_recs, num_cols):
        self._start_days_in_ms = start_days_in_ms
        self._num_recs = num_recs
        self._num_cols = num_cols
        self._stat_num_rows_start = 10000
        self._stat_num_cols_start = 50
        self._stat_num_nulls = 100

    def _get_num_rows(self, i: int):
        return self._stat_num_rows_start + (100 * i)

    def _get_num_cols(self, i: int):
        return self._stat_num_cols_start + i

    def _get_num_nulls(self, i: int, c: int):
        return self._stat_num_nulls + c + (10 * i)

    def _get_event_time_ms(self, i: int):
        return self._start_days_in_ms + (i * HOUR_IN_MS)

    @staticmethod
    def _get_index_row_json(row: IndexRowType) -> str:
        return ",".join(
            [
                f'"{field}" : "{value}"'
                for field, value in zip(MockIndexGenerator.INDEX_FIELD_NAMES, row)
                if value is not None
            ]
        )

    def get_records(self) -> Generator[IndexRowType, None, None]:
        for i in range(self._num_recs):
            # emit one table record
            yield self._get_index_row_json((
                "table_1",
                self._get_num_rows(i),
                self._get_num_cols(i),
                None,
                None,
                self._get_event_time_ms(i),
                HOUR_IN_MS,
                None,
                None,
                None)
            )
            # emit one record per column
            for c in range(self._num_cols):
                yield self._get_index_row_json((
                    f"table_1",
                    None,
                    None,
                    f"col_{c}",
                    self._get_num_nulls(i, c),
                    self._get_event_time_ms(i),
                    HOUR_IN_MS,
                    None,
                    None,
                    None)
                )

    @staticmethod
    def get_props_json() -> str:
        return ",".join(
            [
                f'"{field}" : {{ "type" : "{type}" }}'
                for field, type in zip(
                    MockIndexGenerator.INDEX_FIELD_NAMES,
                    MockIndexGenerator.INDEX_FIELD_TYPES,
                )
            ]
        )


def gen_index_schema() -> None:
    properties_json = MockIndexGenerator.get_props_json()
    index_schema_gen_cmd = (
        f"curl -v -XPUT http://localhost:9200/{MockIndexGenerator.INDEX_NAME} -H 'Content-Type: application/json' -d '"
        + """
        {
            "settings":{},
            "mappings":{
                "properties":{ """
        + f"{properties_json}"
        + """
                }
            }
        }'"""
    )
    print(index_schema_gen_cmd)
    os.system(index_schema_gen_cmd)


def populate_index_data() -> None:
    for id, row in enumerate(
        MockIndexGenerator(START_DAY_IN_MS, 100, 20).get_records()
    ):
        # id = hashlib.md5(row.encode("utf-8")).hexdigest()
        index_row_gen_command = (
            f"curl -v -XPUT http://localhost:9200/{MockIndexGenerator.INDEX_NAME}/_doc/{id} "
            + "-H 'Content-Type: application/json' -d '{ "
            + f"{row}"
            + " }'"
        )
        print(index_row_gen_command)
        os.system(index_row_gen_command)


def generate() -> None:
    #gen_index_schema()
    populate_index_data()


if __name__ == "__main__":
    generate()
