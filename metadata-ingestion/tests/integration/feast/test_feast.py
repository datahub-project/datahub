from feast import Client
from feast.data_format import ParquetFormat
from feast.data_source import FileSource
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_table import FeatureTable
from feast.value_type import ValueType

test_client = Client(core_url="localhost:6565")

# create dummy entity since Feast demands it
entity = Entity(
    name="dummy_entity",
    description="Driver entity for car rides",
    value_type=ValueType.STRING,
    labels={"key": "val"},
)

test_client.apply(entity)

# Create Feature Tables
batch_source = FileSource(
    file_format=ParquetFormat(),
    file_url="file://feast/*",
    event_timestamp_column="ts_col",
    created_timestamp_column="timestamp",
    date_partition_column="date_partition_col",
)

ft1 = FeatureTable(
    name="my_feature_table_1",
    features=[
        Feature(name="test_BYTES_feature", dtype=ValueType.BYTES),
        Feature(name="test_STRING_feature", dtype=ValueType.STRING),
        Feature(name="test_INT32_feature", dtype=ValueType.INT32),
        Feature(name="test_INT64_feature", dtype=ValueType.INT64),
        Feature(name="test_DOUBLE_feature", dtype=ValueType.DOUBLE),
        Feature(name="test_FLOAT_feature", dtype=ValueType.FLOAT),
        Feature(name="test_BOOL_feature", dtype=ValueType.BOOL),
        Feature(name="test_BYTES_LIST_feature", dtype=ValueType.BYTES_LIST),
        Feature(name="test_STRING_LIST_feature", dtype=ValueType.STRING_LIST),
        Feature(name="test_INT32_LIST_feature", dtype=ValueType.INT32_LIST),
        Feature(name="test_INT64_LIST_feature", dtype=ValueType.INT64_LIST),
        Feature(name="test_DOUBLE_LIST_feature", dtype=ValueType.DOUBLE_LIST),
        Feature(name="test_FLOAT_LIST_feature", dtype=ValueType.FLOAT_LIST),
        Feature(name="test_BOOL_LIST_feature", dtype=ValueType.BOOL_LIST),
    ],
    entities=["dummy_entity"],
    labels={"team": "matchmaking"},
    batch_source=batch_source,
)

test_client.apply(ft1)

for table in test_client.list_feature_tables():

    print(test_client.get_feature_table(table.name))
