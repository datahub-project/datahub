import socket

import feast

FEAST_MIN_VERSION = "0.18.0"
if feast.__version__ <= FEAST_MIN_VERSION:
    from feast import Client  # type: ignore
    from feast.data_format import ParquetFormat
    from feast.data_source import FileSource  # type: ignore
    from feast.entity import Entity
    from feast.feature import Feature
    from feast.feature_table import FeatureTable  # type: ignore
    from feast.value_type import ValueType


if __name__ == "__main__":
    if feast.__version__ > FEAST_MIN_VERSION:
        raise Exception(
            f"this code does not work with feast > {FEAST_MIN_VERSION}. Found {feast.__version__}"
        )

    test_client = Client(core_url="testfeast:6565")

    # create dummy entity since Feast demands it
    entity_1 = Entity(
        name="dummy_entity_1",
        description="Dummy entity 1",
        value_type=ValueType.STRING,
        labels={"key": "val"},
    )

    # create dummy entity since Feast demands it
    entity_2 = Entity(
        name="dummy_entity_2",
        description="Dummy entity 2",
        value_type=ValueType.INT32,
        labels={"key": "val"},
    )

    # commit entities
    test_client.apply([entity_1, entity_2])

    # dummy file source
    batch_source = FileSource(
        file_format=ParquetFormat(),
        file_url="file://feast/*",
        event_timestamp_column="ts_col",
        created_timestamp_column="timestamp",
        date_partition_column="date_partition_col",
    )

    # first feature table for testing, with all of Feast's datatypes
    table_1 = FeatureTable(
        name="test_feature_table_all_feature_dtypes",
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
        entities=["dummy_entity_1", "dummy_entity_2"],
        labels={"team": "matchmaking"},
        batch_source=batch_source,
    )

    # second feature table for testing, with just a single feature
    table_2 = FeatureTable(
        name="test_feature_table_single_feature",
        features=[
            Feature(name="test_BYTES_feature", dtype=ValueType.BYTES),
        ],
        entities=["dummy_entity_1"],
        labels={"team": "matchmaking"},
        batch_source=batch_source,
    )

    # third feature table for testing, no labels
    table_3 = FeatureTable(
        name="test_feature_table_no_labels",
        features=[
            Feature(name="test_BYTES_feature", dtype=ValueType.BYTES),
        ],
        entities=["dummy_entity_2"],
        labels={},
        batch_source=batch_source,
    )

    # commit the tables to the feature store
    test_client.apply([table_1, table_2, table_3])

    print("make_tests.py setup finished")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # listen to port 6789 once done so test script knows when to start ingestion
    server_address = ("localhost", 6789)
    sock.bind(server_address)

    sock.listen(1)

    print("make_tests.py listening on 6789")

    while True:
        # Wait for a connection
        connection, client_address = sock.accept()
