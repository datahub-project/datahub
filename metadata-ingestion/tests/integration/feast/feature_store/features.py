from datetime import timedelta

import pandas as pd
from feast import Entity, Feature, FeatureView, FileSource, ValueType
from feast.data_source import RequestDataSource
from feast.on_demand_feature_view import on_demand_feature_view

driver_hourly_stats_source = FileSource(
    path="data/driver_stats_with_string.parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)

driver_entity = Entity(
    name="driver_id", value_type=ValueType.INT64, description="Driver ID"
)

driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=timedelta(days=7),
    features=[
        Feature(
            name="conv_rate",
            dtype=ValueType.FLOAT,
            labels=dict(description="Conv rate"),
        ),
        Feature(
            name="acc_rate", dtype=ValueType.FLOAT, labels=dict(description="Acc rate")
        ),
        Feature(
            name="avg_daily_trips",
            dtype=ValueType.INT64,
            labels=dict(description="Avg daily trips"),
        ),
        Feature(
            name="string_feature",
            dtype=ValueType.STRING,
            labels=dict(description="String feature"),
        ),
    ],
    online=True,
    batch_source=driver_hourly_stats_source,
    tags={},
)

input_request = RequestDataSource(
    name="vals_to_add",
    schema={"val_to_add": ValueType.INT64, "val_to_add_2": ValueType.INT64},
)


@on_demand_feature_view(  # type: ignore
    inputs={
        "driver_hourly_stats": driver_hourly_stats_view,
        "vals_to_add": input_request,
    },
    features=[
        Feature(name="conv_rate_plus_val1", dtype=ValueType.DOUBLE),
        Feature(name="conv_rate_plus_val2", dtype=ValueType.DOUBLE),
    ],
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()

    df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
    df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]

    return df
