from datetime import timedelta

import feast.types
import pandas as pd
from feast import Entity, FeatureView, Field, FileSource, RequestSource, ValueType
from feast.on_demand_feature_view import on_demand_feature_view

driver_hourly_stats_source = FileSource(
    name="driver_hourly_stats_source",
    path="data/driver_stats_with_string.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

driver = Entity(
    # It would be the modern Feast pattern to call this `driver`, but the
    # golden tests have the name as `driver_id`
    name="driver_id",
    join_keys=["driver_id"],
    value_type=ValueType.INT64,
    description="Driver ID",
    owner="MOCK_OWNER",
    tags={"name": "deprecated"},
)

driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=7),
    schema=[
        Field(
            name="conv_rate",
            dtype=feast.types.Float64,
            tags={"name": "needs_documentation", "description": "Conv rate"},
        ),
        Field(
            name="acc_rate",
            dtype=feast.types.Float64,
            tags=dict(description="Acc rate"),
        ),
        Field(
            name="avg_daily_trips",
            dtype=feast.types.Int64,
            tags=dict(description="Avg daily trips"),
        ),
        Field(
            name="string_feature",
            dtype=feast.types.String,
            tags=dict(description="String feature"),
        ),
    ],
    online=True,
    source=driver_hourly_stats_source,
    tags={"name": "deprecated"},
    owner="MOCK_OWNER",
)

input_request = RequestSource(
    name="vals_to_add",
    schema=[
        Field(name="val_to_add", dtype=feast.types.Int64),
        Field(name="val_to_add_2", dtype=feast.types.Int64),
    ],
)


@on_demand_feature_view(  # type: ignore
    sources=[
        driver_hourly_stats_view,
        input_request,
    ],
    schema=[
        Field(name="conv_rate_plus_val1", dtype=feast.types.Float64),
        Field(name="conv_rate_plus_val2", dtype=feast.types.Float64),
    ],
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()

    df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
    df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]

    return df
