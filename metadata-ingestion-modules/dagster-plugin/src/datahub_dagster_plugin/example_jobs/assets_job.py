from dagster import (
    AssetIn,
    AssetOut,
    Definitions,
    Output,
    asset,
    define_asset_job,
    multi_asset,
)
from datahub.api.entities.dataset import Dataset

from datahub_dagster_plugin.sensors.datahub_sensors import datahub_sensor


@multi_asset(
    outs={
        "extract": AssetOut(
            metadata={"datahub.outputs": [Dataset("snowflake", "tableD").urn]}
        ),
    }
)
def extract():
    results = [1, 2, 3, 4]
    metadata = {
        "num_record": len(results),
    }
    return Output(value=results, metadata=metadata)


@asset(
    ins={
        "extract": AssetIn(
            "extract", metadata={"datahub.inputs": [Dataset("snowflake", "tableC").urn]}
        )
    }
)
def transform(extract):
    results = []
    for each in extract:
        results.append(str(each))
    return results


assets_job = define_asset_job(name="assets_job")

defs = Definitions(
    assets=[extract, transform], jobs=[assets_job], sensors=[datahub_sensor]
)
