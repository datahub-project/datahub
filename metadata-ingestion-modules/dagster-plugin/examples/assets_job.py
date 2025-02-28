from dagster import (
    AssetIn,
    AssetOut,
    Definitions,
    Output,
    asset,
    define_asset_job,
    multi_asset,
)

from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub_dagster_plugin.sensors.datahub_sensors import (
    DatahubDagsterSourceConfig,
    make_datahub_sensor,
)


@multi_asset(
    outs={
        "extract": AssetOut(
            metadata={"datahub.outputs": [DatasetUrn("snowflake", "tableD").urn()]}
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
            "extract",
            metadata={"datahub.inputs": [DatasetUrn("snowflake", "tableC").urn()]},
        )
    }
)
def transform(extract):
    results = []
    for each in extract:
        results.append(str(each))
    return results


assets_job = define_asset_job(name="assets_job")

config = DatahubDagsterSourceConfig(
    datahub_client_config=DatahubClientConfig(server="http://localhost:8080"),
    dagster_url="http://localhost:3000",
)

datahub_sensor = make_datahub_sensor(config=config)

defs = Definitions(
    assets=[extract, transform], jobs=[assets_job], sensors=[datahub_sensor]
)
