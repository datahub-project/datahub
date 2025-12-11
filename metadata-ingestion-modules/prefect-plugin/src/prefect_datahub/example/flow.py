# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import List, Tuple

from prefect import flow, task

from prefect_datahub.datahub_emitter import DatahubEmitter
from prefect_datahub.entities import Dataset

datahub_emitter_block = DatahubEmitter.load("datahub-emitter-test")


@task(name="Extract", description="Extract the data")
def extract() -> str:
    data = "This is data"
    return data


@task(name="Transform", description="Transform the data")
def transform(
    data: str, datahub_emitter: DatahubEmitter
) -> Tuple[List[str], DatahubEmitter]:
    data_list_str = data.split(" ")
    datahub_emitter.add_task(
        inputs=[
            Dataset(
                platform="snowflake",
                name="mydb.schema.tableA",
                env=datahub_emitter.env,
                platform_instance=datahub_emitter.platform_instance,
            )
        ],
        outputs=[
            Dataset(
                platform="snowflake",
                name="mydb.schema.tableB",
                env=datahub_emitter.env,
                platform_instance=datahub_emitter.platform_instance,
            )
        ],
    )
    return data_list_str, datahub_emitter


@flow(name="ETL", description="Extract transform load flow")
def etl() -> None:
    datahub_emitter = datahub_emitter_block
    data = extract()
    return_value = transform(data, datahub_emitter)  # type: ignore
    emitter = return_value[1]
    emitter.emit_flow()


etl()
