import asyncio

from prefect import flow, task

from prefect_datahub.datahub_emitter import DatahubEmitter
from prefect_datahub.entities import Dataset


async def load_datahub_emitter():
    datahub_emitter = DatahubEmitter()
    return datahub_emitter.load("datahub-block-7")


@task(name="Extract", description="Extract the data")
def extract():
    data = "This is data"
    return data


@task(name="Transform", description="Transform the data")
def transform(data, datahub_emitter):
    data = data.split(" ")
    datahub_emitter.add_task(
        inputs=[Dataset("snowflake", "mydb.schema.tableX")],
        outputs=[Dataset("snowflake", "mydb.schema.tableY")],
    )
    return data


@flow(name="ETL", description="Extract transform load flow")
def etl():
    datahub_emitter = asyncio.run(load_datahub_emitter())
    data = extract()
    data = transform(data, datahub_emitter)
    datahub_emitter.emit_flow()


etl()
