from prefect import flow, task

from prefect_datahub.datahub_emitter import DatahubEmitter
from prefect_datahub.entities import Dataset

datahub_emitter = DatahubEmitter().load("datahub-block")


@task(name="Extract", description="Extract the data")
def extract():
    data = "This is data"
    return data


@task(name="Transform", description="Transform the data")
def transform(data):
    data = data.split(" ")
    datahub_emitter.add_task(
        inputs=[Dataset("snowflake", "mydb.schema.tableX")],
        outputs=[Dataset("snowflake", "mydb.schema.tableY")],
    )
    return data


@flow(name="ETL", description="Extract transform load flow")
def etl():
    data = extract()
    data = transform(data)
    datahub_emitter.emit_flow()


etl()
