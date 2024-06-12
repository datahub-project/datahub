import asyncio

from prefect_datahub.datahub_emitter import DatahubEmitter


async def save_datahub_emitter():
    datahub_emitter = DatahubEmitter(
        datahub_rest_url="http://localhost:8080",
        env="PROD",
        platform_instance="local_prefect",
    )

    await datahub_emitter.save("datahub-block-7", overwrite=True)


asyncio.run(save_datahub_emitter())
