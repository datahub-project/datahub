import asyncio

from prefect_datahub.datahub_emitter import DatahubEmitter


async def save_datahub_emitter():
    datahub_emitter = DatahubEmitter(
        datahub_rest_url="http://localhost:8080",
        env="DEV",
        platform_instance="local_prefect",
        token=None,  # generate auth token in the datahub and provide here if gms endpoint is secure
    )

    await datahub_emitter.save("BLOCK-ID", overwrite=True)


asyncio.run(save_datahub_emitter())
