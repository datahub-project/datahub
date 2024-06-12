from prefect_datahub.datahub_emitter import DatahubEmitter

DatahubEmitter(
    datahub_rest_url="http://localhost:8080",
    env="DEV",
    platform_instance="local_prefect",
).save("datahub-block", overwrite=True)
