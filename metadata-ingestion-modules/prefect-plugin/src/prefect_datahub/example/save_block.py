from prefect_datahub.datahub_emitter import DatahubEmitter

datahub_emitter = DatahubEmitter(
    datahub_rest_url="http://localhost:8080",
    env="DEV",
    platform_instance="local_prefect",
    token=None,  # generate auth token in the datahub and provide here if gms endpoint is secure
)
datahub_emitter.save("datahub-emitter-test")  # type: ignore
