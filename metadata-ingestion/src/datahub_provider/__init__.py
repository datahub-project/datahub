import datahub


# This is needed to allow Airflow to pick up specific metadata fields it needs for
# certain features. We recognize it's a bit unclean to define these in multiple places,
# but at this point it's the only workaround if you'd like your custom conn type to
# show up in the Airflow UI.
def get_provider_info():
    return {
        "name": "DataHub",
        "description": "`DataHub <https://datahubproject.io/>`__\n",
        "connection-types": [
            {
                "hook-class-name": "datahub_provider.hooks.datahub.DatahubRestHook",
                "connection-type": "datahub_rest",
            },
            {
                "hook-class-name": "datahub_provider.hooks.datahub.DatahubKafkaHook",
                "connection-type": "datahub_kafka",
            },
        ],
        "hook-class-names": [
            "datahub_provider.hooks.datahub.DatahubRestHook",
            "datahub_provider.hooks.datahub.DatahubKafkaHook",
        ],
        "package-name": datahub.__package_name__,
        "versions": [datahub.__version__],
    }
