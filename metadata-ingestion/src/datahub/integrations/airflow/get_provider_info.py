import datahub


def get_provider_info():
    return {
        "name": "DataHub",
        "description": "`DataHub <https://datahubproject.io/>`__\n",
        "hook-class-names": [
            "datahub.integrations.airflow.hooks.DatahubRestHook",
            "datahub.integrations.airflow.hooks.DatahubKafkaHook",
        ],
        "package-name": datahub.__package_name__,
        "versions": [datahub.__version__],
    }
