from datahub import __version__


def get_provider_info():
    return {
        "name": "DataHub",
        "description": "`DataHub <https://datahubproject.io/>`__\n",
        "hook-class-names": [
            "datahub.integrations.airflow.hooks.DatahubRestHook",
        ],
        "package-name": "datahub",
        "versions": [__version__],
    }
