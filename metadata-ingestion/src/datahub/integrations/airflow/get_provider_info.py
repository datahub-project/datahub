from airflow.plugins_manager import AirflowPlugin

import datahub
from datahub.integrations.airflow.hooks import DatahubKafkaHook, DatahubRestHook


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


class DatahubAirflowPlugin(AirflowPlugin):
    # TODO: determine if this is actually necessary.
    # We only want to register the hooks here, so that we can be sure they show up
    # in the Web UI. Registering operators via plugins is deprecated.
    name = "datahub"
    hooks = [DatahubKafkaHook, DatahubRestHook]
