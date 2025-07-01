from datahub_airflow_plugin._version import __package_name__, __version__


def get_provider_info() -> dict:
    # Register our hooks with Airflow.
    return {
        "package-name": f"{__package_name__}",
        "name": f"{__package_name__}",
        "description": "DataHub metadata collector plugin",
        "connection-types": [
            {
                "hook-class-name": "datahub_airflow_plugin.hooks.datahub.DatahubRestHook",
                "connection-type": "datahub-rest",
            },
            {
                "hook-class-name": "datahub_airflow_plugin.hooks.datahub.DatahubKafkaHook",
                "connection-type": "datahub-kafka",
            },
        ],
        # Deprecated method of providing connection types, kept for backwards compatibility.
        # We can remove with Airflow 3.
        "hook-class-names": [
            "datahub_airflow_plugin.hooks.datahub.DatahubRestHook",
            "datahub_airflow_plugin.hooks.datahub.DatahubKafkaHook",
        ],
    }
