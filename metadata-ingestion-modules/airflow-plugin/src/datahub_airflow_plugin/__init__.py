# Published at https://pypi.org/project/acryl-datahub/.
__package_name__ = "acryl-datahub-airflow-plugin"
__version__ = "1!0.0.0.dev0"


def is_dev_mode() -> bool:
    return __version__.endswith("dev0")


def nice_version_name() -> str:
    if is_dev_mode():
        return "unavailable (installed in develop mode)"
    return __version__


def get_provider_info():
    return {
        "package-name": f"{__package_name__}",
        "name": f"{__package_name__}",
        "description": "Datahub metadata collector plugin",
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
