# Published at https://pypi.org/project/acryl-datahub/.
__package_name__ = "acryl-datahub-airflow-plugin"
__version__ = "0.0.0.dev0"


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
    }
