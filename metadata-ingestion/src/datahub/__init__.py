__package_name__ = "acryl-datahub"
__version__ = "0.0.0.dev0"


def nice_version_name() -> str:
    if __version__ == "0.0.0.dev0":
        return "unavailable (installed editable via git)"
    return __version__
