import json
import pathlib

_codegen_config_file = pathlib.Path(__file__).parent / "_codegen_config.json"
_codegen_config = json.loads(_codegen_config_file.read_text())

# Published at https://pypi.org/project/acryl-datahub-cloud/.
__package_name__ = _codegen_config["name"]
__version__ = _codegen_config["version"]


def is_dev_mode() -> bool:
    return __version__.endswith("dev0")


def nice_version_name() -> str:
    if is_dev_mode():
        return "unavailable (installed in develop mode)"
    return __version__
