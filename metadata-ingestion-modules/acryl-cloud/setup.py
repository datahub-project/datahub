import json
import pathlib

from setuptools import setup

_codegen_config_file = pathlib.Path("./src/acryl_datahub_cloud/_codegen_config.json")
_codegen_config = json.loads(_codegen_config_file.read_text())

setup(
    **{
        **_codegen_config,
        "install_requires": [
            *_codegen_config["install_requires"],
        ],
    }
)
