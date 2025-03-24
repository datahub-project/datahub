from setuptools import setup

setup(
    name="custom_transformer",
    version="1.0",
    py_modules=["custom_transform_example"],
    entry_points={
        "datahub.ingestion.transformer.plugins": [
            "custom_transform_example_alias = custom_transform_example:AddCustomOwnership",
        ],
    },
)
