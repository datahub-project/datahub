# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
