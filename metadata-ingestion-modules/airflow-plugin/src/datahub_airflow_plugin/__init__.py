# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
