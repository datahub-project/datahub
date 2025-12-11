# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from prefect_datahub.datahub_emitter import DatahubEmitter

datahub_emitter = DatahubEmitter(
    datahub_rest_url="http://localhost:8080",
    env="DEV",
    platform_instance="local_prefect",
    token=None,  # generate auth token in the datahub and provide here if gms endpoint is secure
)
datahub_emitter.save("datahub-emitter-test")  # type: ignore
