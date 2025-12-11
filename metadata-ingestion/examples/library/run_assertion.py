# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import logging

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

log = logging.getLogger(__name__)

graph = DataHubGraph(
    config=DatahubClientConfig(
        server="http://localhost:8080",
    )
)

assertion_urn = "urn:li:assertion:6e3f9e09-1483-40f9-b9cd-30e5f182694a"

# Run the assertion
assertion_result = graph.run_assertion(urn=assertion_urn, save_result=True)

log.info(
    f"Assertion result (SUCCESS / FAILURE / ERROR): {assertion_result.get('type')}"
)
