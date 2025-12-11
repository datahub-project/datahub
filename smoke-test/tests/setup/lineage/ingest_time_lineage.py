# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import List

from datahub.ingestion.graph.client import DataHubGraph
from tests.setup.lineage.ingest_data_job_change import (
    get_data_job_change_urns,
    ingest_data_job_change,
)
from tests.setup.lineage.ingest_dataset_join_change import (
    get_dataset_join_change_urns,
    ingest_dataset_join_change,
)
from tests.setup.lineage.ingest_input_datasets_change import (
    get_input_datasets_change_urns,
    ingest_input_datasets_change,
)


def ingest_time_lineage(graph_client: DataHubGraph) -> None:
    ingest_input_datasets_change(graph_client)
    ingest_data_job_change(graph_client)
    ingest_dataset_join_change(graph_client)


def get_time_lineage_urns() -> List[str]:
    return (
        get_input_datasets_change_urns()
        + get_data_job_change_urns()
        + get_dataset_join_change_urns()
    )
