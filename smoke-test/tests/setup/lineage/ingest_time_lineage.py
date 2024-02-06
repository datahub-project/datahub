import os
from typing import List

from datahub.emitter.rest_emitter import DatahubRestEmitter

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

SERVER = os.getenv("DATAHUB_SERVER") or "http://localhost:8080"
TOKEN = os.getenv("DATAHUB_TOKEN") or ""
EMITTER = DatahubRestEmitter(gms_server=SERVER, token=TOKEN)


def ingest_time_lineage() -> None:
    ingest_input_datasets_change(EMITTER)
    ingest_data_job_change(EMITTER)
    ingest_dataset_join_change(EMITTER)


def get_time_lineage_urns() -> List[str]:
    return (
        get_input_datasets_change_urns()
        + get_data_job_change_urns()
        + get_dataset_join_change_urns()
    )
