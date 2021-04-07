import time
from typing import TYPE_CHECKING, Optional
from airflow.lineage.backend import LineageBackend


if TYPE_CHECKING:
    from airflow.models.baseoperator import (
        BaseOperator,
    )  # pylint: disable=cyclic-import


class DatahubAirflowLineageBackend(LineageBackend):
    # With Airflow 2.0, this can be an instance method. However, with Airflow 1.10.x, this
    # method is used statically, even though LineageBackend declares it as an instance variable.
    @staticmethod
    def send_lineage(
        operator: "BaseOperator",
        inlets: Optional[list] = None,
        outlets: Optional[list] = None,
        context: Optional[dict] = None,
    ):
        with open(
            "/Users/hsheth/projects/datahub/metadata-ingestion/lineage_calls.txt", "a"
        ) as f:
            f.write(
                f"{time.time()}: {operator} (in = {inlets}, out = {outlets}) ctx {context}\n"
            )
