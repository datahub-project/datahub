try:
    from datahub.integrations.airflow.lineage_backend import (
        DatahubAirflowLineageBackend,
    )
except ModuleNotFoundError:
    # Compat for Airflow 2.x.
    pass
