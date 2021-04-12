try:
    from datahub.integrations.airflow.lineage_backend import (
        DatahubAirflowLineageBackend,
    )
except ImportError:
    # Compat for Airflow 2.x.
    pass
