try:
    from datahub_provider.lineage.datahub import (
        DatahubLineageBackend as DatahubAirflowLineageBackend,
    )
except ModuleNotFoundError:
    # Compat for older versions of Airflow.
    pass
