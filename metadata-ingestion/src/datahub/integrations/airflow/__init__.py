try:
    from datahub_provider.lineage.datahub import DatahubAirflowLineageBackend
except ModuleNotFoundError:
    # Compat for older versions of Airflow.
    pass
