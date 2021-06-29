import warnings

try:
    from datahub_provider.lineage.datahub import (
        DatahubLineageBackend as DatahubAirflowLineageBackend,
    )
except ModuleNotFoundError:
    # Compat for older versions of Airflow.
    pass

warnings.warn(
    "importing from datahub.integrations.airflow.* is deprecated; "
    "use datahub_provider.{hooks,operators,lineage}.datahub instead"
)
