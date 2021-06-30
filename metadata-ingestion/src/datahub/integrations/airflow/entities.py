import warnings

from datahub_provider.entities import Dataset, _Entity  # noqa: F401

warnings.warn(
    "importing from datahub.integrations.airflow.* is deprecated; "
    "use datahub_provider.{hooks,operators,lineage}.datahub instead"
)
