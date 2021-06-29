import warnings

from datahub_provider.hooks.datahub import (  # noqa: F401
    DatahubGenericHook,
    DatahubKafkaHook,
    DatahubRestHook,
)

warnings.warn(
    "importing from datahub.integrations.airflow.* is deprecated; "
    "use datahub_provider.{hooks,operators,lineage}.datahub instead"
)
