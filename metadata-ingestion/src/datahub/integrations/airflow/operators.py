import warnings

from datahub_provider.operators.datahub import (  # noqa: F401
    DatahubBaseOperator,
    DatahubEmitterOperator,
)

warnings.warn(
    "importing from datahub.integrations.airflow.* is deprecated; "
    "use datahub_provider.{hooks,operators,lineage}.datahub instead"
)
