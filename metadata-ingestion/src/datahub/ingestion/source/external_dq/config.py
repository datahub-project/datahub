from typing import Literal, Optional

import pydantic
from pydantic import Field, NonNegativeInt, PositiveInt

from datahub.configuration.common import ConfigModel


class ExternalDQConfig(ConfigModel):
    enabled: bool = Field(
        default=False,
        description="Ingest data-quality rules and results written to two tables that "
        "follow the DataHub external DQ table contract, as externally-managed assertions.",
    )
    rules_table: Optional[str] = Field(
        default=None, description="Fully qualified name of the rules table."
    )
    results_table: Optional[str] = Field(
        default=None,
        description="Fully qualified name of the append-only results table.",
    )
    rule_namespace: str = Field(
        default="default",
        description="Scopes rule_id uniqueness. Assertion identity is "
        "(platform, platform_instance, env, rule_namespace, rule_id). Changing it creates "
        "new assertions.",
    )
    contract_version: Literal[1] = Field(
        default=1, description="Version of the table contract the tables implement."
    )
    strict_column_order: bool = Field(
        default=False,
        description="Fail when contract columns are out of order. When false, order "
        "drift is only reported as a warning.",
    )
    initial_lookback_days: PositiveInt = Field(
        default=7,
        description="How far back to read results when there is no checkpoint "
        "(first run, or stateful ingestion disabled).",
    )
    late_arrival_minutes: NonNegativeInt = Field(
        default=60,
        description="Results whose executed_at is up to this far behind the last "
        "checkpoint are still picked up (and never emitted twice).",
    )

    @pydantic.model_validator(mode="after")
    def _tables_required_when_enabled(self) -> "ExternalDQConfig":
        if self.enabled and not (self.rules_table and self.results_table):
            raise ValueError(
                "external_dq.rules_table and external_dq.results_table are required "
                "when external_dq.enabled is true"
            )
        return self
