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
        default=None,
        description="Fully qualified name of the rules table. rule_id must be unique "
        "in it: every row of a duplicated rule_id is skipped.",
    )
    results_table: Optional[str] = Field(
        default=None,
        description="Fully qualified name of the append-only results table. "
        "(rule_id, run_id) identifies a result and executed_at is its completion time.",
    )
    rule_namespace: str = Field(
        default="default",
        description="Scopes rule_id uniqueness. Assertion identity is "
        "(platform, platform_instance, env, rule_namespace, rule_id); the dataset is "
        "not part of it. Changing it, platform_instance or env creates new assertions. "
        "Set platform_instance or a distinct rule_namespace per rules table, otherwise "
        "two workspaces sharing a rule_id share an assertion.",
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
        "(first run, or stateful ingestion disabled). Also how long results whose "
        "rule is not published yet keep the read window open before they are dropped.",
    )
    late_arrival_minutes: NonNegativeInt = Field(
        default=60,
        description="Results whose executed_at is up to this far behind the newest "
        "published result are still picked up, and never emitted twice. The read "
        "window never moves backward, so raising this only applies to results newer "
        "than the previous run's window start.",
    )

    @pydantic.model_validator(mode="after")
    def _tables_required_when_enabled(self) -> "ExternalDQConfig":
        if self.enabled and not (self.rules_table and self.results_table):
            raise ValueError(
                "external_dq.rules_table and external_dq.results_table are required "
                "when external_dq.enabled is true"
            )
        return self
