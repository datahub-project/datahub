import logging
import os
from pathlib import Path
from typing import Dict, Tuple

from datahub.api.entities.assertion.assertion_config_spec import AssertionsConfigSpec
from datahub.api.entities.assertion.assertion_operator import LessThanOrEqualToOperator
from datahub.api.entities.assertion.assertion_trigger import (
    AssertionTrigger,
    CronTrigger,
    EntityChangeTrigger,
    IntervalTrigger,
)
from datahub.api.entities.assertion.compiler_interface import (
    AssertionCompilationResult,
    AssertionCompiler,
    CompileResultArtifact,
    CompileResultArtifactType,
)
from datahub.api.entities.assertion.datahub_assertion import DataHubAssertion
from datahub.api.entities.assertion.field_assertion import FieldValuesAssertion
from datahub.api.entities.assertion.freshness_assertion import (
    FixedIntervalFreshnessAssertion,
)
from datahub.emitter.mce_builder import make_assertion_urn
from datahub.integrations.assertion.common import get_entity_name, get_entity_schema
from datahub.integrations.assertion.snowflake.dmf_generator import SnowflakeDMFHandler
from datahub.integrations.assertion.snowflake.field_metric_sql_generator import (
    SnowflakeFieldMetricSQLGenerator,
)
from datahub.integrations.assertion.snowflake.field_values_metric_sql_generator import (
    SnowflakeFieldValuesMetricSQLGenerator,
)
from datahub.integrations.assertion.snowflake.metric_operator_sql_generator import (
    SnowflakeMetricEvalOperatorSQLGenerator,
)
from datahub.integrations.assertion.snowflake.metric_sql_generator import (
    SnowflakeMetricSQLGenerator,
)

logger = logging.Logger(__name__)

DMF_DEFINITIONS_FILE_NAME = "dmf_definitions.sql"
DMF_ASSOCIATIONS_FILE_NAME = "dmf_associations.sql"
DMF_SCHEMA_PROPERTY_KEY = "DMF_SCHEMA"


class SnowflakeAssertionCompiler(AssertionCompiler):
    def __init__(self, output_dir: str, extras: Dict[str, str]) -> None:
        self.output_dir = Path(output_dir)
        self.extras = extras
        self.metric_generator = SnowflakeMetricSQLGenerator(
            SnowflakeFieldMetricSQLGenerator(), SnowflakeFieldValuesMetricSQLGenerator()
        )
        self.metric_evaluator = SnowflakeMetricEvalOperatorSQLGenerator()
        self.dmf_handler = SnowflakeDMFHandler()

        self._entity_schedule_history: Dict[str, AssertionTrigger] = dict()

    @classmethod
    def create(
        cls, output_dir: str, extras: Dict[str, str]
    ) -> "SnowflakeAssertionCompiler":
        assert os.path.exists(output_dir), (
            f"Specified location {output_dir} does not exist."
        )

        assert os.path.isdir(output_dir), (
            f"Specified location {output_dir} is not a folder."
        )

        assert any(x.upper() == DMF_SCHEMA_PROPERTY_KEY for x in extras), (
            "Must specify value for DMF schema using -x DMF_SCHEMA=<db.schema>"
        )

        return SnowflakeAssertionCompiler(output_dir, extras)

    def compile(
        self, assertion_config_spec: AssertionsConfigSpec
    ) -> AssertionCompilationResult:
        result = AssertionCompilationResult("snowflake", "success")

        # TODO: Create/Report permissions sql

        dmf_definitions_path = self.output_dir / DMF_DEFINITIONS_FILE_NAME
        dmf_associations_path = self.output_dir / DMF_ASSOCIATIONS_FILE_NAME
        with (dmf_definitions_path).open("w") as definitions, (
            dmf_associations_path
        ).open("w") as associations:
            for assertion_spec in assertion_config_spec.assertions:
                result.report.num_processed += 1
                try:
                    start_line = f"\n-- Start of Assertion {assertion_spec.get_id()}\n"
                    (dmf_definition, dmf_association) = self.process_assertion(
                        assertion_spec
                    )
                    end_line = f"\n-- End of Assertion {assertion_spec.get_id()}\n"

                    definitions.write(start_line)
                    definitions.write(dmf_definition)
                    definitions.write(end_line)

                    associations.write(start_line)
                    associations.write(dmf_association)
                    associations.write(end_line)

                    result.report.num_compile_succeeded += 1
                except Exception as e:
                    result.status = "failure"
                    result.report.report_failure(
                        assertion_spec.get_id(),
                        f"Failed to compile assertion of type {assertion_spec.assertion.type} due to error: {e}",
                    )
                    result.report.num_compile_failed += 1
            if result.report.num_compile_succeeded > 0:
                result.add_artifact(
                    CompileResultArtifact(
                        name=DMF_DEFINITIONS_FILE_NAME,
                        path=dmf_definitions_path,
                        type=CompileResultArtifactType.SQL_QUERIES,
                        description="SQL file containing DMF create definitions equivalent to Datahub Assertions",
                    )
                )
                result.add_artifact(
                    CompileResultArtifact(
                        name=DMF_ASSOCIATIONS_FILE_NAME,
                        path=dmf_associations_path,
                        type=CompileResultArtifactType.SQL_QUERIES,
                        description="ALTER TABLE queries to associate DMFs to table to run on configured schedule.",
                    )
                )

            return result

    def process_assertion(self, assertion: DataHubAssertion) -> Tuple[str, str]:
        # TODO: support schema assertion ?

        # For freshness assertion, metric is difference in seconds between assertion execution time
        # and last time table was updated.
        # For field values assertion, metric is number or percentage of rows that do not satify
        # operator condition.
        # For remaining assertions, numeric metric is discernible in assertion definition itself.
        metric_definition = self.metric_generator.metric_sql(assertion.assertion)

        if isinstance(assertion.assertion, FixedIntervalFreshnessAssertion):
            assertion_sql = self.metric_evaluator.operator_sql(
                LessThanOrEqualToOperator(
                    type="less_than_or_equal_to",
                    value=assertion.assertion.lookback_interval.total_seconds(),
                ),
                metric_definition,
            )
        elif isinstance(assertion.assertion, FieldValuesAssertion):
            assertion_sql = self.metric_evaluator.operator_sql(
                LessThanOrEqualToOperator(
                    type="less_than_or_equal_to",
                    value=assertion.assertion.failure_threshold.value,
                ),
                metric_definition,
            )
        else:
            assertion_sql = self.metric_evaluator.operator_sql(
                assertion.assertion.operator, metric_definition
            )

        dmf_name = get_dmf_name(assertion)
        dmf_schema_name = self.extras[DMF_SCHEMA_PROPERTY_KEY]

        args_create_dmf, args_add_dmf = get_dmf_args(assertion)

        entity_name = get_entity_name(assertion.assertion)

        self._entity_schedule_history.setdefault(
            assertion.assertion.entity, assertion.assertion.trigger
        )
        if (
            assertion.assertion.entity in self._entity_schedule_history
            and self._entity_schedule_history[assertion.assertion.entity]
            != assertion.assertion.trigger
        ):
            raise ValueError(
                "Assertions on same entity must have same schedules as of now."
                f" Found different schedules on entity {assertion.assertion.entity} ->"
                f" ({self._entity_schedule_history[assertion.assertion.entity].trigger}),"
                f" ({assertion.assertion.trigger.trigger})"
            )

        dmf_schedule = get_dmf_schedule(assertion.assertion.trigger)
        dmf_definition = self.dmf_handler.create_dmf(
            f"{dmf_schema_name}.{dmf_name}",
            args_create_dmf,
            assertion.assertion.description
            or f"Created via DataHub for assertion {make_assertion_urn(assertion.get_id())} of type {assertion.assertion.type}",
            assertion_sql,
        )
        dmf_association = self.dmf_handler.add_dmf_to_table(
            f"{dmf_schema_name}.{dmf_name}",
            args_add_dmf,
            dmf_schedule,
            ".".join(entity_name),
        )

        return dmf_definition, dmf_association


def get_dmf_name(assertion: DataHubAssertion) -> str:
    return f"datahub__{assertion.get_id()}"


def get_dmf_args(assertion: DataHubAssertion) -> Tuple[str, str]:
    """Returns Tuple with
    - Args used to create DMF
    - Args used to add DMF to table"""
    # Snowflake does not allow creating custom data metric
    # function without column name argument.
    # So we fetch any one column from table's schema
    args_create_dmf = "ARGT TABLE({col_name} {col_type})"
    args_add_dmf = "{col_name}"
    entity_schema = get_entity_schema(assertion.assertion)
    if entity_schema:
        for col_dict in entity_schema:
            return args_create_dmf.format(
                col_name=col_dict["col"], col_type=col_dict["native_type"]
            ), args_add_dmf.format(col_name=col_dict["col"])

    raise ValueError("entity schema not available")


def get_dmf_schedule(trigger: AssertionTrigger) -> str:
    if isinstance(trigger.trigger, EntityChangeTrigger):
        return "TRIGGER_ON_CHANGES"
    elif isinstance(trigger.trigger, CronTrigger):
        return f"USING CRON {trigger.trigger.cron} {trigger.trigger.timezone}"
    elif isinstance(trigger.trigger, IntervalTrigger):
        return f"{trigger.trigger.interval.seconds / 60} MIN"
    else:
        raise ValueError(f"Unsupported trigger type {type(trigger.trigger)}")
