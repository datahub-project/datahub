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

BOOTSTRAP_SQL_FILE_NAME = "bootstrap.sql"
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

    @classmethod
    def create(
        cls, output_dir: str, extras: Dict[str, str]
    ) -> "SnowflakeAssertionCompiler":
        assert os.path.exists(
            output_dir
        ), f"Specified location {output_dir} does not exist."

        assert os.path.isdir(
            output_dir
        ), f"Specified location {output_dir} is not a folder."

        assert any(
            x.upper() == DMF_SCHEMA_PROPERTY_KEY for x in extras
        ), "Must specify value for DMF schema using -x DMF_SCHEMA=<db.schema>"

        return SnowflakeAssertionCompiler(output_dir, extras)

    def compile(
        self, assertion_config_spec: AssertionsConfigSpec
    ) -> AssertionCompilationResult:
        result = AssertionCompilationResult("snowflake", "success")

        # TODO: Create/Report permissions sql

        bootstrap_sql_path = self.output_dir / BOOTSTRAP_SQL_FILE_NAME
        with (bootstrap_sql_path).open("w") as f:
            for assertion_spec in assertion_config_spec.assertions:
                result.report.num_processed += 1
                try:
                    code: str = f"\n-- Start of Assertion {assertion_spec.get_id()}\n"
                    code += self.process_assertion(assertion_spec)
                    code += f"\n-- End of Assertion {assertion_spec.get_id()}\n"
                    f.write(code)
                    result.report.num_compile_succeeded += 1
                except Exception as e:
                    result.status = "failure"
                    result.report.report_failure(
                        assertion_spec.get_id(),
                        f"Failed to compile assertion due to {e}",
                    )
                    result.report.num_compile_failed += 1
            if result.report.num_compile_succeeded > 0:
                result.add_artifact(
                    CompileResultArtifact(
                        name=BOOTSTRAP_SQL_FILE_NAME,
                        path=bootstrap_sql_path,
                        type=CompileResultArtifactType.SQL_QUERIES,
                        description="SQL file containing DMF create definitions equivalent to Datahub Assertions"
                        "\nand ALTER TABLE queries to associate DMFs to table to run on configured schedule.",
                    )
                )

            return result

    def process_assertion(self, assertion: DataHubAssertion) -> str:

        # TODO: if assertion on same entity requires different schedule than another
        # assertion on entity - report error
        # TODO: support freshness and schema assertion ?

        metric_definition = self.metric_generator.metric_sql(assertion.assertion)

        if isinstance(assertion.assertion, FieldValuesAssertion):
            # metric is number or percentage of rows that do not satify operator condition
            # evaluator returns a boolean value 1 if PASS, 0 if FAIL
            assertion_sql = self.metric_evaluator.operator_sql(
                LessThanOrEqualToOperator(
                    type="less_than_or_equal_to",
                    value=assertion.assertion.fail_threshold.value,
                ),
                metric_definition,
            )
        else:
            # evaluator returns a boolean value 1 if PASS, 0 if FAIL
            assertion_sql = self.metric_evaluator.operator_sql(
                assertion.assertion.operator, metric_definition
            )

        dmf_name = get_dmf_name(assertion)
        dmf_schema_name = self.extras[DMF_SCHEMA_PROPERTY_KEY]

        args_create_dmf, args_add_dmf = get_dmf_args(assertion)

        entity_name = get_entity_name(assertion.assertion)
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

        return "\n".join([dmf_definition, dmf_association])


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
        return f"{trigger.trigger.interval.seconds/60} MIN"
    else:
        raise ValueError(f"Unsupported trigger type {type(trigger.trigger)}")
