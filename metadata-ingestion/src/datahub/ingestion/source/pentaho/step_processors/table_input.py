"""Table Input step processor for Pentaho transformations."""

from typing import Optional
from xml.etree.ElementTree import (
    Element,  # nosec B405 - only for type hints; parsing goes through defusedxml
)

from datahub.ingestion.source.pentaho.context import ProcessingContext
from datahub.ingestion.source.pentaho.step_processors.base import StepProcessor
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result


class TableInputProcessor(StepProcessor):
    """Processor for TableInput steps."""

    def can_process(self, step_type: str) -> bool:
        return step_type == "TableInput"

    def process(
        self,
        step: Element,
        context: ProcessingContext,
        root: Optional[Element] = None,
    ) -> None:
        conn_name = step.findtext("connection")
        table_name = step.findtext("table")
        sql = step.findtext("sql")

        # Get connection type for platform detection
        conn_type = (
            self.source._get_connection_type(root, conn_name or "")
            if root is not None
            else None
        )
        platform = self.source._get_platform_from_connection(
            conn_name or "", "TableInput", conn_type
        )

        # If we have SQL, use the SQL parser
        if sql and sql.strip():
            # create_lineage_sql_parsed_result turns parse failures into a
            # result carrying table_error, so the ordinary error path is a field
            # check rather than an exception. Schema-resolver construction and
            # the close() in its finally block sit outside that guard, so a
            # raise is still possible; both routes report and fall through to
            # the explicit table name below.
            parse_error: Optional[BaseException] = None
            try:
                parsed_result = create_lineage_sql_parsed_result(
                    query=sql,
                    default_db=None,
                    default_schema=None,
                    platform=platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                    # Only in_tables is consumed; column lineage would be
                    # computed and discarded.
                    generate_column_lineage=False,
                )
            except Exception as e:
                parse_error = e
            else:
                parse_error = parsed_result.debug_info.table_error
                if parse_error is None:
                    for table_urn in parsed_result.in_tables:
                        context.add_input_dataset(table_urn)
                    return

            self.source.report.warning(
                message="Failed to parse TableInput SQL; falling back to the step's table name",
                context=f"{context.file_path}: {parse_error}",
                exc=parse_error,
            )

        # Reached when there is no SQL, or when SQL parsing failed above.
        if table_name:
            dataset_urn = self.source._create_dataset_urn(platform, table_name)
            if dataset_urn:
                context.add_input_dataset(dataset_urn)
