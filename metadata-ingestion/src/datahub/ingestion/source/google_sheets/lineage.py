from typing import Any, Dict, List, Optional

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.google_sheets.constants import (
    A1_RANGE_PATTERN,
    BIGQUERY_DIRECT_PATTERN,
    BIGQUERY_JDBC_PATTERN,
    CELL_COLUMN_PATTERN,
    GOOGLE_SHEETS_URL_PATTERN,
    IMPORTRANGE_FULL_PATTERN,
    IMPORTRANGE_URL_PATTERN,
    PLATFORM_NAME,
    POSTGRES_MYSQL_JDBC_PATTERN,
    REDSHIFT_JDBC_PATTERN,
    SHEET_ID_PATTERN,
    SINGLE_CELL_PATTERN,
    SNOWFLAKE_DIRECT_PATTERN,
    SNOWFLAKE_JDBC_PATTERN,
    SNOWFLAKE_KEYWORDS,
    SQL_TABLE_PATTERN,
    TRANSFORM_OPERATION_IMPORTRANGE,
)
from datahub.ingestion.source.google_sheets.models import (
    DatabaseReference,
    FormulaExtractionResult,
    FormulaLocation,
    parse_spreadsheet,
)
from datahub.ingestion.source.google_sheets.report import GoogleSheetsSourceReport
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.urns.urn_iter import lowercase_dataset_urn


class LineageExtractor:
    def __init__(
        self,
        sheets_service: Any,
        platform_instance: Optional[str],
        env: str,
        config: Any,
        report: GoogleSheetsSourceReport,
    ):
        self.sheets_service = sheets_service
        self.platform_instance = platform_instance
        self.env = env
        self.config = config
        self.report = report

    def get_lineage(
        self,
        sheet_id: str,
        sheet_data: Dict[str, Any],
        sheet_name: Optional[str] = None,
    ) -> Optional[UpstreamLineageClass]:
        if not self.config.extract_lineage_from_formulas:
            return None

        upstream_datasets = []
        fine_grained_lineages = []

        sheets = sheet_data["spreadsheet"].sheets

        if sheet_name:
            sheets = [s for s in sheets if s.properties.title == sheet_name]

        for sheet in sheets:
            sheet_name = sheet.properties.title

            try:
                result = self._get_sheet_formulas(sheet_id, sheet_name)
                self.report.report_formulas_processed(len(result.formulas))

                for formula_idx, formula in enumerate(result.formulas):
                    import_range_matches = list(
                        IMPORTRANGE_URL_PATTERN.finditer(formula)
                    )

                    for match in import_range_matches:
                        source_sheet_reference = match.group(1)
                        source_sheet_id = self._extract_sheet_id_from_reference(
                            source_sheet_reference
                        )

                        if source_sheet_id:
                            upstream_datasets.append(
                                UpstreamClass(
                                    dataset=builder.make_dataset_urn_with_platform_instance(
                                        PLATFORM_NAME,
                                        source_sheet_id,
                                        self.platform_instance,
                                        self.env,
                                    ),
                                    type=DatasetLineageTypeClass.TRANSFORMED,
                                )
                            )
                            self.report.report_lineage_edge()

                            if (
                                self.config.extract_column_level_lineage
                                and formula_idx < len(result.locations)
                            ):
                                fine_grained_lineage = (
                                    self._extract_importrange_fine_grained_lineage(
                                        formula,
                                        sheet_id,
                                        source_sheet_id,
                                        sheet_name,
                                        result.locations[formula_idx],
                                    )
                                )
                                if fine_grained_lineage:
                                    fine_grained_lineages.append(fine_grained_lineage)
                                    self.report.report_fine_grained_edge()

                    if self.config.enable_cross_platform_lineage:
                        db_refs = self._extract_database_references(formula)
                        for db_ref in db_refs:
                            platform_instance = None
                            if self.config.database_hostname_to_platform_instance_map:
                                database_name = db_ref.table_identifier.split(".")[0]
                                platform_instance = self.config.database_hostname_to_platform_instance_map.get(
                                    database_name
                                )

                            dataset_urn = (
                                builder.make_dataset_urn_with_platform_instance(
                                    db_ref.platform,
                                    db_ref.table_identifier,
                                    platform_instance,
                                    self.env,
                                )
                            )

                            if self.config.convert_lineage_urns_to_lowercase:
                                dataset_urn = lowercase_dataset_urn(dataset_urn)

                            upstream_datasets.append(
                                UpstreamClass(
                                    dataset=dataset_urn,
                                    type=DatasetLineageTypeClass.TRANSFORMED,
                                )
                            )
                            self.report.report_lineage_edge()
            except Exception:
                continue

        lineage = None
        if upstream_datasets or fine_grained_lineages:
            lineage = UpstreamLineageClass(
                upstreams=upstream_datasets,
                fineGrainedLineages=fine_grained_lineages
                if fine_grained_lineages
                else None,
            )

        return lineage

    def _extract_sheet_id_from_reference(self, reference: str) -> Optional[str]:
        url_match = GOOGLE_SHEETS_URL_PATTERN.search(reference)
        if url_match:
            return url_match.group(1)

        if SHEET_ID_PATTERN.match(reference):
            return reference

        return None

    def _extract_database_references(self, formula: str) -> List[DatabaseReference]:
        """Extract database table references from formulas.

        Returns list of DatabaseReference models.
        """
        db_refs: List[DatabaseReference] = []

        bq_matches = BIGQUERY_DIRECT_PATTERN.finditer(formula)
        for match in bq_matches:
            # Groups: 1=project_id, 2=dataset_id, 3=table_id
            project_id = match.group(1)
            dataset_id = match.group(2)
            table_id = match.group(3)
            db_refs.append(
                DatabaseReference.create_bigquery(project_id, dataset_id, table_id)
            )

        jdbc_bq_matches = BIGQUERY_JDBC_PATTERN.finditer(formula)
        for match in jdbc_bq_matches:
            # Groups: 1=project_id, 2=dataset_id, 3=table_id
            project_id = match.group(1)
            dataset_id = match.group(2)
            table_id = match.group(3)
            db_refs.append(
                DatabaseReference.create_bigquery(project_id, dataset_id, table_id)
            )

        if any(keyword in formula.lower() for keyword in SNOWFLAKE_KEYWORDS):
            sf_matches = SNOWFLAKE_DIRECT_PATTERN.finditer(formula)
            for match in sf_matches:
                database = match.group(1)
                schema = match.group(2)
                table = match.group(3)
                db_refs.append(
                    DatabaseReference.create_snowflake(database, schema, table)
                )

        jdbc_sf_matches = SNOWFLAKE_JDBC_PATTERN.finditer(formula)
        for match in jdbc_sf_matches:
            database = match.group(2)
            schema = match.group(3)
            db_refs.append(DatabaseReference.create_snowflake(database, schema))

        jdbc_pg_matches = POSTGRES_MYSQL_JDBC_PATTERN.finditer(formula)
        for match in jdbc_pg_matches:
            platform = "postgres" if match.group(1).lower() == "postgresql" else "mysql"
            database = match.group(2)
            table_match = SQL_TABLE_PATTERN.search(formula)
            if table_match:
                table = table_match.group(1)
                if platform == "mysql":
                    db_refs.append(DatabaseReference.create_mysql(database, table))
                else:
                    if "." in table:
                        parts = table.split(".", 1)
                        schema, table_name = parts[0], parts[1]
                    else:
                        schema, table_name = "public", table

                    db_refs.append(
                        DatabaseReference.create_postgres(database, schema, table_name)
                    )

        jdbc_rs_matches = REDSHIFT_JDBC_PATTERN.finditer(formula)
        for match in jdbc_rs_matches:
            database = match.group(1)
            table_match = SQL_TABLE_PATTERN.search(formula)
            if table_match:
                table_ref = table_match.group(1)

                if "." in table_ref:
                    parts = table_ref.split(".", 1)
                    schema, table = parts[0], parts[1]
                else:
                    schema, table = "public", table_ref

                db_refs.append(
                    DatabaseReference.create_redshift(database, schema, table)
                )

        return db_refs

    def _get_sheet_formulas(
        self, sheet_id: str, sheet_name: str
    ) -> FormulaExtractionResult:
        formulas = []
        formula_locations: List[FormulaLocation] = []

        result = (
            self.sheets_service.spreadsheets()
            .get(spreadsheetId=sheet_id, ranges=[sheet_name], includeGridData=True)
            .execute()
        )
        spreadsheet = parse_spreadsheet(result)

        if spreadsheet.sheets:
            sheet = spreadsheet.sheets[0]
            if sheet.data:
                grid_data = sheet.data[0]

                headers = []
                if grid_data.rowData and len(grid_data.rowData) > 0:
                    first_row = grid_data.rowData[0]
                    for cell in first_row.values:
                        if cell.formattedValue:
                            headers.append(cell.formattedValue)

                for row_idx, row in enumerate(grid_data.rowData):
                    for col_idx, cell in enumerate(row.values):
                        if cell.userEnteredValue and cell.userEnteredValue.formulaValue:
                            formula = cell.userEnteredValue.formulaValue
                            formulas.append(formula)

                            location = FormulaLocation(
                                row=row_idx,
                                column=col_idx,
                                header=headers[col_idx]
                                if col_idx < len(headers)
                                else f"Column{col_idx}",
                            )
                            formula_locations.append(location)

        return FormulaExtractionResult(formulas=formulas, locations=formula_locations)

    def _extract_importrange_fine_grained_lineage(
        self,
        formula: str,
        sheet_id: str,
        source_sheet_id: str,
        sheet_name: str,
        formula_location: FormulaLocation,
    ) -> Optional[FineGrainedLineageClass]:
        range_match = IMPORTRANGE_FULL_PATTERN.search(formula)

        if not range_match:
            return None

        source_sheet_name = range_match.group(1)
        source_range = range_match.group(2)

        columns = self._parse_range_columns(source_range)

        if not columns:
            return None

        downstream_column = formula_location.header

        upstreams = [
            builder.make_schema_field_urn(
                parent_urn=builder.make_dataset_urn_with_platform_instance(
                    PLATFORM_NAME,
                    source_sheet_id,
                    self.platform_instance,
                    self.env,
                ),
                field_path=f"{source_sheet_name}.{column}",
            )
            for column in columns
        ]

        if not upstreams:
            return None

        return FineGrainedLineageClass(
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            downstreams=[
                builder.make_schema_field_urn(
                    parent_urn=builder.make_dataset_urn_with_platform_instance(
                        PLATFORM_NAME,
                        sheet_id,
                        self.platform_instance,
                        self.env,
                    ),
                    field_path=f"{sheet_name}.{downstream_column}",
                )
            ],
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=upstreams,
            transformOperation=TRANSFORM_OPERATION_IMPORTRANGE,
        )

    def _parse_range_columns(self, range_str: str) -> List[str]:
        """Parse column names from a range string like A1:B10."""
        columns = []

        simple_range_match = A1_RANGE_PATTERN.match(range_str)
        if simple_range_match:
            start_col = simple_range_match.group(1)
            end_col = simple_range_match.group(3)

            start_idx = self._column_letter_to_index(start_col)
            end_idx = self._column_letter_to_index(end_col)

            for idx in range(start_idx, end_idx + 1):
                columns.append(self._index_to_column_letter(idx))

        elif SINGLE_CELL_PATTERN.match(range_str):
            col_match = CELL_COLUMN_PATTERN.match(range_str)
            if col_match:
                columns.append(col_match.group(1))

        return columns

    def _column_letter_to_index(self, column: str) -> int:
        """Convert column letter (A, B, AA, etc.) to index (0, 1, 26, etc.)."""
        index = 0
        for char in column:
            index = index * 26 + (ord(char.upper()) - ord("A") + 1)
        return index - 1

    def _index_to_column_letter(self, index: int) -> str:
        if index < 0:
            return ""

        column = ""
        index += 1  # 1-based for Excel-style columns

        while index > 0:
            remainder = (index - 1) % 26
            column = chr(ord("A") + remainder) + column
            index = (index - 1) // 26

        return column
