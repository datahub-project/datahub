import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from datahub.ingestion.source.ms_fabric.tmdl.core.lineage import LineageAnalyzer
from datahub.ingestion.source.ms_fabric.tmdl.exceptions import ValidationError
from datahub.ingestion.source.ms_fabric.tmdl.models.base import (
    DataType,
    Expression,
    TMDLMetadata,
)
from datahub.ingestion.source.ms_fabric.tmdl.models.table import (
    Column,
    Measure,
    Partition,
    Table,
    TMDLModel,
)
from datahub.ingestion.source.ms_fabric.tmdl.parsers.dax_parser import DAXParser
from datahub.ingestion.source.ms_fabric.tmdl.parsers.m_parser import MParser

logger = logging.getLogger(__name__)


class TMDLParser:
    """Parser for TMDL (Tabular Model Definition Language)."""

    def __init__(self) -> None:
        """Initialize parser with required components."""
        self.dax_parser = DAXParser()
        self.m_parser = MParser()
        self.lineage_analyzer = LineageAnalyzer()
        self._model: Optional[TMDLModel] = None
        self._db_query: Optional[str] = None
        self._tables: Dict[str, Table] = {}

    def parse_raw(self, tmdl_content: str) -> TMDLModel:
        """Parse raw TMDL content."""
        try:
            content_lines = tmdl_content.replace("\r\n", "\n").strip().split("\n")
            tables: List[Table] = []
            current_table: Optional[str] = None
            table_content: List[str] = []
            model_culture = "en-US"
            model_annotations: Dict[str, str] = {}
            model_name = "Model"

            self._db_query = self._extract_database_query(content_lines)

            for line in content_lines:
                stripped = line.strip()
                if stripped.startswith("table "):
                    if current_table and table_content:
                        table = self._parse_table_content(current_table, table_content)
                        if table:
                            tables.append(table)
                            self._tables[table.name] = table
                    current_table = stripped[6:].strip().strip("'")
                    table_content = []
                elif stripped.startswith("model "):
                    model_name = stripped.split(" ", 1)[1].strip()
                elif stripped.startswith("culture:"):
                    model_culture = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("annotation ") and "=" in stripped:
                    key, value = stripped.replace("annotation ", "", 1).split("=", 1)
                    model_annotations[key.strip()] = value.strip().strip('"')
                elif current_table:
                    table_content.append(line)

            if current_table and table_content:
                table = self._parse_table_content(current_table, table_content)
                if table:
                    tables.append(table)
                    self._tables[table.name] = table

            metadata = TMDLMetadata(
                version="1.0",
                created_at=datetime.utcnow(),
                modified_at=datetime.utcnow(),
                compatibility_level=1600,
            )

            self._build_lineage_map()

            model = TMDLModel(
                name=model_name,
                metadata=metadata,
                database_name=tables[0].name if tables else "DefaultDB",
                culture=model_culture,
                tables=tables,
                annotations=model_annotations,
            )

            self._model = model
            return model

        except Exception as e:
            raise ValidationError(f"Failed to parse TMDL content: {str(e)}")

    def _parse_table_content(
        self, table_name: str, content: List[str]
    ) -> Optional[Table]:
        """Parse table content into a Table object.

        Args:
            table_name: Name of the table being parsed
            content: List of strings containing table content

        Returns:
            Table object if parsing successful, None otherwise
        """
        try:
            table_state = self._initialize_table_state()
            current_section = self._initialize_section_state()

            for line in content:
                stripped = line.strip()
                if not stripped:
                    continue

                # Process any complete sections before starting new ones
                if self._is_new_section_marker(stripped):
                    self._process_complete_section(current_section, table_state)
                    current_section = self._start_new_section(stripped)
                else:
                    self._process_line(stripped, table_state, current_section)

            # Process the final section
            self._process_complete_section(current_section, table_state)

            # Debug logging
            self._log_table_debug_info(table_name, table_state)

            return self._create_table_object(table_name, table_state)

        except Exception as e:
            logger.error(f"Error parsing table {table_name}: {str(e)}")
            return None

    def _initialize_table_state(self) -> Dict:
        """Initialize the state dictionary for table parsing."""
        return {
            "columns": [],
            "measures": [],
            "partitions": [],
            "lineage_tag": None,
            "source_lineage_tag": None,
            "annotations": {},
            "is_hidden": False,
            "current_content": [],
        }

    def _initialize_section_state(self) -> Dict:
        """Initialize the state for tracking current section."""
        return {"type": None, "content": []}

    def _is_new_section_marker(self, line: str) -> bool:
        """Check if the line marks the start of a new section."""
        section_markers = ["measure ", "column ", "partition "]
        return any(line.startswith(marker) for marker in section_markers)

    def _process_line(
        self, line: str, table_state: Dict, current_section: Dict
    ) -> None:
        """Process a single line of content."""
        if self._is_table_property(line):
            self._process_table_property(line, table_state)
        elif current_section["type"]:
            current_section["content"].append(line)
        return None

    def _is_table_property(self, line: str) -> bool:
        """Check if the line contains a table-level property."""
        property_markers = [
            "lineageTag:",
            "sourceLineageTag:",
            "changedProperty",
            "annotation ",
        ]
        return any(line.startswith(marker) for marker in property_markers)

    def _process_table_property(self, line: str, table_state: Dict) -> None:
        """Process a table-level property line."""
        if line.startswith("lineageTag:"):
            table_state["lineage_tag"] = line.split(":", 1)[1].strip()
        elif line.startswith("sourceLineageTag:"):
            table_state["source_lineage_tag"] = line.split(":", 1)[1].strip()
        elif line.startswith("changedProperty"):
            if "IsHidden" in line:
                table_state["is_hidden"] = True
        elif line.startswith("annotation ") and "=" in line:
            key, value = line.replace("annotation ", "", 1).split("=", 1)
            table_state["annotations"][key.strip()] = value.strip().strip('"')

    def _start_new_section(self, line: str) -> Dict:
        """Start a new section based on the line content."""
        section_type = None
        content = []

        if line.startswith("measure "):
            section_type = "measure"
        elif line.startswith("column "):
            section_type = "column"
        elif line.startswith("partition "):
            section_type = "partition"

        if section_type:
            content = [line[len(section_type) + 1 :]]

        return {"type": section_type, "content": content}

    def _process_complete_section(self, section: Dict, table_state: Dict) -> None:
        """Process a complete section and update table state."""
        if not section["type"] or not section["content"]:
            return

        if section["type"] == "column":
            col = self._parse_column(
                table_state.get("table_name", ""), section["content"]
            )
            if col:
                table_state["columns"].append(col)
        elif section["type"] == "measure":
            measure = self._parse_measure(section["content"])
            if measure:
                table_state["measures"].append(measure)
        elif section["type"] == "partition":
            partition = self._parse_partition(section["content"])
            if partition:
                table_state["partitions"].append(partition)

    def _log_table_debug_info(self, table_name: str, table_state: Dict) -> None:
        """Log debug information about the parsed table."""
        logger.debug(
            f"Found {len(table_state['measures'])} measures in table {table_name}"
        )
        for measure in table_state["measures"]:
            logger.debug(f"Measure: {measure.name}")

    def _create_table_object(self, table_name: str, table_state: Dict) -> Table:
        """Create a Table object from the parsed state."""
        return Table(
            name=table_name,
            columns=table_state["columns"],
            measures=table_state["measures"],
            partitions=table_state["partitions"],
            lineage_tag=table_state["lineage_tag"],
            source_lineage_tag=table_state["source_lineage_tag"],
            annotations=table_state["annotations"],
            is_hidden=table_state["is_hidden"],
            description=None,
        )

    def _parse_column(
        self, table_name: str, content_lines: List[str]
    ) -> Optional[Column]:
        """Parse column definition."""
        try:
            name = content_lines[0].strip()
            data_type_str = "string"  # Default type
            source_provider_type = None
            lineage_tag = None
            source_lineage_tag = None
            source_column = None
            is_hidden = False
            annotations = {}
            formula = None
            format_string = None
            summarize_by = "none"

            for line in content_lines[1:]:
                stripped = line.strip()
                if stripped.startswith("dataType:"):
                    data_type_str = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("sourceProviderType:"):
                    source_provider_type = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("lineageTag:"):
                    lineage_tag = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("sourceLineageTag:"):
                    source_lineage_tag = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("sourceColumn:"):
                    source_column = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("summarizeBy:"):
                    summarize_by = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("formatString:"):
                    format_string = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("changedProperty"):
                    if "IsHidden" in stripped:
                        is_hidden = True
                elif stripped.startswith("annotation"):
                    if "=" in stripped:
                        key, value = stripped.replace("annotation", "", 1).split("=", 1)
                        annotations[key.strip()] = value.strip().strip('"')
                elif "=" in stripped and not stripped.startswith("expression"):
                    # Capture DAX expressions for calculated columns
                    formula = stripped.split("=", 1)[1].strip()

            data_type = DataType(
                name=data_type_str,
                length=None,
                precision=None,
                scale=None,
            )

            # If we have a formula, this is a calculated column
            expression = None
            if formula:
                expression = Expression(
                    expression_type="DAX",
                    expression=formula,
                )

            return Column(
                name=name,
                data_type=data_type,
                source_column=source_column,
                is_hidden=is_hidden,
                expression=expression,
                annotations=annotations,
                source_lineage_tag=source_lineage_tag,
                source_provider_type=source_provider_type,
                format_string=format_string,
                summarize_by=summarize_by,
                table_name=table_name,
                lineage_tag=lineage_tag,
            )

        except Exception as e:
            logger.error(f"Error parsing column: {str(e)}")
            return None

    def _parse_measure(self, content_lines: List[str]) -> Optional[Measure]:
        """Parse measure definition."""
        try:
            # First line contains measure name and start of formula
            first_line = content_lines[0].strip()
            name_parts = first_line.split("=", 1)
            name = name_parts[0].strip()  # Get measure name

            formula = None
            format_string = None
            annotations = {}
            is_hidden = False
            lineage_tag = None
            expression_lines = []
            in_expression = False

            # If formula starts on first line
            if len(name_parts) > 1:
                expression_lines.append(name_parts[1].strip())
                if name_parts[1].strip().startswith("```"):
                    in_expression = True

            # Process remaining lines
            for line in content_lines[1:]:
                stripped = line.strip()
                if not stripped:
                    continue

                if stripped.startswith("```"):
                    if in_expression:
                        in_expression = False  # End of expression block
                    else:
                        in_expression = True  # Start of expression block
                    continue

                if in_expression:
                    expression_lines.append(stripped)
                elif stripped.startswith("formatString:"):
                    format_string = stripped.split(":", 1)[1].strip().strip('"')
                elif stripped.startswith("lineageTag:"):
                    lineage_tag = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("changedProperty"):
                    if "IsHidden" in stripped:
                        is_hidden = True
                elif stripped.startswith("annotation"):
                    if "=" in stripped:
                        key, value = stripped.replace("annotation", "", 1).split("=", 1)
                        annotations[key.strip()] = value.strip().strip('"')

            # Compile the final formula from expression lines
            formula = "\n".join(
                line for line in expression_lines if not line.startswith("```")
            )
            formula = formula.strip()

            if formula:
                expression = Expression(
                    expression_type="DAX",
                    expression=formula,
                )

                return Measure(
                    name=name,
                    expression=expression,
                    format_string=format_string,
                    is_hidden=is_hidden,
                    annotations=annotations,
                    lineage_tag=lineage_tag,
                )

        except Exception as e:
            logger.error(f"Error parsing measure: {str(e)}")
            return None

    def _parse_partition(self, content_lines: List[str]) -> Optional[Partition]:
        """Parse partition definition."""
        try:
            # Extract partition name
            first_line = content_lines[0].strip()
            name_parts = first_line.split("=", 1)
            name = name_parts[0].strip().strip("'")  # Remove quotes and whitespace

            mode = "import"
            entity_name = None
            schema_name = None
            expression_source = None
            source_expr = None

            # Parse the partition content
            for line in content_lines[1:]:
                stripped = line.strip()
                if not stripped:
                    continue

                if stripped.startswith("mode:"):
                    mode = stripped.split(":", 1)[1].strip()
                elif stripped == "source":
                    continue
                elif stripped.startswith("entityName:"):
                    entity_name = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("schemaName:"):
                    schema_name = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("expressionSource:"):
                    expression_source = stripped.split(":", 1)[1].strip()

            # For DirectLake mode, use the DatabaseQuery information
            if (
                mode == "directLake"
                and self._db_query
                and expression_source == "DatabaseQuery"
            ):
                # We need to keep the raw Sql.Database call in the expression for lineage detection
                server = self._extract_server_from_query(self._db_query)
                database = self._extract_database_from_query(self._db_query)
                source_expr = (
                    f"let\n"
                    f'    Source = Sql.Database("{server}", "{database}"),\n'
                    f"    Entity = Source{{\n"
                    f'        [Schema]="{schema_name}",\n'
                    f'        [Item]="{entity_name}"\n'
                    f"    }}[Data]\n"
                    f"in\n"
                    f"    Entity"
                )
            else:
                source_expr = "let Source = null in Source"

            source = Expression(expression_type="M", expression=source_expr)

            return Partition(
                name=name,
                mode=mode,
                source=source,
            )

        except Exception as e:
            logger.error(f"Error parsing partition: {str(e)}")
            logger.error(f"Content lines: {content_lines}")
            return None

    def _extract_server_from_query(self, query: str) -> str:
        """Extract server from DatabaseQuery expression."""
        try:
            server_pattern = r'Sql\.Database\("([^"]+)"'
            match = re.search(server_pattern, query)
            if match:
                return match.group(1)
        except Exception as e:
            logger.warning(f"Failed to extract server from query: {str(e)}")
        return ""

    def _extract_database_from_query(self, query: str) -> str:
        """Extract database GUID from DatabaseQuery expression."""
        try:
            guid_pattern = r'Sql\.Database\([^,]+,\s*"([^"]+)"'
            match = re.search(guid_pattern, query)
            if match:
                return match.group(1)
        except Exception as e:
            logger.warning(f"Failed to extract database GUID from query: {str(e)}")
        return ""

    def _extract_database_query(self, content_lines: List[str]) -> Optional[str]:
        """Extract DatabaseQuery expression from content."""
        query_parts = []
        in_query = False
        found_let = False

        for line in content_lines:
            stripped = line.strip()

            if stripped.startswith("expression DatabaseQuery ="):
                in_query = True
                continue

            if in_query:
                if stripped.startswith("let"):
                    found_let = True

                if found_let:
                    if (
                        stripped.startswith("lineageTag:")
                        or stripped.startswith("annotation")
                        or stripped == "model Model"
                    ):
                        break

                    query_parts.append(line)

        if query_parts:
            query = "\n".join(query_parts)
            query = query.replace("let\n    database =", "let\ndatabase =")
            query = query.strip()
            return query

        return None

    def _build_lineage_map(self) -> None:
        """Build lineage relationships between tables based on matching lineage tags."""
        column_map: Dict[
            str, List[Tuple[str, str]]
        ] = {}  # Map of lineage_tag to (table_name, column_name)

        # First pass: build map of lineage tags to columns
        for table in self._tables.values():
            for column in table.columns:
                if column.lineage_tag:
                    if column.lineage_tag not in column_map:
                        column_map[column.lineage_tag] = []
                    column_map[column.lineage_tag].append((table.name, column.name))

        # Second pass: set up lineage dependencies
        for table in self._tables.values():
            for column in table.columns:
                if column.lineage_tag and column.lineage_tag in column_map:
                    related_columns = column_map[column.lineage_tag]
                    column.lineage_dependencies = [
                        f"{table_name}.{col_name}"
                        for table_name, col_name in related_columns
                        if table_name != table.name or col_name != column.name
                    ]
