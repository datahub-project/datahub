import logging
import re
import urllib.parse
from typing import Dict, List, Optional

import requests
from azure.identity import ClientSecretCredential

from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.ms_fabric.types import (
    TmdlCalculatedColumn,
    TmdlColumn,
    TmdlMeasure,
    TmdlModel,
    TmdlPartition,
    TmdlTable,
)

logger = logging.getLogger(__name__)


def set_session(
    session: requests.Session,
    azure_config: AzureConnectionConfig,
) -> requests.Session:
    """Sets up a session with Azure authentication."""
    if not isinstance(azure_config, AzureConnectionConfig):
        raise ValueError("azure_config must be an AzureConnectionConfig instance")

    credentials = azure_config.get_credentials()
    if isinstance(credentials, ClientSecretCredential):
        token = credentials.get_token("https://api.fabric.microsoft.com/.default")
        session.headers.update(
            {
                "Authorization": f"Bearer {token.token}",
                "Content-Type": "application/json",
            }
        )
    else:
        if hasattr(credentials, "get_token"):
            token = credentials.get_token("https://api.fabric.microsoft.com/.default")
            session.headers.update(
                {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            )
        else:
            raise ValueError("Credentials must support get_token method")
    return session


def clean_tmdl_content(content: str) -> str:
    """Clean TMDL content by handling URL encoding and normalizing line endings"""
    try:
        # Unescape all URL encoded characters
        content = urllib.parse.unquote(content)

        # Normalize line endings
        content = content.replace("\r\n", "\n")

        # Join lines that are part of the same statement
        lines = []
        current_line = []

        for line in content.split("\n"):
            stripped = line.strip()

            if not stripped:
                if current_line:
                    lines.append(" ".join(current_line))
                    current_line = []
                continue

            if stripped.endswith((",", "=", "+", "-", "*", "/", "&&", "||")):
                current_line.append(stripped)
            elif current_line and stripped.startswith(("&&", "||", "+", "-", "*", "/")):
                current_line.append(stripped)
            else:
                if current_line:
                    lines.append(" ".join(current_line))
                current_line = [stripped]

        if current_line:
            lines.append(" ".join(current_line))

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"Error cleaning TMDL content: {str(e)}")
        return content


def _clean_table_name(table_name: str) -> str:
    """Clean table names by removing brackets and extra whitespace"""
    # Remove leading/trailing brackets and whitespace
    cleaned = table_name.strip("[]").strip()

    # Handle schema.table format with brackets
    parts = cleaned.split(".")
    cleaned_parts = [part.strip("[]").strip() for part in parts]

    return ".".join(cleaned_parts)


class TmdlParser:
    def __init__(self, tmdl_content: str):
        self.raw_content = tmdl_content
        self.content = clean_tmdl_content(tmdl_content)
        self.lines = self.content.split("\n")

    def parse(self) -> TmdlModel:
        """Parse the entire TMDL content into a model"""
        model_name = "DefaultModel"
        model_culture = "en-US"
        tables = []
        current_table = None
        table_content = []

        for line in self.lines:
            stripped = line.strip()

            # Model definition
            if stripped.startswith("model "):
                model_name = stripped[6:].strip()

            # Table definition
            elif stripped.startswith("table "):
                # Complete previous table
                if current_table and table_content:
                    table = self._parse_table_content(current_table, table_content)
                    if table:
                        tables.append(table)

                current_table = stripped[6:].strip()
                table_content = []

            # Culture info
            elif stripped.startswith("culture: "):
                model_culture = stripped[9:].strip()

            # Add line to current table content
            elif current_table:
                table_content.append(line)

        # Handle last table
        if current_table and table_content:
            table = self._parse_table_content(current_table, table_content)
            if table:
                tables.append(table)

        return TmdlModel(
            name=model_name, culture=model_culture, tables=tables, annotations={}
        )

    def _parse_table_content(
        self, table_name: str, content: List[str]
    ) -> Optional[TmdlTable]:
        """Parse individual table content"""
        try:
            table_name = urllib.parse.unquote(table_name).split(" ")[0].strip("\"[]'")
            table_content = "\n".join(content)

            # Extract lineage tags
            lineage_tag = ""
            source_lineage_tag = ""

            lineage_match = re.search(r"lineageTag:\s*([^\n]+)", table_content)
            if lineage_match:
                lineage_tag = lineage_match.group(1).strip()

            source_match = re.search(r"sourceLineageTag:\s*([^\n]+)", table_content)
            if source_match:
                source_lineage_tag = source_match.group(1).strip()

            # Parse components
            columns = self._parse_columns(table_content)
            measures = self._parse_measures(table_content)
            partitions = self._parse_partitions(table_content)
            annotations = self._parse_annotations(table_content)

            return TmdlTable(
                name=table_name,
                lineage_tag=lineage_tag,
                source_lineage_tag=source_lineage_tag,
                columns=columns,
                measures=measures,
                partitions=partitions,
                annotations=annotations,
                calculated_columns=[],  # Will be populated during column parsing
            )

        except Exception as e:
            logger.error(f"Error parsing table {table_name}: {str(e)}")
            return None

    def _parse_columns(self, content: str) -> List[TmdlColumn]:
        """Parse column definitions with improved property handling"""
        columns = []
        current_column = None
        current_properties = {}

        for line in content.split("\n"):
            stripped = line.strip()

            # Start of new column
            if stripped.startswith("column "):
                # Complete previous column
                if current_column:
                    self._complete_column(current_column, current_properties)
                    columns.append(current_column)

                # Initialize new column
                col_name = stripped[7:].strip()
                for char in "\"[]'":
                    col_name = col_name.strip(char)

                current_column = TmdlColumn(
                    name=col_name,
                    data_type="",
                    source_provider_type=None,
                    lineage_tag="",
                    summarize_by="",
                    source_column=None,
                    source_lineage_tag=None,
                    is_hidden=False,
                    annotations={},
                )
                current_properties = {}
                continue

            # Collect column properties
            if current_column and stripped:
                if ":" in stripped:
                    key, value = [x.strip() for x in stripped.split(":", 1)]
                    current_properties[key] = value
                elif stripped.startswith("annotation"):
                    if "=" in stripped:
                        key, value = stripped.replace("annotation", "", 1).split("=", 1)
                        current_column.annotations[key.strip()] = value.strip().strip(
                            '"'
                        )
                elif stripped == "isHidden":
                    current_column.is_hidden = True
                elif stripped.startswith("changedProperty"):
                    prop = stripped.replace("changedProperty", "", 1).strip()
                    if "=" in prop:
                        key, value = prop.split("=", 1)
                        current_column.annotations[f"changedProperty_{key.strip()}"] = (
                            value.strip()
                        )
                    else:
                        current_column.annotations["changedProperty"] = prop.strip()

        # Complete last column
        if current_column:
            self._complete_column(current_column, current_properties)
            columns.append(current_column)

        return columns

    def _complete_column(self, column: TmdlColumn, properties: Dict[str, str]):
        """Apply collected properties to column object"""
        for key, value in properties.items():
            if key == "dataType":
                column.data_type = value
            elif key == "sourceProviderType":
                column.source_provider_type = value
            elif key == "lineageTag":
                column.lineage_tag = value.strip("[]").strip()
            elif key == "summarizeBy":
                column.summarize_by = value
            elif key == "sourceColumn":
                column.source_column = value.strip("[]").strip()
            elif key == "sourceLineageTag":
                column.source_lineage_tag = value.strip("[]").strip()
            elif key == "expression":
                # Clean the formula value
                column.annotations["formula"] = value.strip()

                # Extract and clean referenced columns from expression
                refs = set()
                for match in re.finditer(r"\[([^]]+)]", value):
                    # Clean each referenced column name
                    ref_col = match.group(1).strip("[]").strip()
                    refs.add(ref_col)

                if refs:
                    column.annotations["referencedColumns"] = ",".join(sorted(refs))

    def _parse_calculated_columns(self, content: str) -> List[TmdlCalculatedColumn]:
        """Parse calculated column definitions"""
        calculated_columns = []
        current_column = None
        expression_lines = []

        for line in content.split("\n"):
            stripped = line.strip()

            if (
                stripped.startswith("column ")
                and "expression:" in content[content.index(stripped) :]
            ):
                # Complete previous column
                if current_column and expression_lines:
                    column = self._complete_calculated_column(
                        current_column, expression_lines
                    )
                    if column:
                        calculated_columns.append(column)

                # Start new column
                current_column = stripped[7:].split("=")[0].strip()
                expression_lines = []
                continue

            if current_column and stripped.startswith("expression:"):
                expression_lines.append(stripped.replace("expression:", "").strip())
            elif (
                current_column
                and expression_lines
                and not stripped.startswith(
                    ("lineageTag:", "changedProperty", "annotation")
                )
            ):
                expression_lines.append(stripped)

        # Complete last column
        if current_column and expression_lines:
            column = self._complete_calculated_column(current_column, expression_lines)
            if column:
                calculated_columns.append(column)

        return calculated_columns

    def _complete_calculated_column(
        self, name: str, expression_lines: List[str]
    ) -> Optional[TmdlCalculatedColumn]:
        """Complete calculated column parsing"""
        try:
            expression = " ".join(expression_lines).strip()

            # Extract referenced columns
            referenced_cols = set()
            for match in re.finditer(r"\[([^]]+)]", expression):
                referenced_cols.add(match.group(1))

            return TmdlCalculatedColumn(
                name=name,
                formula=expression,
                referenced_columns=referenced_cols,
                description="",
            )
        except Exception as e:
            logger.error(f"Error completing calculated column {name}: {str(e)}")
            return None

    def _parse_measures(self, content: str) -> List[TmdlMeasure]:
        """Parse measure definitions with improved handling of backtick formulas"""
        measures = []
        current_measure = None
        formula_lines = []
        in_backticks = False

        for line in content.split("\n"):
            stripped = line.strip()

            # Start of new measure
            if stripped.startswith("measure "):
                # Complete previous measure
                if current_measure and formula_lines:
                    measure = self._complete_measure(current_measure, formula_lines)
                    if measure:
                        measures.append(measure)

                # Parse measure name
                current_measure = stripped[8:].split("=")[0].strip()
                formula_lines = []
                continue

            # Handle backtick blocks
            if "```" in stripped:
                in_backticks = not in_backticks
                stripped = stripped.replace("```", "").strip()

            # Collect formula lines
            if current_measure:
                if stripped and not stripped.startswith(
                    ("lineageTag:", "changedProperty", "annotation")
                ):
                    formula_lines.append(stripped)
                elif stripped.startswith(
                    ("lineageTag:", "changedProperty", "annotation")
                ):
                    # Complete measure when we hit metadata
                    if formula_lines:
                        measure = self._complete_measure(current_measure, formula_lines)
                        if measure:
                            measures.append(measure)
                        current_measure = None
                        formula_lines = []

        # Handle last measure
        if current_measure and formula_lines:
            measure = self._complete_measure(current_measure, formula_lines)
            if measure:
                measures.append(measure)

        return measures

    def _complete_measure(
        self, name: str, formula_lines: List[str]
    ) -> Optional[TmdlMeasure]:
        """Complete measure parsing with formula and annotations"""
        try:
            formula = " ".join(formula_lines).strip()

            # Extract referenced columns
            referenced_cols = set()
            for match in re.finditer(r"\[([^]]+)]", formula):
                referenced_cols.add(match.group(1))

            # Create annotations dict
            annotations = {"referencedColumns": ",".join(sorted(referenced_cols))}

            return TmdlMeasure(
                name=name, formula=formula, description="", annotations=annotations
            )
        except Exception as e:
            logger.error(f"Error completing measure {name}: {str(e)}")
            return None

    def _parse_partitions(self, content: str) -> List[TmdlPartition]:
        """Parse partition definitions"""
        partitions = []
        partition_content = []
        current_partition = None
        in_partition = False
        bracket_count = 0

        for line in content.split("\n"):
            stripped = line.strip()

            if stripped.startswith("partition "):
                if current_partition and partition_content:
                    partition = self._complete_partition(
                        current_partition, partition_content
                    )
                    if partition:
                        partitions.append(partition)

                current_partition = stripped[10:].split("=")[0].strip()
                partition_content = []
                in_partition = True
                continue

            if in_partition:
                bracket_count += stripped.count("{") - stripped.count("}")
                partition_content.append(line)

                if bracket_count == 0 and not stripped:
                    partition = self._complete_partition(
                        current_partition, partition_content
                    )
                    if partition:
                        partitions.append(partition)
                    current_partition = None
                    partition_content = []
                    in_partition = False

        if current_partition and partition_content:
            partition = self._complete_partition(current_partition, partition_content)
            if partition:
                partitions.append(partition)

        return partitions

    def _complete_partition(
        self, name: str, content: List[str]
    ) -> Optional[TmdlPartition]:
        """Complete partition parsing"""
        try:
            partition_content = "\n".join(content)

            partition = TmdlPartition(name=name)

            # Extract mode
            mode_match = re.search(r"mode:\s*([^\n,]+)", partition_content)
            if mode_match:
                partition.mode = mode_match.group(1).strip()

            # Extract source query
            source_match = re.search(
                r"source\s*=\s*(.*?)(?=\s*(?:,\s*$|$))", partition_content, re.DOTALL
            )
            if source_match:
                partition.source_query = source_match.group(1).strip()

            return partition

        except Exception as e:
            logger.error(f"Error completing partition {name}: {str(e)}")
            return None

    def _parse_annotations(self, content: str) -> Dict[str, str]:
        """Parse annotations"""
        annotations = {}

        for match in re.finditer(r"annotation\s+([^\n]+)", content):
            ann_def = match.group(1)
            if "=" in ann_def:
                key, value = ann_def.split("=", 1)
                annotations[key.strip()] = value.strip().strip('"')
            else:
                annotations[ann_def.strip()] = "true"

        return annotations
