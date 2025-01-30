import logging
import re
from typing import Dict, List, Set, Union

import requests
from azure.identity import (
    ClientSecretCredential,
)

from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.ms_fabric.types import (
    CalculatedColumnInfo,
    TmdlColumn,
    TmdlPartition,
    TmdlTable,
)
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

logger = logging.getLogger(__name__)


def set_session(
    session: requests.Session,
    azure_config: AzureConnectionConfig,
) -> requests.Session:
    """
    Sets up a session with Azure authentication.

    Args:
        session: The requests session to configure
        azure_config: Azure configuration containing credentials

    Returns:
        Configured requests session

    Raises:
        ValueError: If azure_config is invalid
    """
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
        # For other credential types, ensure they support get_token
        if hasattr(credentials, "get_token"):
            token = credentials.get_token("https://api.fabric.microsoft.com/.default")
            session.headers.update(
                {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            )
        else:
            raise ValueError("Credentials must support get_token method")
    return session


def _map_powerbi_type_to_datahub_type(
    data_type: str,
) -> Union[
    StringTypeClass,
    NumberTypeClass,
    TimeTypeClass,
    DateTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
]:
    """Map Power BI data types to DataHub types"""
    type_mapping = {
        "string": StringTypeClass(),
        "double": NumberTypeClass(),
        "decimal": NumberTypeClass(),
        "integer": NumberTypeClass(),
        "int64": NumberTypeClass(),
        "datetime": TimeTypeClass(),
        "date": DateTypeClass(),
        "time": TimeTypeClass(),
        "boolean": BooleanTypeClass(),
        "binary": BytesTypeClass(),
    }
    return type_mapping.get(data_type.lower(), "string")


def _parse_m_query(query: str) -> List[str]:
    """Parse Power BI's M query to extract source tables"""
    if not query:
        return []

    source_tables = []

    try:
        # Safer regex patterns with escaped characters and no unbalanced parentheses
        patterns = [
            # Basic Source pattern
            r'Source\{[^}]*\[Name="([^"]+)"[^}]*\}',
            # Schema pattern
            r'(\w+)_Schema\{[^}]*\[Name="([^"]+)"[^}]*\}',
            # Database pattern with optional schema/table
            r'(?:[\w.]+)\.Database(?:\.|\\)\w+\("([^"]+)"\)',
            # Direct table references
            r"\[([^\]]+)\]",
            # Named references
            r'#"([^"]+)"',
        ]

        for pattern in patterns:
            try:
                matches = re.finditer(pattern, query, re.IGNORECASE)
                for match in matches:
                    # Get the last group if multiple groups exist, otherwise get the first group
                    table_name = match.group(match.lastindex or 1)
                    if table_name and isinstance(table_name, str):
                        # Clean up the table name
                        cleaned_name = table_name.strip('[]"')
                        if cleaned_name:
                            source_tables.append(cleaned_name)
            except (re.error, IndexError, AttributeError) as e:
                print(f"Error parsing with pattern {pattern}: {str(e)}")
                continue

    except Exception as e:
        logger.error(f"Error parsing M query: {str(e)}")

    # Remove duplicates while preserving order
    return list(dict.fromkeys(source_tables))


def clean_m_query(query: str) -> str:
    """Clean and normalize M-query content for safer parsing"""
    if not query:
        return ""

    try:
        # Replace escaped quotes with temporary markers
        query = query.replace('\\"', "%%QUOTE%%")

        # Normalize newlines
        query = query.replace("\r\n", "\n").replace("\r", "\n")

        # Remove comments
        query = re.sub(r"//.*$", "", query, flags=re.MULTILINE)

        # Clean up whitespace
        query = " ".join(query.split())

        # Restore escaped quotes
        query = query.replace("%%QUOTE%%", '\\"')

        return query

    except Exception as e:
        print(f"Error cleaning M query: {str(e)}")
        return query  # Return original if cleaning fails


def extract_table_reference(reference: str) -> str:
    """Safely extract table name from various reference formats"""
    if not reference:
        return ""

    try:
        # Remove common wrappers
        for wrapper in ["[", "]", '"', "'", "`"]:
            reference = reference.strip(wrapper)

        # Split on common separators and take the last part
        for separator in [".", "\\", "/"]:
            if separator in reference:
                reference = reference.split(separator)[-1]

        return reference.strip()

    except Exception as e:
        print(f"Error extracting table reference: {str(e)}")
        return reference  # Return original if extraction fails


class TmdlParser:
    def __init__(self, tmdl_content: str):
        self.content = tmdl_content
        self.lines = [line for line in tmdl_content.split("\n") if line.strip()]
        self.current_position = 0

    def parse_table(self) -> TmdlTable:
        """Parse TMDL content into a TmdlTable object"""
        table_data = self._find_table_section()
        if not table_data:
            raise ValueError("Invalid TMDL format: No table definition found")

        # Parse basic table info
        columns = self._parse_columns(table_data["content"])
        partitions = self._parse_partitions(table_data["content"])
        annotations = self._parse_annotations(table_data["content"])

        # Get calculated columns info
        calculated_columns = self._parse_calculated_columns(table_data["content"])

        # Add calculation info to column annotations
        for calc in calculated_columns:
            for col in columns:
                if col.name == calc.name:
                    col.annotations["formula"] = calc.formula
                    col.annotations["isCalculated"] = "true"
                    col.annotations["referencedColumns"] = ",".join(
                        calc.referenced_columns
                    )
                    col.annotations["description"] = (
                        f"{col.annotations.get('description', '')} Calculation: {calc.formula}"
                    )

        return TmdlTable(
            name=table_data["name"],
            lineage_tag=table_data.get("lineageTag", ""),
            columns=columns,
            partitions=partitions,
            annotations=annotations,
        )

    def _find_table_section(self) -> Dict:
        """Extract the complete table section and its metadata"""
        table_data = {"content": [], "name": "", "lineageTag": ""}
        in_table = False

        for line in self.lines:
            stripped_line = line.strip()
            if stripped_line.startswith("table "):
                in_table = True
                table_data["name"] = self._extract_table_name(stripped_line[6:])
                continue

            if in_table:
                if stripped_line.startswith("lineageTag:"):
                    table_data["lineageTag"] = stripped_line.split(":", 1)[1].strip()
                table_data["content"].append(line)

                # Check for end of table section
                if stripped_line == "" and any(
                    next_line.strip().startswith("table ")
                    for next_line in self.lines[self.lines.index(line) + 1 :]
                    if next_line.strip()
                ):
                    break

        return table_data

    def _parse_columns(self, table_lines: List[str]) -> List[TmdlColumn]:
        columns = []
        current_column = None

        for line in table_lines:
            stripped_line = line.strip()

            if stripped_line.startswith("column "):
                if current_column:
                    columns.append(current_column)

                column_name = stripped_line[7:].strip()
                current_column = TmdlColumn(
                    name=column_name,
                    data_type="",
                    source_provider_type=None,
                    lineage_tag="",
                    summarize_by="",
                    source_column=None,
                    annotations={},
                )

            elif current_column and line.startswith("\t\t"):
                if ":" in line:
                    key, value = [x.strip() for x in line.split(":", 1)]
                    if key == "dataType":
                        current_column.data_type = value
                    elif key == "sourceProviderType":
                        current_column.source_provider_type = value
                    elif key == "lineageTag":
                        current_column.lineage_tag = value
                    elif key == "summarizeBy":
                        current_column.summarize_by = value
                    elif key == "sourceColumn":
                        current_column.source_column = value
                    else:
                        current_column.annotations[key] = value
                else:
                    # Handle flag attributes
                    key = line.strip()
                    current_column.annotations[key] = "true"

        if current_column:
            columns.append(current_column)

        return columns

    def _parse_partitions(self, table_lines: List[str]) -> List[TmdlPartition]:
        """Parse partition definitions with robust M-query handling"""
        partitions = []
        current_partition = None
        collecting_source = False
        source_lines = []

        for line in table_lines:
            stripped_line = line.strip()

            if stripped_line.startswith("partition "):
                if current_partition:
                    if source_lines:
                        # Clean and join the source query
                        source_query = clean_m_query("\n".join(source_lines))
                        current_partition.source_query = source_query
                    partitions.append(current_partition)

                partition_name = stripped_line[10:].split("=")[0].strip()
                current_partition = TmdlPartition(
                    name=partition_name, mode="", source_query=None
                )
                collecting_source = False
                source_lines = []

            elif current_partition:
                if "mode:" in stripped_line:
                    current_partition.mode = stripped_line.split(":", 1)[1].strip()
                elif "source =" in stripped_line:
                    collecting_source = True
                elif collecting_source and stripped_line:
                    source_lines.append(stripped_line)

        # Handle the last partition
        if current_partition:
            if source_lines:
                source_query = clean_m_query("\n".join(source_lines))
                current_partition.source_query = source_query
            partitions.append(current_partition)

        return partitions

    def _extract_source_info(self, line: str) -> Dict:
        """Extract source information with robust error handling"""
        source_info = {}

        try:
            cleaned_line = clean_m_query(line)

            # Look for source patterns
            if "Source{" in cleaned_line:
                match = re.search(r'Source\{[^}]*\[Name="([^"]+)"', cleaned_line)
                if match:
                    table_ref = extract_table_reference(match.group(1))
                    if "." in table_ref:
                        db, table = table_ref.rsplit(".", 1)
                        source_info["source_database"] = db
                        source_info["source_table"] = table
                    else:
                        source_info["source_table"] = table_ref

            elif "Database(" in cleaned_line:
                db_match = re.search(r'Database\("([^"]+)"\)', cleaned_line)
                schema_match = re.search(r'Schema="([^"]+)"', cleaned_line)

                if db_match:
                    source_info["source_database"] = db_match.group(1)
                if schema_match:
                    source_info["source_table"] = schema_match.group(1)

        except Exception as e:
            print(f"Error extracting source info: {str(e)}")

        return source_info

    def _parse_calculated_columns(
        self, table_lines: List[str]
    ) -> List[CalculatedColumnInfo]:
        calculated_columns = []

        for line in table_lines:
            stripped_line = line.strip()
            if "=" in stripped_line and not stripped_line.startswith("partition"):
                try:
                    name, formula = [x.strip() for x in stripped_line.split("=", 1)]
                    referenced_cols = self._extract_referenced_columns(formula)

                    calc_info = CalculatedColumnInfo(
                        name=name,
                        formula=formula,
                        referenced_columns=referenced_cols,
                        description=f"Calculated column: {formula}",
                    )
                    calculated_columns.append(calc_info)

                except Exception as e:
                    print(f"Error parsing calculation: {str(e)}")

        return calculated_columns

    def _extract_referenced_columns(self, formula: str) -> Set[str]:
        """Extract column names referenced in a calculation"""
        referenced = set()
        words = formula.split()
        for word in words:
            clean_word = word.strip("()[]{},.+-*/= ")
            if clean_word and clean_word[0].isalpha():
                referenced.add(clean_word)
        return referenced

    def _parse_annotations(self, table_lines: List[str]) -> Dict[str, str]:
        annotations = {}
        for line in table_lines:
            if "annotation" in line:
                try:
                    key, value = line.split("=", 1)
                    key = key.replace("annotation", "").strip()
                    value = value.strip().strip('"')
                    annotations[key] = value
                except ValueError:
                    # Handle flag-style annotations
                    key = line.replace("annotation", "").strip()
                    annotations[key] = "true"
        return annotations

    def _extract_table_name(self, full_name: str) -> str:
        """Extract clean table name from the full table declaration"""
        name = full_name
        for char in "\"[]'":
            name = name.replace(char, "")
        return name.split(" ")[0]
