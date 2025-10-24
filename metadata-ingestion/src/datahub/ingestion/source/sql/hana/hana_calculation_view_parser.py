"""
SAP HANA Calculation View Parser.

This module provides functionality to parse SAP HANA calculation view XML definitions
and extract lineage information for DataHub ingestion.
"""

import logging
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Set, Union, cast

logger = logging.getLogger(__name__)


class SAPCalculationViewParser:
    """
    Parser for SAP HANA Calculation Views.

    This class handles parsing of SAP HANA calculation view XML definitions
    to extract lineage information and column mappings. Calculation views are
    virtual views in SAP HANA that can combine data from multiple sources
    including tables, other calculation views, and table functions.
    """

    def __init__(self):
        """Initialize the SAP Calculation View Parser."""
        pass

    def _get_upstream_table_name(self, src_col: Dict[str, Any]) -> str:
        """Get the upstream table name based on source type and source path."""
        source = src_col.get("source")
        source_type = src_col.get("sourceType")
        source_path = src_col.get("sourcePath")

        if source is None:
            return "unknown"

        if source_type == "DATA_BASE_TABLE" and source_path is not None:
            return f"{source_path}.{source}".lower()
        elif source_type == "TABLE_FUNCTION":
            return f"_sys_bic.{source.replace('::', '/')}".lower()
        elif len(source) > 0:
            return f"_sys_bic.{source[1:].replace('/calculationviews/', '/')}".lower()
        else:
            return "unknown"

    @staticmethod
    def _parse_calc_view(view: str, xml: ET.Element) -> Dict[str, Any]:
        """
        Parse calculation view XML definition to extract sources, outputs, and nodes.

        Args:
            view: Name of the calculation view
            xml: Parsed XML ElementTree of the calculation view definition

        Returns:
            Dictionary containing parsed view structure with sources, outputs, and nodes
        """
        sources = {}  # Data sources (tables, views, etc.)
        outputs = {}  # Output columns (attributes and measures)
        nodes = {}  # Calculation nodes (projections, unions, etc.)

        try:
            # Parse data sources - these are the input tables/views
            for child in xml.iter("DataSource"):
                source = {"type": child.attrib["type"]}
                for grandchild in child:
                    if grandchild.tag == "columnObject":
                        # Regular table or view reference
                        source["name"] = grandchild.attrib["columnObjectName"]
                        source["path"] = grandchild.attrib["schemaName"]
                    elif grandchild.tag == "resourceUri":
                        # Calculation view reference
                        if grandchild.text is not None:
                            source["name"] = grandchild.text.split(sep="/")[-1]
                            source["path"] = "/".join(
                                grandchild.text.split(sep="/")[:-1]
                            )
                sources[child.attrib["id"]] = source

            # Parse output attributes (dimensions)
            for child in xml.findall(".//logicalModel/attributes/attribute"):
                key_mapping = child.find("keyMapping")
                output = {
                    "source": key_mapping.attrib["columnName"]
                    if key_mapping is not None
                    else "",
                    "node": key_mapping.attrib["columnObjectName"]
                    if key_mapping is not None
                    else "",
                    "type": "attribute",
                }
                outputs[child.attrib["id"]] = output

            # Parse output measures (facts/metrics)
            for child in xml.findall(".//logicalModel/baseMeasures/measure"):
                measure_mapping = child.find("measureMapping")
                output = {
                    "source": measure_mapping.attrib["columnName"]
                    if measure_mapping is not None
                    else "",
                    "node": measure_mapping.attrib["columnObjectName"]
                    if measure_mapping is not None
                    else "",
                    "type": "measure",
                }
                outputs[child.attrib["id"]] = output

            # Parse calculation nodes
            for child in xml.iter("calculationView"):
                node: Dict[str, Any] = {
                    "type": child.attrib[
                        "{http://www.w3.org/2001/XMLSchema-instance}type"
                    ],
                    "sources": {},
                    "id": child.attrib["id"],
                }

                # Handle different node types
                if node["type"] == "Calculation:UnionView":
                    # Union nodes combine multiple inputs
                    node["unions"] = []
                    unions_list = cast(List[str], node["unions"])
                    for grandchild in child.iter("input"):
                        union_source = grandchild.attrib["node"].split("#")[-1]
                        unions_list.append(union_source)
                        node["sources"][union_source] = {}
                        # Map columns from each union input
                        for mapping in grandchild.iter("mapping"):
                            if "source" in mapping.attrib:
                                sources_dict = cast(
                                    Dict[str, Any], node["sources"][union_source]
                                )
                                sources_dict[mapping.attrib["target"]] = {
                                    "type": "column",
                                    "source": mapping.attrib["source"],
                                }
                else:
                    # Regular nodes (projection, aggregation, etc.)
                    for grandchild in child.iter("input"):
                        input_source = grandchild.attrib["node"].split("#")[-1]
                        node["sources"][input_source] = {}
                        # Map input columns to output columns
                        for mapping in grandchild.iter("mapping"):
                            if "source" in mapping.attrib:
                                sources_dict = cast(
                                    Dict[str, Any], node["sources"][input_source]
                                )
                                sources_dict[mapping.attrib["target"]] = {
                                    "type": "column",
                                    "source": mapping.attrib["source"],
                                }

                # Handle calculated attributes (computed columns)
                for grandchild in child.iter("calculatedViewAttribute"):
                    formula = grandchild.find("formula")
                    if formula is not None:
                        sources_dict = cast(Dict[str, Any], node["sources"])
                        sources_dict[grandchild.attrib["id"]] = {
                            "type": "formula",
                            "source": formula.text,
                        }

                nodes[node["id"]] = node
        except Exception as e:
            logger.error(f"Error parsing calculation view {view}: {e}")

        return {
            "viewName": view,
            "sources": sources,
            "outputs": outputs,
            "nodes": nodes,
        }

    @staticmethod
    def _extract_columns_from_formula(formula: str) -> List[str]:
        """
        Extract column names from calculation view formula expressions.

        Args:
            formula: Formula string containing column references in quotes

        Returns:
            List of column names found in the formula
        """
        return re.findall(r"\"([^\"]*)\"", formula)

    def _find_all_sources(
        self, calc_view: Dict[str, Any], column: str, node: str, visited: Set[str]
    ) -> List[Dict[str, Any]]:
        """
        Recursively find all source columns for a given output column.

        This method traverses the calculation view structure to find the ultimate
        source tables and columns that contribute to a specific output column.

        Args:
            calc_view: Parsed calculation view structure
            column: Target column name to trace
            node: Current node being processed
            visited: Set of already visited nodes to prevent infinite recursion

        Returns:
            List of source column information dictionaries
        """
        sources = []

        try:
            # Prevent infinite recursion in circular dependencies
            if node in visited:
                return []
            visited.add(node)

            node_info = calc_view["nodes"].get(node)

            # Base case: reached a data source (table/view)
            if not node_info:
                source = calc_view["sources"].get(node)
                if source:
                    return [
                        {
                            "column": column,
                            "source": source["name"],
                            "sourceType": source["type"],
                            "sourcePath": source.get("path", ""),
                        }
                    ]
                return []

            # Handle union views - check all union branches
            if node_info["type"] == "Calculation:UnionView":
                for union_source in node_info["unions"]:
                    for col, col_info in node_info["sources"][union_source].items():
                        if col == column:
                            # Recursively trace through union branch
                            new_sources = self._find_all_sources(
                                calc_view,
                                col_info["source"],
                                union_source,
                                visited.copy(),
                            )
                            # If no recursive sources found, this might be a direct source
                            if not new_sources and union_source in calc_view["sources"]:
                                new_sources = [
                                    {
                                        "column": col_info["source"],
                                        "source": f"{calc_view['sources'][union_source]['path']}/{calc_view['sources'][union_source]['name']}"
                                        if calc_view["sources"][union_source]["type"]
                                        == "CALCULATION_VIEW"
                                        else calc_view["sources"][union_source]["name"],
                                        "sourceType": calc_view["sources"][
                                            union_source
                                        ]["type"],
                                        "sourcePath": calc_view["sources"][
                                            union_source
                                        ]["path"],
                                    }
                                ]
                            sources.extend(new_sources)
            # Handle regular calculation nodes
            else:
                for source, columns in node_info["sources"].items():
                    if column in columns:
                        col_info = columns[column]
                        if col_info["type"] == "formula":
                            # Handle calculated columns - extract referenced columns from formula
                            formula_columns = self._extract_columns_from_formula(
                                col_info["source"]
                            )
                            for formula_column in formula_columns:
                                sources.extend(
                                    self._find_all_sources(
                                        calc_view, formula_column, node, visited.copy()
                                    )
                                )
                        elif source in calc_view["sources"]:
                            # Direct reference to a data source
                            sources.append(
                                {
                                    "column": col_info["source"],
                                    "source": f"{calc_view['sources'][source]['path']}/{calc_view['sources'][source]['name']}"
                                    if calc_view["sources"][source]["type"]
                                    == "CALCULATION_VIEW"
                                    else calc_view["sources"][source]["name"],
                                    "sourceType": calc_view["sources"][source]["type"],
                                    "sourcePath": calc_view["sources"][source]["path"],
                                }
                            )
                        else:
                            # Reference to another calculation node - recurse
                            sources.extend(
                                self._find_all_sources(
                                    calc_view,
                                    col_info["source"],
                                    source,
                                    visited.copy(),
                                )
                            )
        except Exception as e:
            logger.error(
                f"Error finding sources for column {column} in node {node}: {e}"
            )

        return sources

    def _get_all_columns_origin(
        self, view: str, definition: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get the origin sources for all columns in a calculation view.

        Args:
            view: Name of the calculation view
            definition: XML definition of the calculation view

        Returns:
            Dictionary mapping column names to their source information
        """
        xml_root = ET.fromstring(definition)
        calc_view = self._parse_calc_view(view, xml_root)
        columns_lineage = {}

        try:
            # Trace lineage for all output columns (attributes and measures)
            for output, output_info in calc_view["outputs"].items():
                sources = self._find_all_sources(
                    calc_view, output_info["source"], output_info["node"], set()
                )
                columns_lineage[output] = sources

            # Handle calculated attributes (computed columns in nodes)
            for node in calc_view["nodes"].values():
                for column, col_info in node["sources"].items():
                    if isinstance(col_info, dict) and col_info.get("type") == "formula":
                        # Extract column dependencies from formula expressions
                        formula_columns = self._extract_columns_from_formula(
                            col_info["source"]
                        )
                        sources = []
                        for formula_column in formula_columns:
                            sources.extend(
                                self._find_all_sources(
                                    calc_view, formula_column, node["id"], set()
                                )
                            )
                        columns_lineage[column] = sources
        except Exception as e:
            logger.error(f"Error getting column origins for view {view}: {e}")
            return {}

        return columns_lineage

    def format_column_lineage(
        self,
        view_name: str,
        view_definition: str,
    ) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        """
        Format column lineage information for DataHub consumption.

        This method processes the calculation view definition and returns
        column-level lineage information in a format suitable for DataHub.

        Args:
            view_name: Name of the calculation view
            view_definition: XML definition of the calculation view

        Returns:
            List of dictionaries containing downstream columns and their upstream sources
        """
        column_dicts: List[Dict[str, Union[str, List[Dict[str, str]]]]] = []

        try:
            output_columns = self._get_all_columns_origin(
                view_name,
                view_definition,
            )

            # Format lineage information for DataHub consumption
            for cols, src in output_columns.items():
                column_dicts.append(
                    {
                        "downstream_column": cols,
                        "upstream": [
                            {
                                # Format upstream table names based on source type
                                "upstream_table": self._get_upstream_table_name(
                                    src_col
                                ),
                                "upstream_column": src_col.get("column") or "",
                            }
                            for src_col in src
                        ],
                    }
                )

        except Exception as e:
            logger.error(f"Error formatting column lineage for view {view_name}: {e}")

        return column_dicts
