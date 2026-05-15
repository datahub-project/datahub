"""Parser for SAP HANA calculation-view XML definitions.

SAP HANA stores activated calculation views as XML blobs in
``_SYS_REPO.ACTIVE_OBJECT``. The XML describes a DAG of *nodes* (projections,
joins, aggregations, unions, …) that combine one or more *data sources*
(tables, table functions, other calculation views) and expose a set of output
columns (attributes and measures).

This module turns that DAG into column-level lineage of the form::

    [
        {
            "downstream_column": "TOTAL_REVENUE",
            "upstreams": [
                {
                    "column": "AMOUNT",
                    "source_type": "DATA_BASE_TABLE",
                    "source_name": "SALES",
                    "source_path": "REPORTING",
                },
                ...
            ],
        },
        ...
    ]

The parser is intentionally pure — no I/O, no DataHub types — so it can be
exercised directly from unit tests against XML fixtures. URN construction
happens in :class:`HanaIdentifierBuilder` on top of these dicts.
"""

import logging
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Set, cast

logger = logging.getLogger(__name__)

# XML schema-instance type attribute used on <calculationView> nodes.
_XSI_TYPE_ATTR = "{http://www.w3.org/2001/XMLSchema-instance}type"


class SAPCalculationViewParser:
    """Extract column-level lineage from a SAP HANA calculation-view XML."""

    @staticmethod
    def _parse_calc_view(view: str, xml: ET.Element) -> Dict[str, Any]:
        """Build an in-memory representation of a calc view's DAG.

        Returns a dict with three sub-maps:

        - ``sources``: ``DataSource id → {name, path, type}`` — leaves.
        - ``outputs``: ``output_id → {source_column, source_node, type}`` —
          attributes and measures exposed by the calc view.
        - ``nodes``: ``node_id → {type, id, sources}`` — calculation nodes
          and their column-mapping edges.
        """
        sources: Dict[str, Dict[str, str]] = {}
        outputs: Dict[str, Dict[str, str]] = {}
        nodes: Dict[str, Dict[str, Any]] = {}

        try:
            for child in xml.iter("DataSource"):
                source: Dict[str, str] = {"type": child.attrib["type"]}
                for grandchild in child:
                    if grandchild.tag == "columnObject":
                        # Reference to a regular table or view.
                        source["name"] = grandchild.attrib["columnObjectName"]
                        source["path"] = grandchild.attrib["schemaName"]
                    elif grandchild.tag == "resourceUri" and grandchild.text:
                        # Reference to another calculation view stored in the
                        # design-time repository. The URI looks like
                        # `/acme.analytics/calculationviews/SalesOverview`;
                        # the leaf is the view name and the parent path is
                        # the package_id (with `/calculationviews` stripped).
                        leaf, _, package = _split_resource_uri(grandchild.text)
                        source["name"] = leaf
                        source["path"] = package
                sources[child.attrib["id"]] = source

            for child in xml.findall(".//logicalModel/attributes/attribute"):
                outputs[child.attrib["id"]] = _read_mapping(
                    child.find("keyMapping"), output_type="attribute"
                )

            for child in xml.findall(".//logicalModel/baseMeasures/measure"):
                outputs[child.attrib["id"]] = _read_mapping(
                    child.find("measureMapping"), output_type="measure"
                )

            for child in xml.iter("calculationView"):
                # Real SAP HANA calc views nest <calculationView> nodes
                # inside a <calculationViews> container under a root such as
                # <Calculation:scenario>. Each nested node carries an
                # ``xsi:type`` attribute identifying it (Projection, Join,
                # Aggregation, Union, …). The root element sometimes shares
                # the bare ``calculationView`` tag in legacy exports and
                # always lacks the ``xsi:type`` attribute, so skip anything
                # that doesn't look like a calculation node.
                node_type = child.attrib.get(_XSI_TYPE_ATTR)
                node_id = child.attrib.get("id")
                if not node_type or not node_id:
                    continue
                node: Dict[str, Any] = {
                    "type": node_type,
                    "id": node_id,
                    "sources": {},
                }
                if node["type"] == "Calculation:UnionView":
                    # Unions are special — every <input> contributes to the
                    # output independently, so we keep an explicit list of
                    # union branches to traverse later.
                    node["unions"] = []
                    unions_list = cast(List[str], node["unions"])
                    for grandchild in child.iter("input"):
                        branch = grandchild.attrib["node"].split("#")[-1]
                        unions_list.append(branch)
                        node["sources"][branch] = {}
                        _collect_column_mappings(grandchild, node["sources"][branch])
                else:
                    for grandchild in child.iter("input"):
                        branch = grandchild.attrib["node"].split("#")[-1]
                        node["sources"][branch] = {}
                        _collect_column_mappings(grandchild, node["sources"][branch])

                # Calculated columns (formulas) live alongside the input
                # mappings; we treat them as a synthetic source for the
                # purpose of recursive lineage traversal.
                for grandchild in child.iter("calculatedViewAttribute"):
                    formula = grandchild.find("formula")
                    if formula is not None and formula.text:
                        node["sources"][grandchild.attrib["id"]] = {
                            "type": "formula",
                            "source": formula.text,
                        }

                nodes[node["id"]] = node
        except Exception as e:
            logger.warning(
                "Error parsing calculation view %s: %s", view, e, exc_info=True
            )

        return {
            "viewName": view,
            "sources": sources,
            "outputs": outputs,
            "nodes": nodes,
        }

    @staticmethod
    def _extract_columns_from_formula(formula: str) -> List[str]:
        """Extract quoted column references from a calc-view formula expression."""
        return re.findall(r'"([^"]*)"', formula)

    def _find_all_sources(
        self,
        calc_view: Dict[str, Any],
        column: str,
        node: str,
        visited: Set[str],
    ) -> List[Dict[str, Any]]:
        """Recursively walk the DAG to find leaf source columns for ``column``.

        ``visited`` is passed by copy from each call site so siblings can
        re-enter the same node along different branches; we only guard
        against cycles within a single path.
        """
        if node in visited:
            return []
        visited.add(node)

        node_info = calc_view["nodes"].get(node)
        if not node_info:
            # Reached a leaf (one of the registered DataSources).
            source = calc_view["sources"].get(node)
            if source:
                return [
                    {
                        "column": column,
                        "source_name": source.get("name", ""),
                        "source_type": source.get("type", ""),
                        "source_path": source.get("path", ""),
                    }
                ]
            return []

        sources: List[Dict[str, Any]] = []
        try:
            if node_info["type"] == "Calculation:UnionView":
                for union_branch in node_info["unions"]:
                    branch_cols = node_info["sources"].get(union_branch, {})
                    if column in branch_cols:
                        col_info = branch_cols[column]
                        sources.extend(
                            self._traverse(
                                calc_view, col_info, union_branch, visited.copy()
                            )
                        )
            else:
                for branch, branch_cols in node_info["sources"].items():
                    if isinstance(branch_cols, dict) and "type" in branch_cols:
                        # Formula entry stored directly under ``sources``.
                        # Only follow if the requested column matches the
                        # formula's id.
                        continue
                    if column in branch_cols:
                        col_info = branch_cols[column]
                        sources.extend(
                            self._traverse(calc_view, col_info, branch, visited.copy())
                        )
                # Formulas live at the node level — if ``column`` matches a
                # formula id, follow each column referenced by the formula.
                formula_entry = node_info["sources"].get(column)
                if (
                    isinstance(formula_entry, dict)
                    and formula_entry.get("type") == "formula"
                ):
                    for formula_column in self._extract_columns_from_formula(
                        formula_entry["source"]
                    ):
                        sources.extend(
                            self._find_all_sources(
                                calc_view, formula_column, node, visited.copy()
                            )
                        )
        except Exception as e:
            logger.warning(
                "Error tracing sources for column %s in node %s: %s",
                column,
                node,
                e,
                exc_info=True,
            )

        return sources

    def _traverse(
        self,
        calc_view: Dict[str, Any],
        col_info: Dict[str, Any],
        branch: str,
        visited: Set[str],
    ) -> List[Dict[str, Any]]:
        """Helper for :meth:`_find_all_sources` to step through one column edge."""
        if col_info.get("type") == "formula":
            return [
                source
                for formula_column in self._extract_columns_from_formula(
                    col_info["source"]
                )
                for source in self._find_all_sources(
                    calc_view, formula_column, branch, visited.copy()
                )
            ]
        return self._find_all_sources(
            calc_view, col_info["source"], branch, visited.copy()
        )

    def column_lineage(
        self, view_name: str, view_definition: str
    ) -> List[Dict[str, Any]]:
        """Return the column-level lineage for a calculation view.

        Each entry has shape::

            {
                "downstream_column": str,
                "upstreams": [
                    {"column": str, "source_name": str,
                     "source_type": str, "source_path": str},
                    ...
                ],
            }

        Returns an empty list if the XML cannot be parsed; the caller can
        decide whether to fall back to table-level lineage only.
        """
        try:
            xml_root = ET.fromstring(view_definition)
        except ET.ParseError as e:
            logger.warning(
                "Failed to parse XML for calculation view %s: %s", view_name, e
            )
            return []

        calc_view = self._parse_calc_view(view_name, xml_root)
        lineage: List[Dict[str, Any]] = []

        for output_id, output_info in calc_view["outputs"].items():
            sources = self._find_all_sources(
                calc_view,
                output_info["source"],
                output_info["node"],
                set(),
            )
            lineage.append(
                {
                    "downstream_column": output_id,
                    "upstreams": sources,
                }
            )

        return lineage


def _read_mapping(
    mapping_element: Optional[ET.Element], *, output_type: str
) -> Dict[str, str]:
    """Build a uniform ``{source, node, type}`` dict for attribute/measure outputs."""
    if mapping_element is None:
        return {"source": "", "node": "", "type": output_type}
    return {
        "source": mapping_element.attrib.get("columnName", ""),
        "node": mapping_element.attrib.get("columnObjectName", ""),
        "type": output_type,
    }


def _collect_column_mappings(
    input_element: ET.Element, target: Dict[str, Dict[str, str]]
) -> None:
    """Populate ``target`` with ``downstream_col → {type: column, source: upstream_col}``."""
    for mapping in input_element.iter("mapping"):
        if "source" in mapping.attrib and "target" in mapping.attrib:
            target[mapping.attrib["target"]] = {
                "type": "column",
                "source": mapping.attrib["source"],
            }


def _split_resource_uri(uri: str) -> tuple[str, str, str]:
    """Parse a ``<resourceUri>`` payload into ``(leaf, _, package_path)``.

    SAP HANA resource URIs look like ``/<package_dot_path>/<kind>/<view>``
    where ``<kind>`` is typically ``calculationviews``. We strip the
    ``/calculationviews/`` segment so the returned package path mirrors the
    dot-separated ``PACKAGE_ID`` that appears in ``_SYS_REPO.ACTIVE_OBJECT``.
    """
    parts = uri.strip().lstrip("/").split("/")
    if not parts:
        return "", "", ""
    leaf = parts[-1]
    parent = "/".join(parts[:-1])
    # Strip the trailing "/calculationviews" or "/calculationview" container,
    # which SAP HANA inserts between the package path and the leaf.
    parent = re.sub(r"/calculation_?views?$", "", parent, flags=re.IGNORECASE)
    package = parent.replace("/", ".")
    return leaf, "", package
