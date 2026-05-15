import logging
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Set, Tuple

import defusedxml.ElementTree as DET
from defusedxml.common import DefusedXmlException

from datahub.ingestion.source.sql.hana.constants import (
    XSI_TYPE_ATTR,
    CalcViewNodeType,
)
from datahub.ingestion.source.sql.hana.models import (
    CalcViewModel,
    CalcViewNode,
    ColumnEdge,
    ColumnLineage,
    DataSourceRef,
    OutputMapping,
    ScriptViewDefinition,
    UpstreamColumnRef,
)

logger = logging.getLogger(__name__)

_QUOTED_COLUMN_RE = re.compile(r'"([^"]*)"')


class SAPCalculationViewParser:
    """Extract column-level lineage from a SAP HANA calculation-view XML."""

    def column_lineage(
        self, view_name: str, view_definition: str
    ) -> List[ColumnLineage]:
        """Return column-level lineage for a calculation view.

        Returns an empty list if the XML cannot be parsed or is rejected by
        the safe parser; the caller can fall back to table-level lineage.
        """
        root = self._parse_xml(view_name, view_definition)
        if root is None:
            return []
        model = self._build_model(view_name, root)
        return [
            ColumnLineage(
                downstream_column=output_id,
                upstreams=self._find_all_sources(
                    model, output.source_column, output.source_node, set()
                ),
            )
            for output_id, output in model.outputs.items()
        ]

    def script_view_definitions(
        self, view_name: str, view_definition: str
    ) -> List[ScriptViewDefinition]:
        """Return SQLScript bodies embedded in ``Calculation:SqlScriptView`` nodes."""
        root = self._parse_xml(view_name, view_definition)
        if root is None:
            return []
        model = self._build_model(view_name, root)
        return [
            ScriptViewDefinition(node_id=node_id, definition=definition)
            for node_id, definition in model.scripts.items()
        ]

    @staticmethod
    def _extract_columns_from_formula(formula: str) -> List[str]:
        """Extract quoted column references from a calc-view formula expression."""
        return _QUOTED_COLUMN_RE.findall(formula)

    @staticmethod
    def _parse_xml(view_name: str, view_definition: str) -> Optional[ET.Element]:
        try:
            return DET.fromstring(view_definition)
        except DefusedXmlException as e:
            logger.warning(
                "Rejected potentially malicious XML for calculation view %s: %s",
                view_name,
                e,
            )
            return None
        except ET.ParseError as e:
            logger.warning(
                "Failed to parse XML for calculation view %s: %s", view_name, e
            )
            return None

    @staticmethod
    def _build_model(view_name: str, xml: ET.Element) -> CalcViewModel:
        model = CalcViewModel(view_name=view_name)

        try:
            _collect_data_sources(xml, model)
            _collect_outputs(xml, model)
            _collect_nodes(xml, model)
        except Exception as e:
            logger.warning(
                "Error parsing calculation view %s: %s", view_name, e, exc_info=True
            )

        return model

    def _find_all_sources(
        self,
        model: CalcViewModel,
        column: str,
        node: str,
        visited: Set[Tuple[str, str]],
    ) -> List[UpstreamColumnRef]:
        """Walk the DAG to find leaf source columns for ``column``.

        ``visited`` tracks ``(node, column)`` pairs along the current path:
        cycles are detected, but formulas can still re-enter the same node
        with a different ``column``.
        """
        key = (node, column)
        if key in visited:
            return []
        visited.add(key)

        node_info = model.nodes.get(node)
        if node_info is None:
            return self._resolve_leaf(model, column, node)

        sources: List[UpstreamColumnRef] = []
        try:
            if node_info.type == CalcViewNodeType.UNION:
                for branch in node_info.union_branches:
                    edge = node_info.branches.get(branch, {}).get(column)
                    if edge is not None:
                        sources.extend(
                            self._traverse(model, edge, branch, visited.copy())
                        )
            else:
                for branch, branch_cols in node_info.branches.items():
                    edge = branch_cols.get(column)
                    if edge is not None:
                        sources.extend(
                            self._traverse(model, edge, branch, visited.copy())
                        )
                formula_expr = node_info.formulas.get(column)
                if formula_expr is not None:
                    for ref_col in self._extract_columns_from_formula(formula_expr):
                        sources.extend(
                            self._find_all_sources(model, ref_col, node, visited.copy())
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

    @staticmethod
    def _resolve_leaf(
        model: CalcViewModel, column: str, node: str
    ) -> List[UpstreamColumnRef]:
        # TREE_BASED calc views reference a DataSource by its
        # columnObject.columnObjectName instead of its DataSource id; the
        # alias map gives us a second lookup path before giving up.
        source = model.sources.get(node)
        if source is None:
            alias_id = model.source_aliases.get(node)
            if alias_id is not None:
                source = model.sources.get(alias_id)
        if source is None:
            return []
        return [
            UpstreamColumnRef(
                column=column,
                source_name=source.name,
                source_type=source.type,
                source_path=source.path,
            )
        ]

    def _traverse(
        self,
        model: CalcViewModel,
        edge: ColumnEdge,
        branch: str,
        visited: Set[Tuple[str, str]],
    ) -> List[UpstreamColumnRef]:
        if edge.kind == "formula":
            return [
                source
                for ref_col in self._extract_columns_from_formula(edge.source)
                for source in self._find_all_sources(
                    model, ref_col, branch, visited.copy()
                )
            ]
        return self._find_all_sources(model, edge.source, branch, visited.copy())


def _collect_data_sources(xml: ET.Element, model: CalcViewModel) -> None:
    for child in xml.iter("DataSource"):
        source = DataSourceRef(type=child.attrib.get("type", ""))
        for grandchild in child:
            if grandchild.tag == "columnObject":
                source.name = grandchild.attrib.get("columnObjectName", "")
                source.path = grandchild.attrib.get("schemaName", "")
            elif grandchild.tag == "resourceUri" and grandchild.text:
                # Reference to a calculation view stored in the design-time
                # repository: ``/<package>/calculationviews/<view>`` →
                # ``<view>`` plus the dot-joined package path.
                leaf, package = _split_resource_uri(grandchild.text)
                source.name = leaf
                source.path = package
        ds_id = child.attrib["id"]
        model.sources[ds_id] = source
        if source.name and source.name != ds_id:
            model.source_aliases.setdefault(source.name, ds_id)


def _collect_outputs(xml: ET.Element, model: CalcViewModel) -> None:
    for attr in xml.findall(".//logicalModel/attributes/attribute"):
        model.outputs[attr.attrib["id"]] = _build_output_mapping(
            attr.find("keyMapping"), kind="attribute"
        )
    for measure in xml.findall(".//logicalModel/baseMeasures/measure"):
        model.outputs[measure.attrib["id"]] = _build_output_mapping(
            measure.find("measureMapping"), kind="measure"
        )


def _collect_nodes(xml: ET.Element, model: CalcViewModel) -> None:
    for child in xml.iter("calculationView"):
        # Skip the outer ``<Calculation:scenario>`` (and legacy exports that
        # share the bare ``calculationView`` tag without ``xsi:type``).
        node_type = child.attrib.get(XSI_TYPE_ATTR)
        node_id = child.attrib.get("id")
        if not node_type or not node_id:
            continue

        if node_type == CalcViewNodeType.SQL_SCRIPT:
            # SqlScript nodes encode lineage in their <definition> body
            # rather than the XML DAG; capture for SQL-based extraction.
            definition_el = child.find("definition")
            if definition_el is not None and definition_el.text:
                model.scripts[node_id] = definition_el.text
            continue

        node = CalcViewNode(id=node_id, type=node_type)

        if node_type == CalcViewNodeType.UNION:
            for input_el in child.iter("input"):
                branch = input_el.attrib["node"].split("#")[-1]
                node.union_branches.append(branch)
                node.branches[branch] = _read_input_mappings(input_el)
        else:
            for input_el in child.iter("input"):
                branch = input_el.attrib["node"].split("#")[-1]
                node.branches[branch] = _read_input_mappings(input_el)

        for calc_attr in child.iter("calculatedViewAttribute"):
            formula = calc_attr.find("formula")
            if formula is not None and formula.text:
                node.formulas[calc_attr.attrib["id"]] = formula.text

        model.nodes[node_id] = node


def _read_input_mappings(input_el: ET.Element) -> Dict[str, ColumnEdge]:
    edges: Dict[str, ColumnEdge] = {}
    for mapping in input_el.iter("mapping"):
        source = mapping.attrib.get("source")
        target = mapping.attrib.get("target")
        if source and target:
            edges[target] = ColumnEdge(source=source, kind="column")
    return edges


def _build_output_mapping(
    mapping_element: Optional[ET.Element], *, kind: str
) -> OutputMapping:
    if mapping_element is None:
        return OutputMapping(source_column="", source_node="", kind=kind)  # type: ignore[arg-type]
    return OutputMapping(
        source_column=mapping_element.attrib.get("columnName", ""),
        source_node=mapping_element.attrib.get("columnObjectName", ""),
        kind=kind,  # type: ignore[arg-type]
    )


def _split_resource_uri(uri: str) -> Tuple[str, str]:
    """Return ``(leaf, package)`` for ``/acme.analytics/calculationviews/Sales``."""
    parts = uri.strip().lstrip("/").split("/")
    if not parts:
        return "", ""
    leaf = parts[-1]
    parent = "/".join(parts[:-1])
    parent = re.sub(r"/calculation_?views?$", "", parent, flags=re.IGNORECASE)
    return leaf, parent.replace("/", ".")
