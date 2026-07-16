import logging
import re
from typing import Dict, List, Optional, Set, Tuple
from xml.etree.ElementTree import (  # nosec B405 - only the Element / ParseError types are imported; parsing always goes through defusedxml
    Element,
    ParseError,
)

import defusedxml.ElementTree as DET
from defusedxml.common import DefusedXmlException

from datahub.ingestion.source.sql.hana.constants import (
    INPUT_NODE_REF_SIGIL,
    LOGICAL_MODEL_ATTRIBUTES_XPATH,
    LOGICAL_MODEL_MEASURES_XPATH,
    XSI_TYPE_ATTR,
    CalcViewNodeType,
    CalcViewXmlAttribute,
    CalcViewXmlElement,
    ColumnEdgeKind,
    HanaSourceType,
    OutputMappingKind,
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

# Trailing ``/calculationviews`` segment that follows the package path in
# a resourceUri (e.g. ``/acme.analytics/calculationviews/Sales``); stripped
# before joining the remaining segments into a dotted package id. Matches
# only the documented SAP form (case-insensitive) — variants like
# ``calculation_views`` are not documented and stay verbatim in the path.
_RESOURCE_URI_VIEWS_SUFFIX_RE = re.compile(r"/calculationviews$", flags=re.IGNORECASE)


class SAPCalculationViewParser:
    """Extract column-level lineage from a SAP HANA calculation-view XML."""

    def column_lineage(
        self, view_name: str, view_definition: str
    ) -> List[ColumnLineage]:
        """Empty list on parse failure or defusedxml rejection — the caller
        can fall back to table-level lineage in that case."""
        root = self._parse_xml(view_name, view_definition)
        if root is None:
            return []
        model = self._build_model(view_name, root)
        lineage: List[ColumnLineage] = []
        for output_id, output in model.outputs.items():
            if output.source_column is None or output.source_node is None:
                lineage.append(ColumnLineage(downstream_column=output_id, upstreams=[]))
                continue
            lineage.append(
                ColumnLineage(
                    downstream_column=output_id,
                    upstreams=self._find_all_sources(
                        model, output.source_column, output.source_node, set()
                    ),
                )
            )
        return lineage

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
        return _QUOTED_COLUMN_RE.findall(formula)

    @staticmethod
    def _parse_xml(view_name: str, view_definition: str) -> Optional[Element]:
        try:
            return DET.fromstring(view_definition)
        except DefusedXmlException as e:
            logger.warning(
                "Rejected potentially malicious XML for calculation view %s: %s",
                view_name,
                e,
            )
            return None
        except ParseError as e:
            logger.warning(
                "Failed to parse XML for calculation view %s: %s", view_name, e
            )
            return None

    @staticmethod
    def _build_model(view_name: str, xml: Element) -> CalcViewModel:
        model = CalcViewModel(view_name=view_name)

        try:
            _collect_data_sources(xml, model)
            _collect_outputs(xml, model)
            _collect_nodes(xml, model)
        except Exception as e:
            logger.warning(
                "Error parsing calculation view %s: %s", view_name, e, exc_info=True
            )
            # Return a fresh, empty model on partial-parse failure rather
            # than the half-populated one. Half-populated models can
            # mislead the DAG traversal into emitting partial lineage
            # that looks complete but silently drops branches the parser
            # never reached.
            return CalcViewModel(view_name=view_name)

        return model

    def _find_all_sources(
        self,
        model: CalcViewModel,
        column: str,
        node: str,
        visited: Set[Tuple[str, str]],
    ) -> List[UpstreamColumnRef]:
        """Walk the DAG to find leaf source columns for ``column``.

        ``visited`` tracks ``(node, column)`` pairs along the current path
        with push/pop semantics: cycles are detected, but formulas can
        still re-enter the same node with a different ``column``.
        """
        key = (node, column)
        if key in visited:
            return []
        visited.add(key)

        node_info = model.nodes.get(node)
        if node_info is None:
            try:
                return self._resolve_leaf(model, column, node)
            finally:
                visited.discard(key)

        sources: List[UpstreamColumnRef] = []
        try:
            if node_info.type == CalcViewNodeType.UNION:
                for branch in node_info.union_branches:
                    edge = node_info.branches.get(branch, {}).get(column)
                    if edge is not None:
                        sources.extend(self._traverse(model, edge, branch, visited))
            else:
                for branch, branch_cols in node_info.branches.items():
                    edge = branch_cols.get(column)
                    if edge is not None:
                        sources.extend(self._traverse(model, edge, branch, visited))
                formula_expr = node_info.formulas.get(column)
                if formula_expr is not None:
                    for ref_col in self._extract_columns_from_formula(formula_expr):
                        sources.extend(
                            self._find_all_sources(model, ref_col, node, visited)
                        )
        except Exception as e:
            logger.warning(
                "Error tracing sources for column %s in node %s: %s",
                column,
                node,
                e,
                exc_info=True,
            )
        finally:
            visited.discard(key)

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
        if source is None or source.name is None:
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
        if edge.kind == ColumnEdgeKind.FORMULA:
            return [
                source
                for ref_col in self._extract_columns_from_formula(edge.source)
                for source in self._find_all_sources(model, ref_col, branch, visited)
            ]
        return self._find_all_sources(model, edge.source, branch, visited)


def _collect_data_sources(xml: Element, model: CalcViewModel) -> None:
    for child in xml.iter(CalcViewXmlElement.DATA_SOURCE):
        type_attr = child.attrib.get(CalcViewXmlAttribute.TYPE)
        if type_attr is None:
            continue
        try:
            source_type = HanaSourceType(type_attr)
        except ValueError:
            # Unknown HANA source type: skip so downstream URN code only
            # ever sees a known enum value.
            continue

        name: Optional[str] = None
        path: Optional[str] = None
        for grandchild in child:
            if grandchild.tag == CalcViewXmlElement.COLUMN_OBJECT:
                name = grandchild.attrib.get(CalcViewXmlAttribute.COLUMN_OBJECT_NAME)
                path = grandchild.attrib.get(CalcViewXmlAttribute.SCHEMA_NAME)
            elif grandchild.tag == CalcViewXmlElement.RESOURCE_URI and grandchild.text:
                # Reference to a calculation view stored in the design-time
                # repository: ``/<package>/calculationviews/<view>`` →
                # ``<view>`` plus the dot-joined package path.
                name, path = _split_resource_uri(grandchild.text)

        source = DataSourceRef.model_validate(
            {"type": source_type, "name": name or None, "path": path or None}
        )
        ds_id = child.attrib[CalcViewXmlAttribute.ID]
        model.sources[ds_id] = source
        if source.name and source.name != ds_id:
            model.source_aliases.setdefault(source.name, ds_id)


def _collect_outputs(xml: Element, model: CalcViewModel) -> None:
    for attr in xml.findall(LOGICAL_MODEL_ATTRIBUTES_XPATH):
        model.outputs[attr.attrib[CalcViewXmlAttribute.ID]] = _build_output_mapping(
            attr.find(CalcViewXmlElement.KEY_MAPPING),
            kind=OutputMappingKind.ATTRIBUTE,
        )
    for measure in xml.findall(LOGICAL_MODEL_MEASURES_XPATH):
        model.outputs[measure.attrib[CalcViewXmlAttribute.ID]] = _build_output_mapping(
            measure.find(CalcViewXmlElement.MEASURE_MAPPING),
            kind=OutputMappingKind.MEASURE,
        )


def _collect_nodes(xml: Element, model: CalcViewModel) -> None:
    for child in xml.iter(CalcViewXmlElement.CALCULATION_VIEW):
        # Skip the outer ``<Calculation:scenario>`` (and legacy exports that
        # share the bare ``calculationView`` tag without ``xsi:type``).
        node_type = child.attrib.get(XSI_TYPE_ATTR)
        node_id = child.attrib.get(CalcViewXmlAttribute.ID)
        if not node_type or not node_id:
            continue

        if node_type == CalcViewNodeType.SQL_SCRIPT:
            # SqlScript nodes encode lineage in their <definition> body
            # rather than the XML DAG; capture for SQL-based extraction.
            definition_el = child.find(CalcViewXmlElement.DEFINITION)
            if definition_el is not None and definition_el.text:
                model.scripts[node_id] = definition_el.text
            continue

        node = CalcViewNode(id=node_id, type=node_type)

        # union_branches preserves sibling order so the DAG walker iterates
        # them deterministically; branches holds mappings for all node types.
        is_union = node_type == CalcViewNodeType.UNION
        for input_el in child.iter(CalcViewXmlElement.INPUT):
            branch = input_el.attrib[CalcViewXmlAttribute.NODE].split(
                INPUT_NODE_REF_SIGIL
            )[-1]
            if is_union:
                node.union_branches.append(branch)
            node.branches[branch] = _read_input_mappings(input_el)

        for calc_attr in child.iter(CalcViewXmlElement.CALCULATED_VIEW_ATTRIBUTE):
            formula = calc_attr.find(CalcViewXmlElement.FORMULA)
            if formula is not None and formula.text:
                node.formulas[calc_attr.attrib[CalcViewXmlAttribute.ID]] = formula.text

        model.nodes[node_id] = node


def _read_input_mappings(input_el: Element) -> Dict[str, ColumnEdge]:
    edges: Dict[str, ColumnEdge] = {}
    for mapping in input_el.iter(CalcViewXmlElement.MAPPING):
        source = mapping.attrib.get(CalcViewXmlAttribute.SOURCE)
        target = mapping.attrib.get(CalcViewXmlAttribute.TARGET)
        if source and target:
            edges[target] = ColumnEdge(source=source, kind=ColumnEdgeKind.COLUMN)
    return edges


def _build_output_mapping(
    mapping_element: Optional[Element],
    *,
    kind: OutputMappingKind,
) -> OutputMapping:
    if mapping_element is None:
        return OutputMapping(kind=kind)
    return OutputMapping(
        source_column=mapping_element.attrib.get(CalcViewXmlAttribute.COLUMN_NAME)
        or None,
        source_node=mapping_element.attrib.get(CalcViewXmlAttribute.COLUMN_OBJECT_NAME)
        or None,
        kind=kind,
    )


def _split_resource_uri(uri: str) -> Tuple[str, str]:
    """Split a HANA design-time resourceUri into ``(leaf_view, dotted_package)``.

    For example ``/acme.analytics/calculationviews/Sales`` →
    ``("Sales", "acme.analytics")``. The ``/calculationviews`` segment
    HANA inserts between the package path and the view name is stripped
    before slashes are converted to dots so the result matches the
    package-id form stored in ``_SYS_REPO.ACTIVE_OBJECT.PACKAGE_ID``.
    """
    parts = uri.strip().lstrip("/").split("/")
    if not parts:
        return "", ""
    leaf = parts[-1]
    parent = "/".join(parts[:-1])
    parent = _RESOURCE_URI_VIEWS_SUFFIX_RE.sub("", parent)
    return leaf, parent.replace("/", ".")
