import json
import logging
import re
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)
from urllib.parse import quote

import requests
from requests import Response
from tableauserverclient import Server

import datahub.emitter.mce_builder as builder
from datahub.emitter.emit_mode import EmitMode
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DialectClass,
    DialectExpressionClass,
    EdgeClass,
    MetricExpressionClass,
    MetricInfoClass,
    MetricRelationshipsClass,
    MetricUpstreamsClass,
    SemanticModelInfoClass,
    StatusClass,
    StructuredPropertiesClass,
    StructuredPropertyDefinitionClass,
    StructuredPropertyValueAssignmentClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import MetricUrn, SchemaFieldUrn, SemanticModelUrn

if TYPE_CHECKING:
    from datahub.ingestion.source.tableau.tableau import (
        TableauConfig,
        TableauSourceReport,
    )

logger = logging.getLogger(__name__)

_PULSE_PAGE_SIZE_CAP = 1000
_DEFINITIONS_PATH = "definitions"
_UNKNOWN_ACTOR_URN = "urn:li:corpuser:unknown"
_DEFINITION_SUBTYPE = "Pulse Metric Definition"
_PROPERTY_PREFIX = "tableau.pulse"
_PROPERTY_VALUE_LIMIT = 32000
_STRING_VALUE_TYPE = "urn:li:dataType:datahub.string"
_SEMANTIC_MODEL_TYPE = "urn:li:entityType:datahub.semanticModel"


def _camel(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(part.capitalize() for part in rest)


def _field(obj: Dict[str, Any], name: str) -> Any:
    if name in obj:
        return obj[name]
    return obj.get(_camel(name))


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _clean_enum(value: Any, prefix: str) -> Optional[str]:
    if not isinstance(value, str) or not value or "UNSPECIFIED" in value:
        return None
    return value.removeprefix(prefix)


def _filter_values(pulse_filter: Dict[str, Any]) -> List[str]:
    values: List[str] = []
    for item in _as_list(_field(pulse_filter, "categorical_values")):
        if not isinstance(item, dict):
            continue
        string_value = _field(item, "string_value")
        if string_value:
            values.append(str(string_value))
            continue
        bool_value = _field(item, "bool_value")
        if bool_value is not None:
            values.append(str(bool_value).lower())
    for item in _as_list(_field(pulse_filter, "values")):
        if item is not None and str(item) not in values:
            values.append(str(item))
    return values


def _filter_label(pulse_filter: Dict[str, Any]) -> Optional[str]:
    field_name = _field(pulse_filter, "field")
    values = _filter_values(pulse_filter)
    if field_name and values:
        return f"{field_name}={','.join(values)}"
    if field_name:
        return str(field_name)
    return None


def _filter_labels(specification: Dict[str, Any]) -> List[str]:
    labels: List[str] = []
    for pulse_filter in _as_list(_field(specification, "filters")):
        if not isinstance(pulse_filter, dict):
            continue
        label = _filter_label(pulse_filter)
        if label:
            labels.append(label)
    return labels


def _basic_specification(definition: Dict[str, Any]) -> Dict[str, Any]:
    return _as_dict(
        _field(_as_dict(_field(definition, "specification")), "basic_specification")
    )


def _measure(definition: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    measure = _as_dict(_field(_basic_specification(definition), "measure"))
    field_name = _field(measure, "field")
    aggregation = _clean_enum(_field(measure, "aggregation"), "AGGREGATION_")
    return (str(field_name) if field_name else None, aggregation)


def _time_dimension(definition: Dict[str, Any]) -> Optional[str]:
    time_dimension = _as_dict(
        _field(_basic_specification(definition), "time_dimension")
    )
    field_name = _field(time_dimension, "field")
    return str(field_name) if field_name else None


# Names stay readable when a metric carries a long filter list. The full
# list remains in the metric description.
_MAX_FILTER_NAME_LENGTH = 48


def _contracted_filters(specification: Dict[str, Any]) -> str:
    raw_filters: List[Dict[str, Any]] = []
    labels: List[str] = []
    for pulse_filter in _as_list(_field(specification, "filters")):
        if not isinstance(pulse_filter, dict):
            continue
        label = _filter_label(pulse_filter)
        if not label:
            continue
        raw_filters.append(pulse_filter)
        labels.append(label)
    if not labels:
        return ""
    full = " | ".join(labels)
    if len(full) <= _MAX_FILTER_NAME_LENGTH:
        return full
    first = raw_filters[0]
    field_name = _field(first, "field")
    values = _filter_values(first)
    if field_name and values:
        head = f"{field_name}={values[0]}"
        hidden = len(values) - 1
    else:
        head = labels[0]
        hidden = 0
    hidden += len(labels) - 1
    if hidden:
        return f"{head} +{hidden}"
    return head


def _metric_qualifier(metric: Dict[str, Any]) -> str:
    specification = _as_dict(_field(metric, "specification"))
    contracted = _contracted_filters(specification)
    parts = [contracted] if contracted else []
    period = _as_dict(_field(specification, "measurement_period"))
    granularity = _field(period, "granularity")
    if (
        isinstance(granularity, str)
        and granularity
        and "UNSPECIFIED" not in granularity
    ):
        parts.append(granularity.removeprefix("GRANULARITY_").replace("_", " ").lower())
    if parts:
        return " | ".join(parts)
    if _field(metric, "is_default"):
        return "default"
    return str(_field(metric, "id") or "")


def _metric_name(definition_name: str, metric: Dict[str, Any]) -> str:
    qualifier = _metric_qualifier(metric)
    if _field(metric, "is_default") or qualifier in {"default", ""}:
        return definition_name
    return f"{definition_name} · {qualifier}"


def _audit_stamp(value: Any) -> Optional[AuditStampClass]:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return AuditStampClass(
        time=int(parsed.timestamp() * 1000), actor=_UNKNOWN_ACTOR_URN
    )


def _native_definition(definition: Dict[str, Any]) -> Optional[str]:
    specification = _as_dict(_field(definition, "specification"))
    measure_field, aggregation = _measure(definition)
    lines: List[str] = []
    if measure_field and aggregation:
        lines.append(f"measure: {aggregation}({measure_field})")
    elif measure_field:
        lines.append(f"measure: {measure_field}")
    time_dimension = _time_dimension(definition)
    if time_dimension:
        lines.append(f"time_dimension: {time_dimension}")
    temporality = _clean_enum(_field(specification, "temporality"), "TEMPORALITY_")
    if temporality:
        lines.append(f"temporality: {temporality}")
    running_total = _field(specification, "is_running_total")
    if isinstance(running_total, bool):
        lines.append(f"is_running_total: {str(running_total).lower()}")
    datasource_id = _field(_as_dict(_field(specification, "datasource")), "id")
    if datasource_id:
        lines.append(f"datasource_id: {datasource_id}")
    return "\n".join(lines) or None


def _scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _definition_property_rows(value: Any, path: str = "") -> List[Tuple[str, str]]:
    if isinstance(value, dict):
        rows: List[Tuple[str, str]] = []
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            rows.extend(_definition_property_rows(child, child_path))
        return rows
    if isinstance(value, list):
        if not path or not value:
            return []
        if any(isinstance(item, (dict, list)) for item in value):
            rendered = json.dumps(value, sort_keys=True)
        else:
            rendered = ", ".join(_scalar(item) for item in value)
        return [(path, rendered)] if rendered else []
    if not path or value is None or value == "":
        return []
    return [(path, _scalar(value))]


def _property_qualified_name(path: str) -> str:
    safe = ".".join(
        re.sub(r"[^A-Za-z0-9_]", "_", segment) for segment in path.split(".") if segment
    )
    return f"{_PROPERTY_PREFIX}.{safe}"


def _property_display_name(path: str) -> str:
    parts = [segment for segment in path.split(".") if segment]
    while len(parts) > 1 and parts[0] in {"specification", "basic_specification"}:
        parts = parts[1:]
    return " ".join(part.replace("_", " ").title() for part in parts)


def _limit_property_value(value: str) -> str:
    encoded = value.encode()
    if len(encoded) <= _PROPERTY_VALUE_LIMIT:
        return value
    return encoded[: _PROPERTY_VALUE_LIMIT - 3].decode(errors="ignore") + "..."


def _metric_expression(definition: Dict[str, Any]) -> Optional[MetricExpressionClass]:
    measure_field, aggregation = _measure(definition)
    if not measure_field:
        return None
    expression = (
        f"{aggregation}([{measure_field}])" if aggregation else f"[{measure_field}]"
    )
    return MetricExpressionClass(
        dialects=[
            DialectExpressionClass(dialect=DialectClass.TABLEAU, expression=expression)
        ]
    )


def _metric_description(definition: Dict[str, Any], metric: Dict[str, Any]) -> str:
    metadata = _as_dict(_field(definition, "metadata"))
    specification = _as_dict(_field(metric, "specification"))
    period = _as_dict(_field(specification, "measurement_period"))
    comparison = _as_dict(_field(specification, "comparison"))
    lines: List[str] = []
    description = _field(metadata, "description")
    if isinstance(description, str) and description:
        lines.append(description)
    metric_id = _field(metric, "id")
    if metric_id:
        lines.append(f"Tableau Pulse metric {metric_id}.")
    if _field(metric, "is_default"):
        lines.append("Default metric for this definition.")
    measure_field, aggregation = _measure(definition)
    if measure_field and aggregation:
        lines.append(f"Measure: {aggregation}({measure_field})")
    elif measure_field:
        lines.append(f"Measure: {measure_field}")
    time_dimension = _time_dimension(definition)
    if time_dimension:
        lines.append(f"Time dimension: {time_dimension}")
    filters = _filter_labels(specification)
    if filters:
        lines.append(f"Filters: {', '.join(filters)}")
    granularity = _clean_enum(_field(period, "granularity"), "GRANULARITY_")
    if granularity:
        lines.append(f"Granularity: {granularity}")
    period_range = _clean_enum(_field(period, "range"), "RANGE_")
    if period_range:
        lines.append(f"Range: {period_range}")
    comparison_name = _clean_enum(_field(comparison, "comparison"), "TIME_COMPARISON_")
    if comparison_name:
        lines.append(f"Comparison: {comparison_name}")
    datasource_luid = _field(metric, "datasource_luid")
    if datasource_luid:
        lines.append(f"Datasource: {datasource_luid}")
    return "\n".join(lines)


def _datasource_keys(
    definition: Dict[str, Any], metric: Optional[Dict[str, Any]] = None
) -> List[str]:
    keys: List[str] = []
    specification = _as_dict(_field(definition, "specification"))
    datasource_id = _field(_as_dict(_field(specification, "datasource")), "id")
    if datasource_id:
        keys.append(str(datasource_id))
    if metric is not None:
        datasource_luid = _field(metric, "datasource_luid")
        if datasource_luid and str(datasource_luid) not in keys:
            keys.append(str(datasource_luid))
    return keys


def _lineage_field_names(
    definition: Dict[str, Any], metric: Dict[str, Any]
) -> List[str]:
    names: List[str] = []
    measure_field, _ = _measure(definition)
    if measure_field:
        names.append(measure_field)
    time_dimension = _time_dimension(definition)
    if time_dimension:
        names.append(time_dimension)
    specifications = [
        _as_dict(_field(definition, "specification")),
        _basic_specification(definition),
        _as_dict(_field(metric, "specification")),
    ]
    for specification in specifications:
        for pulse_filter in _as_list(_field(specification, "filters")):
            if not isinstance(pulse_filter, dict):
                continue
            field_name = _field(pulse_filter, "field")
            if field_name:
                names.append(str(field_name))
    unique: List[str] = []
    for name in names:
        if name not in unique:
            unique.append(name)
    return unique


def _workunits(entity_urn: str, aspects: Sequence[Any]) -> Iterable[MetadataWorkUnit]:
    for aspect in aspects:
        yield MetadataChangeProposalWrapper(
            entityUrn=entity_urn, aspect=aspect
        ).as_workunit()


class TableauPulseExtractor:
    """Lists Pulse metric definitions and emits them into the Metrics area.

    Each definition becomes a semantic model. Each metric in that definition
    becomes a metric entity belonging to the model.
    """

    def __init__(
        self,
        config: "TableauConfig",
        server: Server,
        report: "TableauSourceReport",
        platform: str,
        browse_prefix: str,
        datasource_urns: Optional[Dict[str, str]] = None,
        datasource_fields: Optional[Dict[str, Set[str]]] = None,
        graph: Optional[DataHubGraph] = None,
    ) -> None:
        self.config = config
        self.server = server
        self.report = report
        self.platform = platform
        self.browse_prefix = browse_prefix
        self.datasource_urns = datasource_urns or {}
        self.datasource_fields = datasource_fields or {}
        self.graph = graph
        self._defined_properties: Set[str] = set()

    def emit(self) -> Iterable[MetadataWorkUnit]:
        try:
            definitions = list(self._list(_DEFINITIONS_PATH, "definitions"))
        except Exception as exc:
            self.report.warning(
                title="Failed to list Tableau Pulse metric definitions",
                message=(
                    "Pulse metric definitions were not ingested. Pulse is available "
                    "on Tableau Cloud only."
                ),
                exc=exc,
            )
            return

        for definition in definitions:
            if not isinstance(definition, dict):
                continue
            yield from self._emit_definition(definition)

    def _namespace(self) -> str:
        site = self.config.site or "default"
        if self.config.platform_instance:
            return f"{self.config.platform_instance}.{site}"
        return site

    def _emit_definition(
        self, definition: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        metadata = _as_dict(_field(definition, "metadata"))
        definition_id = _field(metadata, "id")
        if not definition_id:
            self.report.warning(
                title="Skipped Tableau Pulse metric definition",
                message="A Pulse metric definition had no id and was not ingested.",
            )
            return

        definition_id = str(definition_id)
        name = str(_field(metadata, "name") or definition_id)
        try:
            metrics = list(
                self._list(
                    f"definitions/{quote(definition_id, safe='')}/metrics", "metrics"
                )
            )
        except Exception as exc:
            self.report.warning(
                title="Failed to list Tableau Pulse metrics",
                message="The metric definition was ingested without metrics.",
                context=definition_id,
                exc=exc,
            )
            metrics = []

        platform_urn = builder.make_data_platform_urn(self.platform)
        model_urn = str(
            SemanticModelUrn(
                platform=platform_urn, path=self._namespace(), id=definition_id
            )
        )
        created = _audit_stamp(_field(metadata, "created_at"))
        browse_name = name.replace("/", "|")
        datasource_urn = self._resolve_datasource(_datasource_keys(definition))
        if _datasource_keys(definition) and datasource_urn is None:
            self.report.warning(
                title="Tableau Pulse metric has no datasource lineage",
                message=(
                    "The Pulse datasource id did not match an ingested Tableau "
                    "datasource, so lineage was not created."
                ),
                context=definition_id,
            )
        model_aspects: List[Any] = [
            StatusClass(removed=False),
            SemanticModelInfoClass(
                name=name,
                description=_field(metadata, "description"),
                created=created,
                lastModified=created,
                nativeDefinition=_native_definition(definition),
            ),
            self._platform_instance(platform_urn),
            SubTypesClass(typeNames=[_DEFINITION_SUBTYPE]),
            self._browse_path(browse_name),
        ]
        if datasource_urn:
            model_aspects.append(
                UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=datasource_urn,
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        )
                    ]
                )
            )
        if self.config.extract_pulse_metric_definition_properties:
            property_rows = _definition_property_rows(definition)
            yield from self._property_definitions(property_rows)
            properties = self._structured_properties(property_rows)
            if properties is not None:
                model_aspects.append(properties)
        yield from _workunits(model_urn, model_aspects)
        self.report.num_pulse_definitions += 1

        for metric in metrics:
            if not isinstance(metric, dict):
                continue
            yield from self._emit_metric(
                definition,
                definition_id,
                name,
                model_urn,
                platform_urn,
                metric,
                datasource_urn,
            )

    def _emit_metric(
        self,
        definition: Dict[str, Any],
        definition_id: str,
        definition_name: str,
        model_urn: str,
        platform_urn: str,
        metric: Dict[str, Any],
        definition_datasource_urn: Optional[str],
    ) -> Iterable[MetadataWorkUnit]:
        metric_id = _field(metric, "id")
        if not metric_id:
            self.report.warning(
                title="Skipped Tableau Pulse metric",
                message="A Pulse metric had no id and was not ingested.",
                context=definition_name,
            )
            return
        metric_id = str(metric_id)
        metric_urn = str(
            MetricUrn(
                platform=platform_urn,
                path=f"{self._namespace()}.{definition_id}",
                id=metric_id,
            )
        )
        created = _audit_stamp(
            _field(_as_dict(_field(definition, "metadata")), "created_at")
        )
        browse_name = definition_name.replace("/", "|")
        datasource_urn = (
            self._resolve_datasource(_datasource_keys(definition, metric))
            or definition_datasource_urn
        )
        yield from _workunits(
            metric_urn,
            [
                StatusClass(removed=False),
                MetricInfoClass(
                    name=_metric_name(definition_name, metric),
                    description=_metric_description(definition, metric),
                    created=created,
                    lastModified=created,
                    expression=_metric_expression(definition),
                    semanticModel=model_urn,
                ),
                self._platform_instance(platform_urn),
                # Emit relationships with no parent so the metric is a root in
                # the Metrics sidebar (hasParentMetric indexes as false).
                MetricRelationshipsClass(derivedFrom=[]),
                self._metric_upstreams(datasource_urn, definition, metric),
                self._browse_path(browse_name, model_urn),
            ],
        )
        self.report.num_pulse_metrics += 1

    def _property_definitions(
        self, rows: Sequence[Tuple[str, str]]
    ) -> Iterable[MetadataWorkUnit]:
        for path, _value in rows:
            qualified_name = _property_qualified_name(path)
            if qualified_name in self._defined_properties:
                continue
            self._defined_properties.add(qualified_name)
            definition_mcp = MetadataChangeProposalWrapper(
                entityUrn=f"urn:li:structuredProperty:{qualified_name}",
                aspect=StructuredPropertyDefinitionClass(
                    qualifiedName=qualified_name,
                    displayName=_property_display_name(path),
                    description=f"Tableau Pulse metric definition field {path}.",
                    valueType=_STRING_VALUE_TYPE,
                    entityTypes=[_SEMANTIC_MODEL_TYPE],
                ),
            )
            if self.graph is not None:
                try:
                    self.graph.emit_mcp(definition_mcp, emit_mode=EmitMode.SYNC_PRIMARY)
                    continue
                except Exception as exc:
                    self.report.warning(
                        title="Failed to register Tableau Pulse property",
                        message=(
                            "The Pulse field is still emitted as a property. "
                            "DataHub accepts the value once this property exists."
                        ),
                        context=qualified_name,
                        exc=exc,
                    )
            yield definition_mcp.as_workunit()

    def _structured_properties(
        self, rows: Sequence[Tuple[str, str]]
    ) -> Optional[StructuredPropertiesClass]:
        assignments: List[StructuredPropertyValueAssignmentClass] = []
        for path, value in rows:
            limited = _limit_property_value(value)
            if limited != value:
                self.report.warning(
                    title="Truncated Tableau Pulse property value",
                    message=(
                        "A Pulse definition field was longer than a property "
                        "value can store, so it was shortened."
                    ),
                    context=path,
                )
            assignments.append(
                StructuredPropertyValueAssignmentClass(
                    propertyUrn=f"urn:li:structuredProperty:{_property_qualified_name(path)}",
                    values=[limited],
                )
            )
        if not assignments:
            return None
        return StructuredPropertiesClass(properties=assignments)

    def _resolve_datasource(self, keys: Sequence[str]) -> Optional[str]:
        for key in keys:
            urn = self.datasource_urns.get(key)
            if urn:
                return urn
        return None

    def _metric_upstreams(
        self,
        datasource_urn: Optional[str],
        definition: Dict[str, Any],
        metric: Dict[str, Any],
    ) -> MetricUpstreamsClass:
        if not datasource_urn:
            return MetricUpstreamsClass(datasetUpstreams=[], fieldUpstreams=[])
        known_fields = self.datasource_fields.get(datasource_urn, set())
        folded = {field.lower(): field for field in known_fields}
        field_urns: List[str] = []
        for name in _lineage_field_names(definition, metric):
            field_path = name if name in known_fields else folded.get(name.lower())
            if not field_path:
                continue
            field_urn = str(SchemaFieldUrn(datasource_urn, field_path))
            if field_urn not in field_urns:
                field_urns.append(field_urn)
        return MetricUpstreamsClass(
            datasetUpstreams=[EdgeClass(destinationUrn=datasource_urn)],
            fieldUpstreams=[
                EdgeClass(destinationUrn=field_urn) for field_urn in field_urns
            ],
        )

    def _browse_path(
        self, name: str, model_urn: Optional[str] = None
    ) -> BrowsePathsV2Class:
        entries = [
            BrowsePathEntryClass(id=part)
            for part in self.browse_prefix.strip("/").split("/")
            if part
        ]
        entries.append(BrowsePathEntryClass(id="Pulse"))
        entries.append(BrowsePathEntryClass(id=name, urn=model_urn))
        return BrowsePathsV2Class(path=entries)

    def _platform_instance(self, platform_urn: str) -> DataPlatformInstanceClass:
        return DataPlatformInstanceClass(
            platform=platform_urn,
            instance=(
                builder.make_dataplatform_instance_urn(
                    self.platform, self.config.platform_instance
                )
                if self.config.platform_instance
                else None
            ),
        )

    def _list(self, path: str, list_key: str) -> Iterable[Dict[str, Any]]:
        page_token: Optional[str] = None
        seen_tokens: Set[str] = set()
        page_size = min(self.config.page_size, _PULSE_PAGE_SIZE_CAP)
        while True:
            params: Dict[str, Any] = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token
            payload = self._get(path, params)
            for item in _as_list(_field(payload, list_key)):
                if isinstance(item, dict):
                    yield item
            next_token = _field(payload, "next_page_token")
            if not next_token:
                return
            next_token = str(next_token)
            if next_token in seen_tokens:
                self.report.warning(
                    title="Stopped Tableau Pulse pagination",
                    message="The Pulse API repeated a page token, so listing stopped.",
                    context=path,
                )
                return
            seen_tokens.add(next_token)
            page_token = next_token

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        token = getattr(self.server, "auth_token", None)
        if not token:
            raise RuntimeError("Tableau session has no auth token")
        url = f"{self.config.connect_uri}/api/-/pulse/{path}"
        response: Response = self.server._session.get(
            url,
            headers={"X-Tableau-Auth": token, "Accept": "application/json"},
            params=params,
            verify=self.config.ssl_verify,
            timeout=60,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RuntimeError(
                f"Pulse API {path} returned {response.status_code}"
            ) from exc
        body = response.json()
        if not isinstance(body, dict):
            raise RuntimeError(f"Pulse API {path} returned a non-object response")
        return body
