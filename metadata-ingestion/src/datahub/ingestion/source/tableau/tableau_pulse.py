import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set
from urllib.parse import quote

import requests
from requests import Response
from tableauserverclient import Server

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    NumberTypeClass,
    OtherSchema,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    SubTypesClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.tableau.tableau import (
        TableauConfig,
        TableauSourceReport,
    )

logger = logging.getLogger(__name__)

_PULSE_PAGE_SIZE_CAP = 1000
_DEFINITIONS_PATH = "definitions"


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


def _metric_display_name(metric: Dict[str, Any]) -> str:
    specification = _as_dict(_field(metric, "specification"))
    parts: List[str] = []
    for pulse_filter in _as_list(_field(specification, "filters")):
        if not isinstance(pulse_filter, dict):
            continue
        field_name = _field(pulse_filter, "field")
        values = _filter_values(pulse_filter)
        if field_name and values:
            parts.append(f"{field_name}={','.join(values)}")
        elif field_name:
            parts.append(str(field_name))
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


def _metric_description(metric: Dict[str, Any], field_path: str) -> str:
    metric_id = _field(metric, "id")
    pieces = [f"Tableau Pulse metric {metric_id}."]
    if _field(metric, "is_default"):
        pieces.append("Default metric for this definition.")
    if field_path != metric_id:
        pieces.append(f"Shown as column {field_path}.")
    return " ".join(pieces)


def _unique_field_path(name: str, metric_id: str, used: Set[str]) -> str:
    cleaned = name.replace(".", "_").replace("/", "|").strip() or metric_id
    candidate = cleaned
    if candidate in used:
        suffix = metric_id[:8] if metric_id else str(len(used))
        candidate = f"{cleaned}_{suffix}"
    used.add(candidate)
    return candidate


class TableauPulseExtractor:
    """Lists Pulse metric definitions and emits each as a dataset.

    Metrics returned for a definition become schema fields on that dataset.
    """

    def __init__(
        self,
        config: "TableauConfig",
        server: Server,
        report: "TableauSourceReport",
        platform: str,
        browse_prefix: str,
    ) -> None:
        self.config = config
        self.server = server
        self.report = report
        self.platform = platform
        self.browse_prefix = browse_prefix

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
                message="The metric definition was ingested without columns.",
                context=definition_id,
                exc=exc,
            )
            metrics = []

        dataset_urn = builder.make_dataset_urn_with_platform_instance(
            self.platform,
            definition_id,
            self.config.platform_instance,
            self.config.env,
        )
        custom_properties: Dict[str, str] = {"definitionId": definition_id}
        datasource = _as_dict(
            _field(_as_dict(_field(definition, "specification")), "datasource")
        )
        datasource_id = _field(datasource, "id")
        if datasource_id:
            custom_properties["datasourceId"] = str(datasource_id)

        browse_name = name.replace("/", "|")
        snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[
                DataPlatformInstanceClass(
                    platform=builder.make_data_platform_urn(self.platform),
                    instance=(
                        builder.make_dataplatform_instance_urn(
                            self.platform, self.config.platform_instance
                        )
                        if self.config.platform_instance
                        else None
                    ),
                ),
                DatasetPropertiesClass(
                    name=name,
                    description=_field(metadata, "description"),
                    customProperties=custom_properties,
                ),
                BrowsePathsClass(paths=[f"{self.browse_prefix}/Pulse/{browse_name}"]),
            ],
        )
        schema = self._schema(name, metrics)
        if schema is not None:
            snapshot.aspects.append(schema)

        self.report.num_pulse_definitions += 1
        yield MetadataWorkUnit(
            id=dataset_urn, mce=MetadataChangeEvent(proposedSnapshot=snapshot)
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.TABLEAU_METRIC_DEFINITION]),
        ).as_workunit()

    def _schema(self, schema_name: str, metrics: List[Any]) -> Optional[SchemaMetadata]:
        used: Set[str] = set()
        fields: List[SchemaField] = []
        for metric in metrics:
            if not isinstance(metric, dict):
                continue
            metric_id = _field(metric, "id")
            if not metric_id:
                self.report.warning(
                    title="Skipped Tableau Pulse metric",
                    message="A Pulse metric had no id and was not added as a column.",
                    context=schema_name,
                )
                continue
            metric_id = str(metric_id)
            field_path = _unique_field_path(
                _metric_display_name(metric), metric_id, used
            )
            fields.append(
                SchemaField(
                    fieldPath=field_path,
                    type=SchemaFieldDataType(type=NumberTypeClass()),
                    nativeDataType="metric",
                    description=_metric_description(metric, field_path),
                )
            )
            self.report.num_pulse_metrics += 1
        if not fields:
            return None
        return SchemaMetadata(
            schemaName=schema_name,
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            hash="",
            platformSchema=OtherSchema(rawSchema=""),
            fields=fields,
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
