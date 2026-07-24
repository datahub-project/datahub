import logging
import time
from typing import Dict, List, Optional, Type, TypeVar
from xml.etree.ElementTree import (  # nosec B405 - only the Element / ParseError types and tostring (serialization) are imported; all parsing goes through defusedxml
    Element,
    ParseError,
    tostring,
)
from xml.sax.saxutils import escape

import defusedxml.ElementTree as DET
import requests
from azure.core.credentials import AccessToken, TokenCredential
from azure.identity import (
    ClientSecretCredential,
    DeviceCodeCredential,
    InteractiveBrowserCredential,
    UsernamePasswordCredential,
)
from defusedxml.common import DefusedXmlException
from pydantic import ValidationError

from datahub.ingestion.source.azure_analysis_services import constants
from datahub.ingestion.source.azure_analysis_services.config import (
    AasAuthType,
    AzureAnalysisServicesConfig,
)
from datahub.ingestion.source.azure_analysis_services.models import (
    AasCalcDependencyRow,
    AasColumn,
    AasColumnRow,
    AasDataSource,
    AasDataSourceRow,
    AasMeasure,
    AasMeasureRow,
    AasModelRow,
    AasPartition,
    AasPartitionRow,
    AasRelationship,
    AasRelationshipRow,
    AasRole,
    AasRoleRow,
    AasRow,
    AasTable,
    AasTableRow,
    AasTabularModel,
    tom_data_type_to_datahub,
)
from datahub.ingestion.source.azure_analysis_services.report import (
    AzureAnalysisServicesReport,
)

_RowT = TypeVar("_RowT", bound=AasRow)

logger = logging.getLogger(__name__)

# Refresh the bearer token this many seconds before its stated expiry to avoid
# racing a mid-request expiry on long DMV pulls.
_TOKEN_REFRESH_SKEW_SECONDS = 120


class XmlaClientError(Exception):
    pass


def _strip_ns(tag: str) -> str:
    # ElementTree qualifies tags as ``{namespace}local``; the rowset namespace
    # varies by engine version, so we match on the local name only.
    return tag.rsplit("}", 1)[-1]


def _parse_xml(response_text: str) -> Element:
    # All untrusted XML is parsed through defusedxml so entity-expansion and
    # external-entity attacks are rejected; malformed or malicious payloads are
    # surfaced as XmlaClientError so callers can degrade gracefully.
    try:
        return DET.fromstring(response_text)
    except (ParseError, DefusedXmlException) as e:
        raise XmlaClientError(f"Failed to parse XMLA response: {e}") from e


class XmlaClient:
    def __init__(
        self,
        config: AzureAnalysisServicesConfig,
        report: AzureAnalysisServicesReport,
    ) -> None:
        self.config = config
        self.report = report
        self._is_powerbi = False
        self._region: Optional[str] = None
        self._server_name: Optional[str] = None
        self._xmla_url: Optional[str] = None
        self._scope: str = ""
        self._token: Optional[AccessToken] = None
        self._credential: Optional[TokenCredential] = None

        self._resolve_endpoint()
        self._session = requests.Session()

        if not self.config.verify_ssl:
            self.report.warning(
                title="SSL verification disabled",
                message=(
                    "verify_ssl is False; XMLA responses are not certificate-verified. "
                    "Only use this against trusted endpoints."
                ),
            )

    # --- Endpoint / auth --------------------------------------------------

    @property
    def server_display_name(self) -> str:
        # The logical server/workspace name drives container URNs; fall back to
        # the raw endpoint if the regex somehow did not populate it.
        return self._server_name or self.config.server

    def _resolve_endpoint(self) -> None:
        asazure = constants.ASAZURE_ENDPOINT_RE.match(self.config.server)
        if asazure:
            self._region = asazure.group("region")
            self._server_name = asazure.group("server")
            self._scope = constants.ASAZURE_DEFAULT_SCOPE.format(region=self._region)
            return

        powerbi = constants.POWERBI_ENDPOINT_RE.match(self.config.server)
        if powerbi:
            self._is_powerbi = True
            self._server_name = powerbi.group("workspace")
            self._scope = constants.POWERBI_XMLA_SCOPE
            self._xmla_url = self.config.server.replace("powerbi://", "https://", 1)
            return

        raise XmlaClientError(
            "server must be an 'asazure://<region>.asazure.windows.net/<server>' or "
            "'powerbi://api.powerbi.com/v1.0/myorg/<workspace>' endpoint; "
            f"got: {self.config.server}"
        )

    def _build_credential(self) -> TokenCredential:
        auth = self.config.auth_type
        if auth == AasAuthType.SERVICE_PRINCIPAL:
            # tenant/client/secret presence is enforced by config validation.
            return ClientSecretCredential(
                tenant_id=self.config.tenant_id or "",
                client_id=self.config.client_id or "",
                client_secret=(
                    self.config.client_secret.get_secret_value()
                    if self.config.client_secret
                    else ""
                ),
            )
        if auth == AasAuthType.USERNAME_PASSWORD:
            return UsernamePasswordCredential(
                client_id=self.config.client_id or "",
                username=self.config.username or "",
                password=(
                    self.config.password.get_secret_value()
                    if self.config.password
                    else ""
                ),
                tenant_id=self.config.tenant_id,
            )
        if auth == AasAuthType.DEVICE_CODE:
            return DeviceCodeCredential(
                client_id=self.config.client_id or "",
                tenant_id=self.config.tenant_id,
            )
        return InteractiveBrowserCredential(
            client_id=self.config.client_id or "",
            tenant_id=self.config.tenant_id,
        )

    def _bearer_token(self) -> str:
        now = time.time()
        if self._token is not None and self._token.expires_on - now > (
            _TOKEN_REFRESH_SKEW_SECONDS
        ):
            return self._token.token
        if self._credential is None:
            self._credential = self._build_credential()
        self._token = self._credential.get_token(self._scope)
        logger.debug(
            "Acquired bearer token for scope %s (len=%d, expires_on=%s)",
            self._scope,
            len(self._token.token),
            self._token.expires_on,
        )
        return self._token.token

    def _cluster_resolve(self) -> str:
        # Azure AS routes raw SOAP through a per-tenant cluster; the logical
        # region host redirects to the concrete cluster FQDN.
        url = constants.ASAZURE_CLUSTER_RESOLVE_URL.format(region=self._region)
        resp = self._session.post(
            url,
            json={constants.ASAZURE_SERVER_NAME_KEY: self._server_name},
            headers={
                constants.HEADER_AUTHORIZATION: constants.BEARER_PREFIX
                + self._bearer_token(),
            },
            timeout=self.config.request_timeout,
            verify=self.config.verify_ssl,
        )
        resp.raise_for_status()
        payload = resp.json()
        fqdn = payload.get(constants.ASAZURE_CLUSTER_FQDN_KEY)
        if not fqdn:
            raise XmlaClientError(
                f"clusterResolve did not return {constants.ASAZURE_CLUSTER_FQDN_KEY}"
            )
        return constants.ASAZURE_XMLA_URL.format(fqdn=fqdn)

    def _get_xmla_url(self) -> str:
        if self._xmla_url is None:
            self._xmla_url = self._cluster_resolve()
        return self._xmla_url

    # --- SOAP transport ---------------------------------------------------

    def _headers(self, soap_action: str) -> Dict[str, str]:
        headers = {
            constants.HEADER_CONTENT_TYPE: constants.CONTENT_TYPE_XML,
            constants.HEADER_SOAP_ACTION: soap_action,
            constants.HEADER_USER_AGENT: constants.USER_AGENT,
            constants.HEADER_AUTHORIZATION: constants.BEARER_PREFIX
            + self._bearer_token(),
        }
        if not self._is_powerbi:
            headers[constants.HEADER_XMLA_SERVER] = self._server_name or ""
            headers[constants.HEADER_XMLA_NEGOTIATION_FLAGS] = (
                constants.XMLA_NEGOTIATION_FLAGS
            )
        return headers

    def _post_soap(self, soap_action: str, inner_body: str) -> str:
        envelope = (
            f'<soap:Envelope xmlns:soap="{constants.SOAP_ENVELOPE_NAMESPACE}">'
            f"<soap:Body>{inner_body}</soap:Body></soap:Envelope>"
        )
        url = self._get_xmla_url()
        try:
            resp = self._session.post(
                url,
                data=envelope.encode("utf-8"),
                headers=self._headers(soap_action),
                timeout=self.config.request_timeout,
                verify=self.config.verify_ssl,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            raise XmlaClientError(f"XMLA request failed: {e}") from e
        return resp.text

    @staticmethod
    def _properties_xml(catalog: Optional[str], content: Optional[str] = None) -> str:
        parts: List[str] = []
        if catalog:
            parts.append(
                f"<{constants.PROPERTY_CATALOG}>{escape(catalog)}"
                f"</{constants.PROPERTY_CATALOG}>"
            )
        parts.append(
            f"<{constants.PROPERTY_FORMAT}>{constants.FORMAT_TABULAR}"
            f"</{constants.PROPERTY_FORMAT}>"
        )
        if content:
            parts.append(
                f"<{constants.PROPERTY_CONTENT}>{content}"
                f"</{constants.PROPERTY_CONTENT}>"
            )
        return f"<Properties><PropertyList>{''.join(parts)}</PropertyList></Properties>"

    # --- Public API -------------------------------------------------------

    def query_dmv(
        self, dmv: str, catalog: Optional[str] = None
    ) -> List[Dict[str, str]]:
        statement = constants.SELECT_ALL_TEMPLATE.format(dmv=dmv)
        inner = (
            f'<Execute xmlns="{constants.XMLA_NAMESPACE}">'
            f"<Command><Statement>{escape(statement)}</Statement></Command>"
            f"{self._properties_xml(catalog)}"
            f"</Execute>"
        )
        response_text = self._post_soap(constants.SOAP_ACTION_EXECUTE, inner)
        return self._parse_rowset(response_text)

    def discover_databases(self) -> List[str]:
        rows = self.query_dmv(constants.DMV_CATALOGS)
        names: List[str] = []
        for row in rows:
            name = row.get("CATALOG_NAME") or row.get("Name")
            if name:
                names.append(name)
        return names

    def get_model_definition(self, catalog: str) -> Optional[str]:
        inner = (
            f'<Discover xmlns="{constants.XMLA_NAMESPACE}">'
            f"<RequestType>{constants.DISCOVER_XML_METADATA_REQUEST_TYPE}</RequestType>"
            f"<Restrictions><RestrictionList>"
            f"<{constants.DATABASE_ID_RESTRICTION}>{escape(catalog)}"
            f"</{constants.DATABASE_ID_RESTRICTION}>"
            f"<{constants.OBJECT_EXPANSION_RESTRICTION}>"
            f"{constants.OBJECT_EXPANSION_EXPAND_FULL}"
            f"</{constants.OBJECT_EXPANSION_RESTRICTION}>"
            f"</RestrictionList></Restrictions>"
            f"{self._properties_xml(catalog)}"
            f"</Discover>"
        )
        response_text = self._post_soap(constants.SOAP_ACTION_DISCOVER, inner)
        return self._extract_metadata_definition(response_text)

    # --- Response parsing -------------------------------------------------

    @staticmethod
    def _find_root(response_text: str) -> Optional[Element]:
        envelope = _parse_xml(response_text)
        # Walk to the ``root`` element holding the rowset regardless of the
        # SOAP/response namespaces, which differ between Execute and Discover.
        for element in envelope.iter():
            if _strip_ns(element.tag) == "root":
                return element
        return None

    def _parse_rowset(self, response_text: str) -> List[Dict[str, str]]:
        self._raise_on_soap_fault(response_text)
        root = self._find_root(response_text)
        if root is None:
            return []
        rows: List[Dict[str, str]] = []
        for element in root:
            if _strip_ns(element.tag) != "row":
                continue
            row: Dict[str, str] = {}
            for cell in element:
                row[_strip_ns(cell.tag)] = cell.text or ""
            rows.append(row)
        return rows

    def _extract_metadata_definition(self, response_text: str) -> Optional[str]:
        self._raise_on_soap_fault(response_text)
        root = self._find_root(response_text)
        if root is None:
            return None
        # DISCOVER_XML_METADATA returns a single row whose METADATA cell holds
        # the serialized TMSL/ASSL definition.
        for element in root.iter():
            if _strip_ns(element.tag) in ("METADATA", "Metadata"):
                if len(element):
                    return tostring(element[0], encoding="unicode")
                return element.text
        return None

    @staticmethod
    def _raise_on_soap_fault(response_text: str) -> None:
        envelope = _parse_xml(response_text)
        for element in envelope.iter():
            if _strip_ns(element.tag) == "Fault":
                fault_strings = [
                    child.text
                    for child in element.iter()
                    if _strip_ns(child.tag) in ("faultstring", "Description")
                    and child.text
                ]
                raise XmlaClientError(
                    "XMLA endpoint returned a SOAP fault: "
                    + "; ".join(fault_strings or ["unknown fault"])
                )

    # --- Typed fetch + assembly ------------------------------------------

    def _fetch_rows(
        self, dmv: str, model_cls: Type[_RowT], catalog: Optional[str] = None
    ) -> List[_RowT]:
        # One bad DMV must not abort the whole model, and one malformed row must
        # not abort the DMV — both are downgraded to warnings so ingestion
        # continues with partial metadata.
        try:
            raw_rows = self.query_dmv(dmv, catalog)
        except XmlaClientError as e:
            self.report.warning(
                title="DMV query failed",
                message="Could not read a metadata view; related metadata is skipped.",
                context=f"dmv={dmv}, catalog={catalog}",
                exc=e,
            )
            return []

        parsed: List[_RowT] = []
        for raw in raw_rows:
            try:
                parsed.append(model_cls.model_validate(raw))
            except ValidationError as e:
                self.report.warning(
                    title="Malformed DMV row",
                    message="Skipped a metadata row that failed validation.",
                    context=f"dmv={dmv}, catalog={catalog}",
                    exc=e,
                )
        return parsed

    def fetch_tabular_model(self, catalog: str) -> AasTabularModel:
        model_rows = self._fetch_rows(constants.DMV_MODEL, AasModelRow, catalog)
        model_row = model_rows[0] if model_rows else None

        table_rows = self._fetch_rows(constants.DMV_TABLES, AasTableRow, catalog)
        column_rows = self._fetch_rows(constants.DMV_COLUMNS, AasColumnRow, catalog)
        measure_rows = self._fetch_rows(constants.DMV_MEASURES, AasMeasureRow, catalog)
        partition_rows = self._fetch_rows(
            constants.DMV_PARTITIONS, AasPartitionRow, catalog
        )
        relationship_rows = self._fetch_rows(
            constants.DMV_RELATIONSHIPS, AasRelationshipRow, catalog
        )
        role_rows = self._fetch_rows(constants.DMV_ROLES, AasRoleRow, catalog)
        data_source_rows = self._fetch_rows(
            constants.DMV_DATA_SOURCES, AasDataSourceRow, catalog
        )

        columns_by_table: Dict[int, List[AasColumn]] = {}
        column_name_by_id: Dict[int, str] = {}
        for col in column_rows:
            if col.column_type == constants.COLUMN_TYPE_ROW_NUMBER:
                continue
            name = col.resolved_name
            if not name:
                continue
            column_name_by_id[col.id] = name
            columns_by_table.setdefault(col.table_id, []).append(
                AasColumn(
                    name=name,
                    data_type=col.resolved_data_type or 0,
                    datahub_data_type=tom_data_type_to_datahub(col.resolved_data_type),
                    is_calculated=col.column_type == constants.COLUMN_TYPE_CALCULATED,
                    expression=col.expression,
                    description=col.description,
                    is_hidden=col.is_hidden,
                    display_folder=col.display_folder,
                )
            )

        measures_by_table: Dict[int, List[AasMeasure]] = {}
        for measure in measure_rows:
            measures_by_table.setdefault(measure.table_id, []).append(
                AasMeasure(
                    name=measure.name,
                    expression=measure.expression,
                    description=measure.description,
                    format_string=measure.format_string,
                    display_folder=measure.display_folder,
                    is_hidden=measure.is_hidden,
                )
            )

        partitions_by_table: Dict[int, List[AasPartition]] = {}
        for partition in partition_rows:
            partitions_by_table.setdefault(partition.table_id, []).append(
                AasPartition(
                    name=partition.name,
                    query_definition=partition.query_definition,
                    partition_type=partition.partition_type,
                    data_source_id=partition.data_source_id,
                )
            )

        table_name_by_id: Dict[int, str] = {t.id: t.name for t in table_rows}
        tables: List[AasTable] = []
        for table_row in table_rows:
            partitions = partitions_by_table.get(table_row.id, [])
            is_calculated = any(
                p.partition_type == constants.PARTITION_TYPE_CALCULATED
                for p in partitions
            )
            tables.append(
                AasTable(
                    name=table_row.name,
                    description=table_row.description,
                    is_hidden=table_row.is_hidden,
                    is_calculated=is_calculated,
                    columns=columns_by_table.get(table_row.id, []),
                    measures=measures_by_table.get(table_row.id, []),
                    partitions=partitions,
                )
            )

        relationships: List[AasRelationship] = []
        for rel in relationship_rows:
            from_table = table_name_by_id.get(rel.from_table_id)
            to_table = table_name_by_id.get(rel.to_table_id)
            from_column = column_name_by_id.get(rel.from_column_id)
            to_column = column_name_by_id.get(rel.to_column_id)
            if from_table and to_table and from_column and to_column:
                relationships.append(
                    AasRelationship(
                        from_table=from_table,
                        from_column=from_column,
                        to_table=to_table,
                        to_column=to_column,
                        is_active=rel.is_active,
                    )
                )
            else:
                self.report.warning(
                    title="Unresolvable relationship",
                    message="A relationship referenced a table/column that was not found.",
                    context=f"catalog={catalog}, relationship_id={rel.id}",
                )

        roles = [
            AasRole(
                name=r.name,
                description=r.description,
                model_permission=r.model_permission,
            )
            for r in role_rows
        ]
        data_sources = [
            AasDataSource(name=d.name, connection_string=d.connection_string)
            for d in data_source_rows
        ]

        calc_dependencies: List[AasCalcDependencyRow] = []
        if self.config.extract_column_level_lineage:
            calc_dependencies = self._fetch_rows(
                constants.DMV_CALC_DEPENDENCY, AasCalcDependencyRow, catalog
            )

        definition: Optional[str] = None
        if self.config.extract_model_definition:
            try:
                definition = self.get_model_definition(catalog)
            except XmlaClientError as e:
                self.report.model_definition_failures += 1
                self.report.warning(
                    title="Model definition unavailable",
                    message="Could not retrieve the TMSL model definition.",
                    context=f"catalog={catalog}",
                    exc=e,
                )

        return AasTabularModel(
            catalog=catalog,
            name=model_row.name if model_row else catalog,
            description=model_row.description if model_row else None,
            culture=model_row.culture if model_row else None,
            tables=tables,
            relationships=relationships,
            roles=roles,
            data_sources=data_sources,
            calc_dependencies=calc_dependencies,
            definition=definition,
        )

    def test_connection(self) -> None:
        self.discover_databases()

    def close(self) -> None:
        self._session.close()
