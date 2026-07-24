import logging
import time
from dataclasses import dataclass
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
    AasCalcDependency,
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


@dataclass
class _ColumnBuild:
    # Columns grouped by their owning table plus an id->name index used to
    # resolve relationship foreign keys.
    by_table: Dict[int, List[AasColumn]]
    name_by_id: Dict[int, str]


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
        try:
            self._token = self._credential.get_token(self._scope)
        except Exception as e:
            # azure-identity raises its own exception hierarchy (auth failed,
            # credential unavailable, ...). Route it through XmlaClientError so
            # an expired/rotated secret degrades to a report failure instead of
            # crashing the pipeline with a stack trace.
            raise XmlaClientError(f"Failed to acquire bearer token: {e}") from e
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
        try:
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
        except requests.RequestException as e:
            raise XmlaClientError(f"clusterResolve request failed: {e}") from e
        except ValueError as e:
            raise XmlaClientError(f"clusterResolve returned invalid JSON: {e}") from e
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
        envelope = constants.SOAP_ENVELOPE_TEMPLATE.format(
            namespace=constants.SOAP_ENVELOPE_NAMESPACE, body=inner_body
        )
        try:
            # Resolving the cluster URL and minting the token both hit the
            # network; keep them inside the try so their failures surface as
            # XmlaClientError alongside the request itself.
            url = self._get_xmla_url()
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
                constants.XML_ELEMENT_TEMPLATE.format(
                    tag=constants.PROPERTY_CATALOG, value=escape(catalog)
                )
            )
        parts.append(
            constants.XML_ELEMENT_TEMPLATE.format(
                tag=constants.PROPERTY_FORMAT, value=constants.FORMAT_TABULAR
            )
        )
        if content:
            parts.append(
                constants.XML_ELEMENT_TEMPLATE.format(
                    tag=constants.PROPERTY_CONTENT, value=content
                )
            )
        return constants.PROPERTY_LIST_TEMPLATE.format(properties="".join(parts))

    # --- Public API -------------------------------------------------------

    def _query_dmv(
        self, dmv: str, catalog: Optional[str] = None
    ) -> List[Dict[str, str]]:
        statement = constants.SELECT_ALL_TEMPLATE.format(dmv=dmv)
        inner = constants.EXECUTE_REQUEST_TEMPLATE.format(
            namespace=constants.XMLA_NAMESPACE,
            statement=escape(statement),
            properties=self._properties_xml(catalog),
        )
        response_text = self._post_soap(constants.SOAP_ACTION_EXECUTE, inner)
        return self._parse_rowset(response_text, dmv=dmv, catalog=catalog)

    def discover_databases(self) -> List[str]:
        rows = self._query_dmv(constants.DMV_CATALOGS)
        names: List[str] = []
        for row in rows:
            name = row.get(constants.ROW_KEY_CATALOG_NAME) or row.get(
                constants.ROW_KEY_NAME
            )
            if name:
                names.append(name)
        return names

    def get_model_definition(self, catalog: str) -> Optional[str]:
        inner = constants.DISCOVER_METADATA_REQUEST_TEMPLATE.format(
            namespace=constants.XMLA_NAMESPACE,
            request_type=constants.DISCOVER_XML_METADATA_REQUEST_TYPE,
            database_id_tag=constants.DATABASE_ID_RESTRICTION,
            database_id=escape(catalog),
            expansion_tag=constants.OBJECT_EXPANSION_RESTRICTION,
            expansion=constants.OBJECT_EXPANSION_EXPAND_FULL,
            properties=self._properties_xml(catalog),
        )
        response_text = self._post_soap(constants.SOAP_ACTION_DISCOVER, inner)
        return self._extract_metadata_definition(response_text)

    # --- Response parsing -------------------------------------------------

    @staticmethod
    def _find_root_element(envelope: Element) -> Optional[Element]:
        # Walk to the ``root`` element holding the rowset regardless of the
        # SOAP/response namespaces, which differ between Execute and Discover.
        for element in envelope.iter():
            if _strip_ns(element.tag) == constants.ELEMENT_ROOT:
                return element
        return None

    @staticmethod
    def _raise_on_soap_fault(envelope: Element) -> None:
        for element in envelope.iter():
            if _strip_ns(element.tag) != constants.ELEMENT_FAULT:
                continue
            fault_strings: List[str] = []
            for child in element.iter():
                if (
                    _strip_ns(child.tag)
                    in (
                        constants.ELEMENT_FAULT_STRING,
                        constants.ELEMENT_FAULT_DESCRIPTION,
                    )
                    and child.text
                ):
                    fault_strings.append(child.text)
                # Analysis Services Discover faults carry the human-readable
                # message as an ``Error@Description`` attribute rather than text.
                description_attr = child.get(constants.ELEMENT_FAULT_DESCRIPTION)
                if description_attr:
                    fault_strings.append(description_attr)
            raise XmlaClientError(
                "XMLA endpoint returned a SOAP fault: "
                + "; ".join(fault_strings or ["unknown fault"])
            )

    def _parse_rowset(
        self,
        response_text: str,
        dmv: Optional[str] = None,
        catalog: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        # Parse the response once and share the tree between the fault check and
        # the rowset walk.
        envelope = _parse_xml(response_text)
        self._raise_on_soap_fault(envelope)
        root = self._find_root_element(envelope)
        if root is None:
            # A fault-free 200 that carries no rowset (gateway HTML, redirect,
            # empty <return/>) would otherwise look identical to a legitimately
            # empty model. Surface it so operators can tell the two apart.
            self.report.warning(
                title="Unrecognized XMLA response",
                message="A response parsed but contained no rowset; treated as empty.",
                context=(
                    f"dmv={dmv}, catalog={catalog}: "
                    f"{response_text[: constants.ROOTLESS_SNIPPET_LEN]}"
                ),
            )
            return []
        rows: List[Dict[str, str]] = []
        for element in root:
            if _strip_ns(element.tag) != constants.ELEMENT_ROW:
                continue
            row: Dict[str, str] = {}
            for cell in element:
                row[_strip_ns(cell.tag)] = cell.text or ""
            rows.append(row)
        return rows

    def _extract_metadata_definition(self, response_text: str) -> Optional[str]:
        envelope = _parse_xml(response_text)
        self._raise_on_soap_fault(envelope)
        root = self._find_root_element(envelope)
        if root is None:
            return None
        # DISCOVER_XML_METADATA returns a single row whose METADATA cell holds
        # the serialized TMSL/ASSL definition — either as inline text or, more
        # commonly, as a nested XML element.
        for element in root.iter():
            if _strip_ns(element.tag) in (
                constants.ELEMENT_METADATA,
                constants.ELEMENT_METADATA_ALT,
            ):
                if len(element):
                    return tostring(element[0], encoding="unicode")
                return element.text
        return None

    # --- Typed fetch + assembly ------------------------------------------

    def _fetch_rows(
        self, dmv: str, model_cls: Type[_RowT], catalog: Optional[str] = None
    ) -> List[_RowT]:
        # One bad DMV must not abort the whole model, and one malformed row must
        # not abort the DMV — both are downgraded to warnings so ingestion
        # continues with partial metadata.
        try:
            raw_rows = self._query_dmv(dmv, catalog)
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

    def _build_columns(self, column_rows: List[AasColumnRow]) -> _ColumnBuild:
        by_table: Dict[int, List[AasColumn]] = {}
        name_by_id: Dict[int, str] = {}
        for col in column_rows:
            if col.column_type == constants.ColumnType.ROW_NUMBER:
                continue
            name = col.resolved_name
            if not name:
                continue
            name_by_id[col.id] = name
            by_table.setdefault(col.table_id, []).append(
                AasColumn(
                    name=name,
                    data_type=col.resolved_data_type,
                    datahub_data_type=tom_data_type_to_datahub(col.resolved_data_type),
                    is_calculated=col.column_type == constants.ColumnType.CALCULATED,
                    expression=col.expression,
                    description=col.description,
                    is_hidden=col.is_hidden,
                    display_folder=col.display_folder,
                )
            )
        return _ColumnBuild(by_table=by_table, name_by_id=name_by_id)

    @staticmethod
    def _build_measures(
        measure_rows: List[AasMeasureRow],
    ) -> Dict[int, List[AasMeasure]]:
        by_table: Dict[int, List[AasMeasure]] = {}
        for measure in measure_rows:
            by_table.setdefault(measure.table_id, []).append(
                AasMeasure(
                    name=measure.name,
                    expression=measure.expression,
                    description=measure.description,
                    format_string=measure.format_string,
                    display_folder=measure.display_folder,
                    is_hidden=measure.is_hidden,
                )
            )
        return by_table

    @staticmethod
    def _build_partitions(
        partition_rows: List[AasPartitionRow],
    ) -> Dict[int, List[AasPartition]]:
        by_table: Dict[int, List[AasPartition]] = {}
        for partition in partition_rows:
            by_table.setdefault(partition.table_id, []).append(
                AasPartition(
                    name=partition.name,
                    query_definition=partition.query_definition,
                    partition_type=partition.partition_type,
                    data_source_id=partition.data_source_id,
                )
            )
        return by_table

    @staticmethod
    def _build_tables(
        table_rows: List[AasTableRow],
        columns_by_table: Dict[int, List[AasColumn]],
        measures_by_table: Dict[int, List[AasMeasure]],
        partitions_by_table: Dict[int, List[AasPartition]],
    ) -> List[AasTable]:
        tables: List[AasTable] = []
        for table_row in table_rows:
            partitions = partitions_by_table.get(table_row.id, [])
            is_calculated = any(
                p.partition_type == constants.PartitionType.CALCULATED
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
        return tables

    def _build_relationships(
        self,
        relationship_rows: List[AasRelationshipRow],
        table_name_by_id: Dict[int, str],
        column_name_by_id: Dict[int, str],
        catalog: str,
    ) -> List[AasRelationship]:
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
        return relationships

    def _build_calc_dependencies(self, catalog: str) -> List[AasCalcDependency]:
        if not self.config.extract_column_level_lineage:
            return []
        rows = self._fetch_rows(
            constants.DMV_CALC_DEPENDENCY, AasCalcDependencyRow, catalog
        )
        dependencies: List[AasCalcDependency] = []
        for row in rows:
            # Only rows with a complete object/reference pair carry a usable
            # edge; incomplete rows (e.g. table-level dependencies) are dropped.
            if not (row.object_type and row.table and row.object_name):
                continue
            if not (
                row.referenced_object_type
                and row.referenced_table
                and row.referenced_object
            ):
                continue
            dependencies.append(
                AasCalcDependency(
                    object_type=row.object_type,
                    table=row.table,
                    object_name=row.object_name,
                    referenced_object_type=row.referenced_object_type,
                    referenced_table=row.referenced_table,
                    referenced_object=row.referenced_object,
                )
            )
        return dependencies

    def _fetch_model_definition(self, catalog: str) -> Optional[str]:
        if not self.config.extract_model_definition:
            return None
        try:
            return self.get_model_definition(catalog)
        except XmlaClientError as e:
            self.report.model_definition_failures += 1
            self.report.warning(
                title="Model definition unavailable",
                message="Could not retrieve the TMSL model definition.",
                context=f"catalog={catalog}",
                exc=e,
            )
            return None

    def fetch_tabular_model(self, catalog: str) -> AasTabularModel:
        model_rows = self._fetch_rows(constants.DMV_MODEL, AasModelRow, catalog)
        model_row = model_rows[0] if model_rows else None

        table_rows = self._fetch_rows(constants.DMV_TABLES, AasTableRow, catalog)
        column_build = self._build_columns(
            self._fetch_rows(constants.DMV_COLUMNS, AasColumnRow, catalog)
        )
        measures_by_table = self._build_measures(
            self._fetch_rows(constants.DMV_MEASURES, AasMeasureRow, catalog)
        )
        partitions_by_table = self._build_partitions(
            self._fetch_rows(constants.DMV_PARTITIONS, AasPartitionRow, catalog)
        )
        relationship_rows = self._fetch_rows(
            constants.DMV_RELATIONSHIPS, AasRelationshipRow, catalog
        )

        table_name_by_id: Dict[int, str] = {t.id: t.name for t in table_rows}
        tables = self._build_tables(
            table_rows, column_build.by_table, measures_by_table, partitions_by_table
        )
        relationships = self._build_relationships(
            relationship_rows, table_name_by_id, column_build.name_by_id, catalog
        )

        roles = [
            AasRole(
                name=r.name,
                description=r.description,
                model_permission=r.model_permission,
            )
            for r in self._fetch_rows(constants.DMV_ROLES, AasRoleRow, catalog)
        ]
        data_sources = [
            AasDataSource(name=d.name, connection_string=d.connection_string)
            for d in self._fetch_rows(
                constants.DMV_DATA_SOURCES, AasDataSourceRow, catalog
            )
        ]

        return AasTabularModel(
            catalog=catalog,
            name=model_row.name if model_row else catalog,
            description=model_row.description if model_row else None,
            culture=model_row.culture if model_row else None,
            tables=tables,
            relationships=relationships,
            roles=roles,
            data_sources=data_sources,
            calc_dependencies=self._build_calc_dependencies(catalog),
            definition=self._fetch_model_definition(catalog),
        )

    def test_connection(self) -> None:
        self.discover_databases()

    def close(self) -> None:
        self._session.close()
