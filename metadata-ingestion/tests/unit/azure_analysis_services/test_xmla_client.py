from typing import Dict, List, Optional
from unittest import mock

import pytest

from datahub.ingestion.source.azure_analysis_services import constants
from datahub.ingestion.source.azure_analysis_services.config import (
    AzureAnalysisServicesConfig,
)
from datahub.ingestion.source.azure_analysis_services.models import (
    AasRelationshipRow,
    AasTableRow,
)
from datahub.ingestion.source.azure_analysis_services.report import (
    AzureAnalysisServicesReport,
)
from datahub.ingestion.source.azure_analysis_services.xmla_client import (
    XmlaClient,
    XmlaClientError,
)

_POWERBI_SERVER = "powerbi://api.powerbi.com/v1.0/myorg/salesws"


def _powerbi_client() -> XmlaClient:
    config = AzureAnalysisServicesConfig.model_validate(
        {
            "server": _POWERBI_SERVER,
            "tenant_id": "t",
            "client_id": "c",
            "client_secret": "s",
        }
    )
    return XmlaClient(config, AzureAnalysisServicesReport())


_ROWSET = """<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <ExecuteResponse xmlns="urn:schemas-microsoft-com:xml-analysis">
      <return>
        <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset">
          <row><ID>1</ID><Name>Sales</Name></row>
          <row><ID>2</ID><Name>Product</Name></row>
        </root>
      </return>
    </ExecuteResponse>
  </soap:Body>
</soap:Envelope>"""

_FAULT = """<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <soap:Fault>
      <faultstring>Access denied</faultstring>
    </soap:Fault>
  </soap:Body>
</soap:Envelope>"""

_METADATA = """<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <DiscoverResponse xmlns="urn:schemas-microsoft-com:xml-analysis">
      <return>
        <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset">
          <row><METADATA>{"name":"Sales Model"}</METADATA></row>
        </root>
      </return>
    </DiscoverResponse>
  </soap:Body>
</soap:Envelope>"""


@pytest.fixture
def client() -> XmlaClient:
    config = AzureAnalysisServicesConfig.model_validate(
        {
            "server": "asazure://westeurope.asazure.windows.net/myserver",
            "tenant_id": "t",
            "client_id": "c",
            "client_secret": "s",
        }
    )
    return XmlaClient(config, AzureAnalysisServicesReport())


def test_parse_rowset(client: XmlaClient) -> None:
    rows = client._parse_rowset(_ROWSET)
    assert rows == [{"ID": "1", "Name": "Sales"}, {"ID": "2", "Name": "Product"}]


def test_soap_fault_raises(client: XmlaClient) -> None:
    with pytest.raises(XmlaClientError, match="Access denied"):
        client._parse_rowset(_FAULT)


def test_malformed_xml_raises(client: XmlaClient) -> None:
    # A truncated/invalid response is rejected as XmlaClientError (not a bare
    # parse error) so _fetch_rows can downgrade it to a warning and continue.
    with pytest.raises(XmlaClientError, match="Failed to parse"):
        client._parse_rowset("<not-valid-xml")


def test_extract_metadata_definition(client: XmlaClient) -> None:
    assert client._extract_metadata_definition(_METADATA) == '{"name":"Sales Model"}'


def test_fetch_tabular_model_assembly(client: XmlaClient) -> None:
    rows_by_dmv: Dict[str, List[Dict[str, str]]] = {
        constants.DMV_MODEL: [
            {"ID": "1", "Name": "Sales Model", "Description": "d", "Culture": "en-US"}
        ],
        constants.DMV_TABLES: [
            {"ID": "10", "Name": "Sales"},
            {"ID": "11", "Name": "Product"},
            {"ID": "12", "Name": "SalesSummary"},
        ],
        constants.DMV_COLUMNS: [
            {
                "ID": "100",
                "TableID": "10",
                "ExplicitName": "Amount",
                "ExplicitDataType": "8",
                "Type": "1",
            },
            {
                "ID": "101",
                "TableID": "10",
                "ExplicitName": "ProductKey",
                "ExplicitDataType": "6",
                "Type": "1",
            },
            {"ID": "102", "TableID": "10", "InferredName": "RowNo", "Type": "3"},
            {
                "ID": "110",
                "TableID": "11",
                "ExplicitName": "ProductKey",
                "ExplicitDataType": "6",
                "Type": "1",
            },
        ],
        constants.DMV_MEASURES: [
            {
                "ID": "200",
                "TableID": "10",
                "Name": "Total Sales",
                "Expression": "SUM(Sales[Amount])",
            },
        ],
        constants.DMV_PARTITIONS: [
            {
                "ID": "300",
                "TableID": "10",
                "Name": "p",
                "Type": "4",
                "QueryDefinition": "let Source = 1 in Source",
            },
            {
                "ID": "302",
                "TableID": "12",
                "Name": "c",
                "Type": "2",
                "QueryDefinition": "SUMMARIZE(Sales)",
            },
        ],
        constants.DMV_RELATIONSHIPS: [
            {
                "ID": "400",
                "IsActive": "true",
                "FromTableID": "10",
                "FromColumnID": "101",
                "ToTableID": "11",
                "ToColumnID": "110",
            },
        ],
        constants.DMV_ROLES: [
            {"ID": "500", "Name": "Reader", "ModelPermission": "1"},
        ],
        constants.DMV_DATA_SOURCES: [
            {"ID": "600", "Name": "SalesDB", "ConnectionString": "cs"},
        ],
        constants.DMV_CALC_DEPENDENCY: [],
    }

    def fake_query_dmv(dmv: str, catalog: Optional[str] = None) -> List[Dict[str, str]]:
        return rows_by_dmv.get(dmv, [])

    client._query_dmv = fake_query_dmv  # type: ignore[method-assign]
    client.get_model_definition = lambda catalog: "{}"  # type: ignore[method-assign]

    model = client.fetch_tabular_model("SalesModel")

    assert model.name == "Sales Model"
    tables = {t.name: t for t in model.tables}
    assert set(tables) == {"Sales", "Product", "SalesSummary"}

    # Row-number column is dropped; only Amount and ProductKey survive.
    assert [c.name for c in tables["Sales"].columns] == ["Amount", "ProductKey"]
    assert [m.name for m in tables["Sales"].measures] == ["Total Sales"]

    # A type-2 (Calculated / DAX) partition marks the table as calculated.
    assert tables["SalesSummary"].is_calculated is True
    assert tables["Sales"].is_calculated is False

    # Relationship foreign keys are resolved back to table/column names.
    assert len(model.relationships) == 1
    rel = model.relationships[0]
    assert (rel.from_table, rel.from_column) == ("Sales", "ProductKey")
    assert (rel.to_table, rel.to_column) == ("Product", "ProductKey")
    assert model.definition == "{}"


# --- Endpoint resolution --------------------------------------------------


def test_resolve_endpoint_asazure(client: XmlaClient) -> None:
    assert client._is_powerbi is False
    assert client._region == "westeurope"
    assert client._server_name == "myserver"
    assert "westeurope" in client._scope
    # asazure needs a cluster-resolve round-trip, so the URL is not known yet.
    assert client._xmla_url is None


def test_resolve_endpoint_powerbi() -> None:
    pbi = _powerbi_client()
    assert pbi._is_powerbi is True
    assert pbi._server_name == "salesws"
    assert pbi._scope == constants.POWERBI_XMLA_SCOPE
    assert pbi._xmla_url == "https://api.powerbi.com/v1.0/myorg/salesws"


def test_resolve_endpoint_rejects_garbage_server() -> None:
    # model_construct bypasses config validation, exercising the client's own
    # safety net for an endpoint that does not match either scheme.
    config = AzureAnalysisServicesConfig.model_construct(server="ftp://nope")
    with pytest.raises(XmlaClientError, match="server must be"):
        XmlaClient(config, AzureAnalysisServicesReport())


# --- Headers --------------------------------------------------------------


def test_headers_asazure_include_gateway_headers(client: XmlaClient) -> None:
    with mock.patch.object(client, "_bearer_token", return_value="t"):
        headers = client._headers(constants.SOAP_ACTION_EXECUTE)
    assert headers[constants.HEADER_XMLA_SERVER] == "myserver"
    assert headers[constants.HEADER_XMLA_NEGOTIATION_FLAGS] == (
        constants.XMLA_NEGOTIATION_FLAGS
    )


def test_headers_powerbi_omit_gateway_headers() -> None:
    pbi = _powerbi_client()
    with mock.patch.object(pbi, "_bearer_token", return_value="t"):
        headers = pbi._headers(constants.SOAP_ACTION_EXECUTE)
    assert constants.HEADER_XMLA_SERVER not in headers
    assert constants.HEADER_XMLA_NEGOTIATION_FLAGS not in headers


# --- Auth / cluster-resolve error handling --------------------------------


def test_bearer_token_failure_wrapped(client: XmlaClient) -> None:
    with mock.patch.object(client, "_build_credential") as build_credential:
        build_credential.return_value.get_token.side_effect = RuntimeError("auth down")
        with pytest.raises(XmlaClientError, match="Failed to acquire bearer token"):
            client._bearer_token()


def test_cluster_resolve_success(client: XmlaClient) -> None:
    fqdn = "cluster.westeurope.asazure.windows.net"
    resp = mock.Mock()
    resp.json.return_value = {constants.ASAZURE_CLUSTER_FQDN_KEY: fqdn}
    resp.raise_for_status.return_value = None
    with (
        mock.patch.object(client, "_bearer_token", return_value="t"),
        mock.patch.object(client._session, "post", return_value=resp),
    ):
        url = client._get_xmla_url()
    assert url == constants.ASAZURE_XMLA_URL.format(fqdn=fqdn)


def test_cluster_resolve_missing_fqdn_raises(client: XmlaClient) -> None:
    resp = mock.Mock()
    resp.json.return_value = {}
    resp.raise_for_status.return_value = None
    with (
        mock.patch.object(client, "_bearer_token", return_value="t"),
        mock.patch.object(client._session, "post", return_value=resp),
    ):
        with pytest.raises(XmlaClientError, match=constants.ASAZURE_CLUSTER_FQDN_KEY):
            client._get_xmla_url()


# --- Fault variants + rootless response -----------------------------------

_FAULT_DESCRIPTION = """<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <soap:Fault>
      <detail><Error Description="Database not found"/></detail>
    </soap:Fault>
  </soap:Body>
</soap:Envelope>"""

_UNKNOWN_FAULT = """<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <soap:Fault></soap:Fault>
  </soap:Body>
</soap:Envelope>"""

_ROOTLESS = """<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <ExecuteResponse xmlns="urn:schemas-microsoft-com:xml-analysis">
      <return/>
    </ExecuteResponse>
  </soap:Body>
</soap:Envelope>"""

_METADATA_NESTED = """<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <DiscoverResponse xmlns="urn:schemas-microsoft-com:xml-analysis">
      <return>
        <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset">
          <row><METADATA><Model name="Sales"><Table name="T"/></Model></METADATA></row>
        </root>
      </return>
    </DiscoverResponse>
  </soap:Body>
</soap:Envelope>"""


def test_soap_fault_description_variant(client: XmlaClient) -> None:
    with pytest.raises(XmlaClientError, match="Database not found"):
        client._parse_rowset(_FAULT_DESCRIPTION)


def test_soap_fault_without_message(client: XmlaClient) -> None:
    with pytest.raises(XmlaClientError, match="unknown fault"):
        client._parse_rowset(_UNKNOWN_FAULT)


def test_rootless_response_warns_and_returns_empty(client: XmlaClient) -> None:
    rows = client._parse_rowset(_ROOTLESS, dmv=constants.DMV_TABLES, catalog="cat")
    assert rows == []
    assert len(client.report.warnings) == 1


def test_extract_metadata_definition_nested_element(client: XmlaClient) -> None:
    definition = client._extract_metadata_definition(_METADATA_NESTED)
    assert definition is not None
    # The nested element is serialized back to XML (namespace prefixes and all).
    assert "Model" in definition
    assert 'name="Sales"' in definition


# --- Escaping -------------------------------------------------------------


def test_properties_xml_escapes_catalog() -> None:
    xml = XmlaClient._properties_xml("A & B")
    assert "A &amp; B" in xml
    assert "A & B" not in xml


def test_query_dmv_escapes_catalog(client: XmlaClient) -> None:
    captured: Dict[str, str] = {}

    def fake_post(soap_action: str, inner_body: str) -> str:
        captured["inner"] = inner_body
        return _ROWSET

    client._post_soap = fake_post  # type: ignore[method-assign]
    client._query_dmv(constants.DMV_TABLES, "A & B")
    assert "A &amp; B" in captured["inner"]


# --- _fetch_rows degradation ----------------------------------------------


def test_fetch_rows_dmv_failure_degrades(client: XmlaClient) -> None:
    def raising(dmv: str, catalog: Optional[str] = None) -> List[Dict[str, str]]:
        raise XmlaClientError("boom")

    client._query_dmv = raising  # type: ignore[method-assign]
    rows = client._fetch_rows(constants.DMV_TABLES, AasTableRow, "cat")
    assert rows == []
    assert len(client.report.warnings) == 1


def test_fetch_rows_skips_malformed_row(client: XmlaClient) -> None:
    def rows_fn(dmv: str, catalog: Optional[str] = None) -> List[Dict[str, str]]:
        # Second row is missing the required ID and must be skipped, not fatal.
        return [{"ID": "1", "Name": "Good"}, {"Name": "NoId"}]

    client._query_dmv = rows_fn  # type: ignore[method-assign]
    rows = client._fetch_rows(constants.DMV_TABLES, AasTableRow, "cat")
    assert [r.name for r in rows] == ["Good"]
    assert len(client.report.warnings) == 1


# --- Relationship resolution failure --------------------------------------


def test_build_relationships_drops_unresolvable(client: XmlaClient) -> None:
    rel = AasRelationshipRow(
        id=1,
        from_table_id=10,
        from_column_id=100,
        to_table_id=999,
        to_column_id=110,
    )
    relationships = client._build_relationships(
        [rel],
        {10: "Sales", 11: "Product"},
        {100: "ProductKey", 110: "ProductKey"},
        "cat",
    )
    assert relationships == []
    assert len(client.report.warnings) == 1
