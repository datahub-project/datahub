from typing import Dict, List, Optional

import pytest

from datahub.ingestion.source.azure_analysis_services import constants
from datahub.ingestion.source.azure_analysis_services.config import (
    AzureAnalysisServicesConfig,
)
from datahub.ingestion.source.azure_analysis_services.report import (
    AzureAnalysisServicesReport,
)
from datahub.ingestion.source.azure_analysis_services.xmla_client import (
    XmlaClient,
    XmlaClientError,
)

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

    client.query_dmv = fake_query_dmv  # type: ignore[method-assign]
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
