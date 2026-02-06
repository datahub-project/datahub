"""
Power BI XMLA Endpoint Client

This module provides connectivity to Power BI Premium datasets via XMLA endpoints.
XMLA (XML for Analysis) allows reading data model metadata when PBIX export fails
(e.g., for Premium Files datasets).

References:
- https://learn.microsoft.com/en-us/power-bi/enterprise/service-premium-connect-tools
- https://learn.microsoft.com/en-us/analysis-services/tmsl/tabular-model-scripting-language-tmsl-reference
"""

import json
import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)


class XmlaClient:
    """Client for connecting to Power BI XMLA endpoints."""

    XMLA_ENDPOINT = "https://api.powerbi.com/v1.0/myorg"
    NAMESPACE = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "xsd": "http://www.w3.org/2001/XMLSchema",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xmla": "urn:schemas-microsoft-com:xml-analysis",
        "xs": "http://www.w3.org/2001/XMLSchema",
    }

    def __init__(self, access_token: str, timeout: int = 30):
        """
        Initialize XMLA client.

        Args:
            access_token: OAuth2 access token with "Bearer " prefix
            timeout: Request timeout in seconds
        """
        self.access_token = access_token
        self.timeout = timeout

    def execute_tmsl(
        self, workspace_id: str, dataset_id: str, tmsl_command: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Execute a TMSL (Tabular Model Scripting Language) command.

        Args:
            workspace_id: Power BI workspace ID
            dataset_id: Power BI dataset ID
            tmsl_command: TMSL command as a dictionary

        Returns:
            Parsed JSON response or None on failure
        """
        try:
            # Build XMLA SOAP envelope
            soap_body = self._build_soap_envelope(tmsl_command)

            # Execute against the dataset's XMLA endpoint
            endpoint_url = f"{self.XMLA_ENDPOINT}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"

            headers = {
                "Authorization": self.access_token,
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": "urn:schemas-microsoft-com:xml-analysis:Execute",
            }

            logger.debug("Sending TMSL command to XMLA endpoint: %s", endpoint_url)
            logger.debug("TMSL command: %s", json.dumps(tmsl_command, indent=2))

            response = requests.post(
                endpoint_url,
                data=soap_body,
                headers=headers,
                timeout=self.timeout,
            )

            if response.status_code != 200:
                logger.warning(
                    "XMLA request failed with status %s: %s",
                    response.status_code,
                    response.text,
                )
                return None

            # Parse SOAP response
            return self._parse_soap_response(response.text)

        except Exception as e:
            logger.warning("Error executing TMSL command: %s", e, exc_info=True)
            return None

    def get_dataset_metadata(
        self, workspace_id: str, dataset_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get dataset metadata using TMSL.

        Extracts tables, columns, measures, relationships, hierarchies, and roles.

        Args:
            workspace_id: Power BI workspace ID
            dataset_id: Power BI dataset ID

        Returns:
            Dictionary containing dataset metadata or None on failure
        """
        logger.info(
            "Extracting dataset metadata via XMLA for dataset %s in workspace %s",
            dataset_id,
            workspace_id,
        )

        # We use the Power BI REST API executeQueries endpoint with DAX
        # to extract model metadata, as full XMLA over SOAP is complex
        return self._extract_metadata_via_dax(workspace_id, dataset_id)

    def _extract_metadata_via_dax(
        self, workspace_id: str, dataset_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Extract dataset metadata using DAX queries via executeQueries API.

        This is simpler than full XMLA/TMSL and works for Premium Files datasets.

        Args:
            workspace_id: Power BI workspace ID
            dataset_id: Power BI dataset ID

        Returns:
            Dictionary containing dataset metadata
        """
        try:
            # DAX queries to extract model metadata
            metadata_queries = {
                "tables": 'EVALUATE SELECTCOLUMNS(INFO.TABLES(), "Table", [Name], "IsHidden", [IsHidden], "Description", [Description])',
                "columns": 'EVALUATE SELECTCOLUMNS(INFO.COLUMNS(), "Table", [TableName], "Column", [ExplicitName], "DataType", [DataType], "IsHidden", [IsHidden], "Description", [Description])',
                "measures": 'EVALUATE SELECTCOLUMNS(INFO.MEASURES(), "Table", [TableName], "Measure", [Name], "Expression", [Expression], "IsHidden", [IsHidden], "Description", [Description])',
                "relationships": 'EVALUATE SELECTCOLUMNS(INFO.RELATIONSHIPS(), "FromTable", [FromTableName], "FromColumn", [FromColumnName], "ToTable", [ToTableName], "ToColumn", [ToColumnName], "CrossFilterDirection", [CrossFilterDirection], "Cardinality", [Cardinality])',
            }

            endpoint_url = f"{self.XMLA_ENDPOINT}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"

            headers = {
                "Authorization": self.access_token,
                "Content-Type": "application/json",
            }

            result: Dict[str, Any] = {
                "tables": [],
                "columns": [],
                "measures": [],
                "relationships": [],
            }

            # Execute each metadata query
            for query_name, dax_query in metadata_queries.items():
                logger.debug("Executing DAX metadata query: %s", query_name)

                payload = {
                    "queries": [{"query": dax_query}],
                    "serializerSettings": {"includeNulls": True},
                }

                response = requests.post(
                    endpoint_url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout,
                )

                if response.status_code == 200:
                    query_result = response.json()
                    # Extract rows from the result
                    if (
                        query_result
                        and "results" in query_result
                        and len(query_result["results"]) > 0
                    ):
                        tables = query_result["results"][0].get("tables", [])
                        if tables and len(tables) > 0:
                            rows = tables[0].get("rows", [])
                            result[query_name] = rows
                            logger.info(
                                "Extracted %s %s from dataset", len(rows), query_name
                            )
                else:
                    logger.warning(
                        "Failed to execute DAX query '%s': HTTP %s",
                        query_name,
                        response.status_code,
                    )
                    logger.debug("Response: %s", response.text)

            return result if any(result.values()) else None

        except Exception as e:
            logger.warning("Error extracting metadata via DAX: %s", e, exc_info=True)
            return None

    def _build_soap_envelope(self, tmsl_command: Dict[str, Any]) -> str:
        """Build SOAP envelope for XMLA request."""
        tmsl_json = json.dumps(tmsl_command)

        soap_envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
            <Command>
                <Statement>{tmsl_json}</Statement>
            </Command>
            <Properties>
                <PropertyList>
                    <Catalog>Model</Catalog>
                </PropertyList>
            </Properties>
        </Execute>
    </soap:Body>
</soap:Envelope>"""

        return soap_envelope

    def _parse_soap_response(self, response_xml: str) -> Optional[Dict[str, Any]]:
        """Parse SOAP response from XMLA endpoint."""
        try:
            root = ET.fromstring(response_xml)

            # Look for results in the SOAP body
            body = root.find(".//soap:Body", self.NAMESPACE)
            if body is None:
                logger.warning("No SOAP body found in response")
                return None

            # Extract result JSON from response
            result_elem = body.find(".//xmla:return", self.NAMESPACE)
            if result_elem is not None and result_elem.text:
                return json.loads(result_elem.text)

            return None

        except Exception as e:
            logger.warning("Error parsing SOAP response: %s", e, exc_info=True)
            return None


def create_xmla_client(access_token: str, timeout: int = 30) -> XmlaClient:
    """
    Factory function to create an XMLA client.

    Args:
        access_token: OAuth2 access token with "Bearer " prefix
        timeout: Request timeout in seconds

    Returns:
        Configured XmlaClient instance
    """
    return XmlaClient(access_token=access_token, timeout=timeout)
