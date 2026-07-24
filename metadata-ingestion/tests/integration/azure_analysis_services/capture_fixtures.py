import argparse
import pathlib
from typing import Dict

import yaml

from datahub.ingestion.source.azure_analysis_services import constants
from datahub.ingestion.source.azure_analysis_services.config import (
    AzureAnalysisServicesConfig,
)
from datahub.ingestion.source.azure_analysis_services.report import (
    AzureAnalysisServicesReport,
)
from datahub.ingestion.source.azure_analysis_services.xmla_client import XmlaClient

# Maps a token that appears in an outgoing XMLA request body to the fixture
# filename the captured response should be written to. Order matters: the more
# specific Discover request types are checked before the generic DMV SELECTs so
# a DISCOVER_XML_METADATA request is not mistaken for a rowset query.
_BODY_TOKEN_TO_FILE: Dict[str, str] = {
    constants.DISCOVER_XML_METADATA_REQUEST_TYPE: "metadata.xml",
    constants.DMV_CALC_DEPENDENCY: "calc_dependency.xml",
    constants.DMV_CATALOGS: "catalogs.xml",
    constants.DMV_MODEL: "model.xml",
    constants.DMV_TABLES: "tables.xml",
    constants.DMV_COLUMNS: "columns.xml",
    constants.DMV_MEASURES: "measures.xml",
    constants.DMV_PARTITIONS: "partitions.xml",
    constants.DMV_RELATIONSHIPS: "relationships.xml",
    constants.DMV_ROLES: "roles.xml",
    constants.DMV_DATA_SOURCES: "data_sources.xml",
}


def _install_recorder(client: XmlaClient, out_dir: pathlib.Path) -> None:
    original = client._post_soap

    def recording_post_soap(soap_action: str, inner_body: str) -> str:
        response_text = original(soap_action, inner_body)
        for token, filename in _BODY_TOKEN_TO_FILE.items():
            if token in inner_body:
                (out_dir / filename).write_text(response_text, encoding="utf-8")
                print(f"captured {token} -> {filename}")
                break
        return response_text

    # Reuses the client's real auth, endpoint resolution, and SOAP transport;
    # we only tee the raw response to disk.
    client._post_soap = recording_post_soap  # type: ignore[method-assign]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Capture raw XMLA responses from a real Azure Analysis Services / "
            "Power BI Premium endpoint into fixture files for the integration "
            "test. IMPORTANT: the captured XML contains your real catalog, "
            "table, column and connection-string names. Sanitize every file to "
            "generic placeholders before committing (see the confidentiality "
            "rules in AGENTS.md)."
        )
    )
    parser.add_argument(
        "--recipe",
        required=True,
        help="Path to a YAML file holding the source 'config' block (secrets "
        "may live here; keep it under /tmp and never commit it).",
    )
    parser.add_argument(
        "--catalog",
        default=None,
        help="Catalog/model to capture. Defaults to the first discovered model.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(pathlib.Path(__file__).parent / "setup"),
        help="Directory to write the captured fixture XML into.",
    )
    args = parser.parse_args()

    raw = yaml.safe_load(pathlib.Path(args.recipe).read_text(encoding="utf-8"))
    config_dict = raw.get("source", {}).get("config", raw)
    config = AzureAnalysisServicesConfig.model_validate(config_dict)

    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    client = XmlaClient(config, AzureAnalysisServicesReport())
    _install_recorder(client, out_dir)

    catalogs = client.discover_databases()
    if not catalogs:
        raise SystemExit("No catalogs discovered on the endpoint.")
    catalog: str = args.catalog or catalogs[0]
    print(f"capturing model: {catalog}")
    client.fetch_tabular_model(catalog)
    client.close()
    print(
        "Done. Review and sanitize the fixture files before committing, then "
        "regenerate the golden with:\n"
        "  pytest tests/integration/azure_analysis_services/test_aas_ingest.py "
        "--update-golden-files"
    )


if __name__ == "__main__":
    main()
