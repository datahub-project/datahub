import pathlib
from typing import Dict, Optional
from unittest import mock

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.azure_analysis_services.xmla_client import XmlaClient
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-01 00:00:00"

_SETUP_DIR = pathlib.Path(__file__).parent / "setup"

# Route an XMLA request to the captured rowset fixture that answers it. The
# order matters: the more specific Discover request types are matched before
# the generic DMV SELECTs (dict preserves insertion order).
_ROUTES: Dict[str, str] = {
    "DISCOVER_XML_METADATA": "metadata.xml",
    "DISCOVER_CALC_DEPENDENCY": "calc_dependency.xml",
    "DBSCHEMA_CATALOGS": "catalogs.xml",
    "TMSCHEMA_MODEL": "model.xml",
    "TMSCHEMA_TABLES": "tables.xml",
    "TMSCHEMA_COLUMNS": "columns.xml",
    "TMSCHEMA_MEASURES": "measures.xml",
    "TMSCHEMA_PARTITIONS": "partitions.xml",
    "TMSCHEMA_RELATIONSHIPS": "relationships.xml",
    "TMSCHEMA_ROLES": "roles.xml",
    "TMSCHEMA_DATA_SOURCES": "data_sources.xml",
}


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None


def _fixture(name: str) -> str:
    return (_SETUP_DIR / name).read_text(encoding="utf-8")


def _post_side_effect(
    url: str,
    data: Optional[bytes] = None,
    headers: Optional[dict] = None,
    timeout: Optional[int] = None,
    verify: Optional[bool] = None,
    **kwargs: object,
) -> _FakeResponse:
    body = data.decode("utf-8") if isinstance(data, bytes) else str(data)
    for token, filename in _ROUTES.items():
        if token in body:
            return _FakeResponse(_fixture(filename))
    raise AssertionError(f"No fixture matched request body: {body[:200]}")


@pytest.mark.integration
def test_azure_analysis_services_ingest(pytestconfig, tmp_path):
    output_path = tmp_path / "aas_mces.json"
    golden_path = (
        pytestconfig.rootpath
        / "tests/integration/azure_analysis_services/golden/aas_mces_golden.json"
    )

    with (
        mock.patch.object(XmlaClient, "_bearer_token", return_value="test-token"),
        mock.patch("requests.Session.post", side_effect=_post_side_effect),
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "aas-test",
                "source": {
                    "type": "azure-analysis-services",
                    "config": {
                        "server": "powerbi://api.powerbi.com/v1.0/myorg/salesws",
                        "auth_type": "service_principal",
                        "tenant_id": "test-tenant",
                        "client_id": "test-client",
                        "client_secret": "test-secret",
                        "extract_lineage": False,
                        "extract_column_level_lineage": True,
                        "extract_model_definition": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_path)},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(golden_path),
    )
