from starlette.testclient import TestClient

from datahub_ai_service.app import create_app
from datahub_ai_service.config import AiServiceConfig


def test_health_needs_no_credential_or_gms() -> None:
    # Nothing listens on this GMS URL: startup and /health must not need it.
    app = create_app(AiServiceConfig(gms_url="http://127.0.0.1:1"))

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
