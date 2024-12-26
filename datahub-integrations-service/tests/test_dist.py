from fastapi.testclient import TestClient

from datahub_integrations.server import app

client = TestClient(app)


def test_fetch_datahub_wheel() -> None:
    response = client.get(
        "/public/cf-pages/dc0584b7.datahub-wheels/artifacts/wheels/acryl_datahub_airflow_plugin-0.0.0.dev1-py3-none-any.whl"
    )
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/octet-stream"

    # The response is a .whl, which is a zip file.
    # Check that the first two bytes are the zip file signature.
    assert response.content[:2] == b"\x50\x4b"
