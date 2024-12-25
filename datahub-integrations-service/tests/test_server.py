from fastapi.testclient import TestClient

from datahub_integrations.gen_ai.description_v2 import ShellEntityError
from datahub_integrations.server import app

client = TestClient(app)


def test_exception_handlers() -> None:
    @app.get("/tests/exception_handlers/raise_shell_entity_error")
    def raise_shell_entity_error() -> None:
        raise ShellEntityError("test message")

    response = client.get("/tests/exception_handlers/raise_shell_entity_error")

    # We do not want a 500 server error here.
    assert response.status_code == 400
    assert response.json() == {"message": "test message"}
