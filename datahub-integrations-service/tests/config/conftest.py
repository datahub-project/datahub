"""Pytest configuration for config tests - mocks DataHub dependencies."""

import os
from unittest.mock import MagicMock, PropertyMock, patch

# Set environment variables before any imports to avoid network calls
os.environ.setdefault("DATAHUB_GMS_URL", "http://localhost:8080")
os.environ.setdefault("DATAHUB_GMS_TOKEN", "mock-token")

# Create a mock server config object with necessary attributes
mock_server_config = MagicMock()
mock_server_config.raw_config = {
    "datahubVersion": "0.0.0-test",
    "noCode": False,
    "baseUrl": "http://localhost:9002",
    "managedIngestion": {
        "enabled": True,
    },
}
mock_server_config.datahub_version = "0.0.0-test"
mock_server_config.no_code = False
mock_server_config.base_url = "http://localhost:9002"

# Patch the server_config property on DatahubRestEmitter to avoid network calls
server_config_patcher = patch(
    "datahub.emitter.rest_emitter.DatahubRestEmitter.server_config",
    new_callable=PropertyMock,
    return_value=mock_server_config,
)
server_config_patcher.start()

# Also patch fetch_server_config to prevent network calls
fetch_config_patcher = patch(
    "datahub.emitter.rest_emitter.DatahubRestEmitter.fetch_server_config",
    return_value=mock_server_config,
)
fetch_config_patcher.start()
