import os
import unittest.mock

from datahub.testing.pytest_hooks import (  # noqa: F401
    load_golden_flags,
    pytest_addoption,
)

with unittest.mock.patch("datahub.ingestion.graph.client.DataHubGraph") as mock:
    os.environ["DATAHUB_GMS_PROTOCOL"] = "http"
    os.environ["DATAHUB_GMS_HOST"] = "example.com"
    os.environ["DATAHUB_GMS_PORT"] = "8080"

    # The app module initializes a global graph object. We need to mock that object
    # here so that it doesn't fail when testing it's connection.
    import datahub_integrations.app  # noqa: F401
