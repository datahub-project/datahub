# This script generates the OpenAPI spec for the server.
import json
import os
import unittest.mock
from typing import Any

os.environ["DEV_MODE_OVERRIDE_DATAHUB_FRONTEND_URL"] = "http://localhost:9002"

with unittest.mock.patch("datahub.ingestion.graph.client.DataHubGraph") as mock:
    os.environ["DATAHUB_GMS_PROTOCOL"] = "http"
    os.environ["DATAHUB_GMS_HOST"] = "example.com"
    os.environ["DATAHUB_GMS_PORT"] = "8080"

    # The app module initializes a global graph object. We need to mock that object
    # here so that it doesn't fail when testing it's connection.
    from datahub_integrations.server import app


def swizzle(input_dict: Any) -> Any:
    if isinstance(input_dict, dict):
        if "anyOf" in input_dict:
            # we replace union types with a string type blindly
            return {"type": "string"}
        else:
            return {k: swizzle(v) for k, v in input_dict.items()}
    elif isinstance(input_dict, list):
        return [swizzle(x) for x in input_dict]
    else:
        return input_dict


openapi = app.openapi()
openapi_preprocessed = json.loads(json.dumps(openapi))

os.makedirs("generated/openapi", exist_ok=True)

with open("generated/openapi/openapi.json", "w") as f:
    json.dump(swizzle(openapi_preprocessed), f, indent=2)
