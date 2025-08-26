# This script generates the OpenAPI spec for the server.
import json
import os
import sys
import unittest.mock
from typing import Any

# Mock out some slow-to-import libraries to speed up the script.
sys.modules["duckdb"] = unittest.mock.MagicMock()
sys.modules["mlflow"] = unittest.mock.MagicMock()
sys.modules["mlflow.entities"] = unittest.mock.MagicMock()
sys.modules["mlflow.tracing"] = unittest.mock.MagicMock()

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
        if input_dict.get("name") == "lineage_direction":
            # For some `anyOf` types, it's just a union of a type and null.
            # This is because pydantic v2 does a better job of
            # distinguishing required but nullable from optional fields.
            # lineage_direction is the only place where this matters.
            assert "anyOf" in input_dict["schema"]
            input_dict["schema"] = {"$ref": "#/components/schemas/LineageDirection"}
            return input_dict
        elif "anyOf" in input_dict:
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
