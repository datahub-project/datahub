import os
import pathlib

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

import datahub_integrations as di
from datahub_integrations._logging_setup import LOGGING_SETUP_COMPLETE

assert LOGGING_SETUP_COMPLETE

# A global config and graph object that can be used by all routers.
if os.environ.get("DATAHUB_GMS_PORT") != "":
    port_fragment = f":{os.environ.get('DATAHUB_GMS_PORT',8080)}"
else:
    port_fragment = ""
DATAHUB_SERVER = f"{os.environ.get('DATAHUB_GMS_PROTOCOL', 'http')}://{os.environ.get('DATAHUB_GMS_HOST','localhost')}{port_fragment}"
graph = DataHubGraph(
    DatahubClientConfig(
        server=DATAHUB_SERVER,
        # If the DATAHUB_GMS_API_TOKEN env variable is set, then we use it
        # Else, we pass in None.
        # When the token is not set, the client will automatically try to use
        # DATAHUB_SYSTEM_CLIENT_ID and DATAHUB_SYSTEM_CLIENT_SECRET to
        # authenticate, which is what we want in production.
        token=os.environ.get("DATAHUB_GMS_API_TOKEN"),
        extra_headers={
            "User-Agent": f"{di.__package_name__}/{di.__version__}",
        },
    )
)


# For local development, we can enable an env-based override for the frontend URL.
_DEV_MODE_FRONTEND_URL = os.environ.get("DEV_MODE_OVERRIDE_DATAHUB_FRONTEND_URL")
if _DEV_MODE_FRONTEND_URL:
    DATAHUB_FRONTEND_URL = _DEV_MODE_FRONTEND_URL
else:
    DATAHUB_FRONTEND_URL = graph.frontend_base_url

ROOT_DIR = pathlib.Path(__file__).parent / "../.."
STATIC_ASSETS_DIR = ROOT_DIR / "static"
EXTERNAL_STATIC_PATH = f"{DATAHUB_FRONTEND_URL}/integrations/static"
