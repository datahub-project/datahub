import os
import pathlib

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from dotenv import load_dotenv

import datahub_integrations as di
from datahub_integrations._logging_setup import LOGGING_SETUP_COMPLETE

assert LOGGING_SETUP_COMPLETE

# Load environment variables from .env file if it exists
# This is safe to call even if .env doesn't exist - it will just do nothing
load_dotenv()

# A global config and graph object that can be used by all routers.
if os.environ.get("DATAHUB_GMS_URL"):
    DATAHUB_SERVER = os.environ["DATAHUB_GMS_URL"]
else:
    # TODO: This is a somewhat legacy code path. We should migrate
    # to the simpler approach that just uses DATAHUB_GMS_URL.
    if os.environ.get("DATAHUB_GMS_PORT") != "":
        port_fragment = f":{os.environ.get('DATAHUB_GMS_PORT', 8080)}"
    else:
        port_fragment = ""
    DATAHUB_SERVER = f"{os.environ.get('DATAHUB_GMS_PROTOCOL', 'http')}://{os.environ.get('DATAHUB_GMS_HOST', 'localhost')}{port_fragment}"

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

# Multi-tenant router configuration for Teams integration
if os.environ.get("DATAHUB_TEAMS_ROUTER_URL"):
    TEAMS_ROUTER_URL = os.environ["DATAHUB_TEAMS_ROUTER_URL"]
else:
    # Default router URL for development
    if os.environ.get("DATAHUB_TEAMS_ROUTER_PORT"):
        router_port = os.environ["DATAHUB_TEAMS_ROUTER_PORT"]
    else:
        router_port = "9005"  # Default multi-tenant router port

    router_host = os.environ.get("DATAHUB_TEAMS_ROUTER_HOST", "localhost")
    router_protocol = os.environ.get("DATAHUB_TEAMS_ROUTER_PROTOCOL", "http")
    TEAMS_ROUTER_URL = f"{router_protocol}://{router_host}:{router_port}"

# Remote debugging setup
if os.environ.get("DEBUGPY_ENABLED", "").lower() == "true":
    import debugpy

    debug_port = int(os.environ.get("INTEGRATIONS_DEBUG_PORT", "5004"))
    debug_host = "0.0.0.0"

    print(f"🔧 Starting remote debugger on {debug_host}:{debug_port}")
    debugpy.listen((debug_host, debug_port))
