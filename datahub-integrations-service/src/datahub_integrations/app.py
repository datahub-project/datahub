import os
import pathlib

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from fastapi import APIRouter, FastAPI, Response
from fastapi.responses import RedirectResponse

STATIC_ASSETS_DIR = pathlib.Path(__file__).parent / "../../static"

app = FastAPI()

external_router = APIRouter()
internal_router = APIRouter(
    dependencies=[
        # TODO: Add middleware for requiring system auth here.
    ]
)


@app.get("/ping")
def ping() -> str:
    return "pong"


@app.get("/", include_in_schema=False)
def redirect_to_docs() -> Response:
    return RedirectResponse(url="/docs")


# A global config and graph object that can be used by all routers.
DATAHUB_SERVER = f"{os.environ.get('DATAHUB_GMS_PROTOCOL', 'http')}://{os.environ.get('DATAHUB_GMS_HOST','localhost')}:{os.environ.get('DATAHUB_GMS_PORT',8080)}"
graph = DataHubGraph(
    DatahubClientConfig(
        server=DATAHUB_SERVER,
        # When token is not set, the client will automatically try to use
        # DATAHUB_SYSTEM_CLIENT_ID and DATAHUB_SYSTEM_CLIENT_SECRET to authenticate.
        token=None,
    )
)


# For local development, we can enable an env-based override for the frontend URL.
_DEV_MODE_FRONTEND_URL = os.environ.get("DEV_MODE_OVERRIDE_DATAHUB_FRONTEND_URL")
if _DEV_MODE_FRONTEND_URL:
    DATAHUB_FRONTEND_URL = _DEV_MODE_FRONTEND_URL
else:
    DATAHUB_FRONTEND_URL = graph.frontend_base_url
