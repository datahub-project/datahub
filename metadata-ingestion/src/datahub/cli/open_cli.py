import logging
import webbrowser
from typing import Any, Optional
from urllib.parse import urlparse

import click

from datahub.ingestion.graph.client import get_default_graph
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


def get_frontend_url_from_server(server: str, urn: str) -> str:
    if "localhost" in server:
        server_url = "http://localhost:9002"
    else:
        # Parse the server URL and only extract the part before the first slash
        # This is to handle cases where the server URL is a full path to the API
        # and we only want the base URL
        # Server urls can look like https://dev07.acryl.io/api/gms
        parsed_url = urlparse(server)
        server_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    if server_url.endswith("/"):
        server_url = server_url[:-1]

    parsed_urn = Urn.create_from_string(urn)
    return f"{server_url}/{parsed_urn.entity_type}/{urn}"


@click.command()
@click.argument("urn", required=False)
@click.option("--host", required=False, type=str)
@click.pass_context
@upgrade.check_upgrade
@telemetry.with_telemetry()
def open(ctx: Any, urn: Optional[str], host: Optional[str]) -> None:
    """
    Open datahub to the specific urn.
    """
    if not urn:
        raise click.UsageError("Nothing for me to open. Maybe provide an urn?")
    graph = get_default_graph()
    print(graph.get_server_config())
    final_url = get_frontend_url_from_server(
        graph.get_server_config().config.get("baseUrl", {}), urn
    )
    webbrowser.open_new_tab(final_url)
