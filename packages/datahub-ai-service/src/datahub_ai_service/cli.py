import os

import click
import uvicorn

from ._version import __version__
from .app import create_app
from .config import AiServiceConfig, ConfigError


@click.group()
@click.version_option(version=__version__)
def cli() -> None:
    """DataHub's AI service."""


@cli.command()
def serve() -> None:
    """Serve datahub-ai-service over HTTP, configured from the environment (see README)."""
    try:
        config = AiServiceConfig.from_env(os.environ)
    except ConfigError as e:
        raise click.UsageError(str(e)) from e
    uvicorn.run(create_app(config), host=config.host, port=config.port)
