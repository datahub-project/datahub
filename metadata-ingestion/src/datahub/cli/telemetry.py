import click

from datahub.telemetry import telemetry as telemetry_lib


@click.group()
def telemetry() -> None:
    """Toggle telemetry."""
    pass


@telemetry.command()
def enable() -> None:
    """Enable telemetry for the current DataHub instance."""
    telemetry_lib.telemetry_instance.enable()
    telemetry_lib.ping_telemetry("enable")


@telemetry.command()
def disable() -> None:
    """Disable telemetry for the current DataHub instance."""
    telemetry_lib.ping_telemetry("disable")
    telemetry_lib.telemetry_instance.disable()
