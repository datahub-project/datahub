# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import click

from datahub.telemetry import telemetry as telemetry_lib


@click.group()
def telemetry() -> None:
    """Toggle telemetry."""
    pass


@telemetry.command()
@telemetry_lib.with_telemetry()
def enable() -> None:
    """Enable telemetry for the current DataHub instance."""
    telemetry_lib.telemetry_instance.enable()


@telemetry.command()
def disable() -> None:
    """Disable telemetry for the current DataHub instance."""
    telemetry_lib.telemetry_instance.disable()
