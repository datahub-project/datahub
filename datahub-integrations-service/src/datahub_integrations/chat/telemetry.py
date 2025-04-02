import functools

from datahub.telemetry.telemetry import TIMEOUT, _default_telemetry_properties
from mixpanel import Consumer, Mixpanel

from datahub_integrations import __version__
from datahub_integrations.app import graph
from datahub_integrations.chat.telemetry_models import BaseEvent

# GMS does not expose APIs to write to the DataHub Usage Events API -
# only datahub-frontend does. I don't want integrations-service
# to depend on datahub-frontend, so I'm sending to Mixpanel directly.
# This entire thing is a hack, and would be better solved by
# proper APIs in GMS.

# Note that this is different from OSS Mixpanel's token. This one
# corresponds to the SaaS Mixpanel project.
MIXPANEL_TOKEN = "7cee38380de7a8469069c040a1fee320"

telemetry_client = Mixpanel(
    MIXPANEL_TOKEN,
    consumer=Consumer(request_timeout=int(TIMEOUT)),
)


@functools.cache
def _get_server_id() -> str:
    graph.test_connection()
    return graph.server_id


def _default_properties() -> dict:
    return {
        **_default_telemetry_properties(),
        "datahub_integrations_version": __version__,
    }


def track_saas_event(
    event: BaseEvent,
) -> None:
    """Track a SaaS event using Mixpanel.

    Args:
        event: The event to track.
    """
    telemetry_client.track(
        _get_server_id(),
        event.type,
        {
            **_default_properties(),
            **event.dict(),
        },
    )
