import logging
from typing import Any, Dict

# Only to be written to for logging server related information
global_debug: Dict[str, Any] = {}

logger = logging.getLogger(__name__)


def set_gms_config(config: Dict) -> Any:
    global_debug["gms_config"] = config
    _change_telemetry()


def get_gms_config() -> Dict:
    return global_debug.get("gms_config", {})


def _change_telemetry() -> None:
    # Being done to avoid a circular import
    from datahub.telemetry import telemetry as telemetry_lib

    is_enabled = get_gms_config().get("telemetry", {}).get("enabledCli", True)
    if not is_enabled:
        # Note that this will persist the change, so future CLI invocations will
        # also have telemetry disabled. This means that if you ever connect to a
        # server with telemetry disabled, the CLI will permanently disable telemetry
        # unless manually re-enabled.
        logger.debug("Disabling CLI telemetry as per server configs")
        telemetry_lib.telemetry_instance.disable()
