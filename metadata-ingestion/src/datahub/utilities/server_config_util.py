from typing import Any, Dict, Optional

from datahub.telemetry.telemetry import suppress_telemetry

# Only to be written to for logging server related information
global_debug: Dict[str, Any] = {}


def set_gms_config(config: Dict) -> Any:
    global_debug["gms_config"] = config

    cli_telemtry_enabled = is_cli_telemetry_enabled()
    if cli_telemtry_enabled is not None and not cli_telemtry_enabled:
        # server requires telemetry to be disabled on client
        suppress_telemetry()


def get_gms_config() -> Dict:
    return global_debug.get("gms_config", {})


def is_cli_telemetry_enabled() -> Optional[bool]:
    return get_gms_config().get("telemetry", {}).get("enabledCli", None)
