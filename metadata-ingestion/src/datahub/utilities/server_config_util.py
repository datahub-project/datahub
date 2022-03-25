from typing import Any, Dict, Optional

from datahub.telemetry.telemetry import set_telemetry_enable

# Only to be written to for logging server related information
global_debug: Dict[str, Any] = dict()


def set_gms_config(config: Dict) -> Any:
    global_debug["gms_config"] = config

    cli_telemtry_enabled = is_cli_telemetry_enabled()
    if cli_telemtry_enabled is not None:
        set_telemetry_enable(cli_telemtry_enabled)


def get_gms_config() -> Dict:
    return global_debug.get("gms_config", {})


def is_cli_telemetry_enabled() -> Optional[bool]:
    return get_gms_config().get("telemetry", {}).get("enabledCli", None)
