from typing import Any


def build_assertion_notification_id(
    *,
    assertion_urn: str,
    assertion_run_timestamp_millis: Any | None,
    assertion_run_id: Any | None,
) -> str:
    if assertion_run_timestamp_millis is None or assertion_run_id is None:
        return assertion_urn

    ts_millis = str(assertion_run_timestamp_millis)
    run_id = str(assertion_run_id)
    if not ts_millis or not run_id:
        return assertion_urn

    return f"{assertion_urn}|{ts_millis}|{run_id}"
