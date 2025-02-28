import logging
import time
from datetime import datetime
from urllib.parse import quote

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def wait_for_reload_completion(
    action_urn: str, integrations_url: str, timeout: int = 120
):
    action_urn_encoded = quote(action_urn)
    url = f"{integrations_url}/private/actions/{action_urn_encoded}/stats"
    start_time = time.time()
    while time.time() - start_time < timeout:
        response = requests.get(url)
        if response.status_code == 200:
            response_dict = response.json()
            live_status = response_dict.get("live", {}).get("statusCode")
            if live_status and live_status.lower() == "running":
                return
        # we don't raise an exception here because the action may not be initialized yet
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for action {action_urn} to reload")


def wait_until_action_has_processed_event(
    action_urn: str, integrations_url: str, event_time: datetime, timeout: int = 120
):
    """
        Stats response looks like:
        {
      "stats_generated_at": "2024-07-22T02:17:58.892552+00:00",
      "main": {
        "started_at": 1721612518677,
        "success_count": 38,
        "live_report": {
          "last_event_processed_time": "2024-07-22T02:17:02.176840+00:00",
          "last_seen_event_time_success": "2024-07-22T01:42:34.680000+00:00",
          "last_event_processed_time_success": "2024-07-22T02:17:02.177208+00:00",
          "last_event_processed_time_failure": "2024-07-22T01:42:35.002834+00:00"
        }
      },
      "transformers": {},
      "action": {
        "success_count": 38
      }
    }
    """
    action_urn_encoded = quote(action_urn)
    url = f"{integrations_url}/private/actions/{action_urn_encoded}/stats"
    start_time = time.time()
    while time.time() - start_time < timeout:
        response = requests.get(url)
        if response.status_code == 200:
            response_dict = response.json()
            last_event_processed_time_str = (
                response_dict.get("live", {})
                .get("customProperties", {})
                .get("event_processing_stats.last_event_processed_time", "")
            )
            if last_event_processed_time_str:
                last_event_processed_time = datetime.fromisoformat(
                    last_event_processed_time_str
                )
                if last_event_processed_time >= event_time:
                    return
        elif response.status_code != 404:
            # 404 means the action hasn't started processing events yet
            response.raise_for_status()
        time.sleep(1)
    raise TimeoutError(
        f"Timed out waiting for action {action_urn} to process event at {event_time}"
    )


def start_action(
    action_urn: str,
    integrations_url: str,
    wait_for_completion: bool = False,
    timeout: int = 120,
):
    # url encode the action_urn
    action_urn_encoded = quote(action_urn)
    url = f"{integrations_url}/private/actions/{action_urn_encoded}/reload"
    response = requests.post(url, json={})
    response.raise_for_status()
    if wait_for_completion:
        return wait_for_reload_completion(action_urn, integrations_url, timeout=timeout)
    return response.json()


def stop_action(action_urn: str, integrations_url: str):
    # url encode the action_urn
    action_urn_encoded = quote(action_urn)
    url = f"{integrations_url}/private/actions/{action_urn_encoded}/stop"
    response = requests.post(url, json={})
    response.raise_for_status()
    return response.json()


def bootstrap_action(
    action_urn: str,
    integrations_url: str,
    wait_for_completion: bool = False,
    timeout: int = 120,
):
    time_stages = {}
    time_stages["bootstrap_request"] = time.time()
    # url encode the action_urn
    action_urn_encoded = quote(action_urn)
    url = f"{integrations_url}/private/actions/{action_urn_encoded}/bootstrap"
    response = requests.post(url, json={})
    response.raise_for_status()
    time_stages["bootstrap_request"] = time.time() - time_stages["bootstrap_request"]
    if wait_for_completion:
        time_stages["wait_for_bootstrap_completion"] = time.time()
        wait_for_bootstrap_completion(action_urn, integrations_url, timeout=timeout)
        time_stages["wait_for_bootstrap_completion"] = (
            time.time() - time_stages["wait_for_bootstrap_completion"]
        )
    print(f"Bootstrap action {action_urn} took {time_stages}")
    return response.json()


def get_live_logs(action_urn: str, integrations_url: str):
    action_urn_encoded = quote(action_urn)
    url = f"{integrations_url}/private/actions/{action_urn_encoded}/live_logs"
    response = requests.get(url)
    response.raise_for_status()
    return response.content.decode("utf-8")


def wait_for_bootstrap_completion(action_urn: str, integrations_url: str, timeout: int):
    action_urn_encoded = quote(action_urn)
    url = f"{integrations_url}/private/actions/{action_urn_encoded}/stats"
    start_time = time.time()
    while time.time() - start_time < timeout:
        response = requests.get(url)
        if response.status_code == 200:
            status = response.json().get("bootstrap", {}).get("statusCode")
            if status and status.lower() == "success":
                return
        elif response.status_code != 404:
            # 404 means the action hasn't started bootstrapping yet
            response.raise_for_status()
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for action {action_urn} to bootstrap")


def rollback_action(
    action_urn: str,
    integrations_url: str,
    wait_for_completion: bool = False,
    timeout: int = 120,
):
    # url encode the action_urn
    action_urn_encoded = quote(action_urn)
    url = f"{integrations_url}/private/actions/{action_urn_encoded}/rollback"
    start_time = int(time.time() * 1000)
    response = requests.post(url, json={})
    response.raise_for_status()
    if wait_for_completion:
        wait_for_rollback_completion(
            action_urn, integrations_url, timeout=timeout, start_time_millis=start_time
        )
    return response.json()


def wait_for_rollback_completion(
    action_urn: str, integrations_url: str, timeout: int, start_time_millis: int
):
    """
        "rollback": {
      "reportedTime": {
        "time": 1721747526604,
        "actor": "urn:li:corpuser:__datahub_system"
      },
      "statusCode": "SUCCESS",
      "startTime": 1721747526349,
      "endTime": 1721747526592,
      "customProperties": {
        "totalAssetsToProcess": "0",
        "totalAssetsProcessed": "0"
      }
    }
    """
    action_urn_encoded = quote(action_urn)
    url = f"{integrations_url}/private/actions/{action_urn_encoded}/stats"
    monitor_start_time = time.time()
    while time.time() - monitor_start_time < timeout:
        time.sleep(1)
        response = requests.get(url)
        if response.status_code == 200:
            rollback_stats = response.json().get("rollback", {})
            if "startTime" not in rollback_stats:
                continue
            rollback_start_time = rollback_stats["startTime"]
            # rollback_start_time = datetime.fromisoformat(rollback_stats.get(""startTime": 1721747526349"))
            if rollback_start_time >= start_time_millis:
                # we are looking at the correct rollback
                status = rollback_stats.get("statusCode")
                if status and status.lower() == "success":
                    return
        # we suppress other response codes like 404 / 500 because the action may not have started rolling back yet

    raise TimeoutError(f"Timed out waiting for action {action_urn} to rollback")
