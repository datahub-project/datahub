import json
import uuid
from pathlib import Path
from typing import Dict, Optional, Union

import requests

import datahub as datahub_package

GA_VERSION = 1
GA_TID = "UA-214428525-1"

LOCAL_DIR = Path(__file__).resolve().parent
CONFIG_FILE = LOCAL_DIR / "telemetry-config.json"


class Telemetry:

    client_id: str
    enabled: bool = True

    def __init__(self):

        # init the client ID and config if it doesn't exist
        if not CONFIG_FILE.exists():
            self.client_id = str(uuid.uuid4())
            self.update_config()

        else:
            self.load_config()

    def update_config(self) -> None:
        """
        Update the config file with the current client ID and enabled status.
        """

        with open(CONFIG_FILE, "w") as f:
            json.dump({"client_id": self.client_id, "enabled": self.enabled}, f)

    def enable(self) -> None:
        """
        Enable telemetry.
        """

        self.enabled = True
        self.update_config()

    def disable(self) -> None:
        """
        Disable telemetry.
        """

        self.enabled = False
        self.update_config()

    def load_config(self):
        """
        Load the saved config for the telemetry client ID and enabled status.
        """

        with open(CONFIG_FILE, "r") as f:
            config = json.load(f)
            self.client_id = config["client_id"]
            self.enabled = config["enabled"]

    def ping(
        self,
        category: str,
        action: str,
        label: Optional[str] = None,
        value: Optional[int] = None,
    ) -> None:
        """
        Ping Google Analytics with a single event.

        Args:
            category (str): category for the event
            action (str): action taken
            label (Optional[str], optional): label for the event
            value (Optional[int], optional): value for the event
        """

        if not self.enabled:
            return

        req_url = "https://www.google-analytics.com/collect"

        params: Dict[str, Union[str, int]] = {
            "an": "metadata-ingestion",  # app name
            "av": datahub_package.nice_version_name(),  # app version
            "t": "event",  # event type
            "v": GA_VERSION,  # Google Analytics version
            "tid": GA_TID,  # tracking id
            "cid": self.client_id,  # client id
            "ec": category,  # event category
            "ea": action,  # event action
        }

        if label:
            params["el"] = label

        # this has to a non-negative int, otherwise the request will fail
        if value:
            params["ev"] = value

        requests.post(
            req_url,
            data=params,
            headers={"user-agent": f"datahub {datahub_package.nice_version_name()}"},
        )


telemetry_instance = Telemetry()


def ping_quickstart(quickstart_type: str) -> None:
    telemetry_instance.ping("quickstart", quickstart_type)


def ping_ingestion(ingestion_cmd: str) -> None:
    telemetry_instance.ping("ingestion", ingestion_cmd)


def ping_delete(delete_cmd: str) -> None:
    telemetry_instance.ping("delete", delete_cmd)


def ping_get() -> None:
    telemetry_instance.ping("get", "get")


def ping_put() -> None:
    telemetry_instance.ping("put", "put")


def ping_init() -> None:
    telemetry_instance.ping("init", "init")


def ping_docker(docker_cmd: str) -> None:
    telemetry_instance.ping("docker", docker_cmd)


def ping_version() -> None:
    telemetry_instance.ping("version", "check_version")


def ping_telemetry(telemetry_cmd: str) -> None:
    telemetry_instance.ping("telemetry", telemetry_cmd)
