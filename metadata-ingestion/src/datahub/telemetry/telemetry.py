import uuid
from pathlib import Path
from typing import Dict, Optional, Union

import requests

import datahub as datahub_package

GA_V = 1
GA_TID = "UA-214428525-1"
CLIENT_ID_FILE = "./client_id.txt"


class Telemetry:

    client_id: str
    tracking_enabled: bool = False

    def __init__(self):

        self.client_id = self.load_client_id()

    def enable_tracking(self) -> None:
        self.tracking_enabled = True
        self.load_client_id()

    def disable_tracking(self) -> None:
        self.tracking_enabled = False

    def save_client_id(self) -> str:
        client_id = str(uuid.uuid4())

        with open(CLIENT_ID_FILE, "w") as f:
            f.write(client_id)

        return client_id

    def load_client_id(self) -> str:

        if not Path(CLIENT_ID_FILE).exists():
            client_id = self.save_client_id()

        else:
            with open(CLIENT_ID_FILE, "r") as f:
                client_id = f.read().strip()

        return client_id

    def ping(
        self,
        category: str,
        action: str,
        label: Optional[str] = None,
        value: Optional[int] = None,
    ) -> None:

        if self.tracking_enabled:
            return

        req_url = "https://www.google-analytics.com/collect"

        params: Dict[str, Union[str, int]] = {
            "an": "metadata-ingestion",  # app name
            "av": datahub_package.nice_version_name(),  # app version
            "t": "event",  # event type
            "v": GA_V,  # Google Analytics version
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

        requests.post(req_url, data=params, headers={"user-agent": "datahub"})


telemetry = Telemetry()


def ping_quickstart(quickstart_type: str) -> None:
    telemetry.ping("quickstart", quickstart_type)


def ping_ingestion(ingestion_cmd: str) -> None:
    telemetry.ping("ingestion", ingestion_cmd)


def ping_delete(delete_type: str) -> None:
    telemetry.ping("delete", delete_type)


def ping_init() -> None:
    telemetry.ping("init", "init")


def ping_version() -> None:
    telemetry.ping("version", "check_version")
