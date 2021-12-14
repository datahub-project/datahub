import json
import logging
import os
import uuid
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar, Union

import requests

import datahub as datahub_package

logger = logging.getLogger(__name__)

GA_VERSION = 1
GA_TID = "UA-212728656-1"

DATAHUB_FOLDER = Path(os.path.expanduser("~/.datahub"))

CONFIG_FILE = DATAHUB_FOLDER / "telemetry-config.json"

# also fall back to environment variable if config file is not found
ENV_ENABLED = os.environ.get("DATAHUB_TELEMETRY_ENABLED", "true").lower() == "true"


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

        if not DATAHUB_FOLDER.exists():
            os.makedirs(DATAHUB_FOLDER)

        with open(CONFIG_FILE, "w") as f:
            json.dump(
                {"client_id": self.client_id, "enabled": self.enabled}, f, indent=2
            )

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
            self.enabled = config["enabled"] & ENV_ENABLED

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
            "an": "datahub-cli",  # app name
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

        try:
            requests.post(
                req_url,
                data=params,
                headers={
                    "user-agent": f"datahub {datahub_package.nice_version_name()}"
                },
            )
        except Exception as e:

            logger.debug(f"Error reporting telemetry: {e}")


telemetry_instance = Telemetry()

T = TypeVar("T")


def with_telemetry(func: Callable[..., T]) -> Callable[..., T]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        res = func(*args, **kwargs)
        telemetry_instance.ping(func.__module__, func.__name__)
        return res

    return wrapper
