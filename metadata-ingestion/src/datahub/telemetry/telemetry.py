import errno
import json
import logging
import os
import platform
import sys
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
        try:
            with open(CONFIG_FILE, "w") as f:
                json.dump(
                    {"client_id": self.client_id, "enabled": self.enabled}, f, indent=2
                )
        except IOError as x:
            if x.errno == errno.ENOENT:
                logger.debug(
                    f"{CONFIG_FILE} does not exist and could not be created. Please check permissions on the parent folder."
                )
            elif x.errno == errno.EACCES:
                logger.debug(
                    f"{CONFIG_FILE} cannot be read. Please check the permissions on this file."
                )
            else:
                logger.debug(
                    f"{CONFIG_FILE} had an IOError, please inspect this file for issues."
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

        try:
            with open(CONFIG_FILE, "r") as f:
                config = json.load(f)
                self.client_id = config["client_id"]
                self.enabled = config["enabled"] & ENV_ENABLED
        except IOError as x:
            if x.errno == errno.ENOENT:
                logger.debug(
                    f"{CONFIG_FILE} does not exist and could not be created. Please check permissions on the parent folder."
                )
            elif x.errno == errno.EACCES:
                logger.debug(
                    f"{CONFIG_FILE} cannot be read. Please check the permissions on this file."
                )
            else:
                logger.debug(
                    f"{CONFIG_FILE} had an IOError, please inspect this file for issues."
                )

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
            # use custom dimensions to capture OS and Python version
            # see https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters#cd_
            "cd1": platform.system(),  # OS
            "cd2": platform.python_version(),  # Python version
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


def get_full_class_name(obj):
    module = obj.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return obj.__class__.__name__
    return module + "." + obj.__class__.__name__


def with_telemetry(func: Callable[..., T]) -> Callable[..., T]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:

        category = func.__module__
        action = func.__name__

        telemetry_instance.ping(category, action, "started")
        try:
            res = func(*args, **kwargs)
            telemetry_instance.ping(category, action, "completed")
            return res
        # Catch general exceptions
        except Exception as e:
            telemetry_instance.ping(category, action, f"error:{get_full_class_name(e)}")
            raise e
        # System exits (used in ingestion and Docker commands) are not caught by the exception handler,
        # so we need to catch them here.
        except SystemExit as e:
            # Forward successful exits
            if e.code == 0:
                telemetry_instance.ping(category, action, "completed")
                sys.exit(0)
            # Report failed exits
            else:
                telemetry_instance.ping(
                    category, action, f"error:{get_full_class_name(e)}"
                )
                sys.exit(e.code)
        # Catch SIGINTs
        except KeyboardInterrupt:
            telemetry_instance.ping(category, action, "cancelled")
            sys.exit(0)

    return wrapper
