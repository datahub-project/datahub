import errno
import json
import logging
import os
import platform
import uuid
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar

from mixpanel import Consumer, Mixpanel

import datahub as datahub_package
from datahub.cli.cli_utils import DATAHUB_ROOT_FOLDER
from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

DATAHUB_FOLDER = Path(DATAHUB_ROOT_FOLDER)

CONFIG_FILE = DATAHUB_FOLDER / "telemetry-config.json"

# also fall back to environment variable if config file is not found
ENV_ENABLED = os.environ.get("DATAHUB_TELEMETRY_ENABLED", "true").lower() == "true"

# see
# https://adamj.eu/tech/2020/03/09/detect-if-your-tests-are-running-on-ci/
# https://github.com/watson/ci-info
CI_ENV_VARS = {
    "APPCENTER",
    "APPCIRCLE",
    "APPCIRCLEAZURE_PIPELINES",
    "APPVEYOR",
    "AZURE_PIPELINES",
    "BAMBOO",
    "BITBUCKET",
    "BITRISE",
    "BUDDY",
    "BUILDKITE",
    "BUILD_ID",
    "CI",
    "CIRCLE",
    "CIRCLECI",
    "CIRRUS",
    "CIRRUS_CI",
    "CI_NAME",
    "CODEBUILD",
    "CODEBUILD_BUILD_ID",
    "CODEFRESH",
    "CODESHIP",
    "CYPRESS_HOST",
    "DRONE",
    "DSARI",
    "EAS_BUILD",
    "GITHUB_ACTIONS",
    "GITLAB",
    "GITLAB_CI",
    "GOCD",
    "HEROKU_TEST_RUN_ID",
    "HUDSON",
    "JENKINS",
    "JENKINS_URL",
    "LAYERCI",
    "MAGNUM",
    "NETLIFY",
    "NEVERCODE",
    "RENDER",
    "SAIL",
    "SCREWDRIVER",
    "SEMAPHORE",
    "SHIPPABLE",
    "SOLANO",
    "STRIDER",
    "TASKCLUSTER",
    "TEAMCITY",
    "TEAMCITY_VERSION",
    "TF_BUILD",
    "TRAVIS",
    "VERCEL",
    "WERCKER_ROOT",
    "bamboo.buildKey",
}

# disable when running in any CI
if any(var in os.environ for var in CI_ENV_VARS):
    ENV_ENABLED = False

TIMEOUT = int(os.environ.get("DATAHUB_TELEMETRY_TIMEOUT", "10"))
MIXPANEL_TOKEN = "5ee83d940754d63cacbf7d34daa6f44a"


class Telemetry:

    client_id: str
    enabled: bool = True
    tracking_init: bool = False

    def __init__(self):

        # try loading the config if it exists, update it if that fails
        if not CONFIG_FILE.exists() or not self.load_config():
            # set up defaults
            self.client_id = str(uuid.uuid4())
            self.enabled = self.enabled and ENV_ENABLED
            if not self.update_config():
                # If we're not able to persist the client ID, we should default
                # to a standardized value. This prevents us from minting a new
                # client ID every time we start the CLI.
                self.client_id = "00000000-0000-0000-0000-000000000001"

        # send updated user-level properties
        self.mp = None
        if self.enabled:
            try:
                self.mp = Mixpanel(
                    MIXPANEL_TOKEN, consumer=Consumer(request_timeout=int(TIMEOUT))
                )
            except Exception as e:
                logger.debug(f"Error connecting to mixpanel: {e}")

    def update_config(self) -> bool:
        """
        Update the config file with the current client ID and enabled status.
        Return True if the update succeeded, False otherwise
        """
        logger.debug("Updating telemetry config")

        try:
            os.makedirs(DATAHUB_FOLDER, exist_ok=True)
            try:
                with open(CONFIG_FILE, "w") as f:
                    json.dump(
                        {"client_id": self.client_id, "enabled": self.enabled},
                        f,
                        indent=2,
                    )
                return True
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
        except Exception as e:
            logger.debug(f"Failed to update config file at {CONFIG_FILE} due to {e}")

        return False

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

    def load_config(self) -> bool:
        """
        Load the saved config for the telemetry client ID and enabled status.
        Returns True if config was correctly loaded, False otherwise.
        """

        try:
            with open(CONFIG_FILE, "r") as f:
                config = json.load(f)
                self.client_id = config["client_id"]
                self.enabled = config["enabled"] & ENV_ENABLED
                return True
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
        except Exception as e:
            logger.debug(f"Failed to load {CONFIG_FILE} due to {e}")

        return False

    def init_tracking(self) -> None:
        if not self.enabled or self.mp is None or self.tracking_init is True:
            return

        logger.debug("Sending init Telemetry")
        try:
            self.mp.people_set(
                self.client_id,
                {
                    "datahub_version": datahub_package.nice_version_name(),
                    "os": platform.system(),
                    "python_version": platform.python_version(),
                },
            )
        except Exception as e:
            logger.debug(f"Error reporting telemetry: {e}")
        self.init_track = True

    def ping(
        self,
        event_name: str,
        properties: Dict[str, Any] = {},
        server: Optional[DataHubGraph] = None,
    ) -> None:
        """
        Send a single telemetry event.

        Args:
            event_name (str): name of the event to send.
            properties (Optional[Dict[str, Any]]): metadata for the event
        """

        if not self.enabled or self.mp is None:
            return

        # send event
        try:
            logger.debug("Sending Telemetry")
            properties.update(self._server_props(server))
            self.mp.track(self.client_id, event_name, properties)

        except Exception as e:
            logger.debug(f"Error reporting telemetry: {e}")

    def _server_props(self, server: Optional[DataHubGraph]) -> Dict[str, str]:
        if not server:
            return {
                "server_type": "n/a",
                "server_version": "n/a",
                "server_id": "n/a",
            }
        else:
            return {
                "server_type": server.server_config.get("datahub", {}).get(
                    "serverType", "missing"
                ),
                "server_version": server.server_config.get("versions", {})
                .get("linkedin/datahub", {})
                .get("version", "missing"),
                "server_id": server.server_id or "missing",
            }


telemetry_instance = Telemetry()

T = TypeVar("T")


def suppress_telemetry() -> Any:
    """disables telemetry for this invocation, doesn't affect persistent client settings"""
    if telemetry_instance.enabled:
        logger.debug("Disabling telemetry locally due to server config")
    telemetry_instance.enabled = False


def get_full_class_name(obj):
    module = obj.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return obj.__class__.__name__
    return f"{module}.{obj.__class__.__name__}"


def with_telemetry(func: Callable[..., T]) -> Callable[..., T]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:

        function = f"{func.__module__}.{func.__name__}"

        telemetry_instance.init_tracking()
        telemetry_instance.ping(
            "function-call", {"function": function, "status": "start"}
        )
        try:
            res = func(*args, **kwargs)
            telemetry_instance.ping(
                "function-call",
                {"function": function, "status": "completed"},
            )
            return res
        # System exits (used in ingestion and Docker commands) are not caught by the exception handler,
        # so we need to catch them here.
        except SystemExit as e:
            # Forward successful exits
            # 0 or None imply success
            if not e.code:
                telemetry_instance.ping(
                    "function-call",
                    {
                        "function": function,
                        "status": "completed",
                    },
                )
            # Report failed exits
            else:
                telemetry_instance.ping(
                    "function-call",
                    {
                        "function": function,
                        "status": "error",
                        "error": get_full_class_name(e),
                    },
                )
            raise e
        # Catch SIGINTs
        except KeyboardInterrupt as e:
            telemetry_instance.ping(
                "function-call",
                {"function": function, "status": "cancelled"},
            )
            raise e

        # Catch general exceptions
        except Exception as e:
            telemetry_instance.ping(
                "function-call",
                {
                    "function": function,
                    "status": "error",
                    "error": get_full_class_name(e),
                },
            )
            raise e

    return wrapper
