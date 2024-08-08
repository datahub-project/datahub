import errno
import json
import logging
import os
import platform
import sys
import uuid
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar

from mixpanel import Consumer, Mixpanel
from typing_extensions import ParamSpec

import datahub as datahub_package
from datahub.cli.config_utils import DATAHUB_ROOT_FOLDER
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.configuration.common import ExceptionWithProps
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import _custom_package_path
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)

DATAHUB_FOLDER = Path(DATAHUB_ROOT_FOLDER)

CONFIG_FILE = DATAHUB_FOLDER / "telemetry-config.json"

# also fall back to environment variable if config file is not found
ENV_ENABLED = get_boolean_env_variable("DATAHUB_TELEMETRY_ENABLED", True)

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

# Also disable if a custom metadata model package is in use.
if _custom_package_path:
    ENV_ENABLED = False

TIMEOUT = int(os.environ.get("DATAHUB_TELEMETRY_TIMEOUT", "10"))
MIXPANEL_ENDPOINT = "track.datahubproject.io/mp"
MIXPANEL_TOKEN = "5ee83d940754d63cacbf7d34daa6f44a"
SENTRY_DSN: Optional[str] = os.environ.get("SENTRY_DSN", None)
SENTRY_ENVIRONMENT: str = os.environ.get("SENTRY_ENVIRONMENT", "dev")


def _default_telemetry_properties() -> Dict[str, Any]:
    return {
        "datahub_version": datahub_package.nice_version_name(),
        "python_version": platform.python_version(),
        "os": platform.system(),
        "arch": platform.machine(),
    }


class Telemetry:
    client_id: str
    enabled: bool = True
    tracking_init: bool = False
    sentry_enabled: bool = False

    def __init__(self):
        if SENTRY_DSN:
            self.sentry_enabled = True
            try:
                import sentry_sdk

                sentry_sdk.init(
                    dsn=SENTRY_DSN,
                    environment=SENTRY_ENVIRONMENT,
                    release=datahub_package.__version__,
                )
            except Exception as e:
                # We need to print initialization errors to stderr, since logger is not initialized yet
                print(f"Error initializing Sentry: {e}", file=sys.stderr)
                logger.info(f"Error initializing Sentry: {e}")

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
                    MIXPANEL_TOKEN,
                    consumer=Consumer(
                        request_timeout=int(TIMEOUT), api_host=MIXPANEL_ENDPOINT
                    ),
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
            except OSError as x:
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
            with open(CONFIG_FILE) as f:
                config = json.load(f)
                self.client_id = config["client_id"]
                self.enabled = config["enabled"] & ENV_ENABLED
                return True
        except OSError as x:
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

    def update_capture_exception_context(
        self,
        server: Optional[DataHubGraph] = None,
        properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        if self.sentry_enabled:
            from sentry_sdk import set_tag

            properties = {
                **_default_telemetry_properties(),
                **self._server_props(server),
                **(properties or {}),
            }

            for key in properties:
                set_tag(key, properties[key])

    def init_capture_exception(self) -> None:
        if self.sentry_enabled:
            import sentry_sdk

            sentry_sdk.set_user({"client_id": self.client_id})
            sentry_sdk.set_context(
                "environment",
                {
                    "environment": SENTRY_ENVIRONMENT,
                    "datahub_version": datahub_package.nice_version_name(),
                    "os": platform.system(),
                    "python_version": platform.python_version(),
                },
            )

    def capture_exception(self, e: BaseException) -> None:
        try:
            if self.sentry_enabled:
                import sentry_sdk

                sentry_sdk.capture_exception(e)
        except Exception as e:
            logger.warning("Failed to capture exception in Sentry.", exc_info=e)

    def init_tracking(self) -> None:
        if not self.enabled or self.mp is None or self.tracking_init is True:
            return

        logger.debug("Sending init Telemetry")
        try:
            self.mp.people_set(
                self.client_id,
                _default_telemetry_properties(),
            )
        except Exception as e:
            logger.debug(f"Error initializing telemetry: {e}")
        self.init_track = True

    def ping(
        self,
        event_name: str,
        properties: Optional[Dict[str, Any]] = None,
        server: Optional[DataHubGraph] = None,
    ) -> None:
        """
        Send a single telemetry event.

        Args:
            event_name: name of the event to send.
            properties: metadata for the event
        """

        if not self.enabled or self.mp is None:
            return

        # send event
        try:
            logger.debug(f"Sending telemetry for {event_name}")
            properties = {
                **_default_telemetry_properties(),
                **self._server_props(server),
                **(properties or {}),
            }
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
                .get("acryldata/datahub", {})
                .get("version", "missing"),
                "server_id": server.server_id or "missing",
            }


telemetry_instance = Telemetry()


def suppress_telemetry() -> None:
    """disables telemetry for this invocation, doesn't affect persistent client settings"""
    if telemetry_instance.enabled:
        logger.debug("Disabling telemetry locally due to server config")
    telemetry_instance.enabled = False


def get_full_class_name(obj):
    module = obj.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return obj.__class__.__name__
    return f"{module}.{obj.__class__.__name__}"


def _error_props(error: BaseException) -> Dict[str, Any]:
    props = {
        "error": get_full_class_name(error),
    }

    if isinstance(error, ExceptionWithProps):
        try:
            props.update(error.get_telemetry_props())
        except Exception as e:
            logger.debug(f"Error getting telemetry props for {error}: {e}")

    return props


_T = TypeVar("_T")
_P = ParamSpec("_P")


def with_telemetry(
    *, capture_kwargs: Optional[List[str]] = None
) -> Callable[[Callable[_P, _T]], Callable[_P, _T]]:
    kwargs_to_track = capture_kwargs or []

    def with_telemetry_decorator(func: Callable[_P, _T]) -> Callable[_P, _T]:
        function = f"{func.__module__}.{func.__name__}"

        @wraps(func)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
            telemetry_instance.init_tracking()
            telemetry_instance.init_capture_exception()

            call_props: Dict[str, Any] = {"function": function}
            for kwarg in kwargs_to_track:
                call_props[f"arg_{kwarg}"] = kwargs.get(kwarg)

            telemetry_instance.ping(
                "function-call",
                {**call_props, "status": "start"},
            )
            try:
                try:
                    with PerfTimer() as timer:
                        res = func(*args, **kwargs)
                finally:
                    call_props["duration"] = timer.elapsed_seconds()

                telemetry_instance.ping(
                    "function-call",
                    {**call_props, "status": "completed"},
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
                        {**call_props, "status": "completed"},
                    )
                # Report failed exits
                else:
                    telemetry_instance.ping(
                        "function-call",
                        {
                            **call_props,
                            "status": "error",
                            **_error_props(e),
                        },
                    )
                telemetry_instance.capture_exception(e)
                raise e
            # Catch SIGINTs
            except KeyboardInterrupt as e:
                telemetry_instance.ping(
                    "function-call",
                    {**call_props, "status": "cancelled"},
                )
                telemetry_instance.capture_exception(e)
                raise e

            # Catch general exceptions
            except BaseException as e:
                telemetry_instance.ping(
                    "function-call",
                    {
                        **call_props,
                        "status": "error",
                        **_error_props(e),
                    },
                )
                telemetry_instance.capture_exception(e)
                raise e

        return wrapper

    return with_telemetry_decorator
