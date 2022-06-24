import contextlib
import logging
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Callable, Optional, TypeVar

import humanfriendly
import requests
from packaging.version import Version
from pydantic import BaseModel
from termcolor import colored

from datahub import __version__
from datahub.cli import cli_utils
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

log = logging.getLogger(__name__)


T = TypeVar("T")


class VersionStats(BaseModel, arbitrary_types_allowed=True):
    version: Version
    release_date: Optional[datetime]


class ServerVersionStats(BaseModel):
    current: VersionStats
    latest: Optional[VersionStats]
    current_server_type: Optional[str]


class ClientVersionStats(BaseModel):
    current: VersionStats
    latest: Optional[VersionStats]


class DataHubVersionStats(BaseModel):
    server: ServerVersionStats
    client: ClientVersionStats


def retrieve_versions(  # noqa: C901
    server: Optional[DataHubGraph] = None,
) -> Optional[DataHubVersionStats]:

    current_version_string = __version__
    current_version = Version(current_version_string)
    client_version_stats: ClientVersionStats = ClientVersionStats(
        current=VersionStats(version=current_version, release_date=None), latest=None
    )
    server_version_stats: Optional[ServerVersionStats] = None

    try:
        response = requests.get("https://pypi.org/pypi/acryl_datahub/json")
        if response.ok:
            response_json = response.json()
            releases = response_json.get("releases", [])
            sorted_releases = sorted(releases.keys(), key=lambda x: Version(x))
            latest_cli_release_string = [x for x in sorted_releases if "rc" not in x][
                -1
            ]
            latest_cli_release = Version(latest_cli_release_string)
            current_version_info = releases.get(current_version_string)
            current_version_date = None
            if current_version_info:
                current_version_date = datetime.strptime(
                    current_version_info[0].get("upload_time"), "%Y-%m-%dT%H:%M:%S"
                )
            latest_release_info = releases.get(latest_cli_release_string)
            latest_version_date = None
            if latest_release_info:
                latest_version_date = datetime.strptime(
                    latest_release_info[0].get("upload_time"), "%Y-%m-%dT%H:%M:%S"
                )
            client_version_stats = ClientVersionStats(
                current=VersionStats(
                    version=current_version, release_date=current_version_date
                ),
                latest=VersionStats(
                    version=latest_cli_release, release_date=latest_version_date
                ),
            )
    except Exception as e:
        log.debug(f"Failed to determine cli releases from pypi due to {e}")
        pass

    latest_server_version: Optional[Version] = None
    latest_server_date: Optional[datetime] = None
    server_version: Optional[Version] = None

    try:
        gh_response = requests.get(
            "https://api.github.com/repos/datahub-project/datahub/releases",
            headers={"Accept": "application/vnd.github.v3+json"},
        )
        if gh_response.ok:
            gh_response_json = gh_response.json()
            latest_server_version = Version(gh_response_json[0].get("tag_name"))
            latest_server_date = gh_response_json[0].get("published_at")
    except Exception as e:
        log.debug("Failed to get release versions from github", e)

    if not server:
        try:
            # let's get the server from the cli config
            host, token = cli_utils.get_url_and_token()
            server = DataHubGraph(DatahubClientConfig(server=host, token=token))
        except Exception as e:
            log.debug("Failed to get a valid server", e)
            pass

    server_type = None
    if server:
        server_version_string = (
            server.server_config.get("versions", {})
            .get("linkedin/datahub", {})
            .get("version")
        )
        commit_hash = (
            server.server_config.get("versions", {})
            .get("linkedin/datahub", {})
            .get("commit")
        )
        server_type = server.server_config.get("datahub", {}).get(
            "serverType", "unknown"
        )
        current_server_release_date = None
        if server_type == "quickstart" and commit_hash:
            try:
                # get the age of the commit from github
                gh_commit_response = requests.get(
                    f"https://api.github.com/repos/datahub-project/datahub/commits/{commit_hash}",
                    headers={"Accept": "application/vnd.github.v3+json"},
                )
                if gh_commit_response.ok:
                    current_server_release_date = datetime.strptime(
                        gh_commit_response.json()["commit"]["author"]["date"],
                        "%Y-%m-%dT%H:%M:%S%z",
                    )
            except Exception as e:
                log.debug(f"Failed to retrieve commit date due to {e}")
                pass

        if server_version_string and server_version_string.startswith("v"):
            server_version = Version(server_version_string[1:])

        server_version_stats = ServerVersionStats(
            current=VersionStats(
                version=server_version, release_date=current_server_release_date
            ),
            latest=VersionStats(
                version=latest_server_version, release_date=latest_server_date
            )
            if latest_server_version
            else None,
            current_server_type=server_type,
        )

    if client_version_stats and server_version_stats:
        return DataHubVersionStats(
            server=server_version_stats, client=client_version_stats
        )
    else:
        return None


def get_days(time_point: Optional[Any]) -> str:
    if time_point:
        return "(released " + (
            humanfriendly.format_timespan(
                datetime.now(timezone.utc) - time_point, max_units=1
            )
            + " ago)"
        )
    else:
        return ""


def valid_client_version(version: Version) -> bool:
    """Only version strings like 0.4.5 and 0.6.7.8 are valid. 0.8.6.7rc1 is not"""
    if version.is_prerelease or version.is_postrelease or version.is_devrelease:
        return False
    if version.major == 0 and version.minor in [8, 9, 10, 11]:
        return True

    return False


def valid_server_version(version: Version) -> bool:
    """Only version strings like 0.8.x or 0.9.x are valid. 0.1.x is not"""
    if version.is_prerelease or version.is_postrelease or version.is_devrelease:
        return False

    if version.major == 0 and version.minor in [8, 9]:
        return True

    return False


def is_client_server_compatible(client: VersionStats, server: VersionStats) -> int:
    """
    -ve implies client is behind server
    0 implies client and server are aligned
    +ve implies server is ahead of client
    """
    if not valid_client_version(client.version) or not valid_server_version(
        server.version
    ):
        # we cannot evaluate compatibility, choose True as default
        return 0
    return server.version.micro - client.version.micro


def maybe_print_upgrade_message(  # noqa: C901
    server: Optional[DataHubGraph] = None,
) -> None:  # noqa: C901
    days_before_cli_stale = 7
    days_before_quickstart_stale = 7

    encourage_cli_upgrade = False
    client_server_compat = 0
    encourage_quickstart_upgrade = False
    with contextlib.suppress(Exception):
        version_stats = retrieve_versions(server)
        if not version_stats:
            return
        current_release_date = version_stats.client.current.release_date
        latest_release_date = (
            version_stats.client.latest.release_date
            if version_stats.client.latest
            else None
        )
        client_server_compat = is_client_server_compatible(
            version_stats.client.current, version_stats.server.current
        )

        if latest_release_date and current_release_date:
            assert version_stats.client.latest
            time_delta = latest_release_date - current_release_date
            if time_delta > timedelta(days=days_before_cli_stale):
                encourage_cli_upgrade = True
                current_version = version_stats.client.current.version
                latest_version = version_stats.client.latest.version

        if (
            version_stats.server.current_server_type
            and version_stats.server.current_server_type == "quickstart"
        ):
            # if we detect a quickstart server, we encourage upgrading the server too!
            if version_stats.server.current.release_date:
                time_delta = (
                    datetime.now(timezone.utc)
                    - version_stats.server.current.release_date
                )
                if time_delta > timedelta(days=days_before_quickstart_stale):
                    encourage_quickstart_upgrade = True
            if version_stats.server.latest and (
                version_stats.server.latest.version
                > version_stats.server.current.version
            ):
                encourage_quickstart_upgrade = True

    # Compute recommendations and print one
    if client_server_compat < 0:
        with contextlib.suppress(Exception):
            assert version_stats
            print(
                colored("❗Client-Server Incompatible❗", "yellow"),
                colored(
                    f"Your client version {version_stats.client.current.version} is newer than your server version {version_stats.server.current.version}. Downgrading the cli to {version_stats.server.current.version} is recommended.\n",
                    "cyan",
                ),
                colored(
                    f"➡️ Downgrade via `\"pip install 'acryl-datahub=={version_stats.server.current.version}'\"",
                    "cyan",
                ),
            )
    elif client_server_compat > 0:
        with contextlib.suppress(Exception):
            assert version_stats
            print(
                colored("❗Client-Server Incompatible❗", "red"),
                colored(
                    f"Your client version {version_stats.client.current.version} is older than your server version {version_stats.server.current.version}. Upgrading the cli to {version_stats.server.current.version} is recommended.\n",
                    "cyan",
                ),
                colored(
                    f"➡️  Upgrade via \"pip install 'acryl-datahub=={version_stats.server.current.version}'\"",
                    "cyan",
                ),
            )
    elif client_server_compat == 0 and encourage_cli_upgrade:
        with contextlib.suppress(Exception):
            print(
                colored("💡 Upgrade cli!", "yellow"),
                colored(
                    f"You seem to be running an old version of datahub cli: {current_version} {get_days(current_release_date)}. Latest version is {latest_version} {get_days(latest_release_date)}.\nUpgrade via \"pip install -U 'acryl-datahub'\"",
                    "cyan",
                ),
            )
    elif encourage_quickstart_upgrade:
        try:
            assert version_stats
            print(
                colored("💡 Upgrade available!", "yellow"),
                colored(
                    f'You seem to be running a slightly old quickstart image {get_days(version_stats.server.current.release_date)}. Run "datahub docker quickstart" to get the latest updates without losing any data!',
                    "cyan",
                ),
            )
        except Exception as e:
            log.debug(f"Failed to suggest quickstart upgrade due to {e}")
            pass


def check_upgrade(func: Callable[..., T]) -> Callable[..., T]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:

        res = func(*args, **kwargs)
        try:
            # ensure this cannot fail
            maybe_print_upgrade_message()
        except Exception as e:
            log.debug(f"Failed to check for upgrade due to {e}")
            pass

        return res

    return wrapper
