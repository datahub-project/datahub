from contextlib import contextmanager
from typing import Dict, Iterator, List, Optional

from google.auth.exceptions import DefaultCredentialsError, RefreshError
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

from datahub.ingestion.agent.error_policy import http_status_code
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import ProbeProviderBase, take
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.google_drive.constants import (
    CREDENTIAL_APPLICATION_DEFAULT,
    CREDENTIAL_SERVICE_ACCOUNT,
    DRIVE_SCOPES,
    HTTP_FORBIDDEN,
    HTTP_NOT_FOUND,
    OAUTH_INVALID_GRANT,
    OAUTH_UNAUTHORIZED_CLIENT,
    RATE_LIMIT_REASONS,
    ROOT_KIND_MY_DRIVE_FOLDER,
    ROOT_KIND_SHARED_DRIVE,
    ROOT_KIND_SHARED_DRIVE_FOLDER,
    ROOT_PROBLEM_FORBIDDEN,
    ROOT_PROBLEM_NOT_A_FOLDER,
    ROOT_PROBLEM_NOT_FOUND,
)
from datahub.ingestion.source.google_drive.google_drive_client import (
    GoogleDriveClient,
    NotAFolderError,
    http_error_reason,
    http_error_status,
)
from datahub.ingestion.source.google_drive.google_drive_config import (
    GoogleDriveSourceConfig,
)
from datahub.ingestion.source.google_drive.google_drive_report import (
    GoogleDriveSourceReport,
)
from datahub.ingestion.source.google_drive.models import FolderNode


def _oauth_error_code(error: RefreshError) -> Optional[str]:
    # google-auth raises RefreshError(details, response_data); response_data["error"]
    # is the token endpoint's RFC 6749 error code.
    response_data = error.args[1] if len(error.args) > 1 else None
    if isinstance(response_data, dict):
        code = response_data.get("error")
        if isinstance(code, str):
            return code
    return None


def _root_kind(node: FolderNode) -> str:
    if node.drive_id is None:
        return ROOT_KIND_MY_DRIVE_FOLDER
    if node.drive_id == node.id:
        return ROOT_KIND_SHARED_DRIVE
    return ROOT_KIND_SHARED_DRIVE_FOLDER


class GoogleDriveMetadataProbe(ProbeProviderBase):
    """Metadata-only probe over the Drive API, reusing GoogleDriveClient so calls are
    retried and counted as ingestion's are.

    No raw `api` command. Drive's files.list takes a `q` query, and
    `fullText contains '...'` there answers yes or no about a document's text, so a
    caller could reconstruct a document without exporting it. Every query is built
    by the client from validated Drive IDs.
    """

    # googleapiclient logs each request URL, which carries file IDs and field masks.
    silenced_loggers = ("googleapiclient",)

    def __init__(
        self, config: GoogleDriveSourceConfig, service: Optional[Resource] = None
    ) -> None:
        self._config = config
        self._service = service
        self._report = GoogleDriveSourceReport()

    @classmethod
    def for_config(cls, config: GoogleDriveSourceConfig) -> "GoogleDriveMetadataProbe":
        return cls(config)

    @property
    def probe_report(self) -> object:
        return self._report

    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        """The HTTP status of a googleapiclient error, which keeps it on
        `.resp.status` where the framework's generic readers do not look."""
        if isinstance(exc, HttpError):
            return http_status_code(http_error_status(exc))
        return None

    @probe_method()
    def identity(self) -> Dict[str, object]:
        """The account the probe, and ingestion, act as. With delegated_user set,
        `delegation_in_effect` says whether Google issued a token for that user:
        without domain-wide delegation the account only sees what is shared with
        the service account itself."""
        with self._source_access():
            acting_as = self._client().get_authenticated_email()
        config = self._config
        result: Dict[str, object] = {
            "acting_as": acting_as,
            "credential": CREDENTIAL_SERVICE_ACCOUNT
            if config.credential
            else CREDENTIAL_APPLICATION_DEFAULT,
            "service_account": config.credential.client_email
            if config.credential
            else None,
            "delegated_user": config.delegated_user,
        }
        if config.delegated_user:
            result["delegation_in_effect"] = (
                acting_as is not None
                and acting_as.lower() == config.delegated_user.lower()
            )
        return result

    @probe_method()
    def roots(self) -> List[Dict[str, object]]:
        """Each configured root_folder_ids entry: its name and kind (my_drive_folder,
        shared_drive or shared_drive_folder) when the account can open it, or the
        problem and how to fix it when it cannot. One unreachable root does not fail
        the others. Documents in shared drives have no owners."""
        client = self._client()
        return [
            self._root_status(client, root_id)
            for root_id in self._config.root_folder_ids
        ]

    @probe_method(row_limit_param="limit")
    def shared_drives(self, limit: int = 200) -> List[Dict[str, object]]:
        """Shared drives the account is a member of, with the ID to put in
        root_folder_ids. `configured` marks the ones the recipe already ingests."""
        configured = set(self._config.root_folder_ids)
        with self._source_access():
            return take(
                (
                    {
                        "name": drive.name or drive.id,
                        "id": drive.id,
                        "configured": drive.id in configured,
                    }
                    for drive in self._client().iter_shared_drives()
                ),
                limit,
            )

    def _client(self) -> GoogleDriveClient:
        return self._open_once(
            "client", self._open_client, close=GoogleDriveClient.close
        )

    def _open_client(self) -> GoogleDriveClient:
        try:
            return GoogleDriveClient(self._config, self._report, service=self._service)
        except DefaultCredentialsError as e:
            raise ProbeConnectionError(
                "the recipe sets no credential and no Application Default "
                "Credentials were found; set credential to a service account key"
            ) from e

    @contextmanager
    def _source_access(self) -> Iterator[None]:
        # The token is fetched on the first API call, so this is where a bad key or
        # an unauthorized delegation surfaces.
        try:
            yield
        except RefreshError as e:
            raise ProbeConnectionError(self._token_failure_message(e)) from e

    def _token_failure_message(self, error: RefreshError) -> str:
        code = _oauth_error_code(error)
        delegated_user = self._config.delegated_user
        if code == OAUTH_UNAUTHORIZED_CLIENT and delegated_user:
            return (
                "Google refused a token for delegated_user: domain-wide delegation "
                "is not authorized. In the Google Workspace admin console, grant the "
                f"service account's client ID the scope {DRIVE_SCOPES[0]}"
            )
        if code == OAUTH_INVALID_GRANT and delegated_user:
            return (
                "Google refused a token for delegated_user: it is not an active "
                "user in the Workspace domain, or the service account key is no "
                "longer valid"
            )
        if code == OAUTH_INVALID_GRANT:
            return (
                "Google rejected the service account key: it was deleted or "
                "disabled, or this machine's clock is wrong"
            )
        return "Google refused to issue an access token for this credential"

    def _root_status(
        self, client: GoogleDriveClient, root_id: str
    ) -> Dict[str, object]:
        try:
            with self._source_access():
                node = client.get_root(root_id)
        except HttpError as e:
            problem = self._root_problem(e)
            if problem is None:
                raise
            return self._inaccessible(root_id, problem)
        except NotAFolderError:
            return self._inaccessible(root_id, ROOT_PROBLEM_NOT_A_FOLDER)
        return {
            "name": node.title,
            "id": node.id,
            "accessible": True,
            "kind": _root_kind(node),
        }

    @staticmethod
    def _root_problem(error: HttpError) -> Optional[str]:
        status = http_error_status(error)
        if status == HTTP_NOT_FOUND:
            return ROOT_PROBLEM_NOT_FOUND
        if status == HTTP_FORBIDDEN and http_error_reason(error) not in (
            RATE_LIMIT_REASONS
        ):
            return ROOT_PROBLEM_FORBIDDEN
        # Rate limits, 5xx and auth failures say nothing about the root: fail.
        return None

    def _inaccessible(self, root_id: str, problem: str) -> Dict[str, object]:
        return {
            "name": root_id,
            "id": root_id,
            "accessible": False,
            "problem": problem,
            "fix": self._fix(problem),
        }

    def _fix(self, problem: str) -> str:
        if problem == ROOT_PROBLEM_NOT_FOUND:
            # Drive answers "not found" for a folder the account cannot see, so an
            # unshared folder and a wrong ID look the same.
            return (
                f"Share the folder or shared drive with {self._account_hint()}, "
                "or check the ID."
            )
        if problem == ROOT_PROBLEM_FORBIDDEN:
            return (
                "The account can see this root but may not read it. A Workspace "
                "sharing policy may block it; ask the owner or a Workspace admin."
            )
        return (
            "This ID is a file, not a folder or shared drive. Use the ID of the "
            "folder that holds it."
        )

    def _account_hint(self) -> str:
        config = self._config
        if config.delegated_user:
            return config.delegated_user
        if config.credential:
            return config.credential.client_email
        return "the account in use"
