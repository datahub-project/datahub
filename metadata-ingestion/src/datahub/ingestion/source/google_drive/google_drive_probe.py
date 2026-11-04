from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Deque, Dict, Iterator, List, Optional, Sequence, Set

from google.auth.exceptions import DefaultCredentialsError, RefreshError
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

from datahub.ingestion.agent.error_policy import http_status_code
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import ProbeProviderBase, echoed, take
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    Verdict,
    VerdictContext,
)
from datahub.ingestion.source.common.subtypes import DocumentSubTypes
from datahub.ingestion.source.google_drive.constants import (
    ATTR_ANCESTORS,
    ATTR_FOLDERS,
    CHAIN_SEPARATOR,
    CREDENTIAL_APPLICATION_DEFAULT,
    CREDENTIAL_SERVICE_ACCOUNT,
    DRIVE_ID_RE,
    DRIVE_SCOPES,
    DRIVE_URL_FILE_ID_RE,
    FOLDER_PATH_SEPARATOR,
    HTTP_FORBIDDEN,
    HTTP_NOT_FOUND,
    KIND_GOOGLE_DOC,
    MAX_FOLDER_DEPTH,
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
from datahub.ingestion.source.google_drive.google_drive_selection import (
    DriveSelectionConfig,
    exported_text_verdict,
    folder_document_verdict,
    google_doc_verdict,
)
from datahub.ingestion.source.google_drive.google_drive_source import clean_markdown
from datahub.ingestion.source.google_drive.models import DriveFile, FolderNode

KIND_FOLDER = str(DocumentSubTypes.FOLDER)
# Why `explain` says a Doc is not ingested, beyond the config rule names
# google_drive_selection returns and the export failures ExportResult carries.
NOT_A_GOOGLE_DOC = "not_a_google_doc"
TRASHED = "trashed"
OUTSIDE_ROOTS = "outside_roots"
_EXPLANATIONS = {
    NOT_A_GOOGLE_DOC: "Only Google Docs are ingested.",
    TRASHED: "It is in the trash.",
    OUTSIDE_ROOTS: "It is not under any configured root, or the account cannot "
    "see a folder between it and one.",
    "folder_pattern": "folder_pattern skips a folder above it, so the walk never "
    "lists it.",
    "recursive": "It is below a subfolder and recursive is false.",
    "document_pattern": "document_pattern skips its title.",
    "skip_empty_documents": "Its exported text is empty.",
    "min_text_length": "Its exported text is shorter than min_text_length.",
    "max_text_length": "Its exported text is longer than max_text_length; the run "
    "warns and skips it.",
    "not_found": "Google Drive could not export it.",
    "too_large": "Google Drive refuses to export Docs over 10 MB.",
    "export_failed": "Google Drive could not export it; the run reports a failure "
    "and retries it next time.",
}
_SPLIT_PATH_WARNING = (
    "judged by splitting the folder path on '/', so a folder whose name contains "
    "'/' may be judged wrongly; `probe filter --from-run` on a `folders` or "
    "`documents` listing uses the exact folder chain"
)


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


def _split_chain(path: str) -> List[str]:
    parts = path.split(FOLDER_PATH_SEPARATOR)
    return [FOLDER_PATH_SEPARATOR.join(parts[: i + 1]) for i in range(len(parts))]


def _attribute_chain(ctx: VerdictContext, key: str) -> Optional[List[str]]:
    if key not in ctx.attributes:
        return None
    value = ctx.attributes[key]
    return value.split(CHAIN_SEPARATOR) if value else []


def probe_verdict(
    config: DriveSelectionConfig, ctx: VerdictContext
) -> Optional[Verdict]:
    """Ingestion's verdict for a folder (named by its path) or a Doc (named by its
    title), judged on the whole folder chain: the walk never lists a denied folder,
    so nothing beneath one is reached. The chain comes from the listing record's
    facts, or from splitting the path when only a bare name was given."""
    if ctx.kind == KIND_FOLDER:
        ancestors = _attribute_chain(ctx, ATTR_ANCESTORS)
        if ancestors is None:
            ctx.warn(_SPLIT_PATH_WARNING)
            return folder_document_verdict(config, _split_chain(ctx.name))
        return folder_document_verdict(config, [*ancestors, ctx.name])
    if ctx.kind == KIND_GOOGLE_DOC:
        folders = _attribute_chain(ctx, ATTR_FOLDERS)
        if folders is None and ctx.parent_path:
            ctx.warn(_SPLIT_PATH_WARNING)
            folders = _split_chain(ctx.parent_path[-1])
        if not folders:
            ctx.warn(
                "no folder given, so only document_pattern was judged; pass the "
                "Doc's folder path with --parent to judge folder_pattern and "
                "recursive too"
            )
            return None
        return google_doc_verdict(config, folders, ctx.target)
    return None


@dataclass(frozen=True)
class _WalkedFolder:
    node: FolderNode
    # Folder paths above this one, root first.
    ancestors: Sequence[str]

    @property
    def chain(self) -> List[str]:
        return [*self.ancestors, self.node.path]


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
        self.failures: List[str] = []

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

    @probe_method(kind=KIND_FOLDER, row_limit_param="limit")
    def folders(self, limit: int = 200) -> List[Dict[str, object]]:
        """Folders under the configured roots, by the path folder_pattern is matched
        against (the root's name first), including folders the recipe would skip:
        a denied folder and everything beneath it are listed, not hidden, so
        `probe filter --kind Folder` can explain them. Breadth first, so a small
        limit shows the top of the tree. Lists every folder it returns, which a
        large Drive makes slow."""
        with self._source_access():
            return take(
                (
                    {
                        "name": folder.node.path,
                        "id": folder.node.id,
                        "depth": len(folder.ancestors),
                        ATTR_ANCESTORS: CHAIN_SEPARATOR.join(folder.ancestors),
                    }
                    for folder in self._iter_folders()
                ),
                limit,
            )

    @probe_method(
        kind=KIND_GOOGLE_DOC, row_limit_param="limit", parent_params=("folder",)
    )
    def documents(self, folder: str, limit: int = 200) -> List[Dict[str, object]]:
        """Google Docs directly inside the folder at a path as `folders` lists it
        (every such folder, since Drive allows same-named siblings), including Docs
        document_pattern would skip. Titles, IDs and modified times only; never a
        Doc's text or owners. Whether a Doc is ingested also depends on its
        exported text (empty, too short or too long), which this does not read,
        and on max_documents, which stops a run that embeds that many Docs; `scope`
        says whether a run can reach it."""
        with self._source_access():
            return take(
                (
                    {
                        "name": file.name,
                        "id": file.id,
                        "modified_time": file.modified_time.isoformat()
                        if file.modified_time
                        else None,
                        ATTR_FOLDERS: CHAIN_SEPARATOR.join(walked.chain),
                    }
                    for walked in self._resolve_folders(folder)
                    for file in self._children(walked)
                    if file.is_document
                ),
                limit,
            )

    @probe_method()
    def scope(self) -> Dict[str, object]:
        """What a run would pick up, counted the way ingestion counts it: the same
        folder walk and filters, without exporting any Doc. `documents_in_scope` is
        an upper bound on what is ingested, since empty, too short and too long Docs
        are skipped after export. Lists every folder in scope, as a run does."""
        with self._source_access():
            roots = [walked.node for walked in self._open_roots()]
            tree = self._client().walk(roots)
        report = self._report
        max_documents = self._config.max_documents
        return {
            "roots": len(roots),
            "folders_in_scope": len(tree.folders),
            "folders_discovered": report.folders_discovered,
            "folders_filtered": report.folders_filtered,
            "documents_discovered": report.documents_discovered,
            "documents_filtered": report.documents_filtered,
            "documents_in_scope": len(tree.documents),
            "documents_unparseable": report.documents_unparseable,
            "shortcuts_skipped": report.shortcuts_skipped,
            "max_documents": max_documents,
            "may_reach_max_documents": max_documents > 0
            and len(tree.documents) >= max_documents,
        }

    @probe_method()
    def explain(self, document: str) -> Dict[str, object]:
        """Whether one file would be ingested, and if not, which rule decides,
        given its Drive ID or a Docs or Drive URL. Climbs from the file to a
        configured root and applies the folder and title rules ingestion applies;
        only when those pass does it export the Doc, to check the text-length
        rules, and it reports the length, never the text. The file's name is given
        only once it is under a configured root, and owners only when
        extract_owners is on, since only then does ingestion emit them.
        `ingested: true` also assumes the run reaches the Doc: a run stops once
        max_documents Docs are embedded, which `scope` checks."""
        file_id = self._file_id(document)
        with self._source_access():
            file = self._get_visible_file(file_id)
            result: Dict[str, object] = {"id": file.id, "mime_type": file.mime_type}
            if not file.is_document:
                return self._explained(result, NOT_A_GOOGLE_DOC)
            if file.trashed:
                return self._explained(result, TRASHED)
            folders = self._folder_chain(file)
            if folders is None:
                return self._explained(result, OUTSIDE_ROOTS)
            # Named only once it is under a configured root: the credential can
            # see far more of Drive than the recipe ingests.
            result["name"] = file.name
            result["folder"] = folders[-1]
            verdict = google_doc_verdict(self._config, folders, file.name)
            if not verdict.included:
                assert verdict.excluded_by is not None
                if verdict.matched_target:
                    result["decided_by_folder"] = verdict.matched_target
                return self._explained(result, verdict.excluded_by)
            self._add_owners(result, file)
            export = self._client().export_markdown(file.id)
        result["used_html_fallback"] = export.used_html_fallback
        if export.text is None:
            assert export.failure_reason is not None
            return self._explained(result, export.failure_reason)
        text_length = len(clean_markdown(export.text))
        result["text_length"] = text_length
        text_verdict = exported_text_verdict(self._config, text_length)
        if not text_verdict.included:
            assert text_verdict.excluded_by is not None
            return self._explained(result, text_verdict.excluded_by)
        result["ingested"] = True
        result["max_documents"] = self._config.max_documents
        return result

    @staticmethod
    def _explained(result: Dict[str, object], reason: str) -> Dict[str, object]:
        result["ingested"] = False
        result["excluded_by"] = reason
        result["explanation"] = _EXPLANATIONS[reason]
        return result

    @staticmethod
    def _file_id(document: str) -> str:
        candidate = document.strip()
        match = DRIVE_URL_FILE_ID_RE.search(candidate)
        if match:
            candidate = match.group(1)
        if not DRIVE_ID_RE.match(candidate):
            raise ProbeArgumentError(
                f"{echoed(document)} is not a Drive file ID or a Docs or Drive file URL"
            )
        return candidate

    def _get_visible_file(self, file_id: str) -> DriveFile:
        try:
            return self._client().get_file(file_id)
        except HttpError as e:
            if http_error_status(e) != HTTP_NOT_FOUND:
                raise
            raise ProbeArgumentError(
                f"no file '{file_id}' that {self._account_hint()} can see"
            ) from e

    def _folder_chain(self, file: DriveFile) -> Optional[List[str]]:
        """The folder paths from a configured root down to the file's folder, root
        first, as the ingestion walk would build them; None when no configured root
        is above it. Follows the first parent, as Drive gives each item one."""
        roots = {walked.node.id: walked.node for walked in self._open_roots()}
        names: List[str] = []
        parents = file.parents
        for _ in range(MAX_FOLDER_DEPTH):
            if not parents:
                return None
            parent_id = parents[0]
            root = roots.get(parent_id)
            if root is not None:
                chain = [root.path]
                for name in reversed(names):
                    chain.append(chain[-1] + FOLDER_PATH_SEPARATOR + name)
                return chain
            try:
                parent = self._client().get_file(parent_id)
            except HttpError as e:
                # A rate limit or 5xx says nothing about access: raise, rather
                # than answer "outside the roots".
                if self._root_problem(e) is None:
                    raise
                # A folder the account cannot see: the walk could not pass it.
                return None
            names.append(parent.name)
            parents = parent.parents
        return None

    def _add_owners(self, result: Dict[str, object], file: DriveFile) -> None:
        emails: List[str] = []
        for owner in file.owners:
            if owner.email_address and owner.email_address not in emails:
                emails.append(owner.email_address)
        if self._config.extract_owners:
            result["owners"] = emails
        else:
            # Withheld: ingestion does not emit owners with extract_owners off.
            result["owners_withheld"] = len(emails)

    def _open_roots(self) -> List[_WalkedFolder]:
        client = self._client()
        roots: List[_WalkedFolder] = []
        for root_id in self._config.root_folder_ids:
            try:
                node = client.get_root(root_id)
            except HttpError as e:
                problem = self._root_problem(e)
                if problem is None:
                    raise
                self._unopened_root(root_id, problem)
                continue
            except NotAFolderError:
                self._unopened_root(root_id, ROOT_PROBLEM_NOT_A_FOLDER)
                continue
            roots.append(_WalkedFolder(node=node, ancestors=()))
        return roots

    def _unopened_root(self, root_id: str, problem: str) -> None:
        # A failure, not a warning: the tree under it is left unlisted.
        self.failures.append(
            f"root '{root_id}' could not be opened ({problem}); run `roots` for the fix"
        )

    def _children(self, folder: _WalkedFolder) -> List[DriveFile]:
        try:
            return list(self._client().list_children(folder.node.id, []))
        except HttpError as e:
            # Rate limits and 5xx raise; only an access refusal is a gap.
            if self._root_problem(e) is None:
                raise
            status = http_error_status(e)
            # As in ingestion, a folder that cannot be listed leaves the tree
            # incomplete, so it is a failure rather than an empty folder.
            self.failures.append(
                f"folder '{folder.node.path}' could not be listed (HTTP {status})"
            )
            return []

    def _child_folders(self, folder: _WalkedFolder) -> List[_WalkedFolder]:
        return [
            _WalkedFolder(
                node=FolderNode(
                    id=child.id,
                    title=child.name,
                    parent_id=folder.node.id,
                    path=folder.node.path + FOLDER_PATH_SEPARATOR + child.name,
                    drive_id=child.drive_id,
                    web_view_link=child.web_view_link,
                ),
                ancestors=folder.chain,
            )
            for child in self._children(folder)
            if child.is_folder
        ]

    def _iter_folders(self) -> Iterator[_WalkedFolder]:
        # Unlike the ingestion walk, this descends into folders the recipe skips,
        # so their verdicts can be explained.
        # A folder is listed only once the caller wants more than is already
        # queued, so a small limit costs no listing it does not use.
        pending: Deque[_WalkedFolder] = deque(self._open_roots())
        to_expand: Deque[_WalkedFolder] = deque()
        seen: Set[str] = {folder.node.id for folder in pending}
        while pending or to_expand:
            if pending:
                folder = pending.popleft()
                yield folder
                to_expand.append(folder)
                continue
            for child in self._child_folders(to_expand.popleft()):
                if child.node.id not in seen:
                    seen.add(child.node.id)
                    pending.append(child)

    def _resolve_folders(self, path: str) -> List[_WalkedFolder]:
        """Every folder at `path`, found by name from the roots down: Drive allows
        same-named folders side by side, and ingestion walks each of them. Names
        are compared with the listed children, so the caller's text never reaches
        a Drive query, and a name containing '/' still resolves."""
        matches: Dict[str, _WalkedFolder] = {}
        for root in self._open_roots():
            for match in self._descend(root, path):
                matches.setdefault(match.node.id, match)
        if not matches:
            raise ProbeArgumentError(
                f"no folder at '{path}' under the configured roots; list them with "
                "`folders`"
            )
        return list(matches.values())

    def _descend(self, folder: _WalkedFolder, path: str) -> List[_WalkedFolder]:
        own = folder.node.path
        if path == own:
            return [folder]
        if not path.startswith(own + FOLDER_PATH_SEPARATOR):
            return []
        found: List[_WalkedFolder] = []
        for child in self._child_folders(folder):
            found.extend(self._descend(child, path))
        return found

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
