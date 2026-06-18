import logging
from datetime import datetime, timezone
from typing import Dict, Generator, Iterable, List, Optional
from urllib.parse import urlparse

import msal
import requests
from pydantic import BaseModel, field_validator

from datahub.ingestion.source.sharepoint.sharepoint_config import SharePointAuthConfig

logger = logging.getLogger(__name__)

GRAPH_API_BASE = "https://graph.microsoft.com/v1.0"
GRAPH_API_SCOPE = "https://graph.microsoft.com/.default"
_PAGE_SIZE = 200


class SharePointSite(BaseModel):
    id: str
    name: str
    display_name: str
    web_url: str
    server_relative_url: str

    @classmethod
    def from_graph(cls, data: Dict) -> "SharePointSite":
        server_relative = data.get("serverRelativeUrl", "")
        if not server_relative:
            parsed = urlparse(data.get("webUrl", ""))
            server_relative = parsed.path or "/"

        return cls.model_validate(
            {
                "id": data["id"],
                "name": data.get("name", ""),
                "display_name": data.get("displayName", data.get("name", "")),
                "web_url": data.get("webUrl", ""),
                "server_relative_url": server_relative,
            }
        )

    @property
    def path(self) -> str:
        p = self.server_relative_url.strip()
        return p if p.startswith("/") else f"/{p}"


class SharePointDrive(BaseModel):
    id: str
    name: str
    drive_type: str
    web_url: str

    @classmethod
    def from_graph(cls, data: Dict) -> "SharePointDrive":
        return cls.model_validate(
            {
                "id": data["id"],
                "name": data.get("name", ""),
                "drive_type": data.get("driveType", ""),
                "web_url": data.get("webUrl", ""),
            }
        )

    @property
    def is_document_library(self) -> bool:
        return self.drive_type == "documentLibrary"


class SharePointItem(BaseModel):
    id: str
    name: str
    web_url: str
    size: int = 0
    last_modified: datetime
    created: datetime
    mime_type: Optional[str] = None
    download_url: Optional[str] = None
    parent_path: str = ""
    is_folder: bool = False

    @field_validator("last_modified", "created", mode="before")
    @classmethod
    def parse_datetime(cls, v: str) -> datetime:
        if isinstance(v, datetime):
            return v
        dt = datetime.fromisoformat(v.rstrip("Z"))
        return dt.replace(tzinfo=timezone.utc)

    @classmethod
    def from_graph(cls, data: Dict, parent_path: str = "") -> "SharePointItem":
        file_info = data.get("file", {})
        folder_info = data.get("folder", {})

        parent_ref = data.get("parentReference", {})
        computed_parent = parent_ref.get("path", "")
        # Strip the drive root prefix (e.g. "/drives/<id>/root:") if present
        if "root:" in computed_parent:
            computed_parent = computed_parent.split("root:", 1)[1]
        if not computed_parent:
            computed_parent = parent_path

        return cls.model_validate(
            {
                "id": data["id"],
                "name": data.get("name", ""),
                "web_url": data.get("webUrl", ""),
                "size": data.get("size", 0),
                "last_modified": data.get(
                    "lastModifiedDateTime", "1970-01-01T00:00:00Z"
                ),
                "created": data.get("createdDateTime", "1970-01-01T00:00:00Z"),
                "mime_type": file_info.get("mimeType"),
                "download_url": data.get("@microsoft.graph.downloadUrl"),
                "parent_path": computed_parent,
                "is_folder": bool(folder_info),
            }
        )

    @property
    def extension(self) -> str:
        """Lowercase file extension without the leading dot, e.g. 'csv'."""
        dot_idx = self.name.rfind(".")
        if dot_idx == -1 or self.is_folder:
            return ""
        return self.name[dot_idx + 1 :].lower()

    def full_path(self, library_name: str) -> str:
        """Logical path within the library: '<library>/<parent>/<name>'."""
        parts = [library_name]
        if self.parent_path:
            parts.append(self.parent_path.strip("/"))
        parts.append(self.name)
        return "/".join(p for p in parts if p)


class SharePointPage(BaseModel):
    id: str
    title: str
    web_url: str
    last_modified: datetime
    created: datetime
    content_html: str = ""

    @field_validator("last_modified", "created", mode="before")
    @classmethod
    def parse_datetime(cls, v: str) -> datetime:
        if isinstance(v, datetime):
            return v
        dt = datetime.fromisoformat(v.rstrip("Z"))
        return dt.replace(tzinfo=timezone.utc)

    @classmethod
    def from_graph(cls, data: Dict) -> "SharePointPage":
        return cls.model_validate(
            {
                "id": data["id"],
                "title": data.get("title", data.get("name", "")),
                "web_url": data.get("webUrl", ""),
                "last_modified": data.get(
                    "lastModifiedDateTime", "1970-01-01T00:00:00Z"
                ),
                "created": data.get("createdDateTime", "1970-01-01T00:00:00Z"),
                "content_html": "",
            }
        )


class SharePointClientError(Exception):
    """Raised when the Graph API returns an error response."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"Graph API error {status_code}: {message}")


class SharePointClient:
    """Authenticated Microsoft Graph API client for SharePoint.

    Uses MSAL client-credentials flow to obtain OAuth 2.0 tokens. Tokens are
    cached in memory and refreshed automatically when they expire.
    """

    def __init__(self, auth: SharePointAuthConfig, hostname: str) -> None:
        self._auth = auth
        self._hostname = hostname
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})
        self._msal_app: Optional[msal.ConfidentialClientApplication] = None

    def _build_msal_app(self) -> msal.ConfidentialClientApplication:
        authority = f"https://login.microsoftonline.com/{self._auth.tenant_id}"

        if self._auth.client_secret is not None:
            return msal.ConfidentialClientApplication(
                client_id=self._auth.client_id,
                client_credential=self._auth.client_secret.get_secret_value(),
                authority=authority,
            )

        # Certificate-based auth
        assert self._auth.certificate_path is not None
        password = (
            self._auth.certificate_password.get_secret_value()
            if self._auth.certificate_password
            else None
        )
        with open(self._auth.certificate_path, "rb") as cert_file:
            cert_data = cert_file.read()

        # msal accepts either PEM or PFX; determine format by content
        if cert_data.lstrip().startswith(b"-----"):
            credential: Dict = {"private_key": cert_data.decode()}
        else:
            credential = {"private_key_pfx": cert_data, "passphrase": password}

        return msal.ConfidentialClientApplication(
            client_id=self._auth.client_id,
            client_credential=credential,
            authority=authority,
        )

    def _acquire_token(self) -> str:
        if self._msal_app is None:
            self._msal_app = self._build_msal_app()

        result = self._msal_app.acquire_token_for_client(scopes=[GRAPH_API_SCOPE])
        if "access_token" not in result:
            error = result.get("error_description", result.get("error", "unknown"))
            raise SharePointClientError(401, f"Failed to acquire token: {error}")

        return result["access_token"]

    def _refresh_session_token(self) -> None:
        token = self._acquire_token()
        self._session.headers.update({"Authorization": f"Bearer {token}"})

    def _get(self, path: str, **params: str) -> Dict:
        """Authenticated GET against the Graph API; retries once on 401."""
        url = f"{GRAPH_API_BASE}{path}"
        self._refresh_session_token()
        response = self._session.get(url, params=params or None, timeout=60)
        if response.status_code == 401:
            self._refresh_session_token()
            response = self._session.get(url, params=params or None, timeout=60)

        if not response.ok:
            body = response.json() if response.content else {}
            msg = body.get("error", {}).get("message", response.reason)
            raise SharePointClientError(response.status_code, msg)

        return response.json()  # type: ignore[return-value]

    def _get_download(self, url: str) -> bytes:
        response = requests.get(url, timeout=120)
        if not response.ok:
            raise SharePointClientError(
                response.status_code,
                f"Download failed: {response.reason}",
            )
        return response.content

    def _paginate(self, path: str, **params: str) -> Generator[Dict, None, None]:
        """Yield every item from a paginated Graph API list endpoint."""
        params.setdefault("$top", str(_PAGE_SIZE))
        data = self._get(path, **params)
        yield from data.get("value", [])

        next_link: Optional[str] = data.get("@odata.nextLink")
        while next_link:
            self._refresh_session_token()
            response = self._session.get(next_link, timeout=60)
            if not response.ok:
                body = response.json() if response.content else {}
                msg = body.get("error", {}).get("message", response.reason)
                raise SharePointClientError(response.status_code, msg)
            data = response.json()
            yield from data.get("value", [])
            next_link = data.get("@odata.nextLink")

    def list_sites(self) -> Iterable[SharePointSite]:
        """Yield all SharePoint sites accessible to the service principal.

        Uses the Graph search endpoint to discover all sites under the configured
        hostname, falling back to the root site if search is unavailable.
        """
        try:
            for raw in self._paginate(
                "/sites",
                **{
                    "$search": "*",
                    "$select": "id,name,displayName,webUrl,siteCollection,serverRelativeUrl",
                },
            ):
                site = SharePointSite.from_graph(raw)
                if self._hostname.lower() in site.web_url.lower():
                    yield site
        except SharePointClientError as exc:
            logger.warning(
                f"Site search via $search='*' failed ({exc}); "
                "falling back to root site only."
            )
            root = self.get_root_site()
            if root:
                yield root

    def get_root_site(self) -> Optional[SharePointSite]:
        """Return the root site for the configured hostname."""
        try:
            raw = self._get(f"/sites/{self._hostname}")
            return SharePointSite.from_graph(raw)
        except SharePointClientError as exc:
            logger.error(f"Failed to fetch root site for {self._hostname}: {exc}")
            return None

    def list_drives(self, site_id: str) -> Iterable[SharePointDrive]:
        """Yield document libraries for the given site."""
        for raw in self._paginate(f"/sites/{site_id}/drives"):
            yield SharePointDrive.from_graph(raw)

    def list_root_items(self, site_id: str, drive_id: str) -> Iterable[SharePointItem]:
        """Yield immediate children of the drive root."""
        for raw in self._paginate(
            f"/sites/{site_id}/drives/{drive_id}/root/children",
            **{
                "$select": "id,name,webUrl,size,lastModifiedDateTime,createdDateTime,file,folder,parentReference,@microsoft.graph.downloadUrl"
            },
        ):
            yield SharePointItem.from_graph(raw)

    def list_folder_children(
        self, drive_id: str, item_id: str, parent_path: str = ""
    ) -> Iterable[SharePointItem]:
        """Yield immediate children of a specific folder."""
        for raw in self._paginate(
            f"/drives/{drive_id}/items/{item_id}/children",
            **{
                "$select": "id,name,webUrl,size,lastModifiedDateTime,createdDateTime,file,folder,parentReference,@microsoft.graph.downloadUrl"
            },
        ):
            yield SharePointItem.from_graph(raw, parent_path=parent_path)

    def list_items_recursive(
        self,
        site_id: str,
        drive_id: str,
        library_name: str,
    ) -> Generator[SharePointItem, None, None]:
        """Recursively yield all file items (not folders) in a document library.

        Performs a breadth-first traversal using a stack of (item_id, path) pairs.
        """
        stack: List[tuple] = [("root", "")]

        while stack:
            item_id, current_path = stack.pop()

            if item_id == "root":
                children = list(self.list_root_items(site_id, drive_id))
            else:
                children = list(
                    self.list_folder_children(drive_id, item_id, current_path)
                )

            for item in children:
                if item.is_folder:
                    child_path = f"{current_path}/{item.name}".lstrip("/")
                    stack.append((item.id, child_path))
                else:
                    yield item

    def download_file_bytes(self, download_url: str) -> bytes:
        """Download file content using a pre-authorised download URL."""
        return self._get_download(download_url)

    def list_pages(self, site_id: str) -> Iterable[SharePointPage]:
        """Yield SharePoint modern pages for the given site."""
        try:
            for raw in self._paginate(
                f"/sites/{site_id}/pages",
                **{
                    "$select": "id,title,name,webUrl,lastModifiedDateTime,createdDateTime"
                },
            ):
                yield SharePointPage.from_graph(raw)
        except SharePointClientError as exc:
            logger.warning(
                f"Failed to list pages for site {site_id}: {exc}. "
                "The SharePoint Pages API may not be enabled for this tenant."
            )

    def get_page_html(self, site_id: str, page_id: str) -> str:
        """Return best-effort HTML for a SharePoint page's canvas content."""
        try:
            data = self._get(
                f"/sites/{site_id}/pages/{page_id}/microsoft.graph.sitePage",
                **{"$expand": "canvasLayout"},
            )
        except SharePointClientError as exc:
            logger.debug(f"Could not fetch canvas layout for page {page_id}: {exc}")
            return ""

        html_parts: List[str] = []
        canvas = data.get("canvasLayout", {})
        for section in canvas.get("horizontalSections", []):
            for column in section.get("columns", []):
                for webpart in column.get("webparts", []):
                    inner = webpart.get("innerHtml", "")
                    if inner:
                        html_parts.append(inner)

        return "\n".join(html_parts)

    def test_connectivity(self) -> None:
        """Raise SharePointClientError if basic connectivity fails."""
        self._get(f"/sites/{self._hostname}")
