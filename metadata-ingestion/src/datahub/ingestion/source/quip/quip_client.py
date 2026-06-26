import logging
import time
from typing import List, Optional

import requests
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# Quip enforces per-user and per-company rate limits, returning HTTP 429 with an
# X-RateLimit-Reset (epoch seconds) header when exceeded.
RATE_LIMIT_STATUS = 429
MAX_RETRIES = 5
DEFAULT_BACKOFF_SECONDS = 5.0
MAX_BACKOFF_SECONDS = 120.0
DEFAULT_BASE_URL = "https://platform.quip.com"


class QuipUser(BaseModel):
    name: Optional[str] = None
    private_folder_id: Optional[str] = None
    desktop_folder_id: Optional[str] = None
    shared_folder_ids: List[str] = Field(default_factory=list)
    group_folder_ids: List[str] = Field(default_factory=list)

    @property
    def root_folder_ids(self) -> List[str]:
        """Top-level folders accessible to the user, de-duplicated, trash excluded."""
        candidates = [
            self.private_folder_id,
            self.desktop_folder_id,
            *self.shared_folder_ids,
            *self.group_folder_ids,
        ]
        seen: set = set()
        unique: List[str] = []
        for folder_id in candidates:
            if folder_id and folder_id not in seen:
                seen.add(folder_id)
                unique.append(folder_id)
        return unique


class QuipFolderChild(BaseModel):
    thread_id: Optional[str] = None
    folder_id: Optional[str] = None


class QuipFolderInfo(BaseModel):
    id: Optional[str] = None
    title: Optional[str] = None
    created_usec: Optional[int] = None
    updated_usec: Optional[int] = None


class QuipFolder(BaseModel):
    folder: QuipFolderInfo = Field(default_factory=QuipFolderInfo)
    children: List[QuipFolderChild] = Field(default_factory=list)


class QuipThreadInfo(BaseModel):
    id: Optional[str] = None
    title: Optional[str] = None
    type: Optional[str] = None
    link: Optional[str] = None
    author_id: Optional[str] = None
    created_usec: Optional[int] = None
    updated_usec: Optional[int] = None


class QuipThread(BaseModel):
    thread: QuipThreadInfo = Field(default_factory=QuipThreadInfo)
    html: str = ""


class QuipClientError(Exception):
    def __init__(self, status_code: int, message: str):
        super().__init__(f"{status_code}: {message}")
        self.status_code = status_code
        self.message = message


class QuipClient:
    """Read-only wrapper around the GET endpoints of the Quip Automation API.

    Authentication is a Personal Access Token (or OAuth access token) passed as a
    bearer token; generate one at ``<base_url>/dev/token``.
    """

    def __init__(
        self,
        access_token: str,
        base_url: str = DEFAULT_BASE_URL,
        request_timeout: float = 30.0,
        max_retries: int = MAX_RETRIES,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout
        self.max_retries = max_retries
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {access_token}"})

    def get_authenticated_user(self) -> QuipUser:
        return QuipUser.model_validate(self._get("users/current"))

    def get_folder(self, folder_id: str) -> QuipFolder:
        return QuipFolder.model_validate(self._get(f"folders/{folder_id}"))

    def get_thread(self, thread_id: str) -> QuipThread:
        return QuipThread.model_validate(self._get(f"threads/{thread_id}"))

    def _url(self, path: str) -> str:
        return f"{self.base_url}/1/{path.lstrip('/')}"

    def _get(self, path: str) -> dict:
        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self._session.get(
                    self._url(path), timeout=self.request_timeout
                )
            except requests.RequestException as e:
                last_error = e
                if attempt >= self.max_retries:
                    raise QuipClientError(0, f"Request to {path} failed: {e}") from e
                time.sleep(DEFAULT_BACKOFF_SECONDS * (attempt + 1))
                continue

            if response.status_code == RATE_LIMIT_STATUS:
                wait_seconds = self._seconds_until_reset(response)
                logger.warning(
                    "Quip rate limit hit on %s, waiting %.1fs (attempt %d/%d)",
                    path,
                    wait_seconds,
                    attempt + 1,
                    self.max_retries,
                )
                if attempt >= self.max_retries:
                    raise QuipClientError(
                        response.status_code, "Rate limit exceeded after retries"
                    )
                time.sleep(wait_seconds)
                continue

            if not response.ok:
                raise QuipClientError(
                    response.status_code, self._error_message(response)
                )

            return response.json()

        raise QuipClientError(0, f"Request to {path} failed: {last_error}")

    @staticmethod
    def _seconds_until_reset(response: requests.Response) -> float:
        reset = response.headers.get("X-RateLimit-Reset") or response.headers.get(
            "X-Company-RateLimit-Reset"
        )
        if reset:
            try:
                # Add a small buffer to avoid racing the reset boundary.
                delta = float(reset) - time.time()
                if delta > 0:
                    return min(delta + 1.0, MAX_BACKOFF_SECONDS)
            except ValueError:
                pass
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(float(retry_after), MAX_BACKOFF_SECONDS)
            except ValueError:
                pass
        return DEFAULT_BACKOFF_SECONDS

    @staticmethod
    def _error_message(response: requests.Response) -> str:
        try:
            return response.json().get("error_description", response.text)
        except ValueError:
            return response.text or response.reason
