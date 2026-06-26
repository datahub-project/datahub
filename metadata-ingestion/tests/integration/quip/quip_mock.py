from typing import Dict

from datahub.ingestion.source.quip.quip_client import (
    QuipClientError,
    QuipFolder,
    QuipThread,
    QuipUser,
)

# A small Quip site: a root folder with one sub-folder; a thread lives directly
# under each folder, and one thread (T1) is reachable from both folders to
# exercise shallowest-parent de-duplication.
USER_RAW = {
    "name": "Test User",
    "private_folder_id": "FROOT",
    "shared_folder_ids": ["FSHARED"],
}

FOLDERS_RAW = {
    "FROOT": {
        "folder": {
            "id": "FROOT",
            "title": "Root",
            "created_usec": 1_700_000_000_000_000,
            "updated_usec": 1_700_000_500_000_000,
        },
        "children": [{"folder_id": "FSUB"}, {"thread_id": "T1"}],
    },
    "FSUB": {
        "folder": {"id": "FSUB", "title": "Sub", "created_usec": 1_700_000_600_000_000},
        "children": [{"thread_id": "T2"}, {"thread_id": "T1"}],
    },
    "FSHARED": {
        "folder": {"id": "FSHARED", "title": "Shared"},
        "children": [{"thread_id": "T3"}],
    },
}

THREADS_RAW = {
    "T1": {
        "thread": {
            "id": "T1",
            "title": "Welcome",
            "type": "document",
            "link": "https://platform.quip.com/T1",
            "author_id": "U1",
            "created_usec": 1_700_000_100_000_000,
            "updated_usec": 1_700_000_200_000_000,
        },
        "html": "<h1>Welcome</h1><p>This is the welcome document for the team space.</p>",
    },
    "T2": {
        "thread": {
            "id": "T2",
            "title": "Design Doc",
            "type": "document",
            "link": "https://platform.quip.com/T2",
        },
        "html": "<h2>Design</h2><p>Architecture notes that are long enough to index.</p>",
    },
    "T3": {
        "thread": {
            "id": "T3",
            "title": "Team Chat",
            "type": "chat",
            "link": "https://platform.quip.com/T3",
        },
        "html": "<p>Some chat content that comfortably exceeds the minimum length.</p>",
    },
}


class FakeQuipClient:
    """In-memory stand-in for ``QuipClient`` backed by static fixtures.

    Parses the raw fixtures through the real pydantic models so the integration
    test exercises the same response-model code path as production.
    """

    def __init__(
        self,
        user: Dict,
        folders: Dict[str, Dict],
        threads: Dict[str, Dict],
    ) -> None:
        self._user = user
        self._folders = folders
        self._threads = threads

    def get_authenticated_user(self) -> QuipUser:
        return QuipUser.model_validate(self._user)

    def get_folder(self, folder_id: str) -> QuipFolder:
        if folder_id not in self._folders:
            raise QuipClientError(404, f"folder {folder_id} not found")
        return QuipFolder.model_validate(self._folders[folder_id])

    def get_thread(self, thread_id: str) -> QuipThread:
        if thread_id not in self._threads:
            raise QuipClientError(404, f"thread {thread_id} not found")
        return QuipThread.model_validate(self._threads[thread_id])


def build_fake_client() -> FakeQuipClient:
    return FakeQuipClient(user=USER_RAW, folders=FOLDERS_RAW, threads=THREADS_RAW)
