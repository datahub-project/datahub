from typing import Any, Iterator, List, Optional

from atlassian import Confluence
from pydantic import ValidationError

from datahub.ingestion.source.confluence.config import (
    ConfluenceSourceConfig,
)
from datahub.ingestion.source.confluence.constants import (
    PAGE_CONTENT_TYPE,
    PAGE_EXPAND,
    PAGE_PROBE_EXPAND,
)
from datahub.ingestion.source.confluence.models import (
    ConfluencePage,
    ConfluenceUser,
    PageBatch,
    SpaceBatch,
)
from datahub.ingestion.source.confluence.report import (
    ConfluenceSourceReport,
)
from datahub.ingestion.source.confluence.storage_format import UserRef


def create_atlassian_client(config: ConfluenceSourceConfig) -> Confluence:
    """Build the REST client. The config validator has already checked that
    the credentials match ``cloud``."""
    if config.cloud:
        assert config.username and config.api_token
        return Confluence(
            url=config.url,
            username=config.username,
            password=config.api_token.get_secret_value(),
            cloud=True,
        )
    assert config.personal_access_token
    return Confluence(
        url=config.url,
        token=config.personal_access_token.get_secret_value(),
        cloud=False,
    )


class ConfluenceClient:
    """Typed access to the Confluence v1 REST API.

    Every listing returns parsed models, so callers never read raw payloads.
    Pagination policy (caps, when to stop, what to report) belongs to the
    crawler; this class only fetches one batch at a time. API errors are
    raised to the caller, which knows what a failure means for the run.
    """

    def __init__(self, api: Confluence, report: ConfluenceSourceReport):
        self.api = api
        self.report = report

    def list_spaces(self, start: int, limit: int) -> SpaceBatch:
        response = self.api.get_all_spaces(start=start, limit=limit) or {}
        results = response.get("results") or []
        return SpaceBatch(
            keys=[
                str(space["key"])
                for space in results
                if isinstance(space, dict) and space.get("key")
            ],
            fetched=len(results),
            has_next=bool(results) and _has_next(response),
        )

    def get_space(self, space_key: str) -> None:
        """Raises when the space does not exist or cannot be viewed."""
        self.api.get_space(space_key)

    def list_space_pages(
        self, space_key: str, start: int, limit: int, expand: str = PAGE_EXPAND
    ) -> PageBatch:
        response = self.api.get_all_pages_from_space(
            space=space_key, start=start, limit=limit, expand=expand
        )
        # atlassian-python-api returns a bare list of results in some
        # versions and the raw response in others.
        if isinstance(response, list):
            results, has_next = response, bool(response)
        else:
            results = (response or {}).get("results") or []
            has_next = bool(results) and _has_next(response)
        return PageBatch(
            pages=self._parse_pages(results), fetched=len(results), has_next=has_next
        )

    def has_more_space_pages(self, space_key: str, start: int) -> bool:
        return (
            self.list_space_pages(
                space_key, start=start, limit=1, expand=PAGE_PROBE_EXPAND
            ).fetched
            > 0
        )

    def get_page(
        self, page_id: str, expand: str = PAGE_EXPAND
    ) -> Optional[ConfluencePage]:
        raw = self.api.get_page_by_id(page_id, expand=expand)
        pages = self._parse_pages([raw]) if raw else []
        return pages[0] if pages else None

    def iter_child_pages(self, page_id: str) -> Iterator[ConfluencePage]:
        """Direct child pages, with the same expand as every other listing, so
        a recursive crawl needs one call per parent instead of one per child.
        Without start/limit the client pages through all children itself."""
        children = self.api.get_page_child_by_type(
            page_id, type=PAGE_CONTENT_TYPE, expand=PAGE_EXPAND
        )
        for raw in children or []:
            yield from self._parse_pages([raw])

    def get_user(self, ref: UserRef) -> ConfluenceUser:
        if ref.account_id:
            raw = self.api.get_user_details_by_accountid(ref.account_id)
        elif ref.userkey:
            raw = self.api.get_user_details_by_userkey(ref.userkey)
        else:
            raw = self.api.get_user_details_by_username(ref.username)
        return ConfluenceUser.model_validate(raw)

    def _parse_pages(self, results: List[Any]) -> List[ConfluencePage]:
        pages = []
        for raw in results:
            try:
                pages.append(ConfluencePage.model_validate(raw))
            except ValidationError as e:
                # A failure, not a warning: the page (and in a recursive crawl,
                # everything under it) was not read, so stale removal must not
                # soft-delete what was ingested for it before.
                self.report.failure(
                    title="Skipped malformed page",
                    message="The Confluence API returned a page this source "
                    "could not read, so it and any pages under it were not "
                    "ingested. Stale removal is skipped for this run.",
                    context=str(raw.get("id")) if isinstance(raw, dict) else None,
                    exc=e,
                )
        return pages


def _has_next(response: Any) -> bool:
    return isinstance(response, dict) and bool(
        (response.get("_links") or {}).get("next")
    )
