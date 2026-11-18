import logging
from typing import Iterator, List, Set

from datahub.ingestion.source.confluence.client import ConfluenceClient
from datahub.ingestion.source.confluence.config import (
    ConfluenceSourceConfig,
)
from datahub.ingestion.source.confluence.constants import (
    PAGE_PAGE_SIZE,
    SPACE_PAGE_SIZE,
)
from datahub.ingestion.source.confluence.models import ConfluencePage
from datahub.ingestion.source.confluence.report import (
    ConfluenceSourceReport,
)

logger = logging.getLogger(__name__)


class ConfluenceCrawler:
    """Finds the pages a run ingests: every page in the selected spaces, or the
    trees under the ``pages.allow`` seeds.

    Anything that stops pages from being listed is reported as a failure, so
    stale entity removal skips the run rather than soft-deleting pages that
    were not read. With ``continue_on_failure`` off, the error is raised.
    """

    def __init__(
        self,
        client: ConfluenceClient,
        config: ConfluenceSourceConfig,
        report: ConfluenceSourceReport,
    ):
        self.client = client
        self.config = config
        self.report = report

    def iter_pages(self) -> Iterator[ConfluencePage]:
        """Each page once, even when it is reachable from several seeds."""
        seen: Set[str] = set()
        pages = (
            self._pages_from_seeds()
            if self.config._parsed_page_allow is not None
            else self._pages_from_spaces()
        )
        for page in pages:
            if page.id not in seen:
                seen.add(page.id)
                yield page

    def space_keys(self) -> List[str]:
        if self.config._parsed_space_allow is not None:
            spaces = list(self.config._parsed_space_allow)
        else:
            spaces = self._discover_spaces()
        deny = set(self.config._parsed_space_deny or [])
        return [space for space in spaces if space not in deny]

    def _discover_spaces(self) -> List[str]:
        spaces: List[str] = []
        start = 0
        while len(spaces) < self.config.max_spaces:
            try:
                batch = self.client.list_spaces(
                    start=start,
                    limit=min(SPACE_PAGE_SIZE, self.config.max_spaces - len(spaces)),
                )
            except Exception as e:
                # Keep the spaces already listed. The failure already stops
                # stale removal, so ingesting them cannot delete anything.
                self.report.failure(message="Failed to discover spaces", exc=e)
                self._raise_unless_continuing(e)
                return spaces
            spaces.extend(batch.keys)
            if not batch.fetched or not batch.has_next:
                return spaces
            # Advance by what the server returned: it may cap the page size
            # below the requested limit.
            start += batch.fetched

        # A failure rather than a warning: stale removal would otherwise
        # soft-delete every page in the spaces beyond the cap.
        self.report.failure(
            title="Space discovery stopped at max_spaces",
            message="Spaces beyond max_spaces were not ingested, so stale "
            "removal is skipped for this run. Raise max_spaces or list the "
            "spaces to ingest in spaces.allow.",
            context=f"max_spaces={self.config.max_spaces}",
        )
        return spaces

    def _pages_from_spaces(self) -> Iterator[ConfluencePage]:
        spaces = self.space_keys()
        if not spaces:
            logger.warning("No Confluence spaces to ingest")
        for space_key in spaces:
            yield from self._pages_in_space(space_key)

    def _pages_in_space(self, space_key: str) -> Iterator[ConfluencePage]:
        self.report.report_space_scanned(space_key)
        cap = self.config.max_pages_per_space
        start = 0
        count = 0
        try:
            while count < cap:
                batch = self.client.list_space_pages(
                    space_key, start=start, limit=min(PAGE_PAGE_SIZE, cap - count)
                )
                for page in batch.pages:
                    self.report.report_page_scanned()
                    yield page
                count += len(batch.pages)
                if not batch.fetched or not batch.has_next:
                    break
                # Advance by what the server returned, counting pages that could
                # not be parsed: it may cap the page size below the requested
                # limit, and advancing by the limit would skip pages in between.
                start += batch.fetched
            else:
                # The 4.x client returns a bare list with no next link, so a
                # space with exactly `cap` pages looks the same as a larger one.
                # start already points past the last batch.
                if self.client.has_more_space_pages(space_key, start):
                    self.report.report_space_truncated(space_key, cap)
                    return
        except Exception as e:
            self.report.report_space_failed(space_key, e)
            self._raise_unless_continuing(e)
            return
        self.report.report_space_processed(space_key)

    def _pages_from_seeds(self) -> Iterator[ConfluencePage]:
        visited: Set[str] = set()
        for page_id in self.config._parsed_page_allow or []:
            if page_id in visited:
                continue
            try:
                seed = self.client.get_page(page_id)
            except Exception as e:
                self.report.failure(
                    message="Failed to fetch page", context=page_id, exc=e
                )
                self._raise_unless_continuing(e)
                continue
            if seed is None:
                self.report.warning(
                    title="Page in pages.allow not found",
                    message="The page does not exist or the API user cannot view it.",
                    context=page_id,
                )
                continue
            yield from self._page_tree(seed, visited)

    def _page_tree(
        self, root: ConfluencePage, visited: Set[str]
    ) -> Iterator[ConfluencePage]:
        """The root and, with ``recursive``, all pages under it, parents before
        children. Iterative, so deep trees cannot hit the recursion limit."""
        stack = [root]
        while stack:
            page = stack.pop()
            if page.id in visited:
                continue
            visited.add(page.id)
            self.report.report_page_scanned()
            yield page
            if not self.config.recursive:
                continue
            try:
                children = list(self.client.iter_child_pages(page.id))
            except Exception as e:
                # The subtree under this page was not read; as a failure this
                # also stops stale removal from soft-deleting it.
                self.report.failure(
                    title="Failed to list child pages",
                    message="Pages under this page were not ingested.",
                    context=page.id,
                    exc=e,
                )
                self._raise_unless_continuing(e)
                continue
            stack.extend(reversed(children))

    def _raise_unless_continuing(self, e: Exception) -> None:
        if not self.config.advanced.continue_on_failure:
            raise e
