"""Business Glossary ingestion for Dataplex source.

Ingests Dataplex Business Glossaries as DataHub GlossaryNodes and GlossaryTerms,
preserving the Glossary → Category → Term hierarchy. Term-to-asset associations are
optionally resolved via the lookupEntryLinks REST API.
"""

from __future__ import annotations

import logging
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from itertools import islice
from typing import Dict, Iterable, List, Tuple

from google.cloud import dataplex_v1

from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dataplex.dataplex_context import DataplexContext
from datahub.metadata.urns import DatasetUrn, GlossaryNodeUrn, GlossaryTermUrn
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.sdk.glossary_node import GlossaryNode
from datahub.sdk.glossary_term import GlossaryTerm
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)

# Matches entry batching used in dataplex_entries.py and dataplex_lineage.py.
WORKERS_BATCH_SIZE = 200

# Dataplex built-in entry link types live under a GCP-managed system project
# in the fixed "global" location.
#
# Known link types:
#   definition  — term describes / is applied to an asset; maps to DataHub glossaryTerms
#   synonym     — relates two terms that are synonyms of each other (term-to-term only)
#
# We only ingest "definition" links because they represent the term-to-asset
# relationship that DataHub's glossaryTerms aspect models. Synonym and other
# term-to-term link types have no direct DataHub equivalent today.
# TODO: consider ingesting synonym links as GlossaryRelatedTerms in the future.
#
# NOTE: Google returns the system project as a numeric project number (e.g.
# 655216118709) rather than the name "dataplex-types", so we match on the
# stable suffix rather than the full path.
_DEFINITION_LINK_TYPE_SUFFIX = "/entryLinkTypes/definition"
# Role in an entryLink that identifies the asset side (not the term side).
_SOURCE_ROLE = "SOURCE"


# ---------------------------------------------------------------------------
# Glossary URN helpers
# ---------------------------------------------------------------------------


def _glossary_node_urn_id(project_id: str, location: str, glossary_id: str) -> str:
    return f"dataplex.{project_id}.{location}.{glossary_id}"


def _category_node_urn_id(
    project_id: str, location: str, glossary_id: str, category_id: str
) -> str:
    return f"dataplex.{project_id}.{location}.{glossary_id}.{category_id}"


def _term_urn_id(project_id: str, location: str, glossary_id: str, term_id: str) -> str:
    return f"dataplex.{project_id}.{location}.{glossary_id}.{term_id}"


def _resource_id(resource_name: str) -> str:
    """Return the last path segment of a GCP resource name."""
    return resource_name.rsplit("/", 1)[-1]


def _parse_parent_urn(
    parent: str, project_id: str, location: str, glossary_id: str
) -> str:
    """Convert the Dataplex ``parent`` field of a term/category to a DataHub URN string.

    parent ends with:
      ``glossaries/{glossary_id}``              -> glossary GlossaryNode
      ``glossaries/{id}/categories/{cat_id}``   -> category GlossaryNode
    """
    parts = parent.rstrip("/").split("/")
    if parts[-2] == "glossaries":
        return str(
            GlossaryNodeUrn(_glossary_node_urn_id(project_id, location, glossary_id))
        )
    if parts[-2] == "categories":
        cat_id = parts[-1]
        return str(
            GlossaryNodeUrn(
                _category_node_urn_id(project_id, location, glossary_id, cat_id)
            )
        )
    raise ValueError(f"Unexpected Dataplex parent field format: {parent!r}")


@dataclass
class GlossaryTermRef:
    """Identifies a Dataplex glossary term ingested during the glossary phase."""

    project_id: str
    location: str
    glossary_id: str
    term_id: str


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass
class DataplexGlossaryReport(Report):
    """Observability counters for Dataplex Business Glossary ingestion."""

    glossaries_seen: int = 0
    glossaries_processed: int = 0
    glossaries_processed_samples: LossyList[str] = field(default_factory=LossyList)
    glossary_categories_processed: int = 0
    glossary_categories_processed_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    glossary_terms_processed: int = 0
    glossary_terms_processed_samples: LossyList[str] = field(default_factory=LossyList)
    term_associations_emitted: int = 0
    glossary_api: Dict[str, Tuple[int, float]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._lock: threading.Lock = threading.Lock()

    def report_api_call(self, api_name: str, elapsed: float) -> None:
        with self._lock:
            count, total = self.glossary_api.get(api_name, (0, 0.0))
            self.glossary_api[api_name] = (count + 1, total + elapsed)

    def report_glossary(self, name: str) -> None:
        with self._lock:
            self.glossaries_seen += 1
            self.glossaries_processed += 1
            self.glossaries_processed_samples.append(name)

    def report_category(self, name: str) -> None:
        with self._lock:
            self.glossary_categories_processed += 1
            self.glossary_categories_processed_samples.append(name)

    def report_term(self, name: str) -> None:
        with self._lock:
            self.glossary_terms_processed += 1
            self.glossary_terms_processed_samples.append(name)

    def report_association(self) -> None:
        with self._lock:
            self.term_associations_emitted += 1


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------


class DataplexGlossaryProcessor:
    """Ingests Dataplex Business Glossaries as DataHub GlossaryNode/GlossaryTerm entities.

    Two stages:
    1. ``process_glossaries`` — lists all glossaries in configured locations and emits
       GlossaryNode (glossary) / GlossaryNode (category) / GlossaryTerm entities.
    2. ``process_term_associations`` — for each emitted term, calls the Dataplex
       ``lookupEntryLinks`` REST API across all entries_locations to find linked assets
       and emits a ``glossaryTerms`` aspect update on those DataHub Dataset entities.
    """

    def __init__(
        self,
        ctx: DataplexContext,
        glossary_client: dataplex_v1.BusinessGlossaryServiceClient,
        report: DataplexGlossaryReport,
        source_report: SourceReport,
    ) -> None:
        self._ctx = ctx
        self._glossary_client = glossary_client
        self._report = report
        self._source_report = source_report
        self._emitted_terms: List[GlossaryTermRef] = []
        self._emitted_terms_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Phase 1: Glossary ingestion
    # ------------------------------------------------------------------

    def process_glossaries(
        self, project_ids: List[str], max_workers: int
    ) -> Iterable[MetadataWorkUnit]:
        """Yield GlossaryNode/GlossaryTerm workunits for all discovered glossaries."""
        glossary_jobs: List[Tuple[str, str, dataplex_v1.Glossary]] = []

        for project_id in project_ids:
            for location in self._ctx.config.glossary_locations:
                logger.info(
                    "Listing glossaries for project=%s location=%s",
                    project_id,
                    location,
                )
                try:
                    for glossary in self._list_glossaries(project_id, location):
                        glossary_jobs.append((project_id, location, glossary))
                except Exception as exc:
                    self._source_report.warning(
                        title="Dataplex glossary listing failed",
                        message="Failed to list glossaries for project/location. Skipping.",
                        context=f"project_id={project_id}, location={location}",
                        exc=exc,
                    )

        logger.info(
            "Found %d glossaries across all projects/locations", len(glossary_jobs)
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            it = iter(glossary_jobs)
            while batch := list(islice(it, WORKERS_BATCH_SIZE)):
                futures = {
                    executor.submit(
                        self._process_single_glossary, project_id, location, glossary
                    ): (project_id, location, glossary.name)
                    for project_id, location, glossary in batch
                }
                for future in as_completed(futures):
                    project_id, location, glossary_name = futures[future]
                    try:
                        yield from auto_workunit(future.result())
                    except Exception as exc:
                        self._source_report.warning(
                            title="Dataplex glossary processing failed",
                            message="Failed to process glossary. Skipping.",
                            context=f"glossary={glossary_name}",
                            exc=exc,
                        )

    def _list_glossaries(
        self, project_id: str, location: str
    ) -> Iterable[dataplex_v1.Glossary]:
        parent = f"projects/{project_id}/locations/{location}"
        request = dataplex_v1.ListGlossariesRequest(parent=parent)
        with PerfTimer() as timer:
            response = list(self._glossary_client.list_glossaries(request=request))
        self._report.report_api_call("list_glossaries", timer.elapsed_seconds())
        logger.debug(
            "list_glossaries parent=%s returned %d glossaries in %.2fs",
            parent,
            len(response),
            timer.elapsed_seconds(),
        )
        logger.debug("list_glossaries payload: %s", response)
        return response

    def _process_single_glossary(
        self,
        project_id: str,
        location: str,
        glossary: dataplex_v1.Glossary,
    ) -> Iterable[Entity]:
        """Fetch categories + terms for one glossary and yield SDK entities.

        Safe to call from parallel worker threads — all mutations go through
        lock-protected report methods and self._emitted_terms_lock.
        """
        glossary_id = _resource_id(glossary.name)
        self._report.report_glossary(glossary.name)
        dataplex_url = self._ctx.config.dataplex_url.rstrip("/")
        logger.info("Processing glossary %s (%s)", glossary.name, glossary.display_name)

        glossary_node = GlossaryNode(
            id=_glossary_node_urn_id(project_id, location, glossary_id),
            display_name=glossary.display_name or glossary_id,
            definition=getattr(glossary, "description", "") or "",
            custom_properties={
                "project_id": project_id,
                "location": location,
                "glossary_id": glossary_id,
            },
        )
        yield glossary_node

        # Fetch categories and terms.
        categories = self._list_categories(project_id, location, glossary_id)
        terms = self._list_terms(project_id, location, glossary_id)
        logger.debug(
            "Glossary %s: %d categories, %d terms",
            glossary_id,
            len(categories),
            len(terms),
        )

        for category in categories:
            cat_id = _resource_id(category.name)
            self._report.report_category(category.name)
            parent_urn = _parse_parent_urn(
                category.parent, project_id, location, glossary_id
            )
            cat_node = GlossaryNode(
                id=_category_node_urn_id(project_id, location, glossary_id, cat_id),
                display_name=category.display_name or cat_id,
                definition=getattr(category, "description", "") or "",
                parent_node=parent_urn,
                custom_properties={
                    "project_id": project_id,
                    "location": location,
                    "glossary_id": glossary_id,
                    "category_id": cat_id,
                },
            )
            yield cat_node

        for term in terms:
            term_id = _resource_id(term.name)
            self._report.report_term(term.name)
            parent_urn = _parse_parent_urn(
                term.parent, project_id, location, glossary_id
            )
            term_console_url = (
                f"{dataplex_url}/dp-glossaries/projects/{project_id}"
                f"/locations/{location}/glossaries/{glossary_id}/terms/{term_id}"
            )
            glossary_term = GlossaryTerm(
                id=_term_urn_id(project_id, location, glossary_id, term_id),
                display_name=term.display_name or term_id,
                definition=getattr(term, "description", "") or "",
                parent_node=parent_urn,
                term_source="EXTERNAL",
                source_ref="Dataplex",
                source_url=term_console_url,
                custom_properties={
                    "project_id": project_id,
                    "location": location,
                    "glossary_id": glossary_id,
                    "term_id": term_id,
                },
            )
            yield glossary_term

            with self._emitted_terms_lock:
                self._emitted_terms.append(
                    GlossaryTermRef(
                        project_id=project_id,
                        location=location,
                        glossary_id=glossary_id,
                        term_id=term_id,
                    )
                )

    def _list_categories(
        self, project_id: str, location: str, glossary_id: str
    ) -> List[dataplex_v1.GlossaryCategory]:
        parent = f"projects/{project_id}/locations/{location}/glossaries/{glossary_id}"
        request = dataplex_v1.ListGlossaryCategoriesRequest(parent=parent)
        with PerfTimer() as timer:
            categories = list(
                self._glossary_client.list_glossary_categories(request=request)
            )
        self._report.report_api_call(
            "list_glossary_categories", timer.elapsed_seconds()
        )
        logger.debug(
            "list_glossary_categories parent=%s returned %d categories in %.2fs",
            parent,
            len(categories),
            timer.elapsed_seconds(),
        )
        logger.debug("list_glossary_categories payload: %s", categories)
        return categories

    def _list_terms(
        self, project_id: str, location: str, glossary_id: str
    ) -> List[dataplex_v1.GlossaryTerm]:
        parent = f"projects/{project_id}/locations/{location}/glossaries/{glossary_id}"
        request = dataplex_v1.ListGlossaryTermsRequest(parent=parent)
        with PerfTimer() as timer:
            terms = list(self._glossary_client.list_glossary_terms(request=request))
        self._report.report_api_call("list_glossary_terms", timer.elapsed_seconds())
        logger.debug(
            "list_glossary_terms parent=%s returned %d terms in %.2fs",
            parent,
            len(terms),
            timer.elapsed_seconds(),
        )
        logger.debug("list_glossary_terms payload: %s", terms)
        return terms

    # ------------------------------------------------------------------
    # Phase 2: Term-asset associations
    # ------------------------------------------------------------------

    def process_term_associations(
        self, project_ids: List[str], max_workers: int
    ) -> Iterable[MetadataWorkUnit]:
        """Yield glossaryTerms aspect workunits for assets linked to ingested terms.

        Two-phase approach to avoid emitting partial terms per asset:
          Phase 1 (parallel): for each term, collect all linked asset URNs via
            lookupEntryLinks. Build a reverse index: asset_urn -> [term_urns].
          Phase 2 (sequential): emit one glossaryTerms MCP per asset, carrying
            the complete list of all its linked terms.

        Without phase separation, emitting one MCP per (asset, term) pair would
        cause each MCE to overwrite the previous one, leaving an asset with only
        its last-processed term.
        """
        entry_name_to_urn: Dict[str, str] = {
            e.dataplex_entry_name: e.datahub_dataset_urn for e in self._ctx.entry_data
        }

        location_pairs: List[Tuple[str, str]] = [
            (pid, loc)
            for pid in project_ids
            for loc in self._ctx.config.entries_locations
        ]

        logger.info(
            "Resolving term-asset associations for %d terms across %d project/location pairs",
            len(self._emitted_terms),
            len(location_pairs),
        )

        # Phase 1: parallel scan — collect asset_urn -> [term_urns].
        asset_to_terms: Dict[str, List[str]] = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            it = iter(self._emitted_terms)
            while batch := list(islice(it, WORKERS_BATCH_SIZE)):
                futures = {
                    executor.submit(
                        self._collect_asset_links_for_term,
                        ref.project_id,
                        ref.location,
                        ref.glossary_id,
                        ref.term_id,
                        location_pairs,
                        entry_name_to_urn,
                    ): ref
                    for ref in batch
                }
                for future in as_completed(futures):
                    ref = futures[future]
                    try:
                        term_urn_str, linked_asset_urns = future.result()
                        for asset_urn in linked_asset_urns:
                            asset_to_terms.setdefault(asset_urn, []).append(
                                term_urn_str
                            )
                    except Exception as exc:
                        self._source_report.warning(
                            title="Term association lookup failed",
                            message="Failed to resolve term-asset links. Skipping term.",
                            context=(
                                f"project={ref.project_id}, location={ref.location}, "
                                f"glossary={ref.glossary_id}, term={ref.term_id}"
                            ),
                            exc=exc,
                        )

        logger.info(
            "Term-asset scan complete: %d assets linked to at least one term",
            len(asset_to_terms),
        )

        # Phase 2: emit one MCP per asset with its complete terms list.
        for asset_urn, term_urns in asset_to_terms.items():
            dataset_urn_obj = DatasetUrn.from_string(asset_urn)
            dataset = Dataset(
                platform=dataset_urn_obj.platform,
                name=dataset_urn_obj.name,
                env=dataset_urn_obj.env,
            )
            dataset.set_terms(term_urns)
            yield from auto_workunit(dataset.as_mcps())
            self._report.report_association()

    def _collect_asset_links_for_term(
        self,
        project_id: str,
        gl_location: str,
        glossary_id: str,
        term_id: str,
        location_pairs: List[Tuple[str, str]],
        entry_name_to_urn: Dict[str, str],
    ) -> Tuple[str, List[str]]:
        """Return (term_urn_str, asset_urns) for this term across all locations.

        De-duplicates asset URNs within the scan so the same asset is not
        returned more than once even if multiple location scans return it.
        """
        term_urn_str = str(
            GlossaryTermUrn(_term_urn_id(project_id, gl_location, glossary_id, term_id))
        )
        project_number = self._ctx.project_numbers[project_id]
        term_entry_path = (
            f"projects/{project_id}/locations/global/entryGroups/@dataplex/entries/"
            f"projects/{project_number}/locations/{gl_location}"
            f"/glossaries/{glossary_id}/terms/{term_id}"
        )

        seen_asset_urns: set = set()
        linked_asset_urns: List[str] = []
        for lookup_project_id, location in location_pairs:
            try:
                links = self._lookup_entry_links(
                    lookup_project_id, location, term_entry_path
                )
            except Exception as exc:
                self._source_report.warning(
                    title="lookupEntryLinks call failed",
                    message="Skipping location for this term.",
                    context=(
                        f"project={lookup_project_id}, location={location}, "
                        f"term={term_id}"
                    ),
                    exc=exc,
                )
                continue

            for link in links:
                # Skip synonym and any other term-to-term link types.
                if not link.get("entryLinkType", "").endswith(
                    _DEFINITION_LINK_TYPE_SUFFIX
                ):
                    continue
                for ref in link.get("entryReferences", []):
                    if ref.get("type") != _SOURCE_ROLE:
                        continue
                    entry_name = ref.get("name", "")
                    asset_urn = entry_name_to_urn.get(entry_name)
                    if asset_urn is None:
                        logger.debug(
                            "Term link target %r not found in ingested entries; skipping",
                            entry_name,
                        )
                        continue
                    if asset_urn not in seen_asset_urns:
                        seen_asset_urns.add(asset_urn)
                        linked_asset_urns.append(asset_urn)

        return term_urn_str, linked_asset_urns

    def _lookup_entry_links(
        self, project_id: str, location: str, term_entry_path: str
    ) -> List[dict]:
        """Call the Dataplex lookupEntryLinks REST API for one project/location.

        Returns the raw list of entryLink dicts, or an empty list on HTTP 404
        (no links stored in this location).
        """
        assert self._ctx.authed_session is not None, (
            "authed_session must be set on DataplexContext before calling "
            "process_term_associations"
        )
        url = (
            f"https://dataplex.googleapis.com/v1/projects/{project_id}"
            f"/locations/{location}:lookupEntryLinks"
            f"?entry={urllib.parse.quote(term_entry_path, safe='')}"
        )
        with PerfTimer() as timer:
            resp = self._ctx.authed_session.get(url)
        self._report.report_api_call("lookupEntryLinks", timer.elapsed_seconds())
        entry_links = (
            resp.json().get("entryLinks", []) if resp.status_code != 404 else []
        )
        logger.debug(
            "lookupEntryLinks project=%s location=%s status=%d links=%d in %.2fs",
            project_id,
            location,
            resp.status_code,
            len(entry_links),
            timer.elapsed_seconds(),
        )
        logger.debug("lookupEntryLinks payload: %s", entry_links)

        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        return entry_links
