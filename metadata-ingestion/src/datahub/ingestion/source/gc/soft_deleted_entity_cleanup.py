import functools
import logging
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Dict, Iterable, List, Optional

from pydantic import Field

from datahub.cli.delete_cli import DELETE_WITH_REFERENCES_TYPES
from datahub.configuration import ConfigModel
from datahub.configuration.env_vars import get_report_info_sample_size
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter, SearchFilterRule
from datahub.metadata.urns import ContainerUrn
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import TopKDict
from datahub.utilities.urns._urn_base import Urn
from datahub.utilities.urns.error import InvalidUrnError

logger = logging.getLogger(__name__)

# Entity types with a `container` aspect in entity-registry.yml. They are listed
# because an untyped search only covers the server's default search types, which
# leave out dataProcessInstance. mlModelDeployment also has the aspect but no
# GraphQL EntityType, so it cannot be searched here.
_CONTAINER_CHILD_ENTITY_TYPES = [
    "dataset",
    "chart",
    "dashboard",
    "dataFlow",
    "dataJob",
    "dataProcessInstance",
    "container",
    "mlModel",
    "mlModelGroup",
]

# get_urns_by_filter argument -> the GraphQL SearchFlags field it sets.
_CHILD_SEARCH_FLAGS = {
    "include_draft": "includeDraft",
    "include_hidden_lifecycle_stages": "includeHiddenLifecycleStages",
}


class SoftDeletedEntitiesCleanupConfig(ConfigModel):
    enabled: bool = Field(
        default=True, description="Whether to do soft deletion cleanup."
    )
    retention_days: int = Field(
        10,
        description="Number of days to retain metadata in DataHub",
    )

    batch_size: int = Field(
        500,
        description="The number of entities to get in a batch from GraphQL",
    )

    delay: Optional[float] = Field(
        0.25,
        description="Delay between each batch",
    )

    max_workers: int = Field(
        10,
        description="The number of workers to use for deletion",
    )

    entity_types: Optional[List[str]] = Field(
        # A default value is required otherwise QUERY and DATAPROCESS_INSTANCE won't be included
        default=[
            "dataset",
            "dashboard",
            "chart",
            "mlmodel",
            "mlmodelGroup",
            "mlfeatureTable",
            "mlfeature",
            "mlprimaryKey",
            "dataFlow",
            "dataJob",
            "glossaryTerm",
            "glossaryNode",
            "tag",
            "role",
            "corpuser",
            "corpGroup",
            "container",
            "domain",
            "dataProduct",
            "notebook",
            "businessAttribute",
            "schemaField",
            "query",
            "dataProcessInstance",
        ],
        description="List of entity types to cleanup",
    )

    platform: Optional[str] = Field(
        default=None,
        description="Platform to cleanup",
    )

    env: Optional[str] = Field(
        default=None,
        description="Environment to cleanup",
    )

    query: Optional[str] = Field(
        default=None,
        description="Query to filter entities",
    )

    limit_entities_delete: Optional[int] = Field(
        25000, description="Max number of entities to delete."
    )

    futures_max_at_time: int = Field(
        1000, description="Max number of futures to have at a time."
    )

    runtime_limit_seconds: int = Field(
        7200,  # 2 hours by default
        description="Runtime limit in seconds",
    )


@dataclass
class SoftDeletedEntitiesReport(SourceReport):
    num_calls_made: Dict[str, int] = field(default_factory=dict)
    num_entities_found: Dict[str, int] = field(default_factory=dict)
    num_soft_deleted_entity_processed: int = 0
    num_soft_deleted_retained_due_to_age: int = 0
    num_soft_deleted_retained_due_to_age_by_type: TopKDict[str, int] = field(
        default_factory=TopKDict
    )
    num_soft_deleted_entity_removal_started: int = 0
    num_hard_deleted: int = 0
    num_hard_deleted_by_type: TopKDict[str, int] = field(default_factory=TopKDict)
    sample_hard_deleted_aspects_by_type: TopKDict[str, LossyList[str]] = field(
        default_factory=TopKDict
    )
    runtime_limit_reached: bool = False
    deletion_limit_reached: bool = False
    num_soft_deleted_entity_found: int = 0
    num_soft_deleted_entity_invalid_urn: int = 0
    num_soft_deleted_containers_retained_with_children: int = 0


class SoftDeletedEntitiesCleanup:
    """
    Maintenance source to cleanup soft deleted entities in DataHub
    """

    def __init__(
        self,
        ctx: PipelineContext,
        config: SoftDeletedEntitiesCleanupConfig,
        report: SoftDeletedEntitiesReport,
        dry_run: bool = False,
    ):
        if not ctx.graph:
            raise ValueError(" Datahub API is required")

        self.graph: DataHubGraph = ctx.graph
        self.ctx = ctx
        self.config = config
        self.report = report
        self.dry_run = dry_run
        self.start_time = time.time()
        self._report_lock: Lock = Lock()
        self.last_print_time = 0.0
        # A flag is turned off once the server reports it does not support it.
        self._child_search_flags: Dict[str, bool] = {
            flag: True for flag in _CHILD_SEARCH_FLAGS
        }

    def _increment_retained_count(self) -> None:
        """Thread-safe method to update report fields"""
        with self._report_lock:
            self.report.num_soft_deleted_retained_due_to_age += 1

    def _increment_retained_by_type(self, type: str) -> None:
        """Thread-safe method to update report fields"""
        with self._report_lock:
            self.report.num_soft_deleted_retained_due_to_age_by_type[type] = (
                self.report.num_soft_deleted_retained_due_to_age_by_type.get(type, 0)
                + 1
            )

    def _increment_removal_started_count(self) -> None:
        """Thread-safe method to update report fields"""
        with self._report_lock:
            self.report.num_soft_deleted_entity_removal_started += 1

    def _update_report(self, urn: str, entity_type: str) -> None:
        """Thread-safe method to update report fields"""
        with self._report_lock:
            self.report.num_hard_deleted += 1

            current_count = self.report.num_hard_deleted_by_type.get(entity_type, 0)
            self.report.num_hard_deleted_by_type[entity_type] = current_count + 1
            if entity_type not in self.report.sample_hard_deleted_aspects_by_type:
                self.report.sample_hard_deleted_aspects_by_type[entity_type] = (
                    LossyList(max_elements=get_report_info_sample_size())
                )
            self.report.sample_hard_deleted_aspects_by_type[entity_type].append(urn)

    def delete_entity(self, urn: Urn) -> None:
        assert self.ctx.graph

        if self.dry_run:
            logger.info(
                f"Dry run is on otherwise it would have deleted {urn} with hard deletion"
            )
            return
        if self._deletion_limit_reached() or self._times_up():
            return
        self._increment_removal_started_count()
        # Same policy as `datahub delete --hard`. References go first: they are found
        # through graph edges that the hard delete removes asynchronously.
        if urn.entity_type in DELETE_WITH_REFERENCES_TYPES:
            self.ctx.graph.delete_references_to_urn(
                urn=urn.urn(),
                dry_run=False,
            )
        self.ctx.graph.delete_entity(urn=urn.urn(), hard=True)
        self._update_report(urn.urn(), urn.entity_type)

    def _has_children(self, urn: Urn) -> bool:
        # Children in any state count, soft-deleted, draft and hidden ones included:
        # each still holds a `container` link that hard-deleting this container
        # would leave dangling. The `container` field is matched rather than browsePathsV2,
        # which entities emitted outside ingestion sources often lack.
        search = functools.partial(
            self.graph.get_urns_by_filter,
            entity_types=_CONTAINER_CHILD_ENTITY_TYPES,
            status=RemovedStatusFilter.ALL,
            batch_size=1,
            extraFilters=[
                SearchFilterRule(
                    field="container", condition="EQUAL", values=[urn.urn()]
                ).to_raw()
            ],
        )
        while True:
            flags = [flag for flag, on in self._child_search_flags.items() if on]
            try:
                return any(
                    search(
                        include_draft="include_draft" in flags,
                        include_hidden_lifecycle_stages="include_hidden_lifecycle_stages"
                        in flags,
                    )
                )
            except ValueError as e:
                # get_urns_by_filter raises this before searching when the server's
                # SearchFlags lack a field: includeDraft is not in every edition, and
                # older servers lack includeHiddenLifecycleStages.
                unsupported = [
                    flag
                    for flag in flags
                    if f"SearchFlags.{_CHILD_SEARCH_FLAGS[flag]} " in str(e)
                ]
                if not unsupported:
                    raise
                self._disable_child_search_flags(unsupported)

    def _disable_child_search_flags(self, flags: List[str]) -> None:
        with self._report_lock:
            for flag in flags:
                if not self._child_search_flags[flag]:
                    continue
                self._child_search_flags[flag] = False
                if flag == "include_draft":
                    # Draft entities only exist in some editions, so a server
                    # without the flag has no draft children to miss.
                    logger.debug(
                        "Container child check runs without includeDraft, "
                        "which this server does not support"
                    )
                    continue
                self.report.info(
                    title="Container child check runs without an unsupported search flag",
                    message="This DataHub server does not support the search flag, so "
                    "children it covers are not counted when deciding whether a "
                    "soft-deleted container can be hard-deleted.",
                    context=_CHILD_SEARCH_FLAGS[flag],
                )

    def _report_container_retained(self, urn: Urn) -> None:
        with self._report_lock:
            self.report.num_soft_deleted_containers_retained_with_children += 1
            self.report.info(
                title="Soft-deleted container still has children",
                message="Not hard-deleting soft-deleted containers that still have "
                "children. Restore the container, or delete its children first.",
                context=urn.urn(),
                # Retained again on every run; the counter and sampled report
                # entry already record it.
                log=False,
            )

    def delete_soft_deleted_entity(self, urn: Urn) -> None:
        assert self.ctx.graph

        retention_time = (
            int(datetime.now(timezone.utc).timestamp())
            - self.config.retention_days * 24 * 60 * 60
        )

        aspect = self.ctx.graph.get_entity_raw(entity_urn=urn.urn(), aspects=["status"])
        if "status" in aspect["aspects"]:
            if aspect["aspects"]["status"]["value"]["removed"] and aspect["aspects"][
                "status"
            ]["created"]["time"] < (retention_time * 1000):
                if urn.entity_type == ContainerUrn.ENTITY_TYPE:
                    if self._deletion_limit_reached() or self._times_up():
                        return
                    # Hard-deleting a container leaves every child linked to a
                    # container that no longer exists, so the subtree can't be
                    # restored. While children remain it stays soft-deleted, so
                    # restoring it brings the tree back.
                    try:
                        has_children = self._has_children(urn)
                    except Exception as e:
                        with self._report_lock:
                            self.report.warning(
                                title="Could not check children of soft-deleted container",
                                message="Skipped hard-deleting the container because its "
                                "children could not be searched.",
                                context=urn.urn(),
                                exc=e,
                            )
                        return
                    if has_children:
                        self._report_container_retained(urn)
                        return
                logger.debug(f"Hard deleting {urn}")
                self.delete_entity(urn)
            else:
                self._increment_retained_count()
                self._increment_retained_by_type(urn.entity_type)

    def _print_report(self) -> None:
        time_taken = round(time.time() - self.last_print_time, 1)
        # Print report every 2 minutes
        if time_taken > 120:
            self.last_print_time = time.time()
            logger.info(f"\n{self.report.as_string()}")

    def _process_futures(self, futures: Dict[Future, Urn]) -> Dict[Future, Urn]:
        done, not_done = wait(futures, return_when=FIRST_COMPLETED)
        # Keep the urn mapping for completed futures before narrowing `futures`
        # to the pending ones; otherwise a failed future cannot be reported
        # against its urn (KeyError) and the whole stage aborts.
        done_urns = {future: futures[future] for future in done}
        futures = {future: urn for future, urn in futures.items() if future in not_done}

        for future in done:
            self._print_report()
            exc = future.exception()
            if exc:
                self.report.failure(
                    title="Failed to delete entity",
                    message="Failed to delete entity",
                    context=done_urns[future].urn(),
                    exc=exc,
                )
            self.report.num_soft_deleted_entity_processed += 1
            if (
                self.report.num_soft_deleted_entity_processed % self.config.batch_size
                == 0
            ):
                if self.config.delay:
                    logger.debug(
                        f"Sleeping for {self.config.delay} seconds before further processing batch"
                    )
                    time.sleep(self.config.delay)
        return futures

    def _get_urns(self) -> Iterable[str]:
        assert self.ctx.graph
        # Entities created in the retention period are not considered for deletion
        created_from = int(
            (
                datetime.now(timezone.utc).timestamp()
                - self.config.retention_days * 24 * 60 * 60
            )
            * 1000
        )

        entity_types = self.config.entity_types
        # dataProcessInstance is a special case where we need to get the entities separately
        # because we need to filter based on created time we don't stream to many dataProcessInstance entities at once
        # Gc source soft-deletes dataProcessInstance entities which causes to have a lot of soft deleted entities
        if (
            self.config.entity_types
            and "dataProcessInstance" in self.config.entity_types
        ):
            entity_types = self.config.entity_types.copy()
            yield from self.ctx.graph.get_urns_by_filter(
                entity_types=["dataProcessInstance"],
                platform=self.config.platform,
                env=self.config.env,
                query=self.config.query,
                status=RemovedStatusFilter.ONLY_SOFT_DELETED,
                batch_size=self.config.batch_size,
                extraFilters=[
                    SearchFilterRule(
                        field="created",
                        condition="LESS_THAN",
                        values=[f"{created_from}"],
                    ).to_raw()
                ],
            )

            entity_types.remove("dataProcessInstance")

        yield from self.ctx.graph.get_urns_by_filter(
            entity_types=entity_types,
            platform=self.config.platform,
            env=self.config.env,
            query=self.config.query,
            status=RemovedStatusFilter.ONLY_SOFT_DELETED,
            batch_size=self.config.batch_size,
        )

    def _times_up(self) -> bool:
        if (
            self.config.runtime_limit_seconds
            and time.time() - self.start_time > self.config.runtime_limit_seconds
        ):
            with self._report_lock:
                self.report.runtime_limit_reached = True
            return True
        return False

    def _deletion_limit_reached(self) -> bool:
        if (
            self.config.limit_entities_delete
            and self.report.num_hard_deleted > self.config.limit_entities_delete
        ):
            with self._report_lock:
                self.report.deletion_limit_reached = True
            return True
        return False

    def cleanup_soft_deleted_entities(self) -> None:
        if not self.config.enabled:
            return
        self.start_time = time.time()

        futures: Dict[Future, Urn] = dict()
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            for urn in self._get_urns():
                try:
                    self.report.num_soft_deleted_entity_found += 1
                    soft_deleted_urn = Urn.from_string(urn)
                except InvalidUrnError as e:
                    logger.error(f"Failed to parse urn {urn} with error {e}")
                    self.report.num_soft_deleted_entity_invalid_urn += 1
                    continue

                self._print_report()
                while len(futures) >= self.config.futures_max_at_time:
                    futures = self._process_futures(futures)
                if self._deletion_limit_reached() or self._times_up():
                    break
                future = executor.submit(
                    self.delete_soft_deleted_entity, soft_deleted_urn
                )
                futures[future] = soft_deleted_urn

            logger.info(f"Waiting for {len(futures)} futures to complete")
            while len(futures) > 0:
                self._print_report()
                futures = self._process_futures(futures)
