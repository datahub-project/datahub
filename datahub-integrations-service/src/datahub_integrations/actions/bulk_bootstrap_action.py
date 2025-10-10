import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from functools import cached_property
from typing import Callable, Literal, Self, cast

# Import _Aspect directly from codegen - importing from schema_classes does not work for mypy
from datahub._codegen.aspect import _Aspect
from datahub.configuration import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.graph.client import DataHubGraph
from datahub.utilities.urns.urn import Urn
from datahub_actions.pipeline.pipeline_context import PipelineContext
from pydantic import Field

from datahub_integrations.actions.bounded_thread_pool_executor import (
    BoundedThreadPoolExecutor,
)
from datahub_integrations.actions.oss.stats_util import (
    ActionStageReport,
    ReportingAction,
)

# Max number of aspects to read in a single batch when bootstrapping
ASPECTS_PER_BATCH = 360

# Used if setting num slices based on num_remote_workers
DEFAULT_NUM_SLICES_PER_WORKER = 4

# Interval to check whether to call batchGet
BATCH_GET_CHECK_PERIOD = timedelta(milliseconds=100)

# Maximum seconds to wait between batchGet calls
BATCH_GET_MAX_PERIOD = timedelta(seconds=1)

logger = logging.getLogger(__name__)


class BulkBootstrapActionConfig(ConfigModel):
    bootstrap_executor_id: str | None = Field(
        default=None,
        description=(
            "Executor id to use when bootstrapping the action. "
            "If None, run on the datahub_integrations_service locally."
        ),
    )

    num_remote_workers: int | None = Field(
        default=None,
        description=(
            "Number of datahub_executor workers to use for the action. "
            "If None, run on the datahub_integrations_service locally."
        ),
    )

    slice: int | None = Field(
        default=None,
        description=(
            "Slice to use when querying urns to bootstrap via elasticsearch. "
            "If slice is None and `num_slices` is set, "
            "this action will remotely spin up `num_slices` remote action runners. "
            "If slice is set, this action will run locally and only query for that slice's urns."
        ),
    )

    num_slices: int | None = Field(
        default=None,
        description=(
            "Number of slices to use when querying urns to bootstrap. "
            "Overrides `num_urns_per_slice` if set. "
            "Each slice will kick off a celery task when running remotely. "
            "If None, compute based on num_remote_workers or num_urns_per_slice. "
            "Recommended to be higher than num_remote_workers because jobs "
            "are not guaranteed to be evenly distributed across workers."
        ),
    )

    num_threads_per_worker: int = Field(
        default=32 * (os.cpu_count() or 1),
        description="Number of threads to use per worker for the action.",
    )

    max_bootstrap_tasks: int = Field(
        default=100,
        description=(
            "Maximum number of bootstrap tasks to run concurrently. "
            "Avoids memory issues when tasks contain large amounts of data."
        ),
    )

    max_mcps_per_second: int = Field(
        default=10,
        description=(
            "MCP emission rate limit per second. "
            "Divided amongst workers so if workers are not emitting evenly, true rate may be lower than limit."
        ),
    )

    batch_get_batch_size: int | None = Field(
        default=None,
        description=(
            "Batch size for requesting urns and their corresponding aspects to bootstrap. "
            "If None, compute based on the number of aspects being fetched."
        ),
    )

    cache_aspects_on_disk: bool = Field(
        default=False,
        description=(
            "If True, aspects will be cached on disk to avoid fetching them multiple times. "
            "This is useful for actions that fetch the same aspects for multiple bootstrap urns."
        ),
    )

    pit_keep_alive: str | None = Field(
        default=None,
        description=(
            "How long to keep the point-in-time (PIT) alive for when scrolling entities or relationships."
        ),
    )


class BootstrapEndpoint(StrEnum):
    RELATIONSHIP_SCROLL = "relationship_scroll"
    ENTITY_SCROLL = "entity_scroll"


BootstrapUrnsEndpoint = (
    tuple[Literal[BootstrapEndpoint.ENTITY_SCROLL], str]
    | tuple[Literal[BootstrapEndpoint.RELATIONSHIP_SCROLL], str, bool]
)


@dataclass
class EntityWithData:
    urn: str

    # Used for dataset -> schema field propagation
    # and for attach ingestion_description / editable_description in `_get_target_urns`
    field_path: str | None = None

    def __hash__(self) -> int:
        return hash((self.urn, self.field_path))

    # Used only for docs propagation
    ingestion_description: str | None = field(default=None, kw_only=True, compare=False)
    editable_description: str | None = field(default=None, kw_only=True, compare=False)

    # aspect name -> aspect
    aspects: dict[str, _Aspect] = field(
        default_factory=dict, kw_only=True, compare=False
    )

    # relationship name -> list[urns]
    # relationships stored on this entity
    relationships_as_source: dict[str, set[str]] = field(
        default_factory=lambda: defaultdict(set), kw_only=True, compare=False
    )
    # relationships stored on other entities
    relationships_as_destination: dict[str, set[str]] = field(
        default_factory=lambda: defaultdict(set), kw_only=True, compare=False
    )

    def get_aspect(self, aspect: type[Aspect]) -> Aspect | None:
        return cast(Aspect | None, self.aspects.get(aspect.ASPECT_NAME))

    def get_relationships(self, is_source: bool) -> dict[str, set[str]]:
        if is_source:
            return self.relationships_as_source
        else:
            return self.relationships_as_destination

    def merge_in_place(self, other: Self) -> None:
        if self.urn != other.urn or self.field_path != other.field_path:
            logger.warning(
                "Tried to merge entities with different urns or field paths: "
                f"{self.urn} != {other.urn} or {self.field_path} != {other.field_path}"
            )

        self.aspects.update(other.aspects)

        for rel, urns in other.relationships_as_source.items():
            self.relationships_as_source[rel].update(urns)
        for rel, urns in other.relationships_as_destination.items():
            self.relationships_as_destination[rel].update(urns)

        if not self.ingestion_description and other.ingestion_description:
            self.ingestion_description = other.ingestion_description

        if not self.editable_description and other.editable_description:
            self.editable_description = other.editable_description


class BulkBootstrapAction(ReportingAction, ABC):
    def __init__(self, config: BulkBootstrapActionConfig, ctx: PipelineContext):
        super().__init__(ctx)

        self.report = ActionStageReport()

        self.config = config
        self.report.start()

        self.batch_size = self.config.batch_get_batch_size or ASPECTS_PER_BATCH // min(
            len(self.bootstrap_aspects()), 1
        )

    @abstractmethod
    def bootstrap_urns_query(self) -> str:
        """Returns the elasticsearch query to bootstrap the action."""

    @abstractmethod
    def bootstrap_urns_endpoint(self) -> BootstrapUrnsEndpoint:
        """The OpenAPI endpoint used to query for urns to bootstrap.

        Returns a tuple of (endpoint, *args).
        """

    @abstractmethod
    def bootstrap_aspects(self) -> set[type[_Aspect]]:
        """Returns the set of aspects to read when bootstrapping."""
        # TODO: Support multiple sets of aspect lists, e.g. one for bootstrap query, one for batchGet

    def bootstrap_batch(self, entities: list[EntityWithData]) -> None:
        """Bootstraps a batch of urns with the given aspects, with error handling."""
        try:
            self._bootstrap_batch_internal(entities)
        except Exception as e:
            logger.warning(e, exc_info=True)

    @abstractmethod
    def _bootstrap_batch_internal(self, entities: list[EntityWithData]) -> None:
        """Bootstraps a batch of urns with the given aspects, to be implemented by subclasses."""

    def rollback_batch(self, entities: list[EntityWithData]) -> None:
        """Rolls back a batch of urns with the given aspects, with error handling."""
        try:
            self._rollback_batch_internal(entities)
        except Exception as e:
            logger.warning(e, exc_info=True)

    @abstractmethod
    def _rollback_batch_internal(self, entities: list[EntityWithData]) -> None:
        """Rolls back a single urn with the given aspects, to be implemented by subclasses."""

    def close(self) -> None:
        self.report.end(success=False)
        super().close()

    def is_monitoring_process(self) -> bool:
        """True if the action represents a monitoring process.

        If True, action_runner will spin up `num_slices` remote workers to execute the action in slices.
        """
        return (
            self.config.slice is None
            and bool(self.config.bootstrap_executor_id)
            and self.config.num_remote_workers is not None
        )

    def bootstrap(self) -> None:
        """Bootstrap the action, emitting proposals to effect all changes as if the action were always live.

        Should not emit unnecessary proposals.
        """
        if self.is_monitoring_process():
            return

        self.report.start()
        success = True
        try:
            with BoundedThreadPoolExecutor(
                max_workers=self.config.num_threads_per_worker,
                max_pending=self.config.max_bootstrap_tasks,
            ) as executor:
                self._bulk_process(batch_fn=self.bootstrap_batch, executor=executor)
        except Exception as e:
            logger.warning(f"Error bootstrapping action: {e}", exc_info=True)
            success = False
        self.report.end(success=success)

    def rollback(self) -> None:
        """Rollback the action, emitting proposals to undo all changes as if the action never ran.

        Should not emit unnecessary proposals.
        """
        if self.is_monitoring_process():
            return

        self.report.start()
        success = True
        try:
            with BoundedThreadPoolExecutor(
                max_workers=self.config.num_threads_per_worker,
                max_pending=self.config.max_bootstrap_tasks,
            ) as executor:
                self._bulk_process(batch_fn=self.rollback_batch, executor=executor)
        except Exception as e:
            logger.warning(f"Error rolling back action: {e}", exc_info=True)
            success = False
        self.report.end(success=success)

    def _bulk_process(
        self,
        *,
        executor: BoundedThreadPoolExecutor,
        batch_fn: Callable[[list[EntityWithData]], None],
    ) -> None:
        endpoint = self.bootstrap_urns_endpoint()
        match endpoint:
            case BootstrapEndpoint.ENTITY_SCROLL, entity_type:
                self._bulk_process_via_entity_scroll(
                    entity_type=cast(str, entity_type),
                    slice=self.config.slice,
                    executor=executor,
                    batch_fn=batch_fn,
                )
            case (
                BootstrapEndpoint.RELATIONSHIP_SCROLL,
                relationship_type,
                is_source,
            ):
                self._bulk_process_via_relationship_scroll(
                    relationship_type=cast(str, relationship_type),
                    is_source=cast(bool, is_source),
                    slice=self.config.slice,
                    executor=executor,
                    batch_fn=batch_fn,
                )
            case _:
                raise NotImplementedError(
                    f"Bootstrap endpoint and args {endpoint} not supported."
                )

    def monitor_bootstrap(self) -> None:
        """Stay alive to merge reports until all workers finish"""
        pass

    @cached_property
    def num_slices(self) -> int:
        if self.config.num_slices:
            return self.config.num_slices

        if not self.config.num_remote_workers:
            return 1

        return int(self.config.num_remote_workers / DEFAULT_NUM_SLICES_PER_WORKER)

    @property
    def _bootstrap_aspect_names(self) -> list[str]:
        return [aspect.ASPECT_NAME for aspect in self.bootstrap_aspects()]

    def _bulk_process_via_entity_scroll(
        self,
        entity_type: str,
        slice: int | None,
        executor: BoundedThreadPoolExecutor,
        batch_fn: Callable[[list[EntityWithData]], None],
    ) -> None:
        first = True
        scroll_id: str | None = None
        while first or scroll_id:
            first = False
            response = self._execute_entity_scroll(
                entity_type,
                query=self.bootstrap_urns_query(),
                slice_id=slice,
                scroll_id=scroll_id if isinstance(scroll_id, str) else None,
            )
            entities = []
            for search_entity in response.get("entities") or []:
                parsed_entity = self._parse_search_entity(
                    search_entity, list(self.bootstrap_aspects())
                )
                if parsed_entity:
                    entities.append(parsed_entity)

            executor.submit(batch_fn, entities)
            scroll_id = response.get("scrollId")

    def _bulk_process_via_relationship_scroll(
        self,
        relationship_type: str,
        is_source: bool,
        slice: int | None,
        executor: BoundedThreadPoolExecutor,
        batch_fn: Callable[[list[EntityWithData]], None],
    ) -> None:
        first = True
        scroll_id: str | None = None
        while first or scroll_id:
            first = False
            response = self._execute_batch_relationship_query(
                relationship_type,
                slice_id=slice,
                scroll_id=scroll_id if isinstance(scroll_id, str) else None,
            )
            entities = []
            for search_entity in response.get("results") or []:
                source = (search_entity.get("source") or {}).get("urn")
                destination = (search_entity.get("destination") or {}).get("urn")
                if (not source) or (not destination):
                    logger.info(
                        f"Skipping relationship with no source / destination urn: {search_entity}"
                    )
                    continue

                if is_source:
                    entities.append(
                        EntityWithData(
                            source,
                            relationships_as_source={relationship_type: {destination}},
                        )
                    )
                else:
                    entities.append(
                        EntityWithData(
                            destination,
                            relationships_as_destination={relationship_type: {source}},
                        )
                    )

            executor.submit(batch_fn, entities)
            scroll_id = response.get("scrollId")

    def _batch_augment_entities_with_aspects(
        self, entities: list[EntityWithData], aspects_to_fetch: list[type[_Aspect]]
    ) -> None:
        entities_by_urn = {entity.urn: entity for entity in entities}

        urn_objs = set(Urn.from_string(entity.urn) for entity in entities)
        urns_by_entity_type = defaultdict(list)
        for urn_obj in urn_objs:
            urns_by_entity_type[urn_obj.entity_type].append(str(urn_obj))

        for entity_type, urns in urns_by_entity_type.items():
            response = self._execute_batch_get(entity_type, urns, aspects_to_fetch)
            for search_entity in response:
                parsed_entity = self._parse_search_entity(
                    search_entity, aspects_to_fetch
                )
                if parsed_entity and parsed_entity.urn in entities_by_urn:
                    entities_by_urn[parsed_entity.urn].aspects.update(
                        parsed_entity.aspects
                    )

    def _batch_augment_entities_with_relationship(
        self,
        entities: list[EntityWithData],
        relationship_to_fetch: str,
        is_source: bool,
    ) -> None:
        if len(entities) > 1:
            raise NotImplementedError(
                "Batch augment entities with relationship not implemented"
            )

        entity = entities[0]
        try:
            related_entities = self.ctx.graph.graph.get_related_entities(
                entity.urn,
                [relationship_to_fetch],
                DataHubGraph.RelationshipDirection.OUTGOING
                if is_source
                else DataHubGraph.RelationshipDirection.INCOMING,
            )
        except Exception as e:
            logger.warning(
                f"Error fetching related entities for {entity.urn} with relationships {relationship_to_fetch}: {e}",
                exc_info=True,
            )
            return

        for related_entity in related_entities:
            entity.get_relationships(is_source)[relationship_to_fetch].add(
                related_entity.urn
            )

    # TODO: Handle exceptions on making requests
    def _execute_batch_get(
        self, entity_type: str, urns: list[str], aspects_to_fetch: list[type[_Aspect]]
    ) -> dict:
        url = f"{self.ctx.graph.graph._gms_server.rstrip('/')}/openapi/v3/entity/{entity_type}/batchGet"
        params = {"systemMetadata": False}
        body = [
            {
                "urn": urn,
                **{aspect_type.ASPECT_NAME: {} for aspect_type in aspects_to_fetch},
            }
            for urn in urns
        ]

        # Not actually a restli request
        try:
            return self.ctx.graph.graph._send_restli_request(
                "POST", url, params=params, json=body
            )
        except Exception as e:
            logger.warning(
                f"Error executing batch get to {url} with body {body}: {e}",
                exc_info=True,
            )
            return {}

    def _execute_entity_scroll(
        self,
        entity_type: str,
        *,
        query: str | None = None,
        slice_id: int | None = None,
        scroll_id: str | None = None,
    ) -> dict:
        url = f"{self.ctx.graph.graph._gms_server.rstrip('/')}/openapi/v3/entity/{entity_type}"
        params = {
            "systemMetadata": False,
            "includeSoftDelete": False,
            "skipCache": False,
            "aspects": self._bootstrap_aspect_names,
            "query": query,
            "count": self.batch_size,
            "scrollId": scroll_id,
            "sliceId": slice_id,
            "sliceMax": self.num_slices if slice_id else None,
            "pitKeepAlive": self.config.pit_keep_alive,
        }
        try:
            return self.ctx.graph.graph._get_generic(url, params=params)
        except Exception as e:
            logger.warning(
                f"Error executing entity scroll to {url} with params {params}: {e}",
                exc_info=True,
            )
            return {}

    def _execute_batch_relationship_query(
        self,
        relationship_type: str,
        *,
        slice_id: int | None = None,
        scroll_id: str | None = None,
    ) -> dict:
        url = f"{self.ctx.graph.graph._gms_server.rstrip('/')}/openapi/v3/relationship/{relationship_type}"
        params = {
            # TODO: Sort by destinationUrn first
            "includeSoftDelete": False,
            "count": ASPECTS_PER_BATCH,
            "scrollId": scroll_id,
            "sliceId": slice_id,
            "sliceMax": self.num_slices if slice_id else None,
            "pitKeepAlive": self.config.pit_keep_alive,
        }
        try:
            return self.ctx.graph.graph._get_generic(url, params=params)
        except Exception as e:
            logger.warning(
                f"Error executing relationship scroll to {url} with params {params}: {e}",
                exc_info=True,
            )
            return {}

    def _parse_search_entity(
        self, search_entity: dict, aspects_to_fetch: list[type[_Aspect]]
    ) -> EntityWithData | None:
        """Parse a search entity into an EntityWithData object."""
        urn = search_entity.get("urn")
        if not urn:
            return None

        try:
            aspects = {
                aspect_type.ASPECT_NAME: aspect_type.from_obj(
                    (search_entity.get(aspect_type.ASPECT_NAME) or {}).get("value")
                    or {}
                )
                for aspect_type in aspects_to_fetch
                if aspect_type.ASPECT_NAME in search_entity
            }
        except Exception as e:
            logger.warning(
                f"Failed to parse aspects for urn {urn}: {e}, {search_entity}",
                exc_info=True,
            )
            return None

        return EntityWithData(urn, aspects=aspects)
