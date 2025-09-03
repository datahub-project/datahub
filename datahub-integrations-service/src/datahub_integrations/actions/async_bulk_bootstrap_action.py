# import logging
# import os
# import queue
# import threading
# import time
# from abc import ABC, abstractmethod
# from collections import defaultdict
# from concurrent.futures.thread import ThreadPoolExecutor
# from dataclasses import dataclass
# from datetime import datetime, timedelta
# from enum import StrEnum
#
# from datahub.configuration import ConfigModel
# from datahub.emitter.mce_builder import Aspect
# from datahub.utilities.file_backed_collections import FileBackedDict
# from datahub.utilities.urns.urn import Urn
# from datahub_actions.pipeline.pipeline_context import PipelineContext
# from pydantic import Field
#
# from datahub_integrations.actions.oss.stats_util import (
#     ActionStageReport,
#     ReportingAction,
# )
#
# # Max number of aspects to read in a single batch when bootstrapping
# ASPECTS_PER_BATCH = 360
#
# # Used if setting num slices based on num_remote_workers
# DEFAULT_NUM_SLICES_PER_WORKER = 4
#
# # Interval to check whether to call batchGet
# BATCH_GET_CHECK_PERIOD = timedelta(milliseconds=100)
#
# # Maximum seconds to wait between batchGet calls
# BATCH_GET_MAX_PERIOD = timedelta(seconds=1)
#
# logger = logging.getLogger(__name__)
#
#
# class BulkBootstrapActionConfig(ConfigModel):
#     bootstrap_executor_id: str | None = Field(
#         default=None,
#         description=(
#             "Executor id to use when bootstrapping the action. "
#             "If None, run on the datahub_integrations_service locally."
#         ),
#     )
#
#     num_remote_workers: int | None = Field(
#         default=None,
#         description=(
#             "Number of datahub_executor workers to use for the action. "
#             "If None, run on the datahub_integrations_service locally."
#         ),
#     )
#
#     num_threads_per_worker: int = Field(
#         default=4 * os.cpu_count(),
#         description="Number of threads to use per worker for the action.",
#     )
#
#     max_bootstrap_tasks: int = Field(
#         default=100,
#         description=(
#             "Maximum number of bootstrap tasks to run concurrently. "
#             "Avoids memory issues when tasks contain large amounts of data."
#         ),
#     )
#
#     max_mcps_per_second: int = Field(
#         default=10,
#         description=(
#             "MCP emission rate limit per second. "
#             "Divided amongst workers so if workers are not emitting evenly, true rate may be lower than limit."
#         ),
#     )
#
#     batch_get_batch_size: int | None = Field(
#         default=None,
#         description=(
#             "Batch size for requesting urns and their corresponding aspects to bootstrap. "
#             "If None, compute based on the number of aspects being fetched."
#         ),
#     )
#
#     slice: int | None = Field(
#         default=None,
#         description=(
#             "Slice to use when querying urns to bootstrap via elasticsearch. "
#             "If slice is None and `num_slices` is set, "
#             "this action will remotely spin up `num_slices` remote action runners. "
#             "If slice is set, this action will run locally and only query for that slice's urns."
#         ),
#     )
#
#     num_slices: int | None = Field(
#         default=None,
#         description=(
#             "Number of slices to use when querying urns to bootstrap. "
#             "Overrides `num_urns_per_slice` if set. "
#             "Each slice will kick off a celery task when running remotely. "
#             "If None, compute based on num_remote_workers or num_urns_per_slice. "
#             "Recommended to be higher than num_remote_workers because jobs "
#             "are not guaranteed to be evenly distributed across workers."
#         ),
#     )
#
#     num_urns_per_slice: int | None = Field(
#         default=None,
#         description=(
#             "Allows configuring the number of slices based on the total number of urns to boostrap. "
#             "Will issue a preliminary query to determine the approximate number of urns to bootstrap."
#         ),
#     )
#
#     cache_aspects_on_disk: bool = Field(
#         default=False,
#         description=(
#             "If True, aspects will be cached on disk to avoid fetching them multiple times. "
#             "This is useful for actions that fetch the same aspects for multiple bootstrap urns."
#         ),
#     )
#
#
# class BootstrapEndpoint(StrEnum):
#     RELATIONSHIP_SCROLL = "relationship_scroll"
#     ENTITY_SCROLL = "entity_scroll"
#
#
# @dataclass
# class EntityWithAspects:
#     urn: str
#     aspects: dict[str, Aspect]
#
#
# class BulkBootstrapAction(ReportingAction, ABC):
#     def __init__(self, config: BulkBootstrapActionConfig, ctx: PipelineContext):
#         super().__init__(ctx)
#
#         self.report = ActionStageReport()
#
#         self.config = config
#         self.report.start()
#
#         self.batch_size = (
#             self.config.batch_get_batch_size
#             or len(self.bootstrap_aspects()) // ASPECTS_PER_BATCH
#         )
#
#         # Some actions may need to fetch further aspects per bootstrap urn.
#         # For these aspects, `bootstrap_single_urn` will send urns to the `batch_get_thread`
#         # to fetch the aspects for associated entity.
#         self.batch_get_thread = threading.Thread(target=self.batch_getter)
#         self.batch_get_lock = threading.Lock()
#         self.batch_get_urns = queue.Queue(maxsize=self.batch_size * 10)
#         self.batch_get_last_execution: datetime = datetime.now()
#         self.batch_get_data = FileBackedDict()
#         self.batch_get_condition = threading.Condition()
#
#     @abstractmethod
#     def bootstrap_urns_query(self) -> str:
#         """Returns the elasticsearch query to bootstrap the action."""
#
#     @abstractmethod
#     def bootstrap_urns_endpoint(self) -> tuple[BootstrapEndpoint, str, ...]:
#         """The OpenAPI endpoint used to query for urns to bootstrap.
#
#         Returns a tuple of (endpoint, *args).
#         """
#
#     @abstractmethod
#     def bootstrap_aspects(self) -> set[Aspect]:
#         """Returns the set of aspects to read when bootstrapping."""
#         # TODO: Support multiple sets of aspect lists, e.g. one for bootstrap query, one for batchGet
#
#     @abstractmethod
#     def bootstrap_batch(self, entities: list[EntityWithAspects]) -> None:
#         """Bootstraps a single urn with the given aspects.
#
#         Args:
#             entities: Urns with aspects attached to bootstrap.
#         """
#
#     def is_monitoring_process(self) -> bool:
#         """True if the action represents a monitoring process.
#
#         If True, action_runner will spin up `num_slices` remote workers to execute the action in slices.
#         """
#         return (
#             self.config.slice is None
#             and self.config.bootstrap_executor_id
#             and self.config.num_remote_workers is not None
#         )
#
#     def batch_getter(self):
#         """Thread that fetches aspects for urns in batches."""
#
#         if self.batch_get_urns.qsize() >= self.batch_size or (
#             datetime.now() - self.batch_get_last_execution < BATCH_GET_MAX_PERIOD
#         ):
#             with self.batch_get_lock:
#                 batch = self.batch_get_urns
#                 self.batch_get_urns = self.batch_get_urns[self.batch_size :]
#
#             urn_objs = [Urn.from_string(urn) for urn in set(batch)]
#             urns_by_entity_type = defaultdict(list)
#             for urn in urn_objs:
#                 urns_by_entity_type[urn.entity_type].append(urn)
#
#             for entity_type, urns in urns_by_entity_type.items():
#                 response = self._execute_batch_get(entity_type, urns)
#                 for search_entity in response:
#                     urn = search_entity.get("urn")
#                     if not urn:
#                         continue
#
#                     try:
#                         aspects = {
#                             aspect_name: Aspect.from_obj(search_entity.get(aspect_name))
#                             for aspect_name in self._bootstrap_aspect_names
#                         }
#                     except Exception as e:
#                         logger.warning(f"Failed to parse aspects for urn {urn}: {e}")
#                         continue
#
#                     self.batch_get_data[urn] = aspects
#
#                 self.batch_get_condition.notify_all()
#         else:
#             time.sleep(BATCH_GET_CHECK_PERIOD.total_seconds())
#
#     def bootstrap(self) -> None:
#         """Bootstrap the action, emitting proposals to effect all changes as if the action were always live.
#
#         Should not emit unnecessary proposals.
#         """
#         if self.is_monitoring_process():
#             return
#
#         self.report.start()
#         self.batch_get_thread.start()
#         with ThreadPoolExecutor(
#             max_workers=self.config.num_threads_per_worker
#         ) as executor:
#             match self.bootstrap_urns_endpoint():
#                 case BootstrapEndpoint.ENTITY_SCROLL, entity_type, *_args:
#                     self._bootstrap_via_entity_scroll(
#                         entity_type=entity_type,
#                         slice=self.config.slice,
#                         executor=executor,
#                     )
#                 case (
#                     BootstrapEndpoint.RELATIONSHIP_SCROLL,
#                     relationship_type,
#                     is_destination,
#                     *_args,
#                 ):
#                     self._bootstrap_via_relationship_scroll(
#                         relationship_type=relationship_type,
#                         is_destination=is_destination,
#                         slice=self.config.slice,
#                         executor=executor,
#                     )
#                 case _:
#                     raise NotImplementedError(
#                         f"Bootstrap endpoint and args {self.bootstrap_urns_endpoint()} not supported."
#                     )
#
#     def monitor_bootstrap(self) -> None:
#         """Stay alive to merge reports until all workers finish"""
#         pass
#
#     def num_slices(self) -> int:
#         if self.config.num_slices:
#             return self.config.num_slices
#
#         if self.config.num_urns_per_slice:
#             endpoint_info = self.bootstrap_urns_endpoint()
#             match endpoint_info:
#                 case BootstrapEndpoint.ENTITY_SCROLL, entity_type, *_args:
#                     response = self._execute_entity_scroll(
#                         entity_type, only_check_total=True
#                     )
#                 case (
#                     BootstrapEndpoint.RELATIONSHIP_SCROLL,
#                     relationship_type,
#                     *_args,
#                 ):
#                     response = self._execute_batch_relationship_query(relationship_type)
#                 case _:
#                     raise NotImplementedError(
#                         f"Bootstrap endpoint and args {endpoint_info} not supported."
#                     )
#             try:
#                 total = int(response.get("numEntities", 0))
#             except ValueError:
#                 self.report.warnings.append(
#                     f"Failed to compute num_slices based on search total urns on {endpoint_info}."
#                 )
#                 total = None
#             if total:
#                 return int(total / self.config.num_urns_per_slice)
#
#         return int(self.config.num_remote_workers / DEFAULT_NUM_SLICES_PER_WORKER)
#
#     @property
#     def _bootstrap_aspect_names(self) -> list[str]:
#         return [aspect.ASPECT_NAME for aspect in self.bootstrap_aspects()]
#
#     def _bootstrap_via_entity_scroll(
#         self, entity_type: str, slice: int | None, executor: ThreadPoolExecutor
#     ):
#         scroll_id: bool | str = True
#         while scroll_id:
#             batch = self._execute_entity_scroll(
#                 entity_type,
#                 slice_id=slice,
#                 scroll_id=scroll_id if isinstance(scroll_id, str) else None,
#             )
#             entities = []
#             for search_entity in batch or []:
#                 urn = search_entity.get("urn")
#                 try:
#                     aspects = {
#                         aspect_name: Aspect.from_obj(search_entity.get(aspect_name))
#                         for aspect_name in self._bootstrap_aspect_names
#                     }
#                 except Exception as e:
#                     logger.warning(f"Failed to parse aspects for urn {urn}: {e}")
#                     continue
#
#                 entities.append(EntityWithAspects(urn, aspects))
#
#             executor.submit(self.bootstrap_batch, entities)
#             scroll_id = batch.get("scrollId")
#
#     def _bootstrap_via_relationship_scroll(
#         self, relationship_type: str, is_destination: bool, slice: int | None
#     ):
#         scroll_id: bool | str = True
#         while scroll_id:
#             batch = self._execute_batch_relationship_query(
#                 relationship_type,
#                 slice_id=slice,
#                 scroll_id=scroll_id if isinstance(scroll_id, str) else None,
#             )
#             for search_entity in batch.get("results") or []:
#                 if is_destination:
#                     urn = (search_entity.get("destination") or {}).get("urn")
#                 else:
#                     urn = (search_entity.get("source") or {}).get("urn")
#
#                 with ThreadPoolExecutor(
#                     max_workers=self.config.num_threads_per_worker
#                 ) as executor:
#                     executor.submit(self.bootstrap_batch, urn, aspects=None)
#
#             scroll_id = batch.get("scrollId")
#
#     # TODO: Handle exceptions on making requests
#     def _execute_batch_get(self, entity_type: str, urns: list[str]) -> dict:
#         url = f"{self.ctx.graph.graph._gms_server.rstrip('/')}/openapi/v3/entity/{entity_type}/batchGet"
#         params = {"systemMetadata": False}
#         body = [
#             {
#                 "urn": urn,
#                 **{aspect_name: {} for aspect_name in self._bootstrap_aspect_names},
#             }
#             for urn in urns
#         ]
#
#         # Not actually a restli request
#         return self.ctx.graph.graph._send_restli_request(
#             "POST", url, params=params, json=body
#         )
#
#     def _execute_entity_scroll(
#         self,
#         entity_type: str,
#         *,
#         slice_id: str | None = None,
#         scroll_id: str | None = None,
#         only_check_total: bool = False,
#     ) -> dict:
#         url = f"{self.ctx.graph.graph._gms_server.rstrip('/')}/openapi/v3/entity/{entity_type}"
#         params = {
#             "systemMetadata": False,
#             "includeSoftDelete": False,
#             "skipCache": False,
#             "aspects": self._bootstrap_aspect_names,
#             "query": self.bootstrap_urns_query(),
#             "count": 0 if only_check_total else self.batch_size,
#             "scrollId": None if only_check_total else scroll_id,
#             "sliceId": None if only_check_total else slice_id,
#             "sliceMax": None if only_check_total else self.config.num_slices,
#         }
#         return self.ctx.graph.graph._get_generic(url, params=params)
#
#     def _execute_batch_relationship_query(
#         self,
#         relationship_type: str,
#         *,
#         slice_id: str | None = None,
#         scroll_id: str | None = None,
#     ) -> dict:
#         url = f"{self.ctx.graph.graph._gms_server.rstrip('/')}/openapi/v3/relationship/{relationship_type}"
#         params = {
#             # TODO: Sort by destinationUrn first
#             "includeSoftDelete": False,
#             "count": ASPECTS_PER_BATCH,
#             "scrollId": scroll_id,
#             "sliceId": slice_id,
#             "sliceMax": self.config.num_slices,
#         }
#         return self.ctx.graph.graph._get_generic(url, params=params)
