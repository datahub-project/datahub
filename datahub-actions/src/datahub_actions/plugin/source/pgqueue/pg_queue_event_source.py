# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub.configuration.kafka import _get_schema_registry_url
from datahub.emitter.serialization_helper import post_json_transform
from datahub.metadata.schema_classes import GenericPayloadClass, MetadataChangeLogClass
from datahub.pgqueue.config import (
    PayloadRouteKind,
    PgQueueConnectionConfig,
    PgQueueConsumerConfig,
)
from datahub.pgqueue.consumer import (
    DatahubPgQueueConsumer,
    PgQueueConsumedRecord,
    build_pg_queue_event_meta,
)
from datahub.pgqueue.repository import PgQueueMessageHandle
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    METADATA_CHANGE_LOG_EVENT_V1_TYPE,
    RELATIONSHIP_CHANGE_EVENT_V1_TYPE,
    MetadataChangeLogEvent,
    RelationshipChangeEvent,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.kafka.kafka_event_source import (
    DEFAULT_TOPIC_ROUTES,
    ENTITY_CHANGE_EVENT_NAME,
    RELATIONSHIP_CHANGE_EVENT_NAME,
    build_entity_change_event,
)
from datahub_actions.source.event_source import EventSource

logger = logging.getLogger(__name__)

_DEFAULT_PG_QUEUE_PAYLOAD_KINDS: Dict[str, PayloadRouteKind] = {
    "mcl": "mcl",
    "mcl_timeseries": "mcl",
    "pe": "pe",
}


class PgQueueEventSourceConfig(ConfigModel):
    queue: PgQueueConnectionConfig
    schema_registry_url: str = Field(default_factory=_get_schema_registry_url)
    schema_registry_config: Dict[str, object] = Field(default_factory=dict)
    topic_routes: Dict[str, str] = Field(
        default_factory=lambda: dict(DEFAULT_TOPIC_ROUTES),
        description="Logical route keys (mcl, pe, …) → logical topic names in pgQueue.",
    )
    payload_kind_by_route_key: Dict[str, PayloadRouteKind] = Field(
        default_factory=lambda: dict(_DEFAULT_PG_QUEUE_PAYLOAD_KINDS),
    )
    visibility_timeout_seconds: Optional[int] = Field(
        default=None,
        description="Override queue visibility timeout when set.",
    )
    poll_interval_seconds: float = Field(default=2.0, ge=0.1, le=120.0)
    batch_size: int = Field(default=100, ge=1, le=5000)

    def build_consumer_config(self, pipeline_name: str) -> PgQueueConsumerConfig:
        return PgQueueConsumerConfig(
            queue=self.queue,
            schema_registry_url=self.schema_registry_url,
            schema_registry_config=dict(self.schema_registry_config),
            topic_routes=dict(self.topic_routes),
            consumer_group=pipeline_name,
            visibility_timeout_seconds=self.visibility_timeout_seconds,
            payload_kind_by_route_key=dict(self.payload_kind_by_route_key),
        )


@dataclass
class PgQueueEventSource(EventSource):
    """Poll pgQueue using metadata-ingestion client; emits the same envelopes as Kafka source."""

    source_config: PgQueueEventSourceConfig = field(init=False)
    ctx: PipelineContext = field(init=False)
    _consumer: Optional[DatahubPgQueueConsumer] = field(
        default=None, init=False, repr=False
    )
    running: bool = field(default=False, init=False)

    def __init__(self, config: PgQueueEventSourceConfig, ctx: PipelineContext):
        self.source_config = config
        self.ctx = ctx
        consumer_cfg = config.build_consumer_config(ctx.pipeline_name)
        self._consumer = DatahubPgQueueConsumer(consumer_cfg)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> PgQueueEventSource:
        cfg = PgQueueEventSourceConfig.model_validate(config_dict)
        return cls(cfg, ctx)

    def events(self) -> Iterable[EventEnvelope]:
        if self._consumer is None:
            raise RuntimeError("pgQueue consumer is not initialized")

        routes = self.source_config.topic_routes
        preferred = ("mcl", "mcl_timeseries", "pe")
        route_order = [k for k in preferred if k in routes]
        for k in routes:
            if k not in route_order:
                route_order.append(k)

        self.running = True
        logger.info(
            "pgQueue actions source started for pipeline '%s', routes=%s",
            self.ctx.pipeline_name,
            route_order,
        )
        while self.running:
            batch = self._consumer.poll_route_keys(
                route_order, max_messages=self.source_config.batch_size
            )
            if batch:
                for rec in batch:
                    yield from self._records_to_envelopes(rec)
            else:
                time.sleep(self.source_config.poll_interval_seconds)

        logger.info("pgQueue consumer exiting main loop")

    def _records_to_envelopes(
        self, rec: PgQueueConsumedRecord
    ) -> Iterable[EventEnvelope]:
        meta = build_pg_queue_event_meta(rec)
        if isinstance(rec.record, MetadataChangeLogClass):
            mcl_event = MetadataChangeLogEvent.from_class(rec.record)
            yield EventEnvelope(METADATA_CHANGE_LOG_EVENT_V1_TYPE, mcl_event, meta)
            return
        if isinstance(rec.record, dict):
            yield from self._platform_event_envelopes(rec.record, meta)
            return
        logger.warning(
            "Unexpected pgQueue payload type %s for topic %s",
            type(rec.record),
            rec.topic_name,
        )

    def _platform_event_envelopes(
        self, value: Dict[str, Any], meta: Dict[str, Any]
    ) -> Iterable[EventEnvelope]:
        payload: GenericPayloadClass = GenericPayloadClass.from_obj(
            post_json_transform(value["payload"])
        )
        name = value["name"]
        if ENTITY_CHANGE_EVENT_NAME == name:
            ece = build_entity_change_event(payload)
            yield EventEnvelope(ENTITY_CHANGE_EVENT_V1_TYPE, ece, meta)
        elif RELATIONSHIP_CHANGE_EVENT_NAME == name:
            rce = RelationshipChangeEvent.from_json(payload.get("value"))
            yield EventEnvelope(RELATIONSHIP_CHANGE_EVENT_V1_TYPE, rce, meta)

    def ack(self, event: EventEnvelope, processed: bool = True) -> None:
        self.ack_many((event,), processed=processed)

    def ack_many(self, events: Sequence[EventEnvelope], processed: bool = True) -> None:
        """Acknowledge multiple envelopes in one pgQueue round-trip when possible."""
        if not processed or self._consumer is None or not events:
            return
        handles: List[PgQueueMessageHandle] = []
        for event in events:
            pq = event.meta.get("pg_queue") if event.meta else None
            if not pq or "handle" not in pq:
                logger.warning("Missing pg_queue meta; skip ack for one event")
                continue
            handles.append(PgQueueMessageHandle.from_meta_dict(pq["handle"]))
        if handles:
            self._consumer.ack(handles)

    def close(self) -> None:
        self.running = False
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
