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

import logging
import re
from typing import Optional

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub_actions.action.action import Action
from datahub_actions.action.action_registry import action_registry
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.pipeline.pipeline_config import (
    ActionConfig,
    FilterConfig,
    SourceConfig,
    TransformConfig,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.transform.filter.filter_transformer import (
    FilterTransformer,
    FilterTransformerConfig,
)
from datahub_actions.source.event_source import EventSource
from datahub_actions.source.event_source_registry import event_source_registry
from datahub_actions.transform.transformer import Transformer
from datahub_actions.transform.transformer_registry import transformer_registry

logger = logging.getLogger(__name__)


def create_action_context(
    pipeline_name: str, datahub_config: Optional[DatahubClientConfig]
) -> PipelineContext:
    return PipelineContext(
        pipeline_name,
        (
            AcrylDataHubGraph(DataHubGraph(datahub_config))
            if datahub_config is not None
            else None
        ),
    )


def create_event_source(
    source_config: SourceConfig, ctx: PipelineContext
) -> EventSource:
    event_source_type = source_config.type
    event_source_class = event_source_registry.get(event_source_type)
    event_source_instance = None
    try:
        logger.debug(
            f"Attempting to instantiate new Event Source of type {source_config.type}.."
        )
        event_source_config = (
            source_config.config if source_config.config is not None else {}
        )
        event_source_instance = event_source_class.create(event_source_config, ctx)
    except Exception as e:
        raise Exception(
            f"Caught exception while attempting to instantiate Event Source of type {source_config.type}"
        ) from e

    if event_source_instance is None:
        raise Exception(
            f"Failed to create Event Source with type {event_source_type}. Event Source create method returned 'None'."
        )

    return event_source_instance


def create_filter_transformer(
    filter_config: FilterConfig, ctx: PipelineContext
) -> Transformer:
    try:
        logger.debug("Attempting to instantiate filter transformer..")
        filter_transformer_config = FilterTransformerConfig(
            event_type=filter_config.event_type, event=filter_config.event
        )
        return FilterTransformer(filter_transformer_config)
    except Exception as e:
        raise Exception(
            "Caught exception while attempting to instantiate Filter transformer"
        ) from e


def create_transformer(
    transform_config: TransformConfig, ctx: PipelineContext
) -> Transformer:
    transformer_type = transform_config.type
    transformer_class = transformer_registry.get(transformer_type)
    transformer_instance = None
    try:
        logger.debug(
            f"Attempting to instantiate new Transformer of type {transform_config.type}.."
        )
        transformer_config = (
            transform_config.config if transform_config.config is not None else {}
        )
        transformer_instance = transformer_class.create(transformer_config, ctx)
    except Exception as e:
        raise Exception(
            f"Caught exception while attempting to instantiate Transformer with type {transformer_type}"
        ) from e

    if transformer_instance is None:
        raise Exception(
            f"Failed to create transformer with type {transformer_type}. Transformer create method returned 'None'."
        )

    return transformer_instance


def create_action(action_config: ActionConfig, ctx: PipelineContext) -> Action:
    action_type = action_config.type
    action_instance = None
    try:
        logger.debug(
            f"Attempting to instantiate new Action of type {action_config.type}.."
        )
        action_class = action_registry.get(action_type)
        action_config_dict = (
            action_config.config if action_config.config is not None else {}
        )
        action_instance = action_class.create(action_config_dict, ctx)
    except Exception as e:
        raise Exception(
            f"Caught exception while attempting to instantiate Action with type {action_type}. "
        ) from e

    if action_instance is None:
        raise Exception(
            f"Failed to create action with type {action_type}. Action create method returned 'None'."
        )

    return action_instance


def normalize_directory_name(name: str) -> str:
    # Lower case & remove whitespaces + periods.
    return re.sub(r"[^\w\-_]", "_", name.lower())


def get_transformer_name(transformer: Transformer) -> str:
    # TODO: Would be better to compute this using the transformer registry itself.
    return type(transformer).__name__
