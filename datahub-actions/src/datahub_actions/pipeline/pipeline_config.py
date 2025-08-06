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

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel

from datahub.configuration import ConfigModel
from datahub.configuration.common import ConfigEnum
from datahub.ingestion.graph.client import DatahubClientConfig


class FailureMode(ConfigEnum):
    # Log the failed event to the failed events log. Then throw an pipeline exception to stop the pipeline.
    THROW = "THROW"
    # Log the failed event to the failed events log. Then continue processing the event stream.
    CONTINUE = "CONTINUE"


class SourceConfig(ConfigModel):
    type: str
    config: Optional[Dict[str, Any]] = None


class TransformConfig(ConfigModel):
    type: str
    config: Optional[Dict[str, Any]] = None


class FilterConfig(ConfigModel):
    event_type: Union[str, List[str]]
    event: Optional[Dict[str, Any]] = None


class ActionConfig(ConfigModel):
    type: str
    config: Optional[dict]


class PipelineOptions(BaseModel):
    retry_count: Optional[int] = None
    failure_mode: Optional[FailureMode] = None
    failed_events_dir: Optional[str] = (
        None  # The path where failed events should be logged.
    )


class PipelineConfig(ConfigModel):
    """
    Configuration required to create a new Actions Pipeline.

    This exactly matches the structure of the YAML file used
    to configure a Pipeline.
    """

    name: str
    enabled: bool = True
    source: SourceConfig
    filter: Optional[FilterConfig] = None
    transform: Optional[List[TransformConfig]] = None
    action: ActionConfig
    datahub: Optional[DatahubClientConfig] = None
    options: Optional[PipelineOptions] = None
