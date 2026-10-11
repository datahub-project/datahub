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

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from datahub_actions.api.action_graph import AcrylDataHubGraph

if TYPE_CHECKING:
    from datahub_actions.source.event_source import EventSource


@dataclass
class PipelineContext:
    """
    Context which is provided to each component in a Pipeline.
    """

    # The name of the running pipeline.
    pipeline_name: str

    # An instance of a DataHub client.
    graph: Optional[AcrylDataHubGraph]

    # Provided so that actions can manually acknowledge events, e.g. for bulk processing
    event_source: Optional[EventSource] = None
