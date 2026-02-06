# Copyright 2025 Acryl Data, Inc.
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

"""DataHub Agent Context - MCP Tools for AI Agents."""

from datahub_agent_context._version import __version__
from datahub_agent_context.context import (
    DataHubContext,
    get_graph,
    reset_graph,
    set_graph,
)

__all__ = ["__version__", "DataHubContext", "get_graph", "set_graph", "reset_graph"]
