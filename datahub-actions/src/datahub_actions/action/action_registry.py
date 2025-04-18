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

from datahub.ingestion.api.registry import PluginRegistry
from datahub_actions.action.action import Action
from datahub_actions.plugin.action.hello_world.hello_world import HelloWorldAction

action_registry = PluginRegistry[Action]()
action_registry.register_from_entrypoint("datahub_actions.action.plugins")
action_registry.register("hello_world", HelloWorldAction)
