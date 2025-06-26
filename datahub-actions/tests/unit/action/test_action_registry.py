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


from datahub_actions.action.action_registry import action_registry


def test_registry_nonempty():
    assert len(action_registry.mapping) > 0


def test_all_registry_plugins_enabled() -> None:
    for plugin in action_registry.mapping.keys():
        assert action_registry.is_enabled(plugin), f"Plugin {plugin} is not enabled"
