# SPDX-License-Identifier: Apache-2.0
# Originally developed by Acryl Data, Inc.; subsequently adapted, enhanced, and maintained by the National Digital Twin Programme.
#
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

# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.
from datahub.ingestion.api.registry import PluginRegistry
from datahub_actions.plugin.transform.filter.filter_transformer import FilterTransformer
from datahub_actions.transform.transformer import Transformer

transformer_registry = PluginRegistry[Transformer]()
transformer_registry.register_from_entrypoint("datahub_actions.transformer.plugins")
transformer_registry.register("__filter", FilterTransformer)
