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
from typing import List, Optional

from datahub_actions.api.action_graph import AcrylDataHubGraph

logger = logging.getLogger(__name__)


class GlossaryTermsResolver:
    def __init__(
        self,
        glossary_entities: Optional[List[str]] = None,
        graph: Optional[AcrylDataHubGraph] = None,
    ):
        self.graph = graph
        self.glossary_term_registry = {}
        self.glossary_node_registry = {}
        if glossary_entities:
            # isolate the terms that don't seem fully specified
            terms_needing_resolution = [
                d
                for d in glossary_entities
                if (not d.startswith("urn:li:glossaryTerm") and d.count("-") != 4)
            ]
            if terms_needing_resolution and not graph:
                raise ValueError(
                    f"Following terms need server-side resolution {terms_needing_resolution} but a DataHub server wasn't provided. Either use fully qualified glossary term ids (e.g. urn:li:glossaryTerm:ec428203-ce86-4db3-985d-5a8ee6df32ba) or provide a datahub_api config in your recipe."
                )
            for term_identifier in terms_needing_resolution:
                self.glossary_term_registry[term_identifier] = (
                    self._resolve_term_id_to_urn(term_identifier)
                )
            nodes_needing_resolution = [
                d
                for d in glossary_entities
                if (not d.startswith("urn:li:glossaryNode") and d.count("-") != 4)
            ]
            if nodes_needing_resolution and not graph:
                raise ValueError(
                    f"Following term groups (glossary nodes) need server-side resolution {nodes_needing_resolution} but a DataHub server wasn't provided. Either use fully qualified glossary term ids (e.g. urn:li:glossaryTerm:ec428203-ce86-4db3-985d-5a8ee6df32ba) or provide a datahub_api config in your recipe."
                )
            for node_identifier in nodes_needing_resolution:
                self.glossary_node_registry[node_identifier] = (
                    self._resolve_node_id_to_urn(node_identifier)
                )

    def _resolve_term_id_to_urn(self, term_identifier: str) -> Optional[str]:
        assert self.graph
        # first try to check if this domain exists by urn
        maybe_term_urn = f"urn:li:glossaryTerm:{term_identifier}"

        if self.graph.graph.exists(maybe_term_urn):
            self.glossary_term_registry[term_identifier] = maybe_term_urn
        else:
            # try to get this term by name
            term_urn = self.graph.get_glossary_term_urn_by_name(term_identifier)
            if term_urn:
                self.glossary_term_registry[term_identifier] = term_urn
            else:
                logger.error(
                    f"Failed to retrieve glossary term id for term {term_identifier}"
                )
                raise ValueError(
                    f"domain {term_identifier} doesn't seem to be provisioned on DataHub. Either provision it first and re-run ingestion, or provide a fully qualified domain id (e.g. urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba) to skip this check."
                )
        return self.glossary_term_registry.get(term_identifier)

    def _resolve_node_id_to_urn(self, term_identifier: str) -> Optional[str]:
        assert self.graph
        # first try to check if this domain exists by urn
        maybe_term_urn = f"urn:li:glossaryNode:{term_identifier}"

        if self.graph.graph.exists(maybe_term_urn):
            self.glossary_term_registry[term_identifier] = maybe_term_urn
        else:
            # try to get this node by name
            term_urn = self.graph.get_glossary_node_urn_by_name(term_identifier)
            if term_urn:
                self.glossary_node_registry[term_identifier] = term_urn
            else:
                logger.error(
                    f"Failed to retrieve glossary node id for node {term_identifier}"
                )
                raise ValueError(
                    f"domain {term_identifier} doesn't seem to be provisioned on DataHub. Either provision it first and re-run ingestion, or provide a fully qualified domain id (e.g. urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba) to skip this check."
                )
        return self.glossary_node_registry.get(term_identifier)

    def get_glossary_term_urn(self, term_identifier: str) -> Optional[str]:
        return self.glossary_term_registry.get(
            term_identifier
        ) or self._resolve_term_id_to_urn(term_identifier)

    def get_glossary_node_urn(self, node_identifier: str) -> Optional[str]:
        return self.glossary_node_registry.get(
            node_identifier
        ) or self._resolve_node_id_to_urn(node_identifier)
