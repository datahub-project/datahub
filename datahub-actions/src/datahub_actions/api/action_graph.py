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

import json
import logging
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from datahub.configuration.common import OperationalError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    GlossaryTermAssociationClass,
    TagAssociationClass,
)
from datahub.specific.dataset import DatasetPatchBuilder

logger = logging.getLogger(__name__)


@dataclass
class AcrylDataHubGraph:
    def __init__(self, baseGraph: DataHubGraph):
        self.graph = baseGraph

    def get_by_query(
        self,
        query: str,
        entity: str,
        start: int = 0,
        count: int = 100,
        filters: Optional[Dict] = None,
    ) -> List[Dict]:
        url_frag = "/entities?action=search"
        url = f"{self.graph._gms_server}{url_frag}"
        payload = {"input": query, "start": start, "count": count, "entity": entity}
        if filters is not None:
            payload["filter"] = filters

        headers = {
            "X-RestLi-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        }

        try:
            response = self.graph._session.post(
                url, data=json.dumps(payload), headers=headers
            )
            if response.status_code != 200:
                return []
            json_resp = response.json()
            return json_resp.get("value", {}).get("entities")
        except Exception as e:
            print(e)
            return []

    def get_by_graphql_query(self, query: Dict) -> Dict:
        url_frag = "/api/graphql"
        url = f"{self.graph._gms_server}{url_frag}"

        headers = {
            "X-DataHub-Actor": "urn:li:corpuser:admin",
            "Content-Type": "application/json",
        }
        try:
            response = self.graph._session.post(
                url, data=json.dumps(query), headers=headers
            )
            if response.status_code != 200:
                return {}
            json_resp = response.json()
            return json_resp.get("data", {})
        except Exception as e:
            print(e)
            return {}

    def query_constraints_for_dataset(self, dataset_id: str) -> List:
        resp = self.get_by_graphql_query(
            {
                "query": """
query dataset($input: String!) {
  dataset(urn: $input) {
    constraints {
      type
      displayName
      description
      params {
        hasGlossaryTermInNodeParams {
          nodeName
        }
      }
    }
  }
}
""",
                "variables": {"input": dataset_id},
            }
        )
        constraints: List = resp.get("dataset", {}).get("constraints", [])
        return constraints

    def query_execution_result_details(self, execution_id: str) -> Any:
        resp = self.get_by_graphql_query(
            {
                "query": """
query executionRequest($urn: String!) {
  executionRequest(urn: $urn) {
    input {
      task
      arguments {
        key
        value
      }
    }
  }
}
""",
                "variables": {"urn": f"urn:li:dataHubExecutionRequest:{execution_id}"},
            }
        )
        return resp.get("executionRequest", {}).get("input", {})

    def query_ingestion_sources(self) -> List:
        sources = []
        start, count = 0, 10
        while True:
            resp = self.get_by_graphql_query(
                {
                    "query": """
query listIngestionSources($input: ListIngestionSourcesInput!, $execution_start: Int!, $execution_count: Int!) {
  listIngestionSources(input: $input) {
    start
    count
    total
    ingestionSources {
      urn
      type
      name
      executions(start: $execution_start, count: $execution_count) {
        start
        count
        total
        executionRequests {
          urn
        }
      }
    }
  }
}
""",
                    "variables": {
                        "input": {"start": start, "count": count},
                        "execution_start": 0,
                        "execution_count": 10,
                    },
                }
            )
            listIngestionSources = resp.get("listIngestionSources", {})
            sources.extend(listIngestionSources.get("ingestionSources", []))

            cur_total = listIngestionSources.get("total", 0)
            if cur_total > count:
                start += count
            else:
                break
        return sources

    def get_downstreams(
        self, entity_urn: str, max_downstreams: int = 3000
    ) -> List[str]:
        start = 0
        count_per_page = 1000
        entities = []
        done = False
        total_downstreams = 0
        while not done:
            # if start > 0:
            #     breakpoint()
            url_frag = f"/relationships?direction=INCOMING&types=List(DownstreamOf)&urn={urllib.parse.quote(entity_urn)}&count={count_per_page}&start={start}"
            url = f"{self.graph._gms_server}{url_frag}"
            response = self.graph._get_generic(url)
            if response["count"] > 0:
                relnships = response["relationships"]
                entities.extend([x["entity"] for x in relnships])
                start += count_per_page
                total_downstreams += response["count"]
                if start >= response["total"] or total_downstreams >= max_downstreams:
                    done = True
            else:
                done = True
        return entities

    def get_upstreams(self, entity_urn: str, max_upstreams: int = 3000) -> List[str]:
        start = 0
        count_per_page = 100
        entities = []
        done = False
        total_upstreams = 0
        while not done:
            url_frag = f"/relationships?direction=OUTGOING&types=List(DownstreamOf)&urn={urllib.parse.quote(entity_urn)}&count={count_per_page}&start={start}"
            url = f"{self.graph._gms_server}{url_frag}"
            response = self.graph._get_generic(url)
            if response["count"] > 0:
                relnships = response["relationships"]
                entities.extend([x["entity"] for x in relnships])
                start += count_per_page
                total_upstreams += response["count"]
                if start >= response["total"] or total_upstreams >= max_upstreams:
                    done = True
            else:
                done = True
        return entities

    def get_relationships(
        self, entity_urn: str, direction: str, relationship_types: List[str]
    ) -> List[str]:
        url_frag = (
            f"/relationships?"
            f"direction={direction}"
            f"&types=List({','.join(relationship_types)})"
            f"&urn={urllib.parse.quote(entity_urn)}"
        )

        url = f"{self.graph._gms_server}{url_frag}"
        response = self.graph._get_generic(url)
        if response["count"] > 0:
            relnships = response["relationships"]
            entities = [x["entity"] for x in relnships]
            return entities
        return []

    def check_relationship(self, entity_urn, target_urn, relationship_type):
        url_frag = f"/relationships?direction=INCOMING&types=List({relationship_type})&urn={urllib.parse.quote(entity_urn)}"
        url = f"{self.graph._gms_server}{url_frag}"
        response = self.graph._get_generic(url)
        if response["count"] > 0:
            relnships = response["relationships"]
            entities = [x["entity"] for x in relnships]
            return target_urn in entities
        return False

    def add_tags_to_dataset(
        self,
        entity_urn: str,
        dataset_tags: List[str],
        field_tags: Optional[Dict] = None,
        context: Optional[Dict] = None,
    ) -> None:
        if field_tags is None:
            field_tags = {}
        dataset = DatasetPatchBuilder(entity_urn)
        for t in dataset_tags:
            dataset.add_tag(
                tag=TagAssociationClass(
                    tag=t, context=json.dumps(context) if context else None
                )
            )

        for field_path, tags in field_tags.items():
            field_builder = dataset.for_field(field_path=field_path)
            for tag in tags:
                field_builder.add_tag(
                    tag=TagAssociationClass(
                        tag=tag, context=json.dumps(context) if context else None
                    )
                )

        for mcp in dataset.build():
            self.graph.emit(mcp)

    def add_terms_to_dataset(
        self,
        entity_urn: str,
        dataset_terms: List[str],
        field_terms: Optional[Dict] = None,
        context: Optional[Dict] = None,
    ) -> None:
        if field_terms is None:
            field_terms = {}

        dataset = DatasetPatchBuilder(urn=entity_urn)

        for term in dataset_terms:
            dataset.add_term(
                GlossaryTermAssociationClass(
                    term, context=json.dumps(context) if context else None
                )
            )

        for field_path, terms in field_terms.items():
            field_builder = dataset.for_field(field_path=field_path)
            for term in terms:
                field_builder.add_term(
                    GlossaryTermAssociationClass(
                        term, context=json.dumps(context) if context else None
                    )
                )

        for mcp in dataset.build():
            self.graph.emit(mcp)

    def get_corpuser_info(self, urn: str) -> Any:
        return self.get_untyped_aspect(
            urn, "corpUserInfo", "com.linkedin.identity.CorpUserInfo"
        )

    def get_untyped_aspect(
        self,
        entity_urn: str,
        aspect: str,
        aspect_type_name: str,
    ) -> Any:
        url = f"{self.graph._gms_server}/aspects/{urllib.parse.quote(entity_urn)}?aspect={aspect}&version=0"
        response = self.graph._session.get(url)
        if response.status_code == 404:
            # not found
            return None
        response.raise_for_status()
        response_json = response.json()
        aspect_json = response_json.get("aspect", {}).get(aspect_type_name)
        if aspect_json:
            return aspect_json
        else:
            raise OperationalError(
                f"Failed to find {aspect_type_name} in response {response_json}"
            )

    def _get_entity_by_name(
        self,
        name: str,
        entity_type: str,
        indexed_fields: Optional[List[str]] = None,
    ) -> Optional[str]:
        """Retrieve an entity urn based on its name and type. Returns None if there is no match found"""
        if indexed_fields is None:
            indexed_fields = ["name", "displayName"]

        filters = []
        if len(indexed_fields) > 1:
            for indexed_field in indexed_fields:
                filter_criteria = [
                    {
                        "field": indexed_field,
                        "value": name,
                        "condition": "EQUAL",
                    }
                ]
                filters.append({"and": filter_criteria})
            search_body = {
                "input": "*",
                "entity": entity_type,
                "start": 0,
                "count": 10,
                "orFilters": [filters],
            }
        else:
            search_body = {
                "input": "*",
                "entity": entity_type,
                "start": 0,
                "count": 10,
                "filter": {
                    "or": [
                        {
                            "and": [
                                {
                                    "field": indexed_fields[0],
                                    "value": name,
                                    "condition": "EQUAL",
                                }
                            ]
                        }
                    ]
                },
            }
        results: Dict = self.graph._post_generic(
            self.graph._search_endpoint, search_body
        )
        num_entities = results.get("value", {}).get("numEntities", 0)
        if num_entities > 1:
            logger.warning(
                f"Got {num_entities} results for {entity_type} {name}. Will return the first match."
            )
        entities_yielded: int = 0
        entities = []
        for x in results["value"]["entities"]:
            entities_yielded += 1
            logger.debug(f"yielding {x['entity']}")
            entities.append(x["entity"])
        return entities[0] if entities_yielded else None

    def get_glossary_term_urn_by_name(self, term_name: str) -> Optional[str]:
        """Retrieve a glossary term urn based on its name. Returns None if there is no match found"""

        return self._get_entity_by_name(
            term_name, "glossaryTerm", indexed_fields=["name"]
        )

    def get_glossary_node_urn_by_name(self, node_name: str) -> Optional[str]:
        """Retrieve a glossary node urn based on its name. Returns None if there is no match found"""

        return self._get_entity_by_name(node_name, "glossaryNode")
