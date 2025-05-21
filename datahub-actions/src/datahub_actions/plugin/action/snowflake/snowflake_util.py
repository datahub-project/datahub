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

from sqlalchemy import create_engine

from datahub.emitter.mce_builder import dataset_urn_to_key
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeConfig
from datahub.metadata.schema_classes import GlossaryNodeInfoClass, GlossaryTermInfoClass
from datahub.utilities.urns.urn import Urn
from datahub_actions.api.action_graph import AcrylDataHubGraph

logger: logging.Logger = logging.getLogger(__name__)


class SnowflakeTagHelper(Closeable):
    def __init__(self, config: SnowflakeConfig):
        self.config: SnowflakeConfig = config
        url = self.config.get_sql_alchemy_url()
        self.engine = create_engine(url, **self.config.get_options())

    @staticmethod
    def get_term_name_from_id(term_urn: str, graph: AcrylDataHubGraph) -> str:
        term_id = Urn.from_string(term_urn).get_entity_id_as_string()
        if term_id.count("-") == 4:
            # needs resolution
            term_info = graph.graph.get_aspect(term_urn, GlossaryTermInfoClass)
            assert term_info
            assert term_info.name
            term_name = term_info.name
            parent = term_info.parentNode
            while parent:
                parent_id = Urn.from_string(parent).get_entity_id_as_string()
                node_info = graph.graph.get_aspect(parent, GlossaryNodeInfoClass)
                assert node_info
                if parent_id.count("-") == 4:
                    parent_name = node_info.name
                    parent = node_info.parentNode
                else:
                    # terminate
                    parent_name = parent_id
                    parent = None
                term_name = f"{parent_name}.{term_name}"
        else:
            term_name = term_id

        return term_name

    @staticmethod
    def get_label_urn_to_tag(label_urn: str, graph: AcrylDataHubGraph) -> str:
        label_urn_parsed = Urn.from_string(label_urn)
        if label_urn_parsed.get_type() == "tag":
            return label_urn_parsed.get_entity_id_as_string()
        elif label_urn_parsed.get_type() == "glossaryTerm":
            # if this looks like a guid, we want to resolve to human friendly names
            term_name = SnowflakeTagHelper.get_term_name_from_id(label_urn, graph)
            if term_name is not None:
                # terms use `.` for separation, replace with _
                return term_name.replace(".", "_").replace(" ", "_")
            else:
                raise ValueError(f"Invalid tag or term urn {label_urn}")
        else:
            raise Exception(
                f"Unexpected label type: neither tag or term {label_urn_parsed.get_type()}"
            )

    def apply_tag_or_term(
        self, dataset_urn: str, tag_or_term_urn: str, graph: AcrylDataHubGraph
    ) -> None:
        dataset_key = dataset_urn_to_key(dataset_urn)
        assert dataset_key is not None
        if dataset_key.platform != "snowflake":
            return
        tag = self.get_label_urn_to_tag(tag_or_term_urn, graph)
        assert tag is not None
        name_tokens = dataset_key.name.split(".")
        assert len(name_tokens) == 3
        self.run_query(
            name_tokens[0],
            name_tokens[1],
            f"CREATE TAG IF NOT EXISTS {tag} COMMENT = 'Replicated Tag {tag_or_term_urn} from DataHub';",
        )
        self.run_query(
            name_tokens[0],
            name_tokens[1],
            f'ALTER TABLE {name_tokens[2]} SET TAG {tag}="{tag_or_term_urn}";',
        )

    def remove_tag_or_term(
        self, dataset_urn: str, tag_urn: str, graph: AcrylDataHubGraph
    ) -> None:
        dataset_key = dataset_urn_to_key(dataset_urn)
        assert dataset_key is not None
        if dataset_key.platform != "snowflake":
            return
        tag = self.get_label_urn_to_tag(tag_urn, graph)
        assert tag is not None
        name_tokens = dataset_key.name.split(".")
        assert len(name_tokens) == 3
        self.run_query(
            name_tokens[0],
            name_tokens[1],
            f"ALTER TABLE {name_tokens[2]} UNSET TAG {tag};",
        )

    def run_query(self, database: str, schema: str, query: str) -> None:
        try:
            self.engine.execute(f"USE {database}.{schema};")
            self.engine.execute(query)
            logger.info(f"Successfully executed query {query}")
        except Exception as e:
            logger.warning(
                f"Failed to execute snowflake query: {query}. Exception: ", e
            )

    def close(self) -> None:
        return
