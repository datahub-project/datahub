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

from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeConfig
from datahub.metadata._urns.urn_defs import (
    DatasetUrn,
    GlossaryTermUrn,
    SchemaFieldUrn,
    TagUrn,
)
from datahub.metadata.schema_classes import GlossaryNodeInfoClass, GlossaryTermInfoClass
from datahub.utilities.urns.urn import Urn
from datahub_actions.api.action_graph import AcrylDataHubGraph
from sqlalchemy import create_engine

logger: logging.Logger = logging.getLogger(__name__)


def is_snowflake_urn(urn: str) -> bool:
    parsed_urn = Urn.create_from_string(urn)
    if isinstance(parsed_urn, SchemaFieldUrn):
        parsed_urn = Urn.create_from_string(parsed_urn.parent)

    return (
        isinstance(parsed_urn, DatasetUrn)
        and parsed_urn.get_data_platform_urn().platform_name == "snowflake"
    )


class SnowflakeTagHelper(Closeable):
    def __init__(self, config: SnowflakeConfig):
        self.config: SnowflakeConfig = config
        url = self.config.get_sql_alchemy_url()
        self.engine = create_engine(url, **self.config.get_options())

    @staticmethod
    def get_term_name_from_id(term_urn: str, graph: AcrylDataHubGraph) -> str:
        term_id = Urn.create_from_string(term_urn).get_entity_id_as_string()
        # needs resolution
        term_info = graph.graph.get_aspect(term_urn, GlossaryTermInfoClass)
        if not term_info or not term_info.name:
            return term_id

        term_name = term_info.name
        parent = term_info.parentNode
        while parent:
            node_info = graph.graph.get_aspect(parent, GlossaryNodeInfoClass)
            parent_name = node_info.name
            parent = node_info.parentNode
            term_name = f"{parent_name}.{term_name}"

        return term_name

    @staticmethod
    def get_label_urn_to_tag(label_urn: str, graph: AcrylDataHubGraph) -> str:
        label_urn_parsed = Urn.from_string(label_urn)
        if isinstance(label_urn_parsed, TagUrn):
            return label_urn_parsed.name
        elif isinstance(label_urn_parsed, GlossaryTermUrn):
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
        self, entity_urn: str, tag_or_term_urn: str, graph: AcrylDataHubGraph
    ) -> None:
        if not is_snowflake_urn(entity_urn):
            return
        tag = self.get_label_urn_to_tag(tag_or_term_urn, graph)
        assert tag is not None

        parsed_entity_urn = Urn.create_from_string(entity_urn)
        if isinstance(parsed_entity_urn, DatasetUrn):
            dataset_urn = parsed_entity_urn
        elif isinstance(parsed_entity_urn, SchemaFieldUrn):
            dataset_urn = DatasetUrn.create_from_string(parsed_entity_urn.parent)
        else:
            raise ValueError(
                f"Invalid entity urn {entity_urn}, can only handle Dataset and SchemaField urns."
            )

        database, schema, table = dataset_urn.name.split(".")
        self._create_tag(database, schema, tag, tag_or_term_urn)

        if isinstance(parsed_entity_urn, DatasetUrn):
            self._run_query(
                database,
                schema,
                f'ALTER TABLE {table} SET TAG {tag}="{tag_or_term_urn}";',
            )
        elif isinstance(parsed_entity_urn, SchemaFieldUrn):
            self._run_query(
                database,
                schema,
                f'ALTER TABLE {table} MODIFY COLUMN {parsed_entity_urn.field_path} SET TAG {tag}="{tag_or_term_urn}";',
            )

    def remove_tag_or_term(
        self, entity_urn: str, tag_urn: str, graph: AcrylDataHubGraph
    ) -> None:
        if not is_snowflake_urn(entity_urn):
            return
        tag = self.get_label_urn_to_tag(tag_urn, graph)
        assert tag is not None

        parsed_entity_urn = Urn.create_from_string(entity_urn)
        if isinstance(parsed_entity_urn, DatasetUrn):
            dataset_urn = parsed_entity_urn
            database, schema, table = dataset_urn.name.split(".")
            self._run_query(
                database,
                schema,
                f"ALTER TABLE {table} UNSET TAG {tag};",
            )
        elif isinstance(parsed_entity_urn, SchemaFieldUrn):
            dataset_urn = DatasetUrn.create_from_string(parsed_entity_urn.parent)
            database, schema, table = dataset_urn.name.split(".")
            self._run_query(
                database,
                schema,
                f"ALTER TABLE {table} MODIFY COLUMN {parsed_entity_urn.field_path} UNSET TAG {tag};",
            )
        else:
            raise ValueError(
                f"Invalid entity urn {entity_urn}, can only handle Dataset and SchemaField urns."
            )

    def _create_tag(
        self, database: str, schema: str, tag_name: str, tag_or_term_urn: str
    ) -> None:
        self._run_query(
            database,
            schema,
            f"CREATE TAG IF NOT EXISTS {tag_name} COMMENT = 'Replicated Tag {tag_or_term_urn} from DataHub';",
        )

    def _run_query(self, database: str, schema: str, query: str) -> None:
        try:
            self.engine.execute(f"USE {database}.{schema};")
            self.engine.execute(query)
            logger.info(f"Successfully executed query {query}")
        except Exception as e:
            logger.warning(
                f"Failed to execute snowflake query: {query}. Exception: ", e
            )

    def close(self) -> None:
        if self.engine:
            self.engine.dispose()
        logger.info("SnowflakeTagHelper closed.")
