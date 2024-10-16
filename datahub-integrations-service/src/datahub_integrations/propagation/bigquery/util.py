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
import os
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Deque, Dict, List, Optional, Tuple, Union

import cachetools
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.source.bigquery_v2.bigquery_platform_resource_helper import (
    BigQueryLabel,
)
from datahub.metadata._schema_classes import GlossaryNodeInfoClass
from datahub.metadata.com.linkedin.pegasus2avro.glossary import (
    GlossaryNodeInfo,
    GlossaryTermInfo,
)
from datahub.metadata.schema_classes import GlossaryTermInfoClass
from datahub.metadata.urns import DatasetUrn, GlossaryTermUrn, SchemaFieldUrn, TagUrn
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path
from datahub.utilities.urns.urn import Urn
from datahub_actions.api.action_graph import AcrylDataHubGraph
from google.api_core import exceptions as google_exceptions
from google.cloud.bigquery import PolicyTagList, SchemaField
from google.cloud.datacatalog_v1 import DataCatalogClient, PolicyTagManagerClient
from google.cloud.datacatalog_v1.types.policytagmanager import (
    PolicyTag,
    Taxonomy,
    UpdatePolicyTagRequest,
)

from datahub_integrations.propagation.bigquery.bigquery_platform_resource_helper import (
    ExtendedBigQueryPlatformResourceHelper,
)
from datahub_integrations.propagation.bigquery.config import (
    BigqueryConnectionConfigPermissive,
)

logger: logging.Logger = logging.getLogger(__name__)

MAX_ERRORS_PER_HOUR = int(
    os.getenv("MAX_BIGQUERY_ERRORS_PER_HOUR", 15)
)  # To Prevent Locking Out of Bigquery Account.


def is_bigquery_urn(urn: str) -> bool:
    parsed_urn = Urn.create_from_string(urn)
    if isinstance(parsed_urn, SchemaFieldUrn):
        parsed_urn = Urn.create_from_string(parsed_urn.parent)

    return (
        isinstance(parsed_urn, DatasetUrn)
        and parsed_urn.get_data_platform_urn().platform_name == "bigquery"
    )


@dataclass
class DataHubGlossaryNode:
    urn: str
    node: GlossaryNodeInfoClass


@dataclass
class DataHubGlossaryTerm:
    urn: str
    term: GlossaryTermInfoClass


class BigqueryTagHelper(Closeable):
    # platform_resource_cache: Dict[str, PlatformResource] = {}

    platform_resource_cache = cachetools.TTLCache(ttl=300, maxsize=500)  # type: ignore

    def __init__(
        self, config: BigqueryConnectionConfigPermissive, graph: AcrylDataHubGraph
    ):
        self.config: BigqueryConnectionConfigPermissive = config
        self.ptm_client = self.config.get_policy_tag_manager_client()
        self.bq_client = self.config.get_bigquery_client()
        self.dc_client = DataCatalogClient(
            client_options=self.config.extra_client_options
        )
        self.graph = graph
        self.bq_location = self.config.extra_client_options.get(
            "location", "US"
        ).lower()
        self.bq_project = (
            self.config.project_on_behalf
            if self.config.project_on_behalf
            else self.config.credential.project_id if self.config.credential else None
        )
        self.error_timestamps: Deque[datetime] = (
            deque()
        )  # To store timestamps of errors
        self.error_threshold = MAX_ERRORS_PER_HOUR  # Max errors per hour before dropping. To prevent getting locked out.
        self.taxonomies: List[Taxonomy] = []
        self.policy_tags: Dict[str, PolicyTag] = {}
        self.taxonomy_path: Optional[str] = None
        self.bigquery_platform_resource_helper = ExtendedBigQueryPlatformResourceHelper(
            bq_project=self.bq_project, graph=self.graph.graph
        )

        logger.info("BigqueryTagHelper initialized.")

    def create_taxonomy(self) -> None:
        if self.config.taxonomy:
            t = self._create_taxonomy(self.config.taxonomy, "DataHub Taxonomy")
            self.taxonomy_path = t.name
            self.policy_tags = {
                tag.display_name: tag
                for tag in self.list_policy_tags(self.taxonomy_path)
            }

    def get_glossary_nodes(self, glossary_node_urn: str) -> List[DataHubGlossaryNode]:
        node_info = self.graph.graph.get_aspect(glossary_node_urn, GlossaryNodeInfo)
        datahub_glossary_node = DataHubGlossaryNode(
            urn=glossary_node_urn, node=node_info
        )
        if node_info.parentNode:
            return self.get_glossary_nodes(node_info.parentNode) + [node_info]
        else:
            return [datahub_glossary_node]

    @staticmethod
    def str_to_bq_value(label: str) -> str:
        # https://cloud.google.com/bigquery/docs/labels-intro#requirements
        # BigQuery labels must meet the following requirements:
        # Each key must start with a lowercase letter or international character.
        # Each key must consist of only international characters, numbers, or underscores.

        # Replace spaces and other non-allowed characters with underscores
        s = re.sub(r"[^a-zA-Z0-9_-]", "_", label)

        return s

    @staticmethod
    def datahub_label_to_bigquery_label(tag_urn: TagUrn) -> Tuple[str, str]:
        # https://cloud.google.com/bigquery/docs/labels-intro#requirements
        # BigQuery labels must meet the following requirements:
        # Each label must be a key-value pair.
        # Each key must be a string with a length between 1 and 63 characters.
        # Each key must start with a lowercase letter or international character.
        # Each key must consist of only lowercase letters, international characters, numbers, or underscores.

        splits = tag_urn.name.split(":", 2)
        if len(splits) == 2:
            # Keys needs to be lowercase
            key = BigqueryTagHelper.str_to_bq_value(splits[0]).lower()
            value = BigqueryTagHelper.str_to_bq_value(splits[1]).lower()
        else:
            # Keys needs to be lowercase
            key = BigqueryTagHelper.str_to_bq_value(tag_urn.name).lower()
            value = ""

        # Ensure it starts with a letter or international character
        if key and not key[0].isalpha():
            key = "l_" + key

        # Truncate to max_length
        return key[:63], value

    def get_term_name_from_id(
        self, term_urn: GlossaryTermUrn
    ) -> Tuple[DataHubGlossaryTerm, List[DataHubGlossaryNode]]:
        # needs resolution
        term_info = self.graph.graph.get_aspect(term_urn.urn(), GlossaryTermInfoClass)
        if not term_info:
            raise ValueError(f"Term {term_urn} not found in graph.")

        logger.info(f"Resolved term {term_info}")
        datahub_glossary_term = DataHubGlossaryTerm(urn=term_urn.urn(), term=term_info)
        nodes = []
        if term_info and term_info.parentNode:
            nodes = self.get_glossary_nodes(term_info.parentNode)
        if not term_info.name:
            logger.warning(
                f"Term_info.name is empty, using name from urn {term_urn} TermInfo: {term_info}"
            )
            return (
                DataHubGlossaryTerm(
                    urn=term_urn.urn(),
                    term=GlossaryTermInfo(
                        name=term_urn.name,
                        definition=term_info.definition,
                        termSource=term_info.termSource,
                    ),
                ),
                nodes,
            )

        return datahub_glossary_term, nodes

    def convert_tag_to_bigquery_label(self, tag_urn: TagUrn) -> BigQueryLabel:
        (key, value) = self.datahub_label_to_bigquery_label(tag_urn)
        return BigQueryLabel(key=key, value=value)

    def get_glossary_tags_from_urn(
        self, glossary_urn: GlossaryTermUrn
    ) -> Tuple[DataHubGlossaryTerm, list[DataHubGlossaryNode]]:
        # if this looks like a guid, we want to resolve to human friendly names
        term, parents = self.get_term_name_from_id(glossary_urn)
        logger.info(f"Resolved term {term} with parents {parents}")
        if term is not None:
            return term, parents
        else:
            raise ValueError(f"Invalid glossary term urn {glossary_urn}")

    def update_schema_field(
        self,
        schema: List[SchemaField],
        field_path: List[str],
        add_bq_tag: Optional[str] = None,
        remove_bq_tag: Optional[str] = None,
        field_description: Optional[str] = None,
    ) -> List[SchemaField]:
        updated_schema = []
        for field in schema:
            if field.name == field_path[0]:
                if len(field_path) == 1:
                    # Update the field with the new tag
                    new_tags: Optional[PolicyTagList] = (
                        field.policy_tags if field.policy_tags else PolicyTagList()
                    )
                    if add_bq_tag:
                        if not new_tags or add_bq_tag not in new_tags.names:
                            # BigQuery Only supports 1 policy tag on a schema
                            # Otherwise it fails with `Too many policy tags on this field schema (2). The maximum number of policy tags is 1.` error
                            # tags = list(new_tags.names)
                            # tags.append(add_bq_tag)
                            tags = [add_bq_tag]
                            new_tags = PolicyTagList(names=tags)

                    if remove_bq_tag:
                        if new_tags:
                            new_tags = PolicyTagList()
                            # We only remove the tag if it exists and don't check anything
                            # as bigquery currently only supports 1 policy tag on a schema.
                            # if remove_bq_tag in new_tags.names:
                            #    tags = list(new_tags.names)
                            #    tags.remove(remove_bq_tag)
                            #    if tags:
                            #        new_tags = PolicyTagList(names=tags)
                            #    else:
                            #        new_tags = None

                    new_field = SchemaField(
                        name=field.name,
                        field_type=field.field_type,
                        mode=field.mode,
                        description=(
                            field.description
                            if not field_description
                            else field_description
                        ),
                        policy_tags=new_tags,
                        precision=field.precision,
                        range_element_type=field.range_element_type,
                        scale=field.scale,
                        default_value_expression=field.default_value_expression,
                        max_length=field.max_length,
                        fields=field.fields,
                    )
                    updated_schema.append(new_field)
                else:
                    # Recursively update the nested field
                    updated_subfields = self.update_schema_field(
                        schema=field.fields,
                        field_path=field_path[1:],
                        add_bq_tag=add_bq_tag,
                        remove_bq_tag=remove_bq_tag,
                        field_description=field_description,
                    )
                    new_field = SchemaField(
                        name=field.name,
                        field_type=field.field_type,
                        mode=field.mode,
                        description=field.description,
                        policy_tags=field.policy_tags,
                        precision=field.precision,
                        range_element_type=field.range_element_type,
                        scale=field.scale,
                        default_value_expression=field.default_value_expression,
                        max_length=field.max_length,
                        fields=updated_subfields,
                    )
                    updated_schema.append(new_field)
            else:
                updated_schema.append(field)
        return updated_schema

    def apply_description(self, entity_urn: str, docs: str) -> None:
        if not is_bigquery_urn(entity_urn):
            return
        parsed_entity_urn = Urn.create_from_string(entity_urn)
        if isinstance(parsed_entity_urn, DatasetUrn):
            dataset_urn = parsed_entity_urn
        elif isinstance(parsed_entity_urn, SchemaFieldUrn):
            dataset_urn = DatasetUrn.create_from_string(parsed_entity_urn.parent)
        else:
            raise ValueError(
                f"Invalid entity urn {entity_urn}, can only handle Dataset and SchemaField urns."
            )
        table = self.bq_client.get_table(dataset_urn.name)

        if isinstance(parsed_entity_urn, DatasetUrn):
            table.description = docs
            self.bq_client.update_table(table=table, fields=["description"])
            logger.info(f"Applied doc {docs} to table {table.table_id}")
        elif isinstance(parsed_entity_urn, SchemaFieldUrn):
            schema_field_urn = parsed_entity_urn
            simplified_field_path = get_simple_field_path_from_v2_field_path(
                schema_field_urn.field_path
            )
            schema = self.update_schema_field(
                schema=table.schema,
                field_path=simplified_field_path.split("."),
                field_description=docs,
            )
            table.schema = schema
            self.bq_client.update_table(table=table, fields=["schema"])
            logger.info(f"Applied doc {docs} to field {schema_field_urn}")

    def apply_tag_or_term(self, entity_urn: str, tag_or_term_urn: str) -> None:
        if not is_bigquery_urn(entity_urn):
            return

        parsed_entity_urn = Urn.create_from_string(entity_urn)
        if isinstance(parsed_entity_urn, DatasetUrn):
            dataset_urn = parsed_entity_urn
        elif isinstance(parsed_entity_urn, SchemaFieldUrn):
            dataset_urn = DatasetUrn.create_from_string(parsed_entity_urn.parent)
        else:
            raise ValueError(
                f"Invalid entity urn {entity_urn}, can only handle Dataset and SchemaField urns."
            )

        table = self.bq_client.get_table(dataset_urn.name)
        if isinstance(parsed_entity_urn, DatasetUrn):
            logger.info(f"Applying tag {tag_or_term_urn} to entity {entity_urn}")

            tag_urn = Urn.from_string(tag_or_term_urn)
            if not isinstance(tag_urn, TagUrn):
                raise ValueError(
                    f"Invalid tag urn {tag_or_term_urn}, can only handle Tag urns."
                )
            bigquery_label = self.convert_tag_to_bigquery_label(tag_urn)

            logger.info(
                f"Applying tag {bigquery_label.key}  :  {bigquery_label.value} to table {table.table_id} which has existing label: {table.labels}"
            )
            table.labels.update({bigquery_label.key: bigquery_label.value})
            label = BigQueryLabel(key=bigquery_label.key, value=bigquery_label.value)
            try:
                platform_resource = self.bigquery_platform_resource_helper.generate_label_platform_resource(
                    label, tag_urn
                )
                logger.info(f"Created platform resource {platform_resource}")
                try:
                    self.bq_client.update_table(table=table, fields=["labels"])
                except google_exceptions.PreconditionFailed as e:
                    # https://www.googlecloudcommunity.com/gc/Data-Analytics/quot-lt-table-name-gt-did-not-meet-condition-IF-MATCH-quot-error/m-p/775859
                    logger.warning(
                        f"Api call threw exception {e} but this should not affect operation. This usually happens if somebody changes the table while we are updating it."
                    )
                platform_resource.to_datahub(self.graph.graph)
                logger.info(f"Applied tag {tag_or_term_urn} to table {table.table_id}")
            except ValueError as e:
                logger.error(
                    f"Error creating platform resource for label {label} and tag_urn {tag_urn}: {e} due to conflict. Ignoring platform resource creation."
                )
                raise e
        elif isinstance(parsed_entity_urn, SchemaFieldUrn):
            if not tag_or_term_urn.startswith("urn:li:glossaryTerm:"):
                return
            glossary_urn = GlossaryTermUrn.from_string(tag_or_term_urn)
            glossary_term, parents = self.get_glossary_tags_from_urn(glossary_urn)
            bq_tag = self._create_tag(glossary_term, parents)
            simplified_field_path = get_simple_field_path_from_v2_field_path(
                parsed_entity_urn.field_path
            )
            schema = self.update_schema_field(
                schema=table.schema,
                field_path=simplified_field_path.split("."),
                add_bq_tag=bq_tag.name,
            )

            table.schema = schema
            self.bq_client.update_table(table=table, fields=["schema"])
            logger.info(
                f"Applied glossary term {glossary_urn} from field {parsed_entity_urn}"
            )

    def remove_tag_or_term(self, entity_urn: str, tag_urn: str) -> None:
        if not is_bigquery_urn(entity_urn):
            return

        parsed_entity_urn = Urn.create_from_string(entity_urn)
        if isinstance(parsed_entity_urn, DatasetUrn):
            if not tag_urn.startswith("urn:li:tag:"):
                return

            tag_urn_typed = TagUrn.from_string(tag_urn)
            bq_label = self.convert_tag_to_bigquery_label(tag_urn_typed)

            dataset_urn = parsed_entity_urn
            table = self.bq_client.get_table(dataset_urn.name)
            logger.info(
                f"Removing tag {bq_label.key} from table {table.table_id} and tags: {table.labels}"
            )
            if bq_label.key in table.labels.keys():
                table.labels[bq_label.key] = None
            else:
                logger.info(
                    f"Tag {bq_label.key} not found on table {table} labels {table.labels}. Skip removing it"
                )
                return
            logger.info(
                f"Updated tags will be for table {table.table_id}: tags: {table.labels}"
            )
            self.bq_client.update_table(table=table, fields=["labels"])
            logger.info(f"Removed tag {bq_label.key} from table {table.table_id}")

        elif isinstance(parsed_entity_urn, SchemaFieldUrn):
            if not tag_urn.startswith("urn:li:glossaryTerm:"):
                return

            # Bigquery Currently only supports 1 policy tag on a schema.
            # So we don't need to lookup the exact glossary urn to remove.

            # glossary_urn = GlossaryTermUrn.from_string(tag_urn)
            # glossary_term, parents = BigqueryTagHelper.get_glossary_tags_from_urn(
            #    glossary_urn, graph
            # )
            # bq_tag = self._create_tag(glossary_term, parents)

            logger.info(f"Removing tag {tag_urn} from field {parsed_entity_urn}")
            dataset_urn = DatasetUrn.create_from_string(parsed_entity_urn.parent)
            table = self.bq_client.get_table(dataset_urn.name)
            simplified_field_path = get_simple_field_path_from_v2_field_path(
                parsed_entity_urn.field_path
            )

            schema = self.update_schema_field(
                table.schema, simplified_field_path.split("."), remove_bq_tag=tag_urn
            )
            table.schema = schema
            self.bq_client.update_table(table=table, fields=["schema"])
            logger.info(f"Removed tag {tag_urn} from field {parsed_entity_urn}")
        else:
            raise ValueError(
                f"Invalid entity urn {entity_urn}, can only handle Dataset and SchemaField urns."
            )

    def _list_taxonomies(self) -> List[Taxonomy]:
        location: Optional[str] = None
        if self.bq_project and self.bq_location:
            location = PolicyTagManagerClient.common_location_path(
                self.bq_project, self.bq_location
            )

        taxonomies = self.ptm_client.list_taxonomies(parent=location)
        return list(taxonomies)

    def _create_taxonomy(self, taxonomy_name: str, description: str) -> Taxonomy:
        location: Optional[str] = None
        if self.bq_project and self.bq_location:
            location = PolicyTagManagerClient.common_location_path(
                self.bq_project, self.bq_location
            )

        taxonomy = Taxonomy()
        taxonomy.display_name = taxonomy_name
        taxonomy.description = description
        if not self.taxonomies:
            self.taxonomies = self._list_taxonomies()

        for t in self.taxonomies:
            if t.display_name == taxonomy_name:
                if t.description != description:
                    t.description = description
                    self.ptm_client.update_taxonomy(taxonomy=t)
                    logger.debug(f"Taxonomy {taxonomy} updated")
                    return t
                else:
                    logger.debug(f"Taxonomy {taxonomy.display_name} already exists")
                    return t

        created_taxonomy = self.ptm_client.create_taxonomy(
            parent=location, taxonomy=taxonomy
        )
        self.taxonomies.append(created_taxonomy)

        logger.info(f"Taxonomy created: {created_taxonomy.name}")
        return created_taxonomy

    @cachetools.cached(cachetools.TTLCache(ttl=300, maxsize=500))
    def list_policy_tags(self, parent: str) -> List[PolicyTag]:
        return list(self.ptm_client.list_policy_tags(parent=parent))

    def glossary_entity_to_policy_tag_id(
        self, glossary_entity: Union[DataHubGlossaryTerm, DataHubGlossaryNode]
    ) -> str:
        return BigqueryTagHelper.str_to_bq_value(glossary_entity.urn)

    def _create_tag(
        self,
        glossaryTerm: DataHubGlossaryTerm,
        parents: List[DataHubGlossaryNode],
    ) -> PolicyTag:
        # Parents tags in the order of left to right
        created_tag: PolicyTag
        parent_tag: Optional[PolicyTag] = None
        parent_id: Optional[str] = None
        logger.info(f"Creating tag {glossaryTerm.term.name} with parents {parents}")
        for idx, parent in enumerate(parents):
            # parent_names = [self.datahub_label_to_bigquery_label(parent.name) for parent in parents[: idx]]
            # parent_names.append(parent.name)
            # parent_id = "_".join(parent_names)
            parent_id = self.glossary_entity_to_policy_tag_id(parent)
            if parent_id not in self.policy_tags:
                logging.info(f"Creating parent tag {parent_id}")
                pt = PolicyTag(
                    display_name=parent_id,
                    description=parent.node.definition,
                )

                if parent_tag:
                    pt.parent_policy_tag = parent_tag.name

                created_tag = self.ptm_client.create_policy_tag(
                    parent=self.taxonomy_path,
                    policy_tag=pt,
                )

                self.policy_tags[created_tag.display_name] = created_tag
            else:
                if parent.node.definition != self.policy_tags[parent_id].description:
                    self.policy_tags[parent_id].description = parent.node.definition
                    request = UpdatePolicyTagRequest(
                        {
                            "policy_tag": self.policy_tags[parent_id],
                            # "description":glossaryTerm.definition,
                            "update_mask": {"paths": ["description"]},
                        }
                    )

                    created_tag = self.ptm_client.update_policy_tag(request)

                    logger.debug(
                        f"Policy Tag {created_tag.display_name} updated description in taxonomy {self.taxonomy_path}"
                    )
                else:
                    created_tag = self.policy_tags[parent_id]

            parent_tag = created_tag

        # if parent_id:
        #    tag_id = "_".join([parent_id, glossaryTerm.name])
        # else:
        #    tag_id = glossaryTerm.name
        tag_id = self.glossary_entity_to_policy_tag_id(glossaryTerm)

        if tag_id in self.policy_tags:
            if glossaryTerm.term.definition != self.policy_tags[tag_id].description:
                self.policy_tags[tag_id].description = glossaryTerm.term.definition
                request = UpdatePolicyTagRequest(
                    {
                        "policy_tag": self.policy_tags[tag_id],
                        "update_mask": {"paths": ["description"]},
                    }
                )
                created_tag = self.ptm_client.update_policy_tag(request)
                platform_resource = self.bigquery_platform_resource_helper.generate_policy_tag_platform_resource(
                    created_tag, GlossaryTermUrn.from_string(glossaryTerm.urn)
                )
                platform_resource.to_datahub(self.graph.graph)
                logger.info(f"Platform Resource Modified: {platform_resource}")
                logger.info(
                    f"Policy Tag {created_tag.name} updated in taxonomy {self.taxonomy_path}"
                )
            else:
                logger.debug(f"Policy Tag {glossaryTerm.term.name} already exists")

                platform_resource = self.bigquery_platform_resource_helper.generate_policy_tag_platform_resource(
                    self.policy_tags[tag_id],
                    GlossaryTermUrn.from_string(glossaryTerm.urn),
                )
                platform_resource.to_datahub(self.graph.graph)
                logger.info(f"Platform Resource: {platform_resource}")
                return self.policy_tags[tag_id]
        try:

            logger.info(f"Creating tag {tag_id}")
            created_tag = self.ptm_client.create_policy_tag(
                parent=self.taxonomy_path,
                policy_tag=PolicyTag(
                    display_name=tag_id,
                    description=glossaryTerm.term.definition,
                    parent_policy_tag=parent_tag.name if parent_tag else None,
                ),
            )

            platform_resource = self.bigquery_platform_resource_helper.generate_policy_tag_platform_resource(
                created_tag, GlossaryTermUrn.from_string(glossaryTerm.urn)
            )
            logger.info(f"Platform Resource Created: {platform_resource}")
        except ValueError as e:
            logger.error(
                f"Error creating platform resource for glossary term {glossaryTerm.urn} and policy tag {tag_id}: {e} due to conflict. Ignoring platform resource creation."
            )
            raise e

        self.policy_tags[tag_id] = created_tag

        logger.info(
            f"Policy Tag {created_tag.name} created in taxonomy {self.taxonomy_path}"
        )
        return created_tag

    def _cleanup_old_errors(self) -> None:
        one_hour_ago = datetime.now() - timedelta(hours=1)
        while self.error_timestamps and self.error_timestamps[0] < one_hour_ago:
            self.error_timestamps.popleft()

    def _log_error(self) -> None:
        self.error_timestamps.append(datetime.now())
        self._cleanup_old_errors()

    def _too_many_errors(self) -> bool:
        self._cleanup_old_errors()
        logger.info(len(self.error_timestamps))
        return len(self.error_timestamps) >= self.error_threshold

    def close(self) -> None:
        self.bq_client.close()
        logger.info("BigqueryTagHelper closed.")
        if self.config._credentials_path is not None:
            logger.debug(
                f"Deleting temporary credential file at {self.config._credentials_path}"
            )
            os.unlink(self.config._credentials_path)
