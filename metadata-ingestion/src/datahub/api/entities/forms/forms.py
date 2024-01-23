import logging
import uuid
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union

import yaml
from pydantic import validator
from ruamel.yaml import YAML
from typing_extensions import Literal

from datahub.api.entities.forms.forms_graphql_constants import (
    CREATE_DYNAMIC_FORM_ASSIGNMENT,
    FIELD_FILTER_TEMPLATE,
    UPLOAD_ENTITIES_FOR_FORMS,
)
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_group_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import (
    FormInfoClass,
    FormPromptClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StructuredPropertyParamsClass,
)
from datahub.utilities.urns.urn import Urn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PromptType(Enum):
    STRUCTURED_PROPERTY = "STRUCTURED_PROPERTY"
    FIELDS_STRUCTURED_PROPERTY = "FIELDS_STRUCTURED_PROPERTY"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class Prompt(ConfigModel):
    id: Optional[str]
    title: str
    description: Optional[str]
    type: str
    structured_property_id: Optional[str]
    structured_property_urn: Optional[str]
    required: Optional[bool]

    @validator("structured_property_urn", pre=True, always=True)
    def structured_property_urn_must_be_present(cls, v, values):
        if not v and values.get("structured_property_id"):
            return Urn.make_structured_property_urn(values["structured_property_id"])
        return v


class FormType(Enum):
    VERIFICATION = "VERIFICATION"
    DOCUMENTATION = "COMPLETION"

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class Filters(ConfigModel):
    types: Optional[List[str]]
    platforms: Optional[List[str]]
    domains: Optional[List[str]]
    containers: Optional[List[str]]


class Entities(ConfigModel):
    urns: Optional[List[str]]
    filters: Optional[Filters]


class Forms(ConfigModel):
    id: Optional[str]
    urn: Optional[str]
    name: str
    description: Optional[str]
    prompts: List[Prompt] = []
    type: Optional[str]
    version: Optional[Literal[1]]
    entities: Optional[Entities]
    owners: Optional[List[str]]  # can be user IDs or urns
    group_owners: Optional[List[str]]  # can be group IDs or urns

    @validator("urn", pre=True, always=True)
    def urn_must_be_present(cls, v, values):
        if not v:
            assert values.get("id") is not None, "Form id must be present if urn is not"
            return f"urn:li:form:{values['id']}"
        return v

    @staticmethod
    def create(file: str) -> None:
        emitter: DataHubGraph

        with get_default_graph() as emitter:
            with open(file, "r") as fp:
                forms: List[dict] = yaml.safe_load(fp)
                for form_raw in forms:
                    form = Forms.parse_obj(form_raw)

                    try:
                        if not FormType.has_value(form.type):
                            logger.error(
                                f"Form type {form.type} does not exist. Please try again with a valid type."
                            )

                        mcp = MetadataChangeProposalWrapper(
                            entityUrn=form.urn,
                            aspect=FormInfoClass(
                                name=form.name,
                                description=form.description,
                                prompts=form.validate_prompts(emitter),
                                type=form.type,
                            ),
                        )
                        emitter.emit_mcp(mcp)

                        logger.info(f"Created form {form.urn}")

                        if form.owners or form.group_owners:
                            form.add_owners(emitter)

                        if form.entities:
                            if form.entities.urns:
                                # Associate specific entities with a form
                                form.upload_entities_for_form(emitter)

                            if form.entities.filters:
                                # Associate groups of entities with a form based on filters
                                form.create_form_filters(emitter)

                    except Exception as e:
                        logger.error(e)
                        return

    def validate_prompts(self, emitter: DataHubGraph) -> List[FormPromptClass]:
        prompts = []
        if self.prompts:
            for prompt in self.prompts:
                if not prompt.id:
                    prompt.id = str(uuid.uuid4())
                    logger.warning(
                        f"Prompt id not provided. Setting prompt id to {prompt.id}"
                    )
                if prompt.structured_property_urn:
                    structured_property_urn = prompt.structured_property_urn
                    if emitter.exists(structured_property_urn):
                        prompt.structured_property_urn = structured_property_urn
                    else:
                        raise Exception(
                            f"Structured property {structured_property_urn} does not exist. Unable to create form."
                        )
                elif (
                    prompt.type
                    in (
                        PromptType.STRUCTURED_PROPERTY.value,
                        PromptType.FIELDS_STRUCTURED_PROPERTY.value,
                    )
                    and not prompt.structured_property_urn
                ):
                    raise Exception(
                        f"Prompt type is {prompt.type} but no structured properties exist. Unable to create form."
                    )

                prompts.append(
                    FormPromptClass(
                        id=prompt.id,
                        title=prompt.title,
                        description=prompt.description,
                        type=prompt.type,
                        structuredPropertyParams=StructuredPropertyParamsClass(
                            urn=prompt.structured_property_urn
                        )
                        if prompt.structured_property_urn
                        else None,
                        required=prompt.required,
                    )
                )
        else:
            logger.warning(f"No prompts exist on form {self.urn}. Is that intended?")

        return prompts

    def upload_entities_for_form(self, emitter: DataHubGraph) -> Union[None, Exception]:
        if self.entities and self.entities.urns:
            formatted_entity_urns = ", ".join(
                ['"{}"'.format(value) for value in self.entities.urns]
            )
            query = UPLOAD_ENTITIES_FOR_FORMS.format(
                form_urn=self.urn, entity_urns=formatted_entity_urns
            )
            result = emitter.execute_graphql(query=query)
            if not result:
                return Exception(f"Could not bulk upload entities for form {self.urn}.")

        return None

    def create_form_filters(self, emitter: DataHubGraph) -> Union[None, Exception]:
        filters_raw = []
        # Loop through each entity and assign a filter for it
        if self.entities and self.entities.filters:
            filters = self.entities.filters
            if filters.types:
                filters_raw.append(
                    Forms.format_form_filter("_entityType", filters.types)
                )
            if filters.platforms:
                urns = [
                    make_data_platform_urn(platform) for platform in filters.platforms
                ]
                filters_raw.append(Forms.format_form_filter("platform", urns))
            if filters.domains:
                urns = []
                for domain in filters.domains:
                    domain_urn = Forms.validate_domain_urn(domain)
                    if domain_urn:
                        urns.append(domain_urn)
                filters_raw.append(Forms.format_form_filter("domains", urns))
            if filters.containers:
                urns = []
                for container in filters.containers:
                    container_urn = Forms.validate_container_urn(container)
                    if container_urn:
                        urns.append(container_urn)
                filters_raw.append(Forms.format_form_filter("container", urns))

        filters_str = ", ".join(item for item in filters_raw)
        result = emitter.execute_graphql(
            query=CREATE_DYNAMIC_FORM_ASSIGNMENT.format(
                form_urn=self.urn, filters=filters_str
            )
        )
        if not result:
            return Exception(
                f"Could not bulk upload urns or filters for form {self.urn}."
            )

        return None

    def add_owners(self, emitter: DataHubGraph) -> Union[None, Exception]:
        owner_urns: List[str] = []
        if self.owners:
            owner_urns += Forms.format_owners(self.owners)
        if self.group_owners:
            owner_urns += Forms.format_group_owners(self.group_owners)

        ownership = OwnershipClass(
            owners=[
                OwnerClass(owner=urn, type=OwnershipTypeClass.TECHNICAL_OWNER)
                for urn in (owner_urns or [])
            ],
        )

        try:
            mcp = MetadataChangeProposalWrapper(entityUrn=self.urn, aspect=ownership)
            emitter.emit_mcp(mcp)
        except Exception as e:
            logger.error(e)

        return None

    @staticmethod
    def format_form_filter(field: str, urns: List[str]) -> str:
        formatted_urns = ", ".join(['"{}"'.format(urn) for urn in urns])
        return FIELD_FILTER_TEMPLATE.format(field=field, values=formatted_urns)

    @staticmethod
    def validate_domain_urn(domain: str) -> Union[str, None]:
        if domain.startswith("urn:li:domain:"):
            return domain

        logger.warning(f"{domain} is not an urn. Unable to create domain filter.")
        return None

    @staticmethod
    def validate_container_urn(container: str) -> Union[str, None]:
        if container.startswith("urn:li:container:"):
            return container

        logger.warning(f"{container} is not an urn. Unable to create container filter.")
        return None

    @staticmethod
    def from_datahub(graph: DataHubGraph, urn: str) -> "Forms":
        form: Optional[FormInfoClass] = graph.get_aspect(urn, FormInfoClass)
        assert form is not None
        prompts = []
        for prompt_raw in form.prompts:
            prompts.append(
                Prompt(
                    id=prompt_raw.id,
                    title=prompt_raw.title,
                    description=prompt_raw.description,
                    type=prompt_raw.type,
                    structured_property_urn=prompt_raw.structuredPropertyParams.urn
                    if prompt_raw.structuredPropertyParams
                    else None,
                )
            )
        return Forms(
            urn=urn,
            name=form.name,
            description=form.description,
            prompts=prompts,
            type=form.type,
        )

    @staticmethod
    def format_owners(owners: List[str]) -> List[str]:
        formatted_owners: List[str] = []

        for owner in owners:
            if owner.startswith("urn:li:"):
                formatted_owners.append(owner)
            else:
                formatted_owners.append(make_user_urn(owner))

        return formatted_owners

    @staticmethod
    def format_group_owners(owners: List[str]) -> List[str]:
        formatted_owners: List[str] = []

        for owner in owners:
            if owner.startswith("urn:li:"):
                formatted_owners.append(owner)
            else:
                formatted_owners.append(make_group_urn(owner))

        return formatted_owners

    def to_yaml(
        self,
        file: Path,
    ) -> None:
        with open(file, "w") as fp:
            yaml = YAML(typ="rt")  # default, if not specfied, is 'rt' (round-trip)
            yaml.indent(mapping=2, sequence=4, offset=2)
            yaml.default_flow_style = False
            yaml.dump(self.dict(), fp)
