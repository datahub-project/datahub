# pylint: skip-file
# mypy: ignore-errors
# fmt: off
# isort: skip_file
# flake8: noqa
import json
import logging
from typing import Iterable, Optional, Union, cast

from datahub.configuration.kafka import KafkaProducerConnectionConfig
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig
from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import _make_generic_aspect, _try_from_generic_aspect
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    EditableSchemaMetadataClass,
    GenericAspectClass,
    FormPromptTypeClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MetadataChangeLogClass,
    MetadataChangeProposalClass,
    TagAssociationClass,
)
from datahub.specific.dataset import DatasetPatchBuilder
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import METADATA_CHANGE_LOG_EVENT_V1_TYPE
from datahub_actions.pipeline.pipeline_context import PipelineContext
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ForwardingActionConfig(BaseModel):
    kafka_server: Optional[str] = None
    schema_registry_url: Optional[str] = None
    ssl_ca_location: Optional[str] = None
    ssl_cert_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[str] = None
    group_id: Optional[str] = None
    schema_registry_ca_location: Optional[str] = None
    schema_registry_cert_location: Optional[str] = None
    schema_registry_key_location: Optional[str] = None
    mcp_topic: Optional[str] = None
    forms_enabled: bool = False


def create_schema_mcp(
    old_obj: GenericAspectClass,
    new_obj: GenericAspectClass,
    orig_event: MetadataChangeLogClass,
) -> Iterable[MetadataChangeProposalClass]:
    new_schema_obj: EditableSchemaMetadataClass = cast(
        EditableSchemaMetadataClass,
        _try_from_generic_aspect("editableSchemaMetadata", new_obj)[1],
    )
    old_schema_obj: EditableSchemaMetadataClass = cast(
        EditableSchemaMetadataClass,
        _try_from_generic_aspect("editableSchemaMetadata", old_obj)[1],
    )

    new_schema_infos = (
        new_schema_obj.editableSchemaFieldInfo
        if (new_schema_obj and new_schema_obj.editableSchemaFieldInfo)
        else []
    )
    old_schema_infos = (
        old_schema_obj.editableSchemaFieldInfo
        if (old_schema_obj and old_schema_obj.editableSchemaFieldInfo)
        else []
    )
    new_fields_map = {
        field.fieldPath: {
            "tags": set(
                tag.tag
                for tag in (
                    field.globalTags.tags
                    if field.globalTags and field.globalTags.tags
                    else []
                )
            ),
            "terms": set(
                term.urn
                for term in (
                    field.glossaryTerms.terms
                    if field.glossaryTerms and field.glossaryTerms.terms
                    else []
                )
            ),
        }
        for field in new_schema_infos or []
    }
    old_fields_map = {
        field.fieldPath: {
            "tags": set(
                tag.tag
                for tag in (
                    field.globalTags.tags
                    if field.globalTags and field.globalTags.tags
                    else []
                )
            ),
            "terms": set(
                term.urn
                for term in (
                    field.glossaryTerms.terms
                    if field.glossaryTerms and field.glossaryTerms.terms
                    else []
                )
            ),
        }
        for field in old_schema_infos or []
    }
    items_to_add: dict = {}
    items_to_remove: dict = {}
    # detect adds by not in old obj -> in new obj
    for new_field in new_fields_map or []:
        add_tags: set[str] = set()
        add_terms: set[str] = set()
        if new_field not in old_fields_map:
            add_tags.update(new_fields_map[new_field]["tags"] or [])
            add_terms.update(new_fields_map[new_field]["terms"] or [])
        else:
            for new_tag in new_fields_map[new_field]["tags"]:
                if new_tag not in old_fields_map[new_field]["tags"]:
                    add_tags.add(new_tag)
            for new_term in new_fields_map[new_field]["terms"]:
                if new_term not in old_fields_map[new_field]["terms"]:
                    add_terms.add(new_term)
        items_to_add[new_field] = {"tags": add_tags, "terms": add_terms}
    # detect removes by not in new obj -> in old obj
    for old_field in old_fields_map or []:
        remove_tags: set[str] = set()
        remove_terms: set[str] = set()
        if old_field not in new_fields_map:
            remove_tags.update(old_fields_map[old_field]["tags"] or [])
            remove_terms.update(old_fields_map[old_field]["terms"] or [])
        else:
            for old_tag in old_fields_map[old_field]["tags"]:
                if old_tag not in new_fields_map[old_field]["tags"]:
                    remove_tags.add(old_tag)
            for old_term in old_fields_map[old_field]["terms"]:
                if old_term not in new_fields_map[old_field]["terms"]:
                    remove_terms.add(old_term)
        items_to_remove[old_field] = {"tags": remove_tags, "terms": remove_terms}
    dataset = DatasetPatchBuilder(urn=orig_event.get("entityUrn"))
    for field in items_to_add:
        field_builder = dataset.for_field(field_path=field)
        for term in items_to_add[field]["terms"]:
            field_builder.add_term(
                GlossaryTermAssociationClass(
                    term,
                )
            )
        for tag in items_to_add[field]["tags"]:
            field_builder.add_tag(TagAssociationClass(tag=make_tag_urn(tag)))
    for field in items_to_remove:
        field_builder = dataset.for_field(field_path=field)
        for term in items_to_remove[field]["terms"]:
            field_builder.remove_term(
                term,
            )
        for tag in items_to_remove[field]["tags"]:
            field_builder.remove_tag(make_tag_urn(tag))
    return dataset.build()


def create_terms_mcp(
    old_obj: GenericAspectClass,
    new_obj: GenericAspectClass,
    orig_event: MetadataChangeLogClass,
) -> Iterable[MetadataChangeProposalClass]:
    new_glossary_term_obj: GlossaryTermsClass = cast(
        GlossaryTermsClass, _try_from_generic_aspect("glossaryTerms", new_obj)[1]
    )
    old_glossary_term_obj: GlossaryTermsClass = cast(
        GlossaryTermsClass, _try_from_generic_aspect("glossaryTerms", old_obj)[1]
    )

    new_glossary_terms_assc = (
        new_glossary_term_obj.terms
        if new_glossary_term_obj and new_glossary_term_obj.terms
        else []
    )
    old_glossary_terms_assc = (
        old_glossary_term_obj.terms
        if old_glossary_term_obj and old_glossary_term_obj.terms
        else []
    )

    new_glossary_terms = list(term.urn for term in new_glossary_terms_assc or [])
    old_glossary_terms = list(term.urn for term in old_glossary_terms_assc or [])

    terms_to_add: set[str] = set()
    terms_to_remove: set[str] = set()
    # detect adds by not in old obj -> in new obj
    for new_term in new_glossary_terms or []:
        if new_term not in old_glossary_terms:
            terms_to_add.add(new_term)
    # detect removes by not in new obj -> in old obj
    for old_term in old_glossary_terms or []:
        if old_term not in new_glossary_terms:
            terms_to_remove.add(old_term)
    # we use dataset patch builder, but it does a guess type on the entity type regardless of patch builder type
    patch_builder = DatasetPatchBuilder(urn=orig_event.get("entityUrn"))
    for term in terms_to_add:
        patch_builder.add_term(
            GlossaryTermAssociationClass(
                urn=term,
            )
        )
    for term in terms_to_remove:
        patch_builder.remove_term(term)
    return patch_builder.build()


def create_tags_mcp(
    old_obj: GenericAspectClass,
    new_obj: GenericAspectClass,
    orig_event: MetadataChangeLogClass,
) -> Iterable[MetadataChangeProposalClass]:
    new_tags_obj: GlobalTagsClass = cast(
        GlobalTagsClass, _try_from_generic_aspect("globalTags", new_obj)[1]
    )
    old_tags_obj: GlobalTagsClass = cast(
        GlobalTagsClass, _try_from_generic_aspect("globalTags", old_obj)[1]
    )

    new_tags_assc = new_tags_obj.tags if new_tags_obj and new_tags_obj.tags else []
    old_tags_assc = old_tags_obj.tags if old_tags_obj and old_tags_obj.tags else []

    new_tags = list(tag.tag for tag in new_tags_assc or [])
    old_tags = list(tag.tag for tag in old_tags_assc or [])

    tags_to_add: set[str] = set()
    tags_to_remove: set[str] = set()
    # detect adds by not in old obj -> in new obj
    for new_tag in new_tags or []:
        if new_tag not in old_tags:
            tags_to_add.add(new_tag)
    # detect removes by not in new obj -> in old obj
    for old_tag in old_tags or []:
        if old_tag not in new_tags:
            tags_to_remove.add(old_tag)
    # we use dataset patch builder, but it does a guess type on the entity type regardless of patch builder type
    patch_builder = DatasetPatchBuilder(urn=orig_event.get("entityUrn"))
    for tag in tags_to_add:
        patch_builder.add_tag(TagAssociationClass(tag=make_tag_urn(tag)))
    for tag in tags_to_remove:
        patch_builder.remove_tag(tag)
    return patch_builder.build()


class ForwardingAction(Action):
    kafka_emitter: DatahubKafkaEmitter
    forms_enabled: bool

    SUPPORTED_PATCH_ASPECTS = {
        "globalTags",
        "glossaryTerms",
        "editableSchemaMetadata",
    }

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = ForwardingActionConfig.parse_obj(config_dict or {})
        return cls(action_config, ctx)

    def __init__(self, config: ForwardingActionConfig, ctx: PipelineContext) -> None:
        self.config = config
        assert isinstance(self.config.kafka_server, str)
        producer_config = {}
        schema_registry_config = {}
        if self.config.ssl_ca_location is not None:
            producer_config["ssl.ca.location"] = self.config.ssl_ca_location
        if self.config.ssl_cert_location is not None:
            producer_config["ssl.certificate.location"] = self.config.ssl_cert_location
        if self.config.ssl_key_location is not None:
            producer_config["ssl.key.location"] = self.config.ssl_key_location
        if self.config.ssl_key_password is not None:
            producer_config["ssl.key.password"] = self.config.ssl_key_password
        if self.config.group_id is not None:
            producer_config["group.id"] = self.config.group_id
        if producer_config:
            producer_config["security.protocol"] = "ssl"

        if self.config.schema_registry_ca_location is not None:
            schema_registry_config["ssl.ca.location"] = (
                self.config.schema_registry_ca_location
            )
        if self.config.schema_registry_cert_location is not None:
            schema_registry_config["ssl.certificate.location"] = (
                self.config.schema_registry_cert_location
            )
        if self.config.schema_registry_key_location is not None:
            schema_registry_config["ssl.key.location"] = (
                self.config.schema_registry_key_location
            )
        self.kafka_emitter = DatahubKafkaEmitter(
            config=KafkaEmitterConfig(
                connection=KafkaProducerConnectionConfig(
                    bootstrap=self.config.kafka_server,
                    schema_registry_url=self.config.schema_registry_url,
                    producer_config=producer_config,
                    schema_registry_config=schema_registry_config,
                ),
                topic_routes={
                    "MetadataChangeEvent": self.config.mcp_topic,
                    "MetadataChangeProposal": self.config.mcp_topic,
                },
            )
        )
        self.forms_enabled = config.forms_enabled

    def act(self, event: EventEnvelope) -> None:
        """
        This method listens for MetadataChangeLog events, casts it to MetadataChangeProposal,
        and emits it to another datahub instance
        """
        # MetadataChangeProposal only supports UPSERT type for now
        if event.event_type is METADATA_CHANGE_LOG_EVENT_V1_TYPE:
            orig_event = cast(MetadataChangeLogClass, event.event)
            logger.debug(f"received orig_event {orig_event}")
            if ((
                orig_event.systemMetadata
                and orig_event.systemMetadata.properties
                and orig_event.systemMetadata.properties.get("appSource")
                == "metadataTests"
            ) or orig_event.aspectName == "testResults"
              or orig_event.aspectName == "testInfo"
              or ((orig_event.aspectName == "formInfo"
                  or orig_event.aspectName == "dynamicFormAssignment")
                  and self.forms_enabled)):
                mcps = self.buildMcp(orig_event)
                if mcps is not None:
                    for mcp in mcps:
                        logger.info(f"{mcp}")
                        self.emit(mcp)

    def buildMcp(
        self, orig_event: MetadataChangeLogClass
    ) -> Union[Iterable[MetadataChangeProposalClass], None]:
        try:
            # Parse out non-OSS fields to avoid model leakage and errors
            # TODO: once confirmed upgraded to latest OSS version with no chance of rollback, this can be removed
            if orig_event.aspectName == "testResults":
                test_results = _try_from_generic_aspect(
                    "testResults", orig_event.aspect
                )[1]
                for failingObj in test_results.failing or []:
                    failingObj._inner_dict.pop("testDefinitionMd5", None)
                    failingObj._inner_dict.pop("lastComputed", None)

                for passingObj in test_results.passing or []:
                    passingObj._inner_dict.pop("testDefinitionMd5", None)
                    passingObj._inner_dict.pop("lastComputed", None)
                orig_event.aspect = _make_generic_aspect(test_results)

            if orig_event.aspectName == "testInfo":
                test_info = _try_from_generic_aspect(
                    "testInfo", orig_event.aspect
                )[1]
                test_info._inner_dict.pop("lastUpdatedTimestamp", None)
                test_info._inner_dict.pop("lastUpdated", None)
                test_info._inner_dict.pop("md5", None)
                test_info._inner_dict.pop("schedule", None)
                test_info._inner_dict.pop("status", None)
                test_info._inner_dict.pop("source", None)
                test_info._inner_dict.pop("created", None)
                test_info.definition._inner_dict.pop("md5", None)
                test_info.definition._inner_dict.pop("onQuery", None)
                orig_event.aspect = _make_generic_aspect(test_info)

            if orig_event.aspectName == "dynamicFormAssignment":
                dynamic_form_assignment = _try_from_generic_aspect(
                    "dynamicFormAssignment", orig_event.aspect
                )[1]
                dynamic_form_assignment._inner_dict.pop("json", None)
                orig_event.aspect = _make_generic_aspect(dynamic_form_assignment)

            if orig_event.aspectName == "formInfo":
                form_info = _try_from_generic_aspect(
                    "formInfo", orig_event.aspect
                )[1]
                for form_prompt in form_info.prompts or []:
                    if (not (form_prompt.type or "") in [FormPromptTypeClass.STRUCTURED_PROPERTY,
                                                         FormPromptTypeClass.FIELDS_STRUCTURED_PROPERTY]):
                        raise Exception(
                            f"Invalid form prompt type for urn: "
                            f"{orig_event.get('entityUrn')}"
                        )
                form_info._inner_dict.pop("lastModified", None)
                form_info._inner_dict.pop("created", None)
                form_info._inner_dict.pop("status", None)
                orig_event.aspect = _make_generic_aspect(form_info)

            mcp = []
            if orig_event.get("aspectName") in self.SUPPORTED_PATCH_ASPECTS:
                new_obj = orig_event.get("aspect")
                old_obj = orig_event.get("previousAspectValue")
                if orig_event.get("aspectName") == "editableSchemaMetadata":
                    mcp = create_schema_mcp(old_obj, new_obj, orig_event)
                elif orig_event.get("aspectName") == "globalTags":
                    mcp = create_tags_mcp(old_obj, new_obj, orig_event)
                elif orig_event.get("aspectName") == "glossaryTerms":
                    mcp = create_terms_mcp(old_obj, new_obj, orig_event)
            else:
                changeType = orig_event.changeType
                if orig_event.changeType == ChangeTypeClass.RESTATE or orig_event.changeType == "RESTATE":
                    changeType = ChangeTypeClass.UPSERT
                mcp = [
                    MetadataChangeProposalClass(
                        entityType=orig_event.get("entityType"),
                        changeType=changeType,
                        entityUrn=orig_event.get("entityUrn"),
                        entityKeyAspect=orig_event.get("entityKeyAspect"),
                        aspectName=orig_event.get("aspectName"),
                        aspect=orig_event.get("aspect"),
                    )
                ]
            return mcp
        except Exception as ex:
            logger.error(
                f"error when building mcp from mcl {json.dumps(orig_event.to_obj(), indent=4)}"
            )
            logger.error(f"exception: {ex}")
            return None

    def emit(self, mcp: MetadataChangeProposalClass) -> None:
        # Create an emitter to DataHub over REST
        try:
            logger.info(
                f"emitting the mcp: entityType {mcp.entityType}, changeType {mcp.changeType}, urn {mcp.entityUrn}, "
                f"aspect name {mcp.aspectName}"
            )
            self.kafka_emitter.emit(mcp)
            logger.info("successfully emit the mcp")
        except Exception as ex:
            logger.error(
                f"error when emitting mcp, {json.dumps(mcp.to_obj(), indent=4)}"
            )
            logger.error(f"exception: {ex}")

    def close(self) -> None:
        pass
