from typing import Dict, Iterable

from pydantic import Field

from datahub.configuration.common import TransformerSemanticsConfigModel
from datahub.emitter.mce_builder import make_ownership_type_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.transformer.base_transformer import update_work_unit_id
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.utilities.urns.tag_urn import TagUrn
from datahub.utilities.urns.urn import Urn

DEFAULT_TAG_CHAR_MAPPING = {
    "_": ".",
    "-": "@",
    "__": "_",
    "--": "-",
    "_-": "#",
    "-_": " ",
}


class CaptureOwnersFromTagsConfig(TransformerSemanticsConfigModel):
    tag_character_mapping: Dict[str, str] = Field(
        default=DEFAULT_TAG_CHAR_MAPPING,
        description="A mapping of tag character to datahub owner character."
        "Provided mapping will override default mapping.",
    )

    owner_key_pattern: str = Field(
        default="_owner_email",
        description="A pattern which defines what identifies an owner label.",
    )


class CaptureOwnersFromTagsTransformer(Transformer):
    """Transformer that captures owners from tag aspect"""

    ctx: PipelineContext
    config: CaptureOwnersFromTagsConfig

    def __init__(self, config: CaptureOwnersFromTagsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "CaptureOwnersFromTagsTransformer":
        config = CaptureOwnersFromTagsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _convert_tag_value(self, value: str) -> str:
        for key in sorted(
            self.config.tag_character_mapping.keys(),
            key=len,
            reverse=True,
        ):
            value = value.replace(key, self.config.tag_character_mapping[key])
        return value

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for envelope in record_envelopes:
            if isinstance(envelope.record, MetadataChangeProposalWrapper):
                assert envelope.record.entityUrn
                if (
                    envelope.record.aspectName == "globalTags"
                    and envelope.record.aspect
                    and isinstance(envelope.record.aspect, GlobalTagsClass)
                ):
                    owners_to_add: Dict[str, str] = {}

                    for tag_associate in envelope.record.aspect.tags:
                        tag_urn = Urn.from_string(tag_associate.tag)
                        assert isinstance(tag_urn, TagUrn)
                        tag = tag_urn.name.split(":")
                        if len(tag) != 2:  # check if tag is key value pair
                            continue
                        if self.config.owner_key_pattern in tag[0]:
                            owners_to_add[self._convert_tag_value(tag[1])] = tag[
                                0
                            ].split(self.config.owner_key_pattern)[0]

                    if owners_to_add:
                        mcp = MetadataChangeProposalWrapper(
                            entityUrn=envelope.record.entityUrn,
                            aspect=OwnershipClass(
                                owners=[
                                    OwnerClass(
                                        owner=make_user_urn(owner),
                                        type=OwnershipTypeClass.CUSTOM,
                                        typeUrn=make_ownership_type_urn(type),
                                    )
                                    for owner, type in owners_to_add.items()
                                ]
                            ),
                        )

                        if mcp.aspectName and mcp.entityUrn:  # to silent the lint error
                            record_metadata = update_work_unit_id(
                                envelope=envelope,
                                aspect_name=mcp.aspectName,
                                urn=mcp.entityUrn,
                            )
                            yield RecordEnvelope(
                                record=mcp,
                                metadata=record_metadata,
                            )

            yield envelope
