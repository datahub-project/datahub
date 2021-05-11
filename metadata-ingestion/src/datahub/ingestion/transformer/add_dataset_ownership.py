import logging
from typing import Callable, Iterable, List, NoReturn, Union

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.transform import Transformer
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
)

logger = logging.getLogger(__name__)

TABLE_TAGS = {
    "urn:li:dataset:(urn:li:dataPlatform:postgresql,myapp.information_schema.sql_parts,PROD)": {
        "comments": {
            "tags": ["a_tag", "another_tag"],
            "description": "Description for comments",
        }
    }
}


class AddDatasetOwnershipConfig(ConfigModel):
    # Workaround for https://github.com/python/mypy/issues/708.
    # Suggested by https://stackoverflow.com/a/64528725/5004662.
    get_owners_to_add: Union[
        Callable[[DatasetSnapshotClass], List[OwnerClass]], NoReturn
    ]
    default_actor: str = builder.make_user_urn("etl")


class AddDatasetOwnership(Transformer):
    ctx: PipelineContext
    config: AddDatasetOwnershipConfig

    def __init__(self, config: AddDatasetOwnershipConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetOwnership":
        config = AddDatasetOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        for envelope in record_envelopes:
            if isinstance(envelope.record, MetadataChangeEventClass):
                envelope.record = self.transform_one(envelope.record)
            yield envelope

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
            return mce

        owners_to_add = self.config.get_owners_to_add(mce.proposedSnapshot)
        if owners_to_add:
            ownership = builder.get_or_add_aspect(
                mce,
                OwnershipClass(
                    owners=[],
                    lastModified=AuditStampClass(
                        time=builder.get_sys_time(),
                        actor=self.config.default_actor,
                    ),
                ),
            )
            ownership.owners.extend(owners_to_add)

        return mce
