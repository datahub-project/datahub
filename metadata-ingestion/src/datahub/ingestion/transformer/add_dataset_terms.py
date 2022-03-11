from typing import Callable, List, Union

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel, KeyValuePattern
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTermsTransformer
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetSnapshotClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MetadataChangeEventClass,
)


class AddDatasetTermsConfig(ConfigModel):
    # Workaround for https://github.com/python/mypy/issues/708.
    # Suggested by https://stackoverflow.com/a/64528725/5004662.
    get_terms_to_add: Union[
        Callable[[DatasetSnapshotClass], List[GlossaryTermAssociationClass]],
        Callable[[DatasetSnapshotClass], List[GlossaryTermAssociationClass]],
    ]

    _resolve_term_fn = pydantic_resolve_key("get_terms_to_add")


class AddDatasetTerms(DatasetTermsTransformer):
    """Transformer that adds glossary terms to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetTermsConfig

    def __init__(self, config: AddDatasetTermsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetTerms":
        config = AddDatasetTermsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
            return mce
        terms_to_add = self.config.get_terms_to_add(mce.proposedSnapshot)
        if terms_to_add:
            terms = builder.get_or_add_aspect(
                mce,
                GlossaryTermsClass(
                    terms=[],
                    auditStamp=AuditStampClass(
                        time=builder.get_sys_time(), actor="urn:li:corpUser:restEmitter"
                    ),
                ),
            )
            terms.terms.extend(terms_to_add)

        return mce


class SimpleDatasetTermsConfig(ConfigModel):
    term_urns: List[str]


class SimpleAddDatasetTerms(AddDatasetTerms):
    """Transformer that adds a specified set of glossary terms to each dataset."""

    def __init__(self, config: SimpleDatasetTermsConfig, ctx: PipelineContext):
        terms = [GlossaryTermAssociationClass(urn=term) for term in config.term_urns]

        generic_config = AddDatasetTermsConfig(
            get_terms_to_add=lambda _: terms,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SimpleAddDatasetTerms":
        config = SimpleDatasetTermsConfig.parse_obj(config_dict)
        return cls(config, ctx)


class PatternDatasetTermsConfig(ConfigModel):
    term_pattern: KeyValuePattern = KeyValuePattern.all()


class PatternAddDatasetTerms(AddDatasetTerms):
    """Transformer that adds a specified set of glossary terms to each dataset."""

    def __init__(self, config: PatternDatasetTermsConfig, ctx: PipelineContext):
        term_pattern = config.term_pattern
        generic_config = AddDatasetTermsConfig(
            get_terms_to_add=lambda _: [
                GlossaryTermAssociationClass(urn=urn)
                for urn in term_pattern.value(_.urn)
            ],
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternAddDatasetTerms":
        config = PatternDatasetTermsConfig.parse_obj(config_dict)
        return cls(config, ctx)
