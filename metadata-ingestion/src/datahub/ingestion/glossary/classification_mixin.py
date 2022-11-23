import logging
from typing import List, Optional

from datahub_classify.helper_classes import ColumnInfo, Metadata
from pandas import DataFrame
from typing_extensions import Protocol

from datahub.configuration.common import ConfigurationError
from datahub.emitter.mce_builder import get_sys_time, make_term_urn, make_user_urn
from datahub.ingestion.glossary.classifier import ClassificationConfig, Classifier
from datahub.ingestion.glossary.classifier_registry import classifier_registry
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    GlossaryTermAssociation,
    GlossaryTerms,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata


class ClassificationSourceConfigProtocol(Protocol):
    classification: Optional[ClassificationConfig]


class ClassificationSourceProtocol(Protocol):
    @property
    def classifiers(self) -> List[Classifier]:
        ...

    @property
    def logger(self) -> logging.Logger:
        ...

    @property
    def config(self) -> ClassificationSourceConfigProtocol:
        ...

    def is_classification_enabled(self) -> bool:
        ...

    def is_classification_enabled_for_column(
        self, dataset_name: str, column_name: str
    ) -> bool:
        ...

    def is_classification_enabled_for_table(self, dataset_name: str) -> bool:
        ...

    def get_classifiers(self) -> List[Classifier]:
        ...


class ClassificationMixin:
    def is_classification_enabled(self: ClassificationSourceProtocol) -> bool:
        return (
            self.config.classification is not None
            and self.config.classification.enabled
            and len(self.config.classification.classifiers) > 0
        )

    def is_classification_enabled_for_table(
        self: ClassificationSourceProtocol, dataset_name: str
    ) -> bool:

        return (
            self.config.classification is not None
            and self.config.classification.enabled
            and len(self.config.classification.classifiers) > 0
            and self.config.classification.table_pattern.allowed(dataset_name)
        )

    def is_classification_enabled_for_column(
        self: ClassificationSourceProtocol, dataset_name: str, column_name: str
    ) -> bool:

        return (
            self.config.classification is not None
            and self.config.classification.enabled
            and len(self.config.classification.classifiers) > 0
            and self.config.classification.table_pattern.allowed(dataset_name)
            and self.config.classification.column_pattern.allowed(
                f"{dataset_name}.{column_name}"
            )
        )

    def get_classifiers(self: ClassificationSourceProtocol) -> List[Classifier]:

        assert self.config.classification
        classifiers = []

        for classifier in self.config.classification.classifiers:
            classifier_class = classifier_registry.get(classifier.type)
            if classifier_class is None:
                raise ConfigurationError(
                    f"Cannot find classifier class of type={self.config.classification.classifiers[0].type} "
                    " in the registry! Please check the type of the classifier in your config."
                )
            classifiers.append(
                classifier_class.create(
                    config_dict=classifier.config,  # type: ignore
                )
            )

        return classifiers

    def classify_schema_fields(
        self: ClassificationSourceProtocol,
        dataset_name: str,
        schema_metadata: SchemaMetadata,
        sample_data: DataFrame,
    ) -> None:

        assert self.config.classification
        column_infos: List[ColumnInfo] = []

        for field in schema_metadata.fields:
            if not self.is_classification_enabled_for_column(
                dataset_name, field.fieldPath
            ):
                self.logger.debug(
                    f"Skipping column {dataset_name}.{field.fieldPath} from classification"
                )
                continue
            column_infos.append(
                ColumnInfo(
                    metadata=Metadata(
                        {
                            "Name": field.fieldPath,
                            "Description": field.description,
                            "DataType": field.nativeDataType,
                            "Dataset_Name": dataset_name,
                        }
                    ),
                    values=sample_data[field.fieldPath].values
                    if field.fieldPath in sample_data.columns
                    else [],
                )
            )

        if not column_infos:
            self.logger.debug(
                f"No columns in {dataset_name} considered for classification"
            )
            return None

        field_terms = {}
        for classifier in self.classifiers:
            column_info_with_proposals = classifier.classify(column_infos)
            for col_info in column_info_with_proposals:
                if not col_info.infotype_proposals:
                    continue
                infotype_proposal = max(
                    col_info.infotype_proposals, key=lambda p: p.confidence_level
                )
                self.logger.info(
                    f"Info Type Suggestion for Column {col_info.metadata.name} => {infotype_proposal.infotype} with {infotype_proposal.confidence_level} "
                )
                field_terms[
                    col_info.metadata.name
                ] = self.config.classification.info_type_to_term.get(
                    infotype_proposal.infotype, infotype_proposal.infotype
                )

        for field in schema_metadata.fields:
            if field.fieldPath in field_terms:
                field.glossaryTerms = GlossaryTerms(
                    terms=[
                        GlossaryTermAssociation(
                            urn=make_term_urn(field_terms[field.fieldPath])
                        )
                    ],
                    auditStamp=AuditStamp(
                        time=get_sys_time(), actor=make_user_urn("datahub")
                    ),
                )
