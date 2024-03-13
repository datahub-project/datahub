import concurrent.futures
import logging
from dataclasses import dataclass, field
from math import ceil
from typing import Dict, Iterable, List, Optional

from datahub_classify.helper_classes import ColumnInfo, Metadata
from pydantic import Field

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.emitter.mce_builder import get_sys_time, make_term_urn, make_user_urn
from datahub.ingestion.glossary.classifier import ClassificationConfig, Classifier
from datahub.ingestion.glossary.classifier_registry import classifier_registry
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    GlossaryTermAssociation,
    GlossaryTerms,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class ClassificationReportMixin:
    num_tables_classification_attempted: int = 0
    num_tables_classification_failed: int = 0
    num_tables_classified: int = 0

    info_types_detected: LossyDict[str, LossyList[str]] = field(
        default_factory=LossyDict
    )


class ClassificationSourceConfigMixin(ConfigModel):
    classification: ClassificationConfig = Field(
        default=ClassificationConfig(),
        description="For details, refer [Classification](../../../../metadata-ingestion/docs/dev_guides/classification.md).",
    )


class ClassificationHandler:
    def __init__(
        self, config: ClassificationSourceConfigMixin, report: ClassificationReportMixin
    ):
        self.config = config
        self.report = report
        self.classifiers = self.get_classifiers()

    def is_classification_enabled(self) -> bool:
        return (
            self.config.classification is not None
            and self.config.classification.enabled
            and len(self.config.classification.classifiers) > 0
        )

    def is_classification_enabled_for_table(self, dataset_name: str) -> bool:
        return (
            self.config.classification is not None
            and self.config.classification.enabled
            and len(self.config.classification.classifiers) > 0
            and self.config.classification.table_pattern.allowed(dataset_name)
        )

    def is_classification_enabled_for_column(
        self, dataset_name: str, column_name: str
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

    def get_classifiers(self) -> List[Classifier]:
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
        self,
        dataset_name: str,
        schema_metadata: SchemaMetadata,
        sample_data: Dict[str, list],
    ) -> None:
        column_infos = self.get_columns_to_classify(
            dataset_name, schema_metadata, sample_data
        )

        if not column_infos:
            logger.debug(f"No columns in {dataset_name} considered for classification")
            return None

        logger.debug(f"Classifying Table {dataset_name}")

        self.report.num_tables_classification_attempted += 1
        field_terms: Dict[str, str] = {}
        with PerfTimer() as timer:
            try:
                for classifier in self.classifiers:
                    column_infos_with_proposals: Iterable[ColumnInfo]
                    if self.config.classification.max_workers > 1:
                        column_infos_with_proposals = self.async_classify(
                            classifier, column_infos
                        )
                    else:
                        column_infos_with_proposals = classifier.classify(column_infos)

                    for column_info_proposal in column_infos_with_proposals:
                        self.update_field_terms(field_terms, column_info_proposal)

            except Exception:
                self.report.num_tables_classification_failed += 1
                raise
            finally:
                time_taken = timer.elapsed_seconds()
                logger.debug(
                    f"Finished classification {dataset_name}; took {time_taken:.3f} seconds"
                )

        if field_terms:
            self.report.num_tables_classified += 1
            self.populate_terms_in_schema_metadata(schema_metadata, field_terms)

    def update_field_terms(
        self, field_terms: Dict[str, str], col_info: ColumnInfo
    ) -> None:
        term = self.get_terms_for_column(col_info)
        if term:
            field_terms[col_info.metadata.name] = term

    def async_classify(
        self, classifier: Classifier, columns: List[ColumnInfo]
    ) -> Iterable[ColumnInfo]:
        num_columns = len(columns)
        BATCH_SIZE = 5  # Number of columns passed to classify api at a time

        logger.debug(
            f"Will Classify {num_columns} column(s) with {self.config.classification.max_workers} worker(s) with batch size {BATCH_SIZE}."
        )

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=self.config.classification.max_workers,
        ) as executor:
            column_info_proposal_futures = [
                executor.submit(
                    classifier.classify,
                    columns[
                        (i * BATCH_SIZE) : min(i * BATCH_SIZE + BATCH_SIZE, num_columns)
                    ],
                )
                for i in range(ceil(num_columns / BATCH_SIZE))
            ]

            return [
                column_with_proposal
                for proposal_future in concurrent.futures.as_completed(
                    column_info_proposal_futures
                )
                for column_with_proposal in proposal_future.result()
            ]

    def populate_terms_in_schema_metadata(
        self,
        schema_metadata: SchemaMetadata,
        field_terms: Dict[str, str],
    ) -> None:
        for schema_field in schema_metadata.fields:
            if schema_field.fieldPath in field_terms:
                schema_field.glossaryTerms = GlossaryTerms(
                    terms=[
                        GlossaryTermAssociation(
                            urn=make_term_urn(field_terms[schema_field.fieldPath])
                        )
                    ]
                    # Keep existing terms if present
                    + (
                        schema_field.glossaryTerms.terms
                        if schema_field.glossaryTerms
                        else []
                    ),
                    auditStamp=AuditStamp(
                        time=get_sys_time(), actor=make_user_urn("datahub")
                    ),
                )

    def get_terms_for_column(self, col_info: ColumnInfo) -> Optional[str]:
        if not col_info.infotype_proposals:
            return None
        infotype_proposal = max(
            col_info.infotype_proposals, key=lambda p: p.confidence_level
        )
        self.report.info_types_detected.setdefault(
            infotype_proposal.infotype, LossyList()
        ).append(f"{col_info.metadata.dataset_name}.{col_info.metadata.name}")
        term = self.config.classification.info_type_to_term.get(
            infotype_proposal.infotype, infotype_proposal.infotype
        )

        return term

    def get_columns_to_classify(
        self,
        dataset_name: str,
        schema_metadata: SchemaMetadata,
        sample_data: Dict[str, list],
    ) -> List[ColumnInfo]:
        column_infos: List[ColumnInfo] = []

        for schema_field in schema_metadata.fields:
            if not self.is_classification_enabled_for_column(
                dataset_name, schema_field.fieldPath
            ):
                logger.debug(
                    f"Skipping column {dataset_name}.{schema_field.fieldPath} from classification"
                )
                continue

            # TODO: Let's auto-skip passing sample_data for complex(array/struct) columns
            # for initial rollout

            column_infos.append(
                ColumnInfo(
                    metadata=Metadata(
                        {
                            "Name": schema_field.fieldPath,
                            "Description": schema_field.description,
                            "DataType": schema_field.nativeDataType,
                            "Dataset_Name": dataset_name,
                        }
                    ),
                    values=(
                        sample_data[schema_field.fieldPath]
                        if schema_field.fieldPath in sample_data.keys()
                        else []
                    ),
                )
            )

        return column_infos
