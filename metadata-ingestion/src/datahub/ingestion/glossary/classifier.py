from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from datahub_classify.helper_classes import ColumnInfo
from pydantic.fields import Field

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    DynamicTypedConfig,
)


class DynamicTypedClassifierConfig(DynamicTypedConfig):
    # Respecifying the base-class just to override field level docs

    type: str = Field(
        description="The type of the classifier to use. For DataHub,  use `datahub`",
    )
    # This config type is declared Optional[Any] here. The eventual parser for the
    # specified type is responsible for further validation.
    config: Optional[Any] = Field(
        default=None,
        description="The configuration required for initializing the classifier. If not specified, uses defaults for classifer type.",
    )


class ClassificationConfig(ConfigModel):
    enabled: bool = Field(
        default=False,
        description="Whether classification should be used to auto-detect glossary terms",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter tables for classification. This is used in combination with other patterns in parent config. Specify regex to match the entire table name in `database.schema.table` format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )

    column_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter columns for classification. This is used in combination with other patterns in parent config. Specify regex to match the column name in `database.schema.table.column` format.",
    )

    info_type_to_term: Dict[str, str] = Field(
        default=dict(),
        description="Optional mapping to provide glossary term identifier for info type",
    )

    classifiers: List[DynamicTypedClassifierConfig] = Field(
        default=[DynamicTypedClassifierConfig(type="datahub", config=None)],
        description="Classifiers to use to auto-detect glossary terms. If more than one classifier, infotype predictions from the classifier defined later in sequence take precedance.",
    )


@dataclass
class Classifier(metaclass=ABCMeta):
    @abstractmethod
    def classify(self, columns: List[ColumnInfo]) -> List[ColumnInfo]:
        pass

    @classmethod
    def create(cls, config_dict: Dict[str, Any]) -> "Classifier":
        raise NotImplementedError("Sub-classes must override this method.")
