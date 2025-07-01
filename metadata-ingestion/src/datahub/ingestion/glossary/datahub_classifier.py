from typing import Any, Dict, List, Optional

from datahub_classify.helper_classes import ColumnInfo
from datahub_classify.infotype_predictor import predict_infotypes
from datahub_classify.reference_input import input1 as default_config
from pydantic import validator
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.configuration.pydantic_migration_helpers import PYDANTIC_VERSION_2
from datahub.ingestion.glossary.classifier import Classifier
from datahub.utilities.str_enum import StrEnum


class NameFactorConfig(ConfigModel):
    regex: List[str] = Field(
        default=[".*"],
        description="List of regex patterns the column name follows for the info type",
    )


class DescriptionFactorConfig(ConfigModel):
    regex: List[str] = Field(
        default=[".*"],
        description="List of regex patterns the column description follows for the info type",
    )


class DataTypeFactorConfig(ConfigModel):
    type: List[str] = Field(
        default=[".*"],
        description="List of data types for the info type",
    )


class ValuePredictionType(StrEnum):
    REGEX = "regex"
    LIBRARY = "library"


class ValuesFactorConfig(ConfigModel):
    prediction_type: ValuePredictionType
    regex: Optional[List[str]] = Field(
        default=None,
        description="List of regex patterns the column value follows for the info type",
    )
    library: Optional[List[str]] = Field(
        default=None, description="Library used for prediction"
    )


class PredictionFactorsAndWeights(ConfigModel):
    class Config:
        if PYDANTIC_VERSION_2:
            populate_by_name = True
        else:
            allow_population_by_field_name = True

    Name: float = Field(alias="name")
    Description: float = Field(alias="description")
    Datatype: float = Field(alias="datatype")
    Values: float = Field(alias="values")


class InfoTypeConfig(ConfigModel):
    class Config:
        if PYDANTIC_VERSION_2:
            populate_by_name = True
        else:
            allow_population_by_field_name = True

    Prediction_Factors_and_Weights: PredictionFactorsAndWeights = Field(
        description="Factors and their weights to consider when predicting info types",
        alias="prediction_factors_and_weights",
    )
    ExcludeName: Optional[List[str]] = Field(
        default=None,
        alias="exclude_name",
        description="List of exact column names to exclude from classification for this info type",
    )
    Name: Optional[NameFactorConfig] = Field(default=None, alias="name")

    Description: Optional[DescriptionFactorConfig] = Field(
        default=None, alias="description"
    )

    Datatype: Optional[DataTypeFactorConfig] = Field(default=None, alias="datatype")

    Values: Optional[ValuesFactorConfig] = Field(default=None, alias="values")


DEFAULT_CLASSIFIER_CONFIG = {
    k: InfoTypeConfig.parse_obj(v) for k, v in default_config.items()
}


# TODO: Generate Classification doc (classification.md) from python source.
class DataHubClassifierConfig(ConfigModel):
    confidence_level_threshold: float = Field(
        default=0.68,
        description="The confidence threshold above which the prediction is considered as a proposal",
    )
    strip_exclusion_formatting: bool = Field(default=True)
    info_types: Optional[List[str]] = Field(
        default=None,
        description="List of infotypes to be predicted. By default, all supported infotypes are considered, along with any custom infotypes configured in `info_types_config`.",
    )
    info_types_config: Dict[str, InfoTypeConfig] = Field(
        default=DEFAULT_CLASSIFIER_CONFIG,
        description="Configuration details for infotypes. See [reference_input.py](https://github.com/acryldata/datahub-classify/blob/main/datahub-classify/src/datahub_classify/reference_input.py) for default configuration.",
    )
    minimum_values_threshold: int = Field(
        default=50,
        description="Minimum number of non-null column values required to process `values` prediction factor.",
    )

    @validator("info_types_config")
    def input_config_selectively_overrides_default_config(cls, info_types_config):
        for infotype, infotype_config in DEFAULT_CLASSIFIER_CONFIG.items():
            if infotype not in info_types_config:
                # if config for some info type is not provided by user, use default config for that info type.
                info_types_config[infotype] = infotype_config
            else:
                # if config for info type is provided by user but config for its prediction factor is missing,
                # use default config for that prediction factor.
                for factor, weight in (
                    info_types_config[infotype]
                    .Prediction_Factors_and_Weights.dict()
                    .items()
                ):
                    if (
                        weight > 0
                        and getattr(info_types_config[infotype], factor) is None
                    ):
                        setattr(
                            info_types_config[infotype],
                            factor,
                            getattr(infotype_config, factor),
                        )
        # Custom info type
        custom_infotypes = info_types_config.keys() - DEFAULT_CLASSIFIER_CONFIG.keys()

        for custom_infotype in custom_infotypes:
            custom_infotype_config = info_types_config[custom_infotype]
            # for custom infotype, config for every prediction factor must be specified.
            for (
                factor,
                weight,
            ) in custom_infotype_config.Prediction_Factors_and_Weights.dict().items():
                if weight > 0:
                    assert getattr(custom_infotype_config, factor) is not None, (
                        f"Missing Configuration for Prediction Factor {factor} for Custom Info Type {custom_infotype}"
                    )

            # Custom infotype supports only regex based prediction for column values
            if custom_infotype_config.Prediction_Factors_and_Weights.Values > 0:
                assert custom_infotype_config.Values
                assert (
                    custom_infotype_config.Values.prediction_type
                    == ValuePredictionType.REGEX
                ), (
                    f"Invalid Prediction Type for Values for Custom Info Type {custom_infotype}. Only `regex` is supported."
                )

        return info_types_config


class DataHubClassifier(Classifier):
    def __init__(self, config: DataHubClassifierConfig):
        self.config = config

    @classmethod
    def create(cls, config_dict: Optional[Dict[str, Any]]) -> "DataHubClassifier":
        # This could be replaced by parsing to particular class, if required
        if config_dict is not None:
            config = DataHubClassifierConfig.parse_obj(config_dict)
        else:
            config = DataHubClassifierConfig()
        return cls(config)

    def classify(self, columns: List[ColumnInfo]) -> List[ColumnInfo]:
        columns = predict_infotypes(
            column_infos=columns,
            confidence_level_threshold=self.config.confidence_level_threshold,
            global_config={
                k: v.dict() for k, v in self.config.info_types_config.items()
            },
            infotypes=self.config.info_types,
            minimum_values_threshold=self.config.minimum_values_threshold,
        )

        return columns
