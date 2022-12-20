from typing import Any, Dict, List, Optional

from datahub_classify.helper_classes import ColumnInfo
from datahub_classify.infotype_predictor import predict_infotypes
from datahub_classify.reference_input import input1 as default_config
from pydantic.class_validators import root_validator
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.ingestion.glossary.classifier import Classifier


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


class ValuesFactorConfig(ConfigModel):
    prediction_type: str
    regex: Optional[List[str]] = Field(
        description="List of regex patterns the column value follows for the info type",
    )
    library: Optional[List[str]] = Field(description="Library used for prediction")


class PredictionFactorsAndWeights(ConfigModel):
    class Config:
        allow_population_by_field_name = True

    Name: float = Field(alias="name")
    Description: float = Field(alias="description")
    Datatype: float = Field(alias="datatype")
    Values: float = Field(alias="values")


class InfoTypeConfig(ConfigModel):
    class Config:
        allow_population_by_field_name = True

    Prediction_Factors_and_Weights: PredictionFactorsAndWeights = Field(
        description="Factors and their weights to consider when predicting info types",
        alias="prediction_factors_and_weights",
    )
    Name: Optional[NameFactorConfig] = Field(alias="name")

    Description: Optional[DescriptionFactorConfig] = Field(alias="description")

    Datatype: Optional[DataTypeFactorConfig] = Field(alias="datatype")

    Values: Optional[ValuesFactorConfig] = Field(alias="values")


# TODO: Generate Classification doc (classification.md) from python source.
class DataHubClassifierConfig(ConfigModel):
    confidence_level_threshold: float = Field(
        default=0.6,
        init=False,
        description="The confidence threshold above which the prediction is considered as a proposal",
    )
    info_types: Optional[List[str]] = Field(
        default=None,
        init=False,
        description=f"List of infotypes to be predicted. By default, all supported infotypes are considered. If specified. this should be subset of {list(default_config.keys())}.",
    )
    info_types_config: Dict[str, InfoTypeConfig] = Field(
        default={k: InfoTypeConfig.parse_obj(v) for k, v in default_config.items()},
        init=False,
        description="Configuration details for infotypes",
    )

    @root_validator
    def provided_config_selectively_overrides_default_config(cls, values):
        override: Dict[str, InfoTypeConfig] = values.get("info_types_config")
        base = {k: InfoTypeConfig.parse_obj(v) for k, v in default_config.items()}
        for k, v in base.items():
            if k not in override.keys():
                # use default InfoTypeConfig for info type key if not specified in recipe
                values["info_types_config"][k] = v
            else:
                for factor, _ in (
                    override[k].Prediction_Factors_and_Weights.dict().items()
                ):
                    # use default FactorConfig for factor if not specified in recipe
                    if getattr(override[k], factor) is None:
                        setattr(
                            values["info_types_config"][k], factor, getattr(v, factor)
                        )
        return values


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
            columns,
            self.config.confidence_level_threshold,
            {k: v.dict() for k, v in self.config.info_types_config.items()},
            self.config.info_types,
        )
        return columns
