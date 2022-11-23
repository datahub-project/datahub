from typing import Any, Dict, List, Optional

from datahub_classify.helper_classes import ColumnInfo
from datahub_classify.infotype_predictor import predict_infotypes
from pydantic.class_validators import root_validator
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.ingestion.glossary.classifier import Classifier

default_config = {
    "Email_Address": {
        "Prediction_Factors_and_Weights": {
            "Name": 0.4,
            "Description": 0,
            "Datatype": 0,
            "Values": 0.6,
        },
        "Name": {
            "regex": [
                "^.*mail.*id.*$",
                "^.*id.*mail.*$",
                "^.*mail.*add.*$",
                "^.*add.*mail.*$",
                "email",
                "mail",
            ]
        },
        "Description": {
            "regex": ["^.*mail.*id.*$", "^.*mail.*add.*$", "email", "mail"]
        },
        "Datatype": {"type": ["str"]},
        "Values": {
            "prediction_type": "regex",
            "regex": ["[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}"],
            "library": [],
        },
    },
    "Gender": {
        "Prediction_Factors_and_Weights": {
            "Name": 0.4,
            "Description": 0,
            "Datatype": 0,
            "Values": 0.6,
        },
        "Name": {"regex": ["^.*gender.*$", "^.*sex.*$", "gender", "sex"]},
        "Description": {"regex": ["^.*gender.*$", "^.*sex.*$", "gender", "sex"]},
        "Datatype": {"type": ["int", "str"]},
        "Values": {
            "prediction_type": "regex",
            "regex": ["male", "female", "man", "woman", "m", "f", "w", "men", "women"],
            "library": [],
        },
    },
    "Credit_Debit_Card_Number": {
        "Prediction_Factors_and_Weights": {
            "Name": 0.4,
            "Description": 0,
            "Datatype": 0,
            "Values": 0.6,
        },
        "Name": {
            "regex": [
                "^.*card.*number.*$",
                "^.*number.*card.*$",
                "^.*credit.*card.*$",
                "^.*debit.*card.*$",
            ]
        },
        "Description": {
            "regex": [
                "^.*card.*number.*$",
                "^.*number.*card.*$",
                "^.*credit.*card.*$",
                "^.*debit.*card.*$",
            ]
        },
        "Datatype": {"type": ["str", "int"]},
        "Values": {
            "prediction_type": "regex",
            "regex": [
                "^4[0-9]{12}(?:[0-9]{3})?$",
                "^(?:5[1-5][0-9]{2}|222[1-9]|22[3-9][0-9]|2[3-6][0-9]{2}|27[01][0-9]|2720)[0-9]{12}$",
                "^3[47][0-9]{13}$",
                "^3(?:0[0-5]|[68][0-9])[0-9]{11}$",
                "^6(?:011|5[0-9]{2})[0-9]{12}$",
                "^(?:2131|1800|35\\d{3})\\d{11}$",
                "^(6541|6556)[0-9]{12}$",
                "^389[0-9]{11}$",
                "^63[7-9][0-9]{13}$",
                "^9[0-9]{15}$",
                "^(6304|6706|6709|6771)[0-9]{12,15}$",
                "^(5018|5020|5038|6304|6759|6761|6763)[0-9]{8,15}$",
                "^(62[0-9]{14,17})$",
                "^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14})$",
                "^(4903|4905|4911|4936|6333|6759)[0-9]{12}|(4903|4905|4911|4936|6333|6759)[0-9]{14}|(4903|4905|4911|4936|6333|6759)[0-9]{15}|564182[0-9]{10}|564182[0-9]{12}|564182[0-9]{13}|633110[0-9]{10}|633110[0-9]{12}|633110[0-9]{13}$",
                "^(6334|6767)[0-9]{12}|(6334|6767)[0-9]{14}|(6334|6767)[0-9]{15}$",
            ],
            "library": [],
        },
    },
    "Phone_Number": {
        "Prediction_Factors_and_Weights": {
            "Name": 0.4,
            "Description": 0,
            "Datatype": 0,
            "Values": 0.6,
        },
        "Name": {
            "regex": [
                ".*phone.*(num|no).*",
                ".*(num|no).*phone.*",
                ".*[^a-z]+ph[^a-z]+.*(num|no).*",
                ".*(num|no).*[^a-z]+ph[^a-z]+.*",
                ".*mobile.*(num|no).*",
                ".*(num|no).*mobile.*",
                ".*telephone.*(num|no).*",
                ".*(num|no).*telephone.*",
                ".*cell.*(num|no).*",
                ".*(num|no).*cell.*",
                ".*contact.*(num|no).*",
                ".*(num|no).*contact.*",
                ".*landline.*(num|no).*",
                ".*(num|no).*landline.*",
                ".*fax.*(num|no).*",
                ".*(num|no).*fax.*",
                "phone",
                "telephone",
                "landline",
                "mobile",
                "tel",
                "fax",
                "cell",
                "contact",
            ]
        },
        "Description": {
            "regex": [
                ".*phone.*(num|no).*",
                ".*(num|no).*phone.*",
                ".*[^a-z]+ph[^a-z]+.*(num|no).*",
                ".*(num|no).*[^a-z]+ph[^a-z]+.*",
                ".*mobile.*(num|no).*",
                ".*(num|no).*mobile.*",
                ".*telephone.*(num|no).*",
                ".*(num|no).*telephone.*",
                ".*cell.*(num|no).*",
                ".*(num|no).*cell.*",
                ".*contact.*(num|no).*",
                ".*(num|no).*contact.*",
                ".*landline.*(num|no).*",
                ".*(num|no).*landline.*",
                ".*fax.*(num|no).*",
                ".*(num|no).*fax.*",
                "phone",
                "telephone",
                "landline",
                "mobile",
                "tel",
                "fax",
                "cell",
                "contact",
            ]
        },
        "Datatype": {"type": ["int", "str"]},
        "Values": {
            "prediction_type": "library",
            "regex": [],
            "library": ["phonenumbers"],
        },
    },
    "Street_Address": {
        "Prediction_Factors_and_Weights": {
            "Name": 0.5,
            "Description": 0,
            "Datatype": 0,
            "Values": 0.5,
        },
        "Name": {
            "regex": [
                ".*street.*add.*",
                ".*add.*street.*",
                ".*full.*add.*",
                ".*add.*full.*",
                ".*mail.*add.*",
                ".*add.*mail.*",
                "add[^a-z]+",
                "address",
                "street",
            ]
        },
        "Description": {
            "regex": [
                ".*street.*add.*",
                ".*add.*street.*",
                ".*full.*add.*",
                ".*add.*full.*",
                ".*mail.*add.*",
                ".*add.*mail.*",
                "add[^a-z]+",
                "address",
                "street",
            ]
        },
        "Datatype": {"type": ["str"]},
        "Values": {"prediction_type": "library", "regex": [], "library": ["spacy"]},
    },
    "Full_Name": {
        "Prediction_Factors_and_Weights": {
            "Name": 0.3,
            "Description": 0,
            "Datatype": 0,
            "Values": 0.7,
        },
        "Name": {
            "regex": [
                ".*person.*name.*",
                ".*name.*person.*",
                ".*user.*name.*",
                ".*name.*user.*",
                ".*full.*name.*",
                ".*name.*full.*",
                "fullname",
                "name",
                "person",
                "user",
            ]
        },
        "Description": {
            "regex": [
                ".*person.*name.*",
                ".*name.*person.*",
                ".*user.*name.*",
                ".*name.*user.*",
                ".*full.*name.*",
                ".*name.*full.*",
                "fullname",
                "name",
                "person",
                "user",
            ]
        },
        "Datatype": {"type": ["str"]},
        "Values": {"prediction_type": "library", "regex": [], "library": ["spacy"]},
    },
    "Age": {
        "Prediction_Factors_and_Weights": {
            "Name": 0.65,
            "Description": 0,
            "Datatype": 0,
            "Values": 0.35,
        },
        "Name": {
            "regex": ["age[^a-z]+.*", ".*[^a-z]+age", ".*[^a-z]+age[^a-z]+.*", "age"]
        },
        "Description": {
            "regex": ["age[^a-z]+.*", ".*[^a-z]+age", ".*[^a-z]+age[^a-z]+.*", "age"]
        },
        "Datatype": {"type": ["int"]},
        "Values": {
            "prediction_type": "library",
            "regex": [],
            "library": ["rule_based_logic"],
        },
    },
}


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
