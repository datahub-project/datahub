import pytest
from pydantic import ValidationError

from datahub.ingestion.glossary.classifier import ClassificationConfig
from datahub.ingestion.glossary.datahub_classifier import (
    DataHubClassifier,
    DataHubClassifierConfig,
)


def test_default_classification_config():
    default_classification_config = ClassificationConfig(enabled=True)
    assert default_classification_config.classifiers[0].type == "datahub"
    assert default_classification_config.classifiers[0].config is None


def test_default_datahub_classifier_config():
    default_datahub_classifier = DataHubClassifier.create(config_dict=None)
    assert default_datahub_classifier.config.info_types_config is not None


def test_selective_datahub_classifier_config_override():
    simple_config_override = DataHubClassifier.create(
        config_dict={"confidence_level_threshold": 0.7}
    ).config
    assert simple_config_override.info_types_config is not None

    complex_config_override = DataHubClassifier.create(
        config_dict={
            "confidence_level_threshold": 0.7,
            "info_types_config": {
                "Age": {
                    "Prediction_Factors_and_Weights": {
                        "Name": 0.5,
                        "Description": 0,
                        "Datatype": 0,
                        "Values": 0.5,
                    },
                },
            },
        }
    ).config

    assert complex_config_override.info_types_config is not None
    assert (
        complex_config_override.info_types_config[
            "Age"
        ].Prediction_Factors_and_Weights.Name
        == 0.5
    )
    assert (
        complex_config_override.info_types_config[
            "Age"
        ].Prediction_Factors_and_Weights.Values
        == 0.5
    )
    assert complex_config_override.info_types_config["Age"].Name is not None
    assert complex_config_override.info_types_config["Age"].Values is not None

    assert complex_config_override.info_types_config["Email_Address"] is not None


def test_custom_info_type_config():
    custom_info_type_config = DataHubClassifier.create(
        config_dict={
            "confidence_level_threshold": 0.7,
            "info_types_config": {
                "CloudRegion": {
                    "Prediction_Factors_and_Weights": {
                        "Name": 0.5,
                        "Description": 0,
                        "Datatype": 0,
                        "Values": 0.5,
                    },
                    "Name": {
                        "regex": [
                            ".*region.*id",
                            ".*cloud.*region.*",
                        ]
                    },
                    "Values": {
                        "prediction_type": "regex",
                        "regex": [
                            r"(af|ap|ca|eu|me|sa|us)-(central|north|(north(?:east|west))|south|south(?:east|west)|east|west)-\d+"
                        ],
                    },
                },
            },
        }
    ).config

    assert custom_info_type_config.info_types_config
    assert (
        custom_info_type_config.info_types_config[
            "CloudRegion"
        ].Prediction_Factors_and_Weights.Name
        == 0.5
    )
    assert (
        custom_info_type_config.info_types_config[
            "CloudRegion"
        ].Prediction_Factors_and_Weights.Values
        == 0.5
    )

    # Default Info Type Configurations should exist
    assert custom_info_type_config.info_types_config["Age"]
    assert custom_info_type_config.info_types_config["Email_Address"]


def test_incorrect_custom_info_type_config():
    with pytest.raises(
        ValidationError, match="Missing Configuration for Prediction Factor"
    ):
        DataHubClassifierConfig.parse_obj(
            {
                "confidence_level_threshold": 0.7,
                "info_types_config": {
                    "CloudRegion": {
                        "Prediction_Factors_and_Weights": {
                            "Name": 0.5,
                            "Description": 0,
                            "Datatype": 0,
                            "Values": 0.5,
                        },
                        "Name": {
                            "regex": [
                                ".*region.*id",
                                ".*cloud.*region.*",
                            ]
                        },
                    },
                },
            }
        )

    with pytest.raises(ValidationError, match="Invalid Prediction Type"):
        DataHubClassifierConfig.parse_obj(
            {
                "confidence_level_threshold": 0.7,
                "info_types_config": {
                    "CloudRegion": {
                        "Prediction_Factors_and_Weights": {
                            "Name": 0.5,
                            "Description": 0,
                            "Datatype": 0,
                            "Values": 0.5,
                        },
                        "Name": {
                            "regex": [
                                ".*region.*id",
                                ".*cloud.*region.*",
                            ]
                        },
                        "Values": {"prediction_type": "library", "library": ["spacy"]},
                    },
                },
            }
        )


def test_exclude_name_config():
    config = DataHubClassifier.create(
        config_dict={
            "confidence_level_threshold": 0.7,
            "info_types_config": {
                "Email_Address": {
                    "Prediction_Factors_and_Weights": {
                        "Name": 1,
                        "Description": 0,
                        "Datatype": 0,
                        "Values": 0,
                    },
                    "ExcludeName": ["email_sent", "email_received"],
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
                    "Description": {"regex": []},
                    "Datatype": {"type": ["str"]},
                    "Values": {"prediction_type": "regex", "regex": [], "library": []},
                }
            },
        }
    ).config
    assert config.info_types_config["Email_Address"].ExcludeName is not None
    assert config.info_types_config["Email_Address"].ExcludeName == [
        "email_sent",
        "email_received",
    ]


def test_no_exclude_name_config():
    config = DataHubClassifier.create(
        config_dict={
            "confidence_level_threshold": 0.7,
            "info_types_config": {
                "Email_Address": {
                    "Prediction_Factors_and_Weights": {
                        "Name": 1,
                        "Description": 0,
                        "Datatype": 0,
                        "Values": 0,
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
                    "Description": {"regex": []},
                    "Datatype": {"type": ["str"]},
                    "Values": {"prediction_type": "regex", "regex": [], "library": []},
                }
            },
        }
    ).config
    assert config.info_types_config["Email_Address"].ExcludeName is None
