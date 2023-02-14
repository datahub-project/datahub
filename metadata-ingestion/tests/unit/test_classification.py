from datahub.ingestion.glossary.classifier import ClassificationConfig
from datahub.ingestion.glossary.datahub_classifier import DataHubClassifier


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
