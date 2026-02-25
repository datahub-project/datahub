from datahub.ingestion.glossary.classification_mixin import (
    ClassificationHandler,
    ClassificationReportMixin,
    ClassificationSourceConfigMixin,
)
from datahub.ingestion.glossary.classifier import ClassificationConfig


def test_get_classifiers_without_classification_config():
    # Create a config without classification attribute
    class TestConfig(ClassificationSourceConfigMixin):
        classification: ClassificationConfig = None  # type: ignore

    config = TestConfig()
    report = ClassificationReportMixin()

    # Create handler with config that has no classification attribute
    handler = ClassificationHandler(config, report)

    # Should return empty list when classification is not set
    assert handler.get_classifiers() == []


def test_get_classifiers_with_none_classification():
    # Create a config with classification set to None
    class TestConfig(ClassificationSourceConfigMixin):
        classification: ClassificationConfig = None  # type: ignore

    config = TestConfig()
    report = ClassificationReportMixin()

    # Create handler with config that has None classification
    handler = ClassificationHandler(config, report)

    # Should return empty list when classification is None
    assert handler.get_classifiers() == []
