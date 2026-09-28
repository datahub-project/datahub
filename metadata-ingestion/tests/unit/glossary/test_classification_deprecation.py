import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationHandler,
    ClassificationReportMixin,
    ClassificationSourceConfigMixin,
)
from datahub.ingestion.glossary.classifier import ClassificationConfig


def test_no_classification_is_accepted() -> None:
    # Sources that don't configure classification keep working unchanged: the
    # framework is retained, no classifier is resolved when classification is off.
    config = ClassificationSourceConfigMixin()
    handler = ClassificationHandler(config, ClassificationReportMixin())
    assert handler.get_classifiers() == []


def test_disabled_classification_does_not_resolve_builtin() -> None:
    # The default `classifiers=[datahub]` references the removed built-in, but a
    # disabled config must not attempt to resolve it (otherwise every source would
    # fail on construction).
    config = ClassificationSourceConfigMixin(
        classification=ClassificationConfig(enabled=False)
    )
    handler = ClassificationHandler(config, ClassificationReportMixin())
    assert handler.get_classifiers() == []


def test_enabling_builtin_classifier_fails_with_guidance() -> None:
    # The built-in `datahub` classifier was removed alongside the
    # `acryl-datahub-classify` dependency. Enabling classification without
    # registering a replacement must fail fast with guidance pointing at the last
    # release that still supports it.
    config = ClassificationSourceConfigMixin(
        classification=ClassificationConfig(enabled=True)
    )
    with pytest.raises(ConfigurationError, match="1.6.0.5"):
        ClassificationHandler(config, ClassificationReportMixin())
