import pytest

from datahub.ingestion.glossary.classifier import ClassificationSourceConfigMixin


def test_no_classification_is_accepted():
    # Sources that don't configure classification keep working unchanged.
    ClassificationSourceConfigMixin()


def test_setting_classification_fails_with_guidance():
    # Classification has been removed; configuring it must fail fast with guidance
    # pointing at the last release that still supports it.
    with pytest.raises(ValueError, match="1.6.0.5"):
        ClassificationSourceConfigMixin.parse_obj({"classification": {"enabled": True}})
