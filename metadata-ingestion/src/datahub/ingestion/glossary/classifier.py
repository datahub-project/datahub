from typing import Any

from pydantic import model_validator

from datahub.configuration.common import ConfigModel

# Column-level classification relied on the unmaintained `acryl-datahub-classify`
# package, which pinned numpy<2 and an outdated spaCy stack. The feature has been
# removed; this message tells users how to keep using it if they still need it.
CLASSIFICATION_REMOVED_MESSAGE = (
    "Column-level classification has been removed from acryl-datahub because it "
    "depended on the unmaintained `acryl-datahub-classify` package. If you still "
    "need classification, install the last release that supports it: "
    "`pip install 'acryl-datahub==1.6.0.5'`."
)


class ClassificationSourceConfigMixin(ConfigModel):
    # `classification` is intentionally undeclared so it no longer appears in source
    # config schemas/docs. A recipe that still sets it fails fast with guidance.
    @model_validator(mode="before")
    @classmethod
    def _reject_removed_classification(cls, values: Any) -> Any:
        if isinstance(values, dict) and "classification" in values:
            raise ValueError(CLASSIFICATION_REMOVED_MESSAGE)
        return values
