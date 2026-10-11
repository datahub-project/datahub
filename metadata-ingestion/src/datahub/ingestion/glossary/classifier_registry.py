from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.glossary.classifier import Classifier

# The built-in `datahub` classifier (DataHubClassifier) has been removed because it
# depended on the unmaintained `acryl-datahub-classify` package, which pinned
# numpy<2 and an outdated spaCy stack and blocked dependency upgrades. The
# classification framework (this registry, the `Classifier` interface, and the
# orchestration mixin) is intentionally retained so custom classifiers can still be
# registered. No classifier is registered by default.
DEFAULT_CLASSIFIER_TYPE = "datahub"

BUILTIN_CLASSIFIER_REMOVED_MESSAGE = (
    "The built-in 'datahub' column-level classifier has been removed from "
    "acryl-datahub because it depended on the unmaintained `acryl-datahub-classify` "
    "package (which pinned numpy<2). To keep using it, install the last release that "
    "supports it: `pip install 'acryl-datahub==1.6.0.5'`. Otherwise, register your "
    "own classifier implementation or disable classification "
    "(`classification.enabled: false`)."
)

classifier_registry = PluginRegistry[Classifier]()
