from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.glossary.classifier import Classifier
from datahub.ingestion.glossary.datahub_classifier import DataHubClassifier

classifier_registry = PluginRegistry[Classifier]()

classifier_registry.register("datahub", DataHubClassifier)
