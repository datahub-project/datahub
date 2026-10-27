from typing import Dict, List, Type

from datahub.sdk.chart import Chart
from datahub.sdk.container import Container
from datahub.sdk.dashboard import Dashboard
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.dataset import Dataset
from datahub.sdk.document import Document
from datahub.sdk.entity import Entity
from datahub.sdk.glossary_node import GlossaryNode
from datahub.sdk.glossary_term import GlossaryTerm
from datahub.sdk.metric import Metric
from datahub.sdk.mlmodel import MLModel
from datahub.sdk.mlmodelgroup import MLModelGroup
from datahub.sdk.semantic_model import SemanticModel
from datahub.sdk.tag import Tag

# Base entity classes that don't have circular dependencies
# Those that do are imported in the EntityClient where needed
# TODO: Is there a better way to declare this?
ENTITY_CLASSES_LIST: List[Type[Entity]] = [
    Container,
    Dataset,
    Document,
    MLModel,
    MLModelGroup,
    DataFlow,
    DataJob,
    Dashboard,
    Chart,
    GlossaryNode,
    GlossaryTerm,
    Tag,
    SemanticModel,
    # NOTE: SemanticModelDataset is intentionally NOT registered here. It is a
    # Dataset subtype sharing the "dataset" entity type, so registering it would
    # overwrite Dataset in the ENTITY_CLASSES map below (last write wins) and
    # every dataset URN would hydrate as a SemanticModelDataset. Reading logical
    # datasets back as Dataset is the correct behavior.
    Metric,
]

# Create the mapping of entity types to classes
ENTITY_CLASSES: Dict[str, Type[Entity]] = {
    cls.get_urn_type().ENTITY_TYPE: cls for cls in ENTITY_CLASSES_LIST
}
