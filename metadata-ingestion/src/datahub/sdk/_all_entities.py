from typing import Dict, List, Type

from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.sdk.mlmodel import MLModel
from datahub.sdk.mlmodelgroup import MLModelGroup

# TODO: Is there a better way to declare this?
ENTITY_CLASSES_LIST: List[Type[Entity]] = [
    Container,
    Dataset,
    MLModel,
    MLModelGroup,
]

ENTITY_CLASSES: Dict[str, Type[Entity]] = {
    cls.get_urn_type().ENTITY_TYPE: cls for cls in ENTITY_CLASSES_LIST
}
