from typing import Dict, List, Type

from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity

# TODO: Is there a better way to declare this?
ENTITY_CLASSES_LIST: List[Type[Entity]] = [
    Container,
    Dataset,
]

ENTITY_CLASSES: Dict[str, Type[Entity]] = {
    cls.get_urn_type().ENTITY_TYPE: cls for cls in ENTITY_CLASSES_LIST
}
