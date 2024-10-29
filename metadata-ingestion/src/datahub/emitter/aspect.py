from typing import Dict, Type

from datahub.metadata.schema_classes import ASPECT_CLASSES, _Aspect

ASPECT_MAP: Dict[str, Type[_Aspect]] = {
    AspectClass.get_aspect_name(): AspectClass for AspectClass in ASPECT_CLASSES
}

TIMESERIES_ASPECT_MAP: Dict[str, Type[_Aspect]] = {
    name: klass
    for name, klass in ASPECT_MAP.items()
    if klass.get_aspect_type() == "timeseries"
}

JSON_CONTENT_TYPE = "application/json"
JSON_PATCH_CONTENT_TYPE = "application/json-patch+json"
