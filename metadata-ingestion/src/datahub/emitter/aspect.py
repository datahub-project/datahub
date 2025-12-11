# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
