from typing import Dict, Type, Union

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

PLATFORM = "salesforce"

DEFAULT_DATASPACE = "default"

DMO_LIST_PATH = "ssot/data-model-objects"
DMO_RELATIONSHIPS_PATH = "ssot/data-model-objects/{dmo_name}/relationships"

# The DMO list has no nextPageUrl and pages by offset. It takes ~0.2 s per row,
# so 100-row pages stay well under the request timeout.
DMO_PAGE_SIZE = 100
# Guards against an org that ignores offset and keeps returning the first page.
MAX_DMO_PAGES = 200
RELATIONSHIPS_PAGE_SIZE = 500

DMO_CREATION_TYPE_CUSTOM = "Custom"
RELATIONSHIP_STATUS_ACTIVE = "ACTIVE"
CARDINALITY_ONE_TO_MANY = "OneToMany"

FIELD_TYPE_MAPPING: Dict[
    str,
    Union[
        Type[BooleanTypeClass],
        Type[DateTypeClass],
        Type[NumberTypeClass],
        Type[StringTypeClass],
        Type[TimeTypeClass],
    ],
] = {
    "Boolean": BooleanTypeClass,
    "Currency": NumberTypeClass,
    "Date": DateTypeClass,
    "DateTime": TimeTypeClass,
    "Email": StringTypeClass,
    "Number": NumberTypeClass,
    "Percent": NumberTypeClass,
    "Phone": StringTypeClass,
    "Text": StringTypeClass,
    "Url": StringTypeClass,
}
