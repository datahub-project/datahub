from typing import Optional

import defusedxml.ElementTree

from datahub.ingestion.source.boomi.constants import (
    XML_ATTR_FOLDER_FULL_PATH,
    XML_ELEMENT_DESCRIPTION,
    XML_NAMESPACE_END,
)
from datahub.ingestion.source.boomi.models import ProcessComponentInfo


def local_name(tag: str) -> str:
    # Real exports mix `bns:`-prefixed elements with `xmlns=""` children, so
    # elements are matched on their local name regardless of namespace.
    return tag.rsplit(XML_NAMESPACE_END, 1)[-1]


def parse_process_component(xml_text: str) -> ProcessComponentInfo:
    root = defusedxml.ElementTree.fromstring(xml_text)
    description: Optional[str] = None
    for child in root:
        if local_name(child.tag) == XML_ELEMENT_DESCRIPTION:
            description = (child.text or "").strip() or None
            break
    return ProcessComponentInfo(
        description=description,
        folder_full_path=root.attrib.get(XML_ATTR_FOLDER_FULL_PATH),
    )
