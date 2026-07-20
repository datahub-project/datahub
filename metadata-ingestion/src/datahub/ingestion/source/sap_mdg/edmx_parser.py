from typing import Iterator, List, Optional
from xml.etree.ElementTree import (  # nosec B405 - only the Element type is imported for annotations; parsing always goes through defusedxml
    Element,
)

import defusedxml.ElementTree as DET

from datahub.ingestion.source.sap_mdg.constants import (
    ATTR_ENTITY_TYPE,
    ATTR_FROM_ROLE,
    ATTR_MAX_LENGTH,
    ATTR_NAME,
    ATTR_NAMESPACE,
    ATTR_NULLABLE,
    ATTR_PRECISION,
    ATTR_PROPERTY,
    ATTR_REFERENCED_PROPERTY,
    ATTR_RELATIONSHIP,
    ATTR_ROLE,
    ATTR_SCALE,
    ATTR_TO_ROLE,
    ATTR_TYPE,
    COLLECTION_TYPE_PATTERN,
    EDM_V2_NAMESPACE_PATTERN,
    EDM_V4_NAMESPACE_PATTERN,
    SAP_ANNOTATION_LABEL,
    SAP_ANNOTATION_QUICKINFO,
    TAG_ASSOCIATION,
    TAG_DEPENDENT,
    TAG_END,
    TAG_ENTITY_CONTAINER,
    TAG_ENTITY_SET,
    TAG_ENTITY_TYPE,
    TAG_KEY,
    TAG_NAVIGATION_PROPERTY,
    TAG_PRINCIPAL,
    TAG_PROPERTY,
    TAG_PROPERTY_REF,
    TAG_REFERENTIAL_CONSTRAINT,
    TAG_SCHEMA,
    XML_BOOLEAN_FALSE,
    XML_BOOLEAN_TRUE,
    XML_NAMESPACE_CLOSE,
    XML_NAMESPACE_OPEN,
)
from datahub.ingestion.source.sap_mdg.models import (
    ODataAssociation,
    ODataAssociationEnd,
    ODataEntitySet,
    ODataEntityType,
    ODataMetadata,
    ODataNavigationProperty,
    ODataProperty,
    ODataReferentialConstraint,
    ODataVersion,
)


def parse_metadata(content: bytes) -> ODataMetadata:
    root = DET.fromstring(content)
    version = _detect_version(root)

    entity_types: List[ODataEntityType] = []
    entity_sets: List[ODataEntitySet] = []
    associations: List[ODataAssociation] = []

    for schema in _iter_descendants(root, TAG_SCHEMA):
        namespace = _attr(schema, ATTR_NAMESPACE) or ""
        for child in schema:
            local = _local_name(child.tag)
            if local == TAG_ENTITY_TYPE:
                entity_types.append(_parse_entity_type(child, namespace))
            elif local == TAG_ASSOCIATION:
                associations.append(_parse_association(child, namespace))
            elif local == TAG_ENTITY_CONTAINER:
                entity_sets.extend(_parse_entity_sets(child, namespace))

    return ODataMetadata(
        version=version,
        entity_types=entity_types,
        entity_sets=entity_sets,
        associations=associations,
    )


def _local_name(tag: str) -> str:
    return tag.rsplit(XML_NAMESPACE_CLOSE, 1)[-1]


def _namespace_uri(tag: str) -> Optional[str]:
    if tag.startswith(XML_NAMESPACE_OPEN):
        return tag[1:].split(XML_NAMESPACE_CLOSE, 1)[0]
    return None


def _attr(element: Element, local_name: str) -> Optional[str]:
    # SAP annotations live in the sap: namespace while structural attributes are
    # unqualified, so we match attribute keys on their namespace-stripped name.
    for key, value in element.attrib.items():
        if _local_name(key) == local_name:
            return value
    return None


def _children(element: Element, local_name: str) -> Iterator[Element]:
    for child in element:
        if _local_name(child.tag) == local_name:
            yield child


def _iter_descendants(root: Element, local_name: str) -> Iterator[Element]:
    for element in root.iter():
        if _local_name(element.tag) == local_name:
            yield element


def _to_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        # OData permits non-numeric facets such as MaxLength="Max".
        return None


def _unwrap_collection(type_name: str) -> str:
    match = COLLECTION_TYPE_PATTERN.match(type_name)
    return match.group("inner") if match else type_name


def _detect_version(root: Element) -> ODataVersion:
    for element in root.iter():
        uri = _namespace_uri(element.tag)
        if uri is None:
            continue
        if EDM_V4_NAMESPACE_PATTERN.search(uri):
            return ODataVersion.V4
        if EDM_V2_NAMESPACE_PATTERN.search(uri):
            return ODataVersion.V2
    return ODataVersion.UNKNOWN


def _parse_property(element: Element) -> ODataProperty:
    return ODataProperty(
        name=_attr(element, ATTR_NAME) or "",
        type_name=_attr(element, ATTR_TYPE) or "",
        nullable=(_attr(element, ATTR_NULLABLE) or XML_BOOLEAN_TRUE).lower()
        != XML_BOOLEAN_FALSE,
        max_length=_to_int(_attr(element, ATTR_MAX_LENGTH)),
        precision=_to_int(_attr(element, ATTR_PRECISION)),
        scale=_to_int(_attr(element, ATTR_SCALE)),
        label=_attr(element, SAP_ANNOTATION_LABEL),
        quickinfo=_attr(element, SAP_ANNOTATION_QUICKINFO),
    )


def _parse_navigation(element: Element) -> ODataNavigationProperty:
    type_attr = _attr(element, ATTR_TYPE)
    target_type_fqn = _unwrap_collection(type_attr) if type_attr else None

    constraints: List[ODataReferentialConstraint] = []
    for constraint in _children(element, TAG_REFERENTIAL_CONSTRAINT):
        dependent = _attr(constraint, ATTR_PROPERTY)
        principal = _attr(constraint, ATTR_REFERENCED_PROPERTY)
        if dependent and principal:
            constraints.append(
                ODataReferentialConstraint(
                    dependent_property=dependent, principal_property=principal
                )
            )

    return ODataNavigationProperty(
        name=_attr(element, ATTR_NAME) or "",
        target_type_fqn=target_type_fqn,
        relationship=_attr(element, ATTR_RELATIONSHIP),
        from_role=_attr(element, ATTR_FROM_ROLE),
        to_role=_attr(element, ATTR_TO_ROLE),
        referential_constraints=constraints,
    )


def _parse_entity_type(element: Element, namespace: str) -> ODataEntityType:
    key_element = next(_children(element, TAG_KEY), None)
    key_property_names = (
        [
            _attr(ref, ATTR_NAME) or ""
            for ref in _children(key_element, TAG_PROPERTY_REF)
        ]
        if key_element is not None
        else []
    )

    return ODataEntityType(
        namespace=namespace,
        name=_attr(element, ATTR_NAME) or "",
        key_property_names=[name for name in key_property_names if name],
        properties=[_parse_property(p) for p in _children(element, TAG_PROPERTY)],
        navigation_properties=[
            _parse_navigation(n) for n in _children(element, TAG_NAVIGATION_PROPERTY)
        ],
        label=_attr(element, SAP_ANNOTATION_LABEL),
    )


def _parse_association(element: Element, namespace: str) -> ODataAssociation:
    ends = [
        ODataAssociationEnd(
            role=_attr(end, ATTR_ROLE), type_fqn=_attr(end, ATTR_TYPE) or ""
        )
        for end in _children(element, TAG_END)
    ]

    constraints: List[ODataReferentialConstraint] = []
    constraint_element = next(_children(element, TAG_REFERENTIAL_CONSTRAINT), None)
    if constraint_element is not None:
        principal = next(_children(constraint_element, TAG_PRINCIPAL), None)
        dependent = next(_children(constraint_element, TAG_DEPENDENT), None)
        principal_refs = (
            [_attr(r, ATTR_NAME) or "" for r in _children(principal, TAG_PROPERTY_REF)]
            if principal is not None
            else []
        )
        dependent_refs = (
            [_attr(r, ATTR_NAME) or "" for r in _children(dependent, TAG_PROPERTY_REF)]
            if dependent is not None
            else []
        )
        for dependent_ref, principal_ref in zip(
            dependent_refs, principal_refs, strict=False
        ):
            if dependent_ref and principal_ref:
                constraints.append(
                    ODataReferentialConstraint(
                        dependent_property=dependent_ref,
                        principal_property=principal_ref,
                    )
                )

    return ODataAssociation(
        namespace=namespace,
        name=_attr(element, ATTR_NAME) or "",
        ends=ends,
        referential_constraints=constraints,
    )


def _parse_entity_sets(container: Element, namespace: str) -> List[ODataEntitySet]:
    return [
        ODataEntitySet(
            name=_attr(entity_set, ATTR_NAME) or "",
            entity_type_fqn=_attr(entity_set, ATTR_ENTITY_TYPE) or "",
            container_namespace=namespace,
            label=_attr(entity_set, SAP_ANNOTATION_LABEL),
        )
        for entity_set in _children(container, TAG_ENTITY_SET)
    ]
