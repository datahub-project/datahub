import re
from typing import Dict, Iterator, List, Optional
from xml.etree.ElementTree import Element

import defusedxml.ElementTree

from datahub.ingestion.source.boomi.constants import (
    DB_ACTION_PROFILE_ATTRIBUTES,
    FIELD_ID_QUERY,
    FIELD_ID_SCHEMA_NAME,
    FIELD_ID_URL,
    GENERIC_OPERATION_PROFILE_ATTRIBUTES,
    GET_ACTION_SUFFIX,
    JDBC_VALUE_PREFIX,
    PROFILE_FIELD_ELEMENTS,
    PROFILE_PATH_SEPARATOR,
    READ_OPERATION_TYPES,
    SEND_ACTION_SUFFIX,
    SHAPE_TYPE_CONNECTOR_ACTION,
    SHAPE_TYPE_MAP,
    SHAPE_TYPE_PROCESS_CALL,
    SQL_FIELD_ID_MARKERS,
    VALUE_FALSE,
    WRITE_OPERATION_TYPES,
    XML_ATTR_ACTION_TYPE,
    XML_ATTR_ADDITIONAL,
    XML_ATTR_CONNECTION_ID,
    XML_ATTR_CONNECTOR_TYPE,
    XML_ATTR_CUSTOM_OPERATION_TYPE,
    XML_ATTR_DB_NAME,
    XML_ATTR_DRIVER_ID,
    XML_ATTR_FOLDER_FULL_PATH,
    XML_ATTR_FROM_FUNCTION,
    XML_ATTR_FROM_KEY,
    XML_ATTR_FROM_PROFILE,
    XML_ATTR_FROM_TYPE,
    XML_ATTR_HOST,
    XML_ATTR_ID,
    XML_ATTR_IS_MAPPABLE,
    XML_ATTR_KEY,
    XML_ATTR_MAP_ID,
    XML_ATTR_NAME,
    XML_ATTR_OBJECT_NAME,
    XML_ATTR_OBJECT_TYPE_ID,
    XML_ATTR_OBJECT_TYPE_NAME,
    XML_ATTR_OPERATION_ID,
    XML_ATTR_OPERATION_TYPE,
    XML_ATTR_PORT,
    XML_ATTR_PROCESS_ID,
    XML_ATTR_PROFILE_ID,
    XML_ATTR_SHAPE_TYPE,
    XML_ATTR_STATEMENT_TYPE,
    XML_ATTR_SUB_TYPE,
    XML_ATTR_TABLE_NAME,
    XML_ATTR_TO_FUNCTION,
    XML_ATTR_TO_KEY,
    XML_ATTR_TO_PROFILE,
    XML_ATTR_TO_SHAPE,
    XML_ATTR_TO_TYPE,
    XML_ATTR_TYPE,
    XML_ATTR_URL_FORMAT,
    XML_ATTR_VALUE,
    XML_DB_PROFILE_ELEMENTS,
    XML_ELEMENT_CONNECTOR_ACTION,
    XML_ELEMENT_DATA_ELEMENTS,
    XML_ELEMENT_DB_CONNECTION_SETTINGS,
    XML_ELEMENT_DB_STATEMENT,
    XML_ELEMENT_DESCRIPTION,
    XML_ELEMENT_DRAGPOINT,
    XML_ELEMENT_FIELD,
    XML_ELEMENT_FUNCTION_STEP,
    XML_ELEMENT_GENERIC_CONNECTION_CONFIG,
    XML_ELEMENT_GENERIC_OPERATION_CONFIG,
    XML_ELEMENT_MAP,
    XML_ELEMENT_MAP_SHAPE,
    XML_ELEMENT_MAPPING,
    XML_ELEMENT_PROCESS_CALL,
    XML_ELEMENT_SHAPE,
    XML_ELEMENT_SQL,
    XML_NAMESPACE_END,
    OperationDirection,
)
from datahub.ingestion.source.boomi.models import (
    ConnectionComponent,
    ConnectorActionShape,
    MapComponent,
    MapFunction,
    MapMapping,
    MapShape,
    OperationComponent,
    ProcessComponentInfo,
    ProfileComponent,
    ProfileField,
)

# Field ids alone are not enough: settings such as `queryTimeout` also match
# the id markers, so a value only counts as SQL when it opens with a statement.
_SQL_STATEMENT_RE = re.compile(
    r"^\s*(?:select|with|insert|update|delete|merge)\b", re.IGNORECASE
)


def local_name(tag: str) -> str:
    # Real exports mix `bns:`-prefixed elements with `xmlns=""` children, so
    # elements are matched on their local name regardless of namespace.
    return tag.rsplit(XML_NAMESPACE_END, 1)[-1]


def _iter_local(root: Element, name: str) -> Iterator[Element]:
    for element in root.iter():
        if local_name(element.tag) == name:
            yield element


def _find_local(root: Optional[Element], name: str) -> Optional[Element]:
    if root is None:
        return None
    return next(_iter_local(root, name), None)


def _attr(element: Optional[Element], name: str) -> Optional[str]:
    if element is None:
        return None
    return (element.attrib.get(name) or "").strip() or None


def _field_values(element: Optional[Element]) -> Dict[str, str]:
    # Generic connector configs hold their settings as <field id=".." value="..">.
    if element is None:
        return {}
    values: Dict[str, str] = {}
    for field in _iter_local(element, XML_ELEMENT_FIELD):
        field_id = _attr(field, XML_ATTR_ID)
        value = _attr(field, XML_ATTR_VALUE)
        if field_id and value:
            values[field_id] = value
    return values


def _distinct(values: List[str]) -> List[str]:
    return list(dict.fromkeys(values))


def _jdbc_urls(fields: Dict[str, str]) -> List[str]:
    return _distinct(
        [
            value
            for value in fields.values()
            if value.lower().startswith(JDBC_VALUE_PREFIX)
        ]
    )


def _sql_statements(fields: Dict[str, str]) -> List[str]:
    return _distinct(
        [
            value
            for field_id, value in fields.items()
            if any(marker in field_id.lower() for marker in SQL_FIELD_ID_MARKERS)
            and _SQL_STATEMENT_RE.match(value)
        ]
    )


def parse_process_component(xml_text: str) -> ProcessComponentInfo:
    root = defusedxml.ElementTree.fromstring(xml_text)
    description: Optional[str] = None
    for child in root:
        if local_name(child.tag) == XML_ELEMENT_DESCRIPTION:
            description = (child.text or "").strip() or None
            break
    connector_actions: List[ConnectorActionShape] = []
    process_call_ids: List[str] = []
    map_shapes: List[MapShape] = []
    next_shapes: Dict[str, List[str]] = {}
    for shape in _iter_local(root, XML_ELEMENT_SHAPE):
        shape_type = _attr(shape, XML_ATTR_SHAPE_TYPE)
        shape_name = _attr(shape, XML_ATTR_NAME)
        if shape_name:
            targets = [
                _attr(dragpoint, XML_ATTR_TO_SHAPE)
                for dragpoint in _iter_local(shape, XML_ELEMENT_DRAGPOINT)
            ]
            next_shapes[shape_name] = [target for target in targets if target]
        if shape_type == SHAPE_TYPE_CONNECTOR_ACTION:
            action = _find_local(shape, XML_ELEMENT_CONNECTOR_ACTION)
            if action is not None:
                connector_actions.append(
                    ConnectorActionShape(
                        shape_name=shape_name,
                        connector_type=_attr(action, XML_ATTR_CONNECTOR_TYPE),
                        action_type=_attr(action, XML_ATTR_ACTION_TYPE),
                        connection_id=_attr(action, XML_ATTR_CONNECTION_ID),
                        operation_id=_attr(action, XML_ATTR_OPERATION_ID),
                    )
                )
        elif shape_type == SHAPE_TYPE_PROCESS_CALL:
            process_id = _attr(
                _find_local(shape, XML_ELEMENT_PROCESS_CALL), XML_ATTR_PROCESS_ID
            )
            if process_id:
                process_call_ids.append(process_id)
        elif shape_type == SHAPE_TYPE_MAP:
            map_id = _attr(_find_local(shape, XML_ELEMENT_MAP_SHAPE), XML_ATTR_MAP_ID)
            if shape_name and map_id:
                map_shapes.append(MapShape(shape_name=shape_name, map_id=map_id))
    return ProcessComponentInfo(
        description=description,
        folder_full_path=root.attrib.get(XML_ATTR_FOLDER_FULL_PATH),
        connector_actions=connector_actions,
        process_call_ids=process_call_ids,
        map_shapes=map_shapes,
        next_shapes=next_shapes,
    )


def parse_connection_component(xml_text: str) -> ConnectionComponent:
    root = defusedxml.ElementTree.fromstring(xml_text)
    settings = _find_local(root, XML_ELEMENT_DB_CONNECTION_SETTINGS)
    fields = _field_values(_find_local(root, XML_ELEMENT_GENERIC_CONNECTION_CONFIG))
    return ConnectionComponent(
        sub_type=_attr(root, XML_ATTR_SUB_TYPE),
        driver_id=_attr(settings, XML_ATTR_DRIVER_ID),
        host=_attr(settings, XML_ATTR_HOST),
        port=_attr(settings, XML_ATTR_PORT),
        dbname=_attr(settings, XML_ATTR_DB_NAME),
        additional=_attr(settings, XML_ATTR_ADDITIONAL),
        url_format=_attr(settings, XML_ATTR_URL_FORMAT),
        url=fields.get(FIELD_ID_URL),
        schema_name=fields.get(FIELD_ID_SCHEMA_NAME),
        jdbc_urls=_jdbc_urls(fields),
    )


def operation_direction(
    action_element: Optional[str],
    operation_type: Optional[str],
    custom_operation_type: Optional[str],
) -> OperationDirection:
    if action_element:
        if action_element.endswith(GET_ACTION_SUFFIX):
            return OperationDirection.READ
        if action_element.endswith(SEND_ACTION_SUFFIX):
            return OperationDirection.WRITE
    # customOperationType is the more specific of the two when both are set.
    for value in (custom_operation_type, operation_type):
        normalized = (value or "").upper()
        if normalized in READ_OPERATION_TYPES:
            return OperationDirection.READ
        if normalized in WRITE_OPERATION_TYPES:
            return OperationDirection.WRITE
    return OperationDirection.UNKNOWN


def _find_action_element(root: Element) -> Optional[Element]:
    for element in root.iter():
        name = local_name(element.tag)
        if name.endswith(GET_ACTION_SUFFIX) or name.endswith(SEND_ACTION_SUFFIX):
            return element
    return None


def parse_operation_component(xml_text: str) -> OperationComponent:
    root = defusedxml.ElementTree.fromstring(xml_text)
    action = _find_action_element(root)
    action_name = local_name(action.tag) if action is not None else None
    generic = _find_local(root, XML_ELEMENT_GENERIC_OPERATION_CONFIG)
    fields = _field_values(generic)
    operation_type = _attr(generic, XML_ATTR_OPERATION_TYPE)
    custom_operation_type = _attr(generic, XML_ATTR_CUSTOM_OPERATION_TYPE)
    profile_ids: List[str] = []
    candidates: List[Optional[str]] = []
    if action is not None:
        candidates.extend(
            _attr(child, XML_ATTR_PROFILE_ID)
            for child in action
            if local_name(child.tag) in XML_DB_PROFILE_ELEMENTS
        )
    candidates.extend(
        _attr(action, attribute) for attribute in DB_ACTION_PROFILE_ATTRIBUTES
    )
    candidates.extend(
        _attr(generic, attribute) for attribute in GENERIC_OPERATION_PROFILE_ATTRIBUTES
    )
    for profile_id in candidates:
        if profile_id and profile_id not in profile_ids:
            profile_ids.append(profile_id)
    return OperationComponent(
        sub_type=_attr(root, XML_ATTR_SUB_TYPE),
        direction=operation_direction(
            action_name, operation_type, custom_operation_type
        ),
        action_element=action_name,
        operation_type=operation_type,
        custom_operation_type=custom_operation_type,
        object_type_id=_attr(generic, XML_ATTR_OBJECT_TYPE_ID),
        object_type_name=_attr(generic, XML_ATTR_OBJECT_TYPE_NAME),
        query=fields.get(FIELD_ID_QUERY),
        sql_statements=_sql_statements(fields),
        object_name=_attr(action, XML_ATTR_OBJECT_NAME),
        profile_ids=profile_ids,
    )


def _profile_fields(
    element: Element,
    path: List[str],
    depth: int,
    fields: Dict[str, ProfileField],
) -> None:
    for child in element:
        name = _attr(child, XML_ATTR_NAME)
        key = _attr(child, XML_ATTR_KEY)
        child_path = path + [name] if name else path
        child_depth = depth
        if local_name(child.tag) in PROFILE_FIELD_ELEMENTS and name and key:
            child_depth += 1
            # Elements without isMappable, such as database fields, are values.
            if (_attr(child, XML_ATTR_IS_MAPPABLE) or "").lower() != VALUE_FALSE:
                fields[key] = ProfileField(
                    name=name,
                    path=PROFILE_PATH_SEPARATOR.join(child_path),
                    depth=depth,
                )
        _profile_fields(child, child_path, child_depth, fields)


def parse_profile_component(xml_text: str) -> ProfileComponent:
    root = defusedxml.ElementTree.fromstring(xml_text)
    statement = _find_local(root, XML_ELEMENT_DB_STATEMENT)
    sql_element = _find_local(statement, XML_ELEMENT_SQL)
    sql = (sql_element.text or "").strip() if sql_element is not None else ""
    fields: Dict[str, ProfileField] = {}
    data_elements = _find_local(root, XML_ELEMENT_DATA_ELEMENTS)
    if data_elements is not None:
        _profile_fields(data_elements, [], 0, fields)
    return ProfileComponent(
        component_type=_attr(root, XML_ATTR_TYPE),
        table_name=_attr(statement, XML_ATTR_TABLE_NAME),
        statement_type=_attr(statement, XML_ATTR_STATEMENT_TYPE),
        sql=sql or None,
        fields=fields,
    )


def parse_map_component(xml_text: str) -> MapComponent:
    root = defusedxml.ElementTree.fromstring(xml_text)
    map_element = _find_local(root, XML_ELEMENT_MAP)
    mappings: List[MapMapping] = []
    functions: Dict[str, MapFunction] = {}
    if map_element is not None:
        for mapping in _iter_local(map_element, XML_ELEMENT_MAPPING):
            from_key = _attr(mapping, XML_ATTR_FROM_KEY)
            from_type = _attr(mapping, XML_ATTR_FROM_TYPE)
            to_key = _attr(mapping, XML_ATTR_TO_KEY)
            to_type = _attr(mapping, XML_ATTR_TO_TYPE)
            if not (from_key and from_type and to_key and to_type):
                continue
            mappings.append(
                MapMapping(
                    from_key=from_key,
                    from_type=from_type,
                    from_function=_attr(mapping, XML_ATTR_FROM_FUNCTION),
                    to_key=to_key,
                    to_type=to_type,
                    to_function=_attr(mapping, XML_ATTR_TO_FUNCTION),
                )
            )
        for step in _iter_local(map_element, XML_ELEMENT_FUNCTION_STEP):
            key = _attr(step, XML_ATTR_KEY)
            if key:
                functions[key] = MapFunction(
                    key=key,
                    type=_attr(step, XML_ATTR_TYPE),
                    name=_attr(step, XML_ATTR_NAME),
                )
    return MapComponent(
        from_profile=_attr(map_element, XML_ATTR_FROM_PROFILE),
        to_profile=_attr(map_element, XML_ATTR_TO_PROFILE),
        mappings=mappings,
        functions=functions,
    )
