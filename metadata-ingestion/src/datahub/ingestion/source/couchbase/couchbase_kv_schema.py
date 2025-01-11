from collections import Counter
from typing import Any, Counter as CounterType, Dict, Tuple, Union, List

from typing_extensions import TypedDict


class BasicSchemaDescription(TypedDict):
    types: CounterType[Union[type, str]]  # field types and times seen
    count: int  # times the field was seen


class SchemaDescription(BasicSchemaDescription):
    delimited_name: str  # collapsed field name
    # we use 'mixed' to denote mixed types, so we need a str here
    type: Union[type, str]  # collapsed type
    nullable: bool  # if field is ever missing


class CouchbaseFieldData:
    types: Tuple[str] = ()
    samples: List[Any] = []


def json_schema(schema: dict, upper_key: str = None, samples: List[Any] = None) -> Union[dict, List[dict], CouchbaseFieldData]:

    def process_samples(_key: str, _samples: List[Any]):
        if len(_samples) > 0 and type(_samples[0]) is list:
            if type(_samples[0][0]) is dict and _key is not None:
                subset = []
                for array in _samples:
                    for item in array:
                        if item.get(_key):
                            subset.append(item[_key])
                return subset
            subset = []
            for array in _samples:
                subset.extend(array)
            return subset
        return _samples

    if schema.get('type') == 'object':
        if upper_key is None:
            return json_schema(schema['properties'], samples=samples)
        else:
            return {upper_key: json_schema(schema['properties'], samples=samples)}

    elif schema.get('type') == 'array':
        if upper_key is None:
            return [json_schema(schema['items'], samples=schema.get('samples'))]
        else:
            return {upper_key: [json_schema(schema['items'], samples=schema.get('samples'))]}

    elif schema.get('type'):
        field_data_ = CouchbaseFieldData()
        if isinstance(schema.get('type'), list):
            for type_ in schema.get('type'):
                field_data_.types += (type_,)
        else:
            field_data_.types += (schema.get('type'),)
        field_data_.samples = process_samples(upper_key, samples) if samples else process_samples(upper_key, schema.get('samples'))
        if upper_key is None:
            return field_data_
        else:
            return {upper_key: field_data_}

    else:
        result = {}
        for key, value in schema.items():
            result.update(json_schema(value, key, samples=samples))
        return result


def flatten(path: List[str], data: Any, truncate: bool = True) -> Tuple[str, CouchbaseFieldData]:

    if isinstance(data, dict):
        if path and truncate:
            field_data_ = CouchbaseFieldData()
            field_data_.types = ('object',)
            yield '.'.join(path), field_data_
        for key, value in data.items():
            path.append(key)
            yield from flatten(path, value)
            if isinstance(value, dict) or isinstance(value, list):
                del path[-1]

    elif isinstance(data, list):
        if path and not isinstance(data[0], CouchbaseFieldData):
            field_data_ = CouchbaseFieldData()
            field_data_.types = ('array',)
            yield '.'.join(path), field_data_
        else:
            data[0].types += ('array',)
        for value in data:
            yield from flatten(path, value, False)

    else:
        yield '.'.join(path), data
        if len(path) > 0 and truncate:
            del path[-1]


def discard(_tuple: Tuple[str, ...], element: str) -> Tuple[str, ...]:
    if element in _tuple:
        _list = list(_tuple)
        _list.remove(element)
        _tuple = tuple(_list)
        return _tuple
    return _tuple


def construct_schema(
    collection: Dict[str, Any]
) -> Dict[Tuple[str, ...], SchemaDescription]:
    """
    Construct JSON schema.

    For each field (represented as a tuple to handle nested items), reports the following:
        - `types`: Python types of field values
        - `count`: Number of times the field was encountered
        - `type`: type of the field if `types` is just a single value, otherwise `mixed`
        - `nullable`: if field is ever null/missing
        - `delimited_name`: name of the field, joined by a given delimiter

    Parameters
    ----------
        collection:
            the JSON schema of the collection.
    """

    extended_schema: Dict[Tuple[str, ...], SchemaDescription] = {}

    parsed = json_schema(collection)

    for field_path, field_data in flatten([], parsed):
        field_type: Union[str, type] = "mixed"

        if len(field_data.types) == 1:
            field_type = next(iter(field_data.types))
        elif len(field_data.types) > 1 and "array" in field_data.types:
            field_data.types = discard(field_data.types, "array")
            if len(field_data.types) == 1:
                field_type = next(iter(field_data.types))
        is_nullable = "null" in field_data.types
        field_extended: SchemaDescription = {
            "types": Counter(field_data.types),
            "count": len(field_path.split('.')),
            "nullable": is_nullable,
            "delimited_name": field_path,
            "type": field_type,
        }

        extended_schema[field_path] = field_extended

    return extended_schema
