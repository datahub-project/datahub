from typing import List, Tuple, Union

from avro.schema import Field, RecordSchema

from datahub.metadata.schema_classes import DictWrapper

_Path = List[Union[str, int]]


def list_urns_with_path(model: DictWrapper) -> List[Tuple[str, _Path]]:
    schema: RecordSchema = model.RECORD_SCHEMA

    urns: List[Tuple[str, _Path]] = []

    for key, value in model.items():
        if not value:
            continue

        field_schema: Field = schema.fields_dict[key]
        is_urn = field_schema.get_prop("Urn") is not None

        if isinstance(value, DictWrapper):
            for urn, path in list_urns_with_path(value):
                urns.append((urn, [key, *path]))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, DictWrapper):
                    for urn, path in list_urns_with_path(item):
                        urns.append((urn, [key, i, *path]))
                elif is_urn:
                    urns.append((item, [key, i]))
        elif is_urn:
            urns.append((value, [key]))

    return urns
