from typing import List, Optional, Tuple, Union

from avro.schema import Field, RecordSchema

from datahub.metadata.schema_classes import DictWrapper

_Path = List[Union[str, int]]


def _field_java_class(field_schema: Field) -> Optional[str]:
    return field_schema.props.get("java", {}).get("class")


def _is_urn_array_field(model: DictWrapper, field: str) -> bool:
    fullname = model.RECORD_SCHEMA.fullname
    if fullname == "com.linkedin.pegasus2avro.dataset.FineGrainedLineage":
        if field in {"upstreams", "downstreams"}:
            return True

    return False


def list_urns_with_path(model: DictWrapper) -> List[Tuple[str, _Path]]:
    schema: RecordSchema = model.RECORD_SCHEMA

    urns: List[Tuple[str, _Path]] = []

    for key, value in model.items():
        if not value:
            continue

        field_schema: Field = schema.fields_dict[key]
        java_class = _field_java_class(field_schema)

        is_urn_field = java_class and java_class.endswith("Urn")
        is_urn_array = _is_urn_array_field(model, key)

        if isinstance(value, DictWrapper):
            for urn, path in list_urns_with_path(value):
                urns.append((urn, [key, *path]))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, DictWrapper):
                    for urn, path in list_urns_with_path(item):
                        urns.append((urn, [key, i, *path]))
                elif is_urn_array:
                    urns.append((item, [key, i]))
        elif is_urn_field:
            urns.append((value, [key]))

    return urns
