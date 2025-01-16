import html
import json
import re
from typing import Any, Dict, Iterable, List, Optional, Type

from pydantic import BaseModel, Field

from datahub.ingestion.extractor.json_schema_util import JsonSchemaTranslator
from datahub.metadata.schema_classes import SchemaFieldClass

DEFAULT_VALUE_MAX_LENGTH = 50
DEFAULT_VALUE_TRUNCATION_MESSAGE = "..."


def _truncate_default_value(value: str) -> str:
    if len(value) > DEFAULT_VALUE_MAX_LENGTH:
        return value[:DEFAULT_VALUE_MAX_LENGTH] + DEFAULT_VALUE_TRUNCATION_MESSAGE
    return value


def _format_path_component(path: str) -> str:
    """
    Given a path like 'a.b.c', adds css tags to the components.
    """
    path_components = path.rsplit(".", maxsplit=1)
    if len(path_components) == 1:
        return f'<span className="path-main">{path_components[0]}</span>'

    return (
        f'<span className="path-prefix">{path_components[0]}.</span>'
        f'<span className="path-main">{path_components[1]}</span>'
    )


def _format_type_name(type_name: str) -> str:
    return f'<span className="type-name">{type_name}</span>'


def _format_default_line(default_value: str, has_desc_above: bool) -> str:
    default_value = _truncate_default_value(default_value)
    escaped_value = (
        html.escape(default_value)
        # Replace curly braces to avoid JSX issues.
        .replace("{", "&#123;")
        .replace("}", "&#125;")
        # We also need to replace markdown special characters.
        .replace("*", "&#42;")
        .replace("_", "&#95;")
        .replace("[", "&#91;")
        .replace("]", "&#93;")
        .replace("|", "&#124;")
        .replace("`", "&#96;")
    )
    value_elem = f'<span className="default-value">{escaped_value}</span>'
    return f'<div className="default-line {"default-line-with-docs" if has_desc_above else ""}">Default: {value_elem}</div>'


class FieldRow(BaseModel):
    path: str
    parent: Optional[str]
    type_name: str
    required: bool
    has_default: bool
    default: str
    description: str
    inner_fields: List["FieldRow"] = Field(default_factory=list)
    discriminated_type: Optional[str] = None

    class Component(BaseModel):
        type: str
        field_name: Optional[str]

    # matches any [...] style section inside a field path
    _V2_FIELD_PATH_TOKEN_MATCHER = r"\[[\w.]*[=]*[\w\(\-\ \_\).]*\][\.]*"
    # matches a .?[...] style section inside a field path anchored to the beginning
    _V2_FIELD_PATH_TOKEN_MATCHER_PREFIX = rf"^[\.]*{_V2_FIELD_PATH_TOKEN_MATCHER}"
    _V2_FIELD_PATH_FIELD_NAME_MATCHER = r"^\w+"

    @staticmethod
    def map_field_path_to_components(field_path: str) -> List[Component]:
        m = re.match(FieldRow._V2_FIELD_PATH_TOKEN_MATCHER_PREFIX, field_path)
        v = re.match(FieldRow._V2_FIELD_PATH_FIELD_NAME_MATCHER, field_path)
        components: List[FieldRow.Component] = []
        while m or v:
            token = m.group() if m else v.group()  # type: ignore
            if v:
                if components:
                    if components[-1].field_name is None:
                        components[-1].field_name = token
                    else:
                        components.append(
                            FieldRow.Component(type="non_map_type", field_name=token)
                        )
                else:
                    components.append(
                        FieldRow.Component(type="non_map_type", field_name=token)
                    )

            if m:
                if token.startswith("[version="):
                    pass
                elif "[type=" in token:
                    type_match = re.match(r"[\.]*\[type=(.*)\]", token)
                    if type_match:
                        type_string = type_match.group(1)
                        if components and components[-1].type == "map":
                            if components[-1].field_name is None:
                                pass
                            else:
                                new_component = FieldRow.Component(
                                    type="map_key", field_name="`key`"
                                )
                                components.append(new_component)
                                new_component = FieldRow.Component(
                                    type=type_string, field_name=None
                                )
                                components.append(new_component)
                        if type_string == "map":
                            new_component = FieldRow.Component(
                                type=type_string, field_name=None
                            )
                            components.append(new_component)

            field_path = field_path[m.span()[1] :] if m else field_path[v.span()[1] :]  # type: ignore
            m = re.match(FieldRow._V2_FIELD_PATH_TOKEN_MATCHER_PREFIX, field_path)
            v = re.match(FieldRow._V2_FIELD_PATH_FIELD_NAME_MATCHER, field_path)

        return components

    @staticmethod
    def field_path_to_components(field_path: str) -> List[str]:
        """
        Inverts the field_path v2 format to get the canonical field path
        [version=2.0].[type=x].foo.[type=string(format=uri)].bar => ["foo","bar"]
        """
        if "type=map" not in field_path:
            return re.sub(FieldRow._V2_FIELD_PATH_TOKEN_MATCHER, "", field_path).split(
                "."
            )
        else:
            # fields with maps in them need special handling to insert the `key` fragment
            return [
                c.field_name
                for c in FieldRow.map_field_path_to_components(field_path)
                if c.field_name
            ]

    @classmethod
    def from_schema_field(cls, schema_field: SchemaFieldClass) -> "FieldRow":
        path_components = FieldRow.field_path_to_components(schema_field.fieldPath)

        parent = path_components[-2] if len(path_components) >= 2 else None
        if parent == "`key`":
            # the real parent node is one index above
            parent = path_components[-3]
        json_props = (
            json.loads(schema_field.jsonProps) if schema_field.jsonProps else {}
        )

        required = json_props.get("required", True)
        has_default = "default" in json_props
        default_value = str(json_props.get("default"))

        field_path = ".".join(path_components)

        return FieldRow(
            path=field_path,
            parent=parent,
            type_name=str(schema_field.nativeDataType),
            required=required,
            has_default=has_default,
            default=default_value,
            description=schema_field.description,
            inner_fields=[],
            discriminated_type=schema_field.nativeDataType,
        )

    def get_checkbox(self) -> str:
        if self.required and not self.has_default:
            # Using a non-breaking space to prevent the checkbox from being
            # broken into a new line.
            if not self.parent:  # None and empty string both count
                return '&nbsp;<abbr title="Required">✅</abbr>'
            else:
                return f'&nbsp;<abbr title="Required if {self.parent} is set">❓</abbr>'
        else:
            return ""

    def to_md_line(self) -> str:
        if self.inner_fields:
            if len(self.inner_fields) == 1:
                type_name = self.inner_fields[0].type_name or self.type_name
            else:
                # To deal with unions that have essentially the same simple field path,
                # we combine the type names into a single string.
                type_name = "One of " + ", ".join(
                    [x.type_name for x in self.inner_fields if x.discriminated_type]
                )
        else:
            type_name = self.type_name

        description = self.description.strip()
        description = self.description.replace(
            "\n", " <br /> "
        )  # descriptions with newlines in them break markdown rendering

        md_line = (
            f'| <div className="path-line">{_format_path_component(self.path)}'
            f"{self.get_checkbox()}</div>"
            f' <div className="type-name-line">{_format_type_name(type_name)}</div> '
            f"| {description} "
            f"{_format_default_line(self.default, bool(description)) if self.has_default else ''} |\n"
        )
        return md_line


class FieldHeader(FieldRow):
    def to_md_line(self) -> str:
        return "\n".join(
            [
                "| Field | Description |",
                "|:--- |:--- |",
                "",
            ]
        )

    def __init__(self):
        pass


def get_prefixed_name(field_prefix: Optional[str], field_name: Optional[str]) -> str:
    assert (
        field_prefix or field_name
    ), "One of field_prefix or field_name should be present"
    return (
        f"{field_prefix}.{field_name}"  # type: ignore
        if field_prefix and field_name
        else field_name
        if not field_prefix
        else field_prefix
    )


def custom_comparator(path: str) -> str:
    """
    Projects a string onto a separate space
    Low_prio string will start with Z else start with A
    Number of field paths will add the second set of letters: 00 - 99

    """
    opt1 = path
    prio_value = priority_value(opt1)
    projection = f"{prio_value}"
    projection = f"{projection}{opt1}"
    return projection


class FieldTree:
    """
    A helper class that re-constructs the tree hierarchy of schema fields
    to help sort fields by importance while keeping nesting intact
    """

    def __init__(self, field: Optional[FieldRow] = None):
        self.field = field
        self.fields: Dict[str, "FieldTree"] = {}

    def add_field(self, row: FieldRow, path: Optional[str] = None) -> "FieldTree":
        # logger.warn(f"Add field: path:{path}, row:{row}")
        if self.field and self.field.path == row.path:
            # we have an incoming field with the same path as us, this is probably a union variant
            # attach to existing field
            self.field.inner_fields.append(row)
        else:
            path = path if path is not None else row.path
            top_level_field = path.split(".")[0]
            if top_level_field in self.fields:
                self.fields[top_level_field].add_field(
                    row, ".".join(path.split(".")[1:])
                )
            else:
                self.fields[top_level_field] = FieldTree(field=row)
        # logger.warn(f"{self}")
        return self

    def sort(self):
        # Required fields before optionals
        required_fields = {
            k: v for k, v in self.fields.items() if v.field and v.field.required
        }
        optional_fields = {
            k: v for k, v in self.fields.items() if v.field and not v.field.required
        }

        self.sorted_fields = []
        for field_map in [required_fields, optional_fields]:
            # Top-level fields before fields with nesting
            self.sorted_fields.extend(
                sorted(
                    [f for f, val in field_map.items() if val.fields == {}],
                    key=custom_comparator,
                )
            )
            self.sorted_fields.extend(
                sorted(
                    [f for f, val in field_map.items() if val.fields != {}],
                    key=custom_comparator,
                )
            )

        for field_tree in self.fields.values():
            field_tree.sort()

    def get_fields(self) -> Iterable[FieldRow]:
        if self.field:
            yield self.field
        for key in self.sorted_fields:
            yield from self.fields[key].get_fields()

    def __repr__(self) -> str:
        result = {}
        if self.field:
            result["_self"] = json.loads(json.dumps(self.field.dict()))
        for f in self.fields:
            result[f] = json.loads(str(self.fields[f]))
        return json.dumps(result, indent=2)


def priority_value(path: str) -> str:
    # A map of low value tokens to their relative importance
    low_value_token_map = {
        "env": "X",
        "classification": "Y",
        "profiling": "Y",
        "stateful_ingestion": "Z",
    }
    tokens = path.split(".")
    for low_value_token in low_value_token_map:
        if low_value_token in tokens:
            return low_value_token_map[low_value_token]

    # everything else high-prio
    return "A"


def gen_md_table_from_json_schema(schema_dict: Dict[str, Any]) -> str:
    # we don't want default field values to be injected into the description of the field
    JsonSchemaTranslator._INJECT_DEFAULTS_INTO_DESCRIPTION = False
    schema_fields = list(JsonSchemaTranslator.get_fields_from_schema(schema_dict))
    result: List[str] = [FieldHeader().to_md_line()]

    field_tree = FieldTree(field=None)
    for field in schema_fields:
        row: FieldRow = FieldRow.from_schema_field(field)
        field_tree.add_field(row)

    field_tree.sort()

    for row in field_tree.get_fields():
        result.append(row.to_md_line())

    # Wrap with a .config-table div.
    result = ["\n<div className='config-table'>\n\n", *result, "\n</div>\n"]

    return "".join(result)


def gen_md_table_from_pydantic(model: Type[BaseModel]) -> str:
    return gen_md_table_from_json_schema(model.schema())


if __name__ == "__main__":
    # Simple test code.
    from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config

    print("".join(gen_md_table_from_pydantic(SnowflakeV2Config)))
