import collections
import copy
import json
import re
import textwrap
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Union

import avro.schema
import click
import pydantic
import yaml
from avrogen import write_schema_files

ENTITY_CATEGORY_UNSET = "_unset_"


class EntityType(pydantic.BaseModel):
    name: str
    doc: Optional[str] = None
    category: str = ENTITY_CATEGORY_UNSET

    keyAspect: str
    aspects: List[str]


def load_entity_registry(entity_registry_file: Path) -> List[EntityType]:
    with entity_registry_file.open() as f:
        raw_entity_registry = yaml.safe_load(f)

    entities = pydantic.parse_obj_as(List[EntityType], raw_entity_registry["entities"])
    return entities


def load_schema_file(schema_file: Union[str, Path]) -> dict:
    raw_schema_text = Path(schema_file).read_text()
    return json.loads(raw_schema_text)


def load_schemas(schemas_path: str) -> Dict[str, dict]:
    required_schema_files = {
        "mxe/MetadataChangeEvent.avsc",
        "mxe/MetadataChangeProposal.avsc",
        "usage/UsageAggregation.avsc",
        "mxe/MetadataChangeLog.avsc",
        "mxe/PlatformEvent.avsc",
        "platform/event/v1/EntityChangeEvent.avsc",
        "metadata/query/filter/Filter.avsc",  # temporarily added to test reserved keywords support
    }

    # Find all the aspect schemas / other important schemas.
    schema_files: List[Path] = []
    for schema_file in Path(schemas_path).glob("**/*.avsc"):
        relative_path = schema_file.relative_to(schemas_path).as_posix()
        if relative_path in required_schema_files:
            schema_files.append(schema_file)
            required_schema_files.remove(relative_path)
        elif load_schema_file(schema_file).get("Aspect"):
            schema_files.append(schema_file)

    assert not required_schema_files, f"Schema files not found: {required_schema_files}"

    schemas: Dict[str, dict] = {}
    for schema_file in schema_files:
        schema = load_schema_file(schema_file)
        schemas[Path(schema_file).stem] = schema

    return schemas


def patch_schemas(schemas: Dict[str, dict], pdl_path: Path) -> Dict[str, dict]:
    # We can easily find normal urn types using the generated avro schema,
    # but for arrays of urns there's nothing in the avro schema and hence
    # we have to look in the PDL files instead.
    urn_arrays: Dict[
        str, List[Tuple[str, str]]
    ] = {}  # schema name -> list of (field name, type)

    # First, we need to load the PDL files and find all urn arrays.
    for pdl_file in Path(pdl_path).glob("**/*.pdl"):
        pdl_text = pdl_file.read_text()

        # TRICKY: We assume that all urn types end with "Urn".
        arrays = re.findall(
            r"^\s*(\w+)\s*:\s*(?:optional\s+)?array\[(\w*Urn)\]",
            pdl_text,
            re.MULTILINE,
        )
        if arrays:
            schema_name = pdl_file.stem
            urn_arrays[schema_name] = [(item[0], item[1]) for item in arrays]

    # Then, we can patch each schema.
    patched_schemas = {}
    for name, schema in schemas.items():
        patched_schemas[name] = patch_schema(schema, urn_arrays)

    return patched_schemas


def patch_schema(schema: dict, urn_arrays: Dict[str, List[Tuple[str, str]]]) -> dict:
    """
    This method patches the schema to add an "Urn" property to all urn fields.
    Because the inner type in an array is not a named Avro schema, for urn arrays
    we annotate the array field and add an "urn_is_array" property.
    """

    # We're using Names() to generate a full list of embedded schemas.
    all_schemas = avro.schema.Names()
    patched = avro.schema.make_avsc_object(schema, names=all_schemas)

    for nested in all_schemas.names.values():
        if isinstance(nested, (avro.schema.EnumSchema, avro.schema.FixedSchema)):
            continue
        assert isinstance(nested, avro.schema.RecordSchema)

        # Patch normal urn types.
        field: avro.schema.Field
        for field in nested.fields:
            field_props: dict = field.props  # type: ignore
            java_props: dict = field_props.get("java", {})
            java_class: Optional[str] = java_props.get("class")
            if java_class and java_class.startswith(
                "com.linkedin.pegasus2avro.common.urn."
            ):
                type = java_class.split(".")[-1]
                entity_types = field_props.get("Relationship", {}).get(
                    "entityTypes", []
                )

                field.set_prop("Urn", type)
                if entity_types:
                    field.set_prop("entityTypes", entity_types)

        # Patch array urn types.
        if nested.name in urn_arrays:
            mapping = urn_arrays[nested.name]

            for field_name, type in mapping:
                field = nested.fields_dict[field_name]
                field.set_prop("Urn", type)
                field.set_prop("urn_is_array", True)

    return patched.to_json()  # type: ignore


def merge_schemas(schemas_obj: List[dict]) -> str:
    # Combine schemas as a "union" of all of the types.
    merged = ["null"] + schemas_obj

    # Patch add_name method to NOT complain about duplicate names.
    class NamesWithDups(avro.schema.Names):
        def add_name(self, name_attr, space_attr, new_schema):
            to_add = avro.schema.Name(name_attr, space_attr, self.default_namespace)
            assert to_add.fullname
            self.names[to_add.fullname] = new_schema
            return to_add

    cleaned_schema = avro.schema.make_avsc_object(merged, names=NamesWithDups())

    # Convert back to an Avro schema JSON representation.
    out_schema = cleaned_schema.to_json()
    encoded = json.dumps(out_schema, indent=2)
    return encoded


autogen_header = """# mypy: ignore-errors
# flake8: noqa

# This file is autogenerated by /metadata-ingestion/scripts/avro_codegen.py
# Do not modify manually!

# pylint: skip-file
# fmt: off
"""
autogen_footer = """
# fmt: on
"""


def suppress_checks_in_file(filepath: Union[str, Path]) -> None:
    """
    Adds a couple lines to the top of an autogenerated file:
        - Comments to suppress flake8 and black.
        - A note stating that the file was autogenerated.
    """

    with open(filepath, "r+") as f:
        contents = f.read()

        f.seek(0, 0)
        f.write(autogen_header)
        f.write(contents)
        f.write(autogen_footer)


def add_avro_python3_warning(filepath: Path) -> None:
    contents = filepath.read_text()

    contents = f"""
# The SchemaFromJSONData method only exists in avro-python3, but is called make_avsc_object in avro.
# We can use this fact to detect conflicts between the two packages. Pip won't detect those conflicts
# because both are namespace packages, and hence are allowed to overwrite files from each other.
# This means that installation order matters, which is a pretty unintuitive outcome.
# See https://github.com/pypa/pip/issues/4625 for details.
try:
    from avro.schema import SchemaFromJSONData  # type: ignore
    import warnings

    warnings.warn("It seems like 'avro-python3' is installed, which conflicts with the 'avro' package used by datahub. "
                + "Try running `pip uninstall avro-python3 && pip install --upgrade --force-reinstall avro` to fix this issue.")
except ImportError:
    pass

{contents}
    """

    filepath.write_text(contents)


load_schema_method = """
import functools
import pathlib

@functools.lru_cache(maxsize=None)
def _load_schema(schema_name: str) -> str:
    return (pathlib.Path(__file__).parent / f"{schema_name}.avsc").read_text()
"""
individual_schema_method = """
def get{schema_name}Schema() -> str:
    return _load_schema("{schema_name}")
"""


def make_load_schema_methods(schemas: Iterable[str]) -> str:
    return load_schema_method + "".join(
        individual_schema_method.format(schema_name=schema) for schema in schemas
    )


def save_raw_schemas(schema_save_dir: Path, schemas: Dict[str, dict]) -> None:
    # Save raw avsc files.
    for name, schema in schemas.items():
        (schema_save_dir / f"{name}.avsc").write_text(json.dumps(schema, indent=2))

    # Add getXSchema methods.
    with open(schema_save_dir / "__init__.py", "w") as schema_dir_init:
        schema_dir_init.write(make_load_schema_methods(schemas.keys()))


def annotate_aspects(aspects: List[dict], schema_class_file: Path) -> None:
    schema_classes_lines = schema_class_file.read_text().splitlines()
    line_lookup_table = {line: i for i, line in enumerate(schema_classes_lines)}

    # Import the _Aspect class.
    schema_classes_lines[
        line_lookup_table["__SCHEMAS: Dict[str, RecordSchema] = {}"]
    ] += """

from datahub._codegen.aspect import _Aspect
"""

    for aspect in aspects:
        className = f'{aspect["name"]}Class'
        aspectName = aspect["Aspect"]["name"]
        class_def_original = f"class {className}(DictWrapper):"

        # Make the aspects inherit from the Aspect class.
        class_def_line = line_lookup_table[class_def_original]
        schema_classes_lines[class_def_line] = f"class {className}(_Aspect):"

        # Define the ASPECT_NAME class attribute.
        # There's always an empty line between the docstring and the RECORD_SCHEMA class attribute.
        # We need to find it and insert our line of code there.
        empty_line = class_def_line + 1
        while not (
            schema_classes_lines[empty_line].strip() == ""
            and schema_classes_lines[empty_line + 1]
            .strip()
            .startswith("RECORD_SCHEMA = ")
        ):
            empty_line += 1
        schema_classes_lines[empty_line] = "\n"
        schema_classes_lines[empty_line] += f"\n    ASPECT_NAME = '{aspectName}'"
        if "type" in aspect["Aspect"]:
            schema_classes_lines[
                empty_line
            ] += f"\n    ASPECT_TYPE = '{aspect['Aspect']['type']}'"

        aspect_info = {
            k: v for k, v in aspect["Aspect"].items() if k not in {"name", "type"}
        }
        schema_classes_lines[empty_line] += f"\n    ASPECT_INFO = {aspect_info}"

        schema_classes_lines[empty_line + 1] += "\n"

    # Finally, generate a big list of all available aspects.
    newline = "\n"
    schema_classes_lines.append(
        f"""
ASPECT_CLASSES: List[Type[_Aspect]] = [
    {f',{newline}    '.join(f"{aspect['name']}Class" for aspect in aspects)}
]

ASPECT_NAME_MAP: Dict[str, Type[_Aspect]] = {{
    aspect.get_aspect_name(): aspect
    for aspect in ASPECT_CLASSES
}}

from typing_extensions import TypedDict

class AspectBag(TypedDict, total=False):
    {f'{newline}    '.join(f"{aspect['Aspect']['name']}: {aspect['name']}Class" for aspect in aspects)}


KEY_ASPECTS: Dict[str, Type[_Aspect]] = {{
    {f',{newline}    '.join(f"'{aspect['Aspect']['keyForEntity']}': {aspect['name']}Class" for aspect in aspects if aspect['Aspect'].get('keyForEntity'))}
}}
"""
    )

    schema_class_file.write_text("\n".join(schema_classes_lines))


def write_urn_classes(key_aspects: List[dict], urn_dir: Path) -> None:
    urn_dir.mkdir()

    (urn_dir / "__init__.py").write_text("\n# This file is intentionally left empty.")

    code = """
# This file contains classes corresponding to entity URNs.

from typing import ClassVar, List, Optional, Type, TYPE_CHECKING

import functools
from deprecated.sphinx import deprecated as _sphinx_deprecated

from datahub.utilities.urn_encoder import UrnEncoder
from datahub.utilities.urns._urn_base import _SpecificUrn, Urn
from datahub.utilities.urns.error import InvalidUrnError

deprecated = functools.partial(_sphinx_deprecated, version="0.12.0.2")
"""

    for aspect in key_aspects:
        entity_type = aspect["Aspect"]["keyForEntity"]
        if aspect["Aspect"]["entityCategory"] == "internal":
            continue

        code += generate_urn_class(entity_type, aspect)

    (urn_dir / "urn_defs.py").write_text(code)


def capitalize_entity_name(entity_name: str) -> str:
    # Examples:
    # corpuser -> CorpUser
    # corpGroup -> CorpGroup
    # mlModelDeployment -> MlModelDeployment

    if entity_name == "corpuser":
        return "CorpUser"

    return f"{entity_name[0].upper()}{entity_name[1:]}"


def python_type(avro_type: str) -> str:
    if avro_type == "string":
        return "str"
    elif (
        isinstance(avro_type, dict)
        and avro_type.get("type") == "enum"
        and avro_type.get("name") == "FabricType"
    ):
        # TODO: make this stricter using an enum
        return "str"
    raise ValueError(f"unknown type {avro_type}")


def field_type(field: dict) -> str:
    return python_type(field["type"])


def field_name(field: dict) -> str:
    manual_mapping = {
        "origin": "env",
        "platformName": "platform_name",
    }

    name: str = field["name"]
    if name in manual_mapping:
        return manual_mapping[name]

    # If the name is mixed case, convert to snake case.
    if name.lower() != name:
        # Inject an underscore before each capital letter, and then convert to lowercase.
        return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()

    return name


_create_from_id = """
@classmethod
@deprecated(reason="Use the constructor instead")
def create_from_id(cls, id: str) -> "{class_name}":
    return cls(id)
"""
_extra_urn_methods: Dict[str, List[str]] = {
    "corpGroup": [_create_from_id.format(class_name="CorpGroupUrn")],
    "corpuser": [_create_from_id.format(class_name="CorpUserUrn")],
    "dataFlow": [
        """
@classmethod
def create_from_ids(
    cls,
    orchestrator: str,
    flow_id: str,
    env: str,
    platform_instance: Optional[str] = None,
) -> "DataFlowUrn":
    return cls(
        orchestrator=orchestrator,
        flow_id=f"{platform_instance}.{flow_id}" if platform_instance else flow_id,
        cluster=env,
    )

@deprecated(reason="Use .orchestrator instead")
def get_orchestrator_name(self) -> str:
    return self.orchestrator

@deprecated(reason="Use .flow_id instead")
def get_flow_id(self) -> str:
    return self.flow_id

@deprecated(reason="Use .cluster instead")
def get_env(self) -> str:
    return self.cluster
""",
    ],
    "dataJob": [
        """
@classmethod
def create_from_ids(cls, data_flow_urn: str, job_id: str) -> "DataJobUrn":
    return cls(data_flow_urn, job_id)

def get_data_flow_urn(self) -> "DataFlowUrn":
    return DataFlowUrn.from_string(self.flow)

@deprecated(reason="Use .job_id instead")
def get_job_id(self) -> str:
    return self.job_id
"""
    ],
    "dataPlatform": [_create_from_id.format(class_name="DataPlatformUrn")],
    "dataProcessInstance": [
        _create_from_id.format(class_name="DataProcessInstanceUrn"),
        """
@deprecated(reason="Use .id instead")
def get_dataprocessinstance_id(self) -> str:
    return self.id
""",
    ],
    "dataset": [
        """
@classmethod
def create_from_ids(
    cls,
    platform_id: str,
    table_name: str,
    env: str,
    platform_instance: Optional[str] = None,
) -> "DatasetUrn":
    return DatasetUrn(
        platform=platform_id,
        name=f"{platform_instance}.{table_name}" if platform_instance else table_name,
        env=env,
    )

from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path as _get_simple_field_path_from_v2_field_path

get_simple_field_path_from_v2_field_path = staticmethod(deprecated(reason='Use the function from the field_paths module instead')(_get_simple_field_path_from_v2_field_path))

def get_data_platform_urn(self) -> "DataPlatformUrn":
    return DataPlatformUrn.from_string(self.platform)

@deprecated(reason="Use .name instead")
def get_dataset_name(self) -> str:
    return self.name

@deprecated(reason="Use .env instead")
def get_env(self) -> str:
    return self.env
"""
    ],
    "domain": [_create_from_id.format(class_name="DomainUrn")],
    "notebook": [
        """
@deprecated(reason="Use .notebook_tool instead")
def get_platform_id(self) -> str:
    return self.notebook_tool

@deprecated(reason="Use .notebook_id instead")
def get_notebook_id(self) -> str:
    return self.notebook_id
"""
    ],
    "tag": [_create_from_id.format(class_name="TagUrn")],
}


def generate_urn_class(entity_type: str, key_aspect: dict) -> str:
    """Generate a class definition for this entity.

    The class definition has the following structure:
    - A class attribute ENTITY_TYPE, which is the entity type string.
    - A class attribute URN_PARTS, which is the number of parts in the URN.
    - A constructor that takes the URN parts as arguments. The field names
      will match the key aspect's field names. It will also have a _allow_coercion
      flag, which will allow for some normalization (e.g. upper case env).
      Then, each part will be validated (including nested calls for urn subparts).
    - Utilities for converting to/from the key aspect.
    - Any additional methods that are required for this entity type, defined above.
      These are primarily for backwards compatibility.
    - Getter methods for each field.
    """

    class_name = f"{capitalize_entity_name(entity_type)}Urn"

    fields = copy.deepcopy(key_aspect["fields"])
    if entity_type == "container":
        # The annotations say guid is optional, but it is required.
        # This is a quick fix of the annotations.
        assert field_name(fields[0]) == "guid"
        assert fields[0]["type"] == ["null", "string"]
        fields[0]["type"] = "string"

    _init_arg_parts: List[str] = []
    for field in fields:
        default = '"PROD"' if field_name(field) == "env" else None
        _arg_part = f"{field_name(field)}: {field_type(field)}"
        if default:
            _arg_part += f" = {default}"
        _init_arg_parts.append(_arg_part)
    init_args = ", ".join(_init_arg_parts)

    super_init_args = ", ".join(field_name(field) for field in fields)

    arg_count = len(fields)
    parse_ids_mapping = ", ".join(
        f"{field_name(field)}=entity_ids[{i}]" for i, field in enumerate(fields)
    )

    key_aspect_class = f"{key_aspect['name']}Class"
    to_key_aspect_args = ", ".join(
        # The LHS bypasses any field name aliases.
        f"{field['name']}=self.{field_name(field)}"
        for field in fields
    )
    from_key_aspect_args = ", ".join(
        f"{field_name(field)}=key_aspect.{field['name']}" for field in fields
    )

    init_coercion = ""
    init_validation = ""
    for field in fields:
        init_validation += f'if not {field_name(field)}:\n    raise InvalidUrnError("{field_name(field)} cannot be empty")\n'

        # Generalized mechanism for validating embedded urns.
        field_urn_type_class = None
        if field_name(field) == "platform":
            field_urn_type_class = "DataPlatformUrn"
        elif field.get("Urn"):
            if len(field.get("entityTypes", [])) == 1:
                field_entity_type = field["entityTypes"][0]
                field_urn_type_class = f"{capitalize_entity_name(field_entity_type)}Urn"
            else:
                field_urn_type_class = "Urn"

        if field_urn_type_class:
            init_validation += f"{field_name(field)} = str({field_name(field)})\n"
            init_validation += (
                f"assert {field_urn_type_class}.from_string({field_name(field)})\n"
            )
        else:
            init_validation += (
                f"assert not UrnEncoder.contains_reserved_char({field_name(field)})\n"
            )

        if field_name(field) == "env":
            init_coercion += "env = env.upper()\n"
        # TODO add ALL_ENV_TYPES validation
        elif entity_type == "dataPlatform" and field_name(field) == "platform_name":
            init_coercion += 'if platform_name.startswith("urn:li:dataPlatform:"):\n'
            init_coercion += "    platform_name = DataPlatformUrn.from_string(platform_name).platform_name\n"

        if field_name(field) == "platform":
            init_coercion += "platform = DataPlatformUrn(platform).urn()\n"
        elif field_urn_type_class is None:
            # For all non-urns, run the value through the UrnEncoder.
            init_coercion += (
                f"{field_name(field)} = UrnEncoder.encode_string({field_name(field)})\n"
            )
    if not init_coercion:
        init_coercion = "pass"

    # TODO include the docs for each field

    code = f"""
if TYPE_CHECKING:
    from datahub.metadata.schema_classes import {key_aspect_class}

class {class_name}(_SpecificUrn):
    ENTITY_TYPE: ClassVar[str] = "{entity_type}"
    URN_PARTS: ClassVar[int] = {arg_count}

    def __init__(self, {init_args}, *, _allow_coercion: bool = True) -> None:
        if _allow_coercion:
            # Field coercion logic (if any is required).
{textwrap.indent(init_coercion.strip(), prefix=" "*4*3)}

        # Validation logic.
{textwrap.indent(init_validation.strip(), prefix=" "*4*2)}

        super().__init__(self.ENTITY_TYPE, [{super_init_args}])

    @classmethod
    def _parse_ids(cls, entity_ids: List[str]) -> "{class_name}":
        if len(entity_ids) != cls.URN_PARTS:
            raise InvalidUrnError(f"{class_name} should have {{cls.URN_PARTS}} parts, got {{len(entity_ids)}}: {{entity_ids}}")
        return cls({parse_ids_mapping}, _allow_coercion=False)

    @classmethod
    def underlying_key_aspect_type(cls) -> Type["{key_aspect_class}"]:
        from datahub.metadata.schema_classes import {key_aspect_class}

        return {key_aspect_class}

    def to_key_aspect(self) -> "{key_aspect_class}":
        from datahub.metadata.schema_classes import {key_aspect_class}

        return {key_aspect_class}({to_key_aspect_args})

    @classmethod
    def from_key_aspect(cls, key_aspect: "{key_aspect_class}") -> "{class_name}":
        return cls({from_key_aspect_args})
"""

    for extra_method in _extra_urn_methods.get(entity_type, []):
        code += textwrap.indent(extra_method, prefix=" " * 4)

    for i, field in enumerate(fields):
        code += f"""
    @property
    def {field_name(field)}(self) -> {field_type(field)}:
        return self.entity_ids[{i}]
"""

    return code


@click.command()
@click.argument(
    "entity_registry", type=click.Path(exists=True, dir_okay=False), required=True
)
@click.argument(
    "pdl_path", type=click.Path(exists=True, file_okay=False), required=True
)
@click.argument(
    "schemas_path", type=click.Path(exists=True, file_okay=False), required=True
)
@click.argument("outdir", type=click.Path(), required=True)
@click.option("--check-unused-aspects", is_flag=True, default=False)
@click.option("--enable-custom-loader", is_flag=True, default=True)
def generate(
    entity_registry: str,
    pdl_path: str,
    schemas_path: str,
    outdir: str,
    check_unused_aspects: bool,
    enable_custom_loader: bool,
) -> None:
    entities = load_entity_registry(Path(entity_registry))
    schemas = load_schemas(schemas_path)

    # Patch the avsc files.
    schemas = patch_schemas(schemas, Path(pdl_path))

    # Special handling for aspects.
    aspects = {
        schema["Aspect"]["name"]: schema
        for schema in schemas.values()
        if schema.get("Aspect")
    }

    # Copy entity registry info into the corresponding key aspect.
    for entity in entities:
        # This implicitly requires that all keyAspects are resolvable.
        aspect = aspects[entity.keyAspect]

        # This requires that entities cannot share a keyAspect.
        if (
            "keyForEntity" in aspect["Aspect"]
            and aspect["Aspect"]["keyForEntity"] != entity.name
        ):
            raise ValueError(
                f'Entity key {entity.keyAspect} is used by {aspect["Aspect"]["keyForEntity"]} and {entity.name}'
            )

        # Also require that the aspect list is deduplicated.
        duplicate_aspects = collections.Counter(entity.aspects) - collections.Counter(
            set(entity.aspects)
        )
        if duplicate_aspects:
            raise ValueError(
                f"Entity {entity.name} has duplicate aspects: {duplicate_aspects}"
            )

        aspect["Aspect"]["keyForEntity"] = entity.name
        aspect["Aspect"]["entityCategory"] = entity.category
        aspect["Aspect"]["entityAspects"] = entity.aspects
        if entity.doc is not None:
            aspect["Aspect"]["entityDoc"] = entity.doc

    # Check for unused aspects. We currently have quite a few.
    if check_unused_aspects:
        unused_aspects = set(aspects.keys()) - set().union(
            {entity.keyAspect for entity in entities},
            *(set(entity.aspects) for entity in entities),
        )
        if unused_aspects:
            raise ValueError(f"Unused aspects: {unused_aspects}")

    merged_schema = merge_schemas(list(schemas.values()))
    write_schema_files(merged_schema, outdir)

    # Schema files post-processing.
    (Path(outdir) / "__init__.py").write_text("# This file is intentionally empty.\n")
    add_avro_python3_warning(Path(outdir) / "schema_classes.py")
    annotate_aspects(
        list(aspects.values()),
        Path(outdir) / "schema_classes.py",
    )

    if enable_custom_loader:
        # Move schema_classes.py -> _schema_classes.py
        # and add a custom loader.
        (Path(outdir) / "_schema_classes.py").write_text(
            (Path(outdir) / "schema_classes.py").read_text()
        )
        (Path(outdir) / "schema_classes.py").write_text(
            """
# This is a specialized shim layer that allows us to dynamically load custom models from elsewhere.

import importlib
from typing import TYPE_CHECKING

from datahub._codegen.aspect import _Aspect
from datahub.utilities.docs_build import IS_SPHINX_BUILD
from datahub.utilities._custom_package_loader import get_custom_models_package

_custom_package_path = get_custom_models_package()

if TYPE_CHECKING or not _custom_package_path:
    from ._schema_classes import *

    # Required explicitly because __all__ doesn't include _ prefixed names.
    from ._schema_classes import __SCHEMA_TYPES

    if IS_SPHINX_BUILD:
        # Set __module__ to the current module so that Sphinx will document the
        # classes as belonging to this module instead of the custom package.
        for _cls in list(globals().values()):
            if hasattr(_cls, "__module__") and "datahub.metadata._schema_classes" in _cls.__module__:
                _cls.__module__ = __name__
else:
    _custom_package = importlib.import_module(_custom_package_path)
    globals().update(_custom_package.__dict__)
"""
        )

        (Path(outdir) / "urns.py").write_text(
            """
# This is a specialized shim layer that allows us to dynamically load custom URN types from elsewhere.

import importlib
from typing import TYPE_CHECKING

from datahub.utilities.docs_build import IS_SPHINX_BUILD
from datahub.utilities._custom_package_loader import get_custom_urns_package
from datahub.utilities.urns._urn_base import Urn  # noqa: F401

_custom_package_path = get_custom_urns_package()

if TYPE_CHECKING or not _custom_package_path:
    from ._urns.urn_defs import *  # noqa: F401

    if IS_SPHINX_BUILD:
        # Set __module__ to the current module so that Sphinx will document the
        # classes as belonging to this module instead of the custom package.
        for _cls in list(globals().values()):
            if hasattr(_cls, "__module__") and ("datahub.metadata._urns.urn_defs" in _cls.__module__ or _cls is Urn):
                _cls.__module__ = __name__
else:
    _custom_package = importlib.import_module(_custom_package_path)
    globals().update(_custom_package.__dict__)
"""
        )

    # Generate URN classes.
    urn_dir = Path(outdir) / "_urns"
    write_urn_classes(
        [aspect for aspect in aspects.values() if aspect["Aspect"].get("keyForEntity")],
        urn_dir,
    )

    # Save raw schema files in codegen as well.
    schema_save_dir = Path(outdir) / "schemas"
    schema_save_dir.mkdir()
    for schema_out_file, schema in schemas.items():
        (schema_save_dir / f"{schema_out_file}.avsc").write_text(
            json.dumps(schema, indent=2)
        )

    # Keep a copy of a few raw avsc files.
    required_avsc_schemas = {"MetadataChangeEvent", "MetadataChangeProposal"}
    save_raw_schemas(
        schema_save_dir,
        {
            name: schema
            for name, schema in schemas.items()
            if name in required_avsc_schemas
        },
    )

    # Add headers for all generated files
    generated_files = Path(outdir).glob("**/*.py")
    for file in generated_files:
        suppress_checks_in_file(file)


if __name__ == "__main__":
    generate()
