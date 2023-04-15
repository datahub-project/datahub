import collections
import json
import re
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
            java_class: Optional[str] = field.props.get("java", {}).get("class")
            if java_class and java_class.startswith(
                "com.linkedin.pegasus2avro.common.urn."
            ):
                field.set_prop("Urn", java_class.split(".")[-1])

        # Patch array urn types.
        if nested.name in urn_arrays:
            mapping = urn_arrays[nested.name]

            for field_name, type in mapping:
                field = nested.fields_dict[field_name]
                field.set_prop("Urn", type)
                field.set_prop("urn_is_array", True)

    return patched.to_json()


def merge_schemas(schemas_obj: List[dict]) -> str:
    # Combine schemas as a "union" of all of the types.
    merged = ["null"] + schemas_obj

    # Patch add_name method to NOT complain about duplicate names.
    class NamesWithDups(avro.schema.Names):
        def add_name(self, name_attr, space_attr, new_schema):
            to_add = avro.schema.Name(name_attr, space_attr, self.default_namespace)
            self.names[to_add.fullname] = new_schema
            return to_add

    cleaned_schema = avro.schema.make_avsc_object(merged, names=NamesWithDups())

    # Convert back to an Avro schema JSON representation.
    out_schema = cleaned_schema.to_json()
    encoded = json.dumps(out_schema, indent=2)
    return encoded


autogen_header = """# flake8: noqa

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
    from avro.schema import SchemaFromJSONData
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
    schema_save_dir.mkdir()
    for name, schema in schemas.items():
        (schema_save_dir / f"{name}.avsc").write_text(json.dumps(schema, indent=2))

    # Add getXSchema methods.
    with open(schema_save_dir / "__init__.py", "w") as schema_dir_init:
        schema_dir_init.write(make_load_schema_methods(schemas.keys()))


def annotate_aspects(aspects: List[dict], schema_class_file: Path) -> None:
    schema_classes_lines = schema_class_file.read_text().splitlines()
    line_lookup_table = {line: i for i, line in enumerate(schema_classes_lines)}

    # Create the Aspect class.
    # We ensure that it cannot be instantiated directly, as
    # per https://stackoverflow.com/a/7989101/5004662.
    schema_classes_lines[
        line_lookup_table["__SCHEMAS: Dict[str, RecordSchema] = {}"]
    ] += """

class _Aspect(DictWrapper):
    ASPECT_NAME: ClassVar[str] = None  # type: ignore
    ASPECT_TYPE: ClassVar[str] = "default"
    ASPECT_INFO: ClassVar[dict] = None  # type: ignore

    def __init__(self):
        if type(self) is _Aspect:
            raise TypeError("_Aspect is an abstract class, and cannot be instantiated directly.")
        super().__init__()

    @classmethod
    def get_aspect_name(cls) -> str:
        return cls.ASPECT_NAME  # type: ignore

    @classmethod
    def get_aspect_type(cls) -> str:
        return cls.ASPECT_TYPE

    @classmethod
    def get_aspect_info(cls) -> dict:
        return cls.ASPECT_INFO
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

KEY_ASPECTS: Dict[str, Type[_Aspect]] = {{
    {f',{newline}    '.join(f"'{aspect['Aspect']['keyForEntity']}': {aspect['name']}Class" for aspect in aspects if aspect['Aspect'].get('keyForEntity'))}
}}
"""
    )

    schema_class_file.write_text("\n".join(schema_classes_lines))


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
def generate(
    entity_registry: str, pdl_path: str, schemas_path: str, outdir: str
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
    # unused_aspects = set(aspects.keys()) - set().union(
    #     {entity.keyAspect for entity in entities},
    #     *(set(entity.aspects) for entity in entities),
    # )

    merged_schema = merge_schemas(list(schemas.values()))
    write_schema_files(merged_schema, outdir)

    # Schema files post-processing.
    (Path(outdir) / "__init__.py").write_text("# This file is intentionally empty.\n")
    add_avro_python3_warning(Path(outdir) / "schema_classes.py")
    annotate_aspects(
        list(aspects.values()),
        Path(outdir) / "schema_classes.py",
    )

    # Keep a copy of a few raw avsc files.
    required_avsc_schemas = {"MetadataChangeEvent", "MetadataChangeProposal"}
    schema_save_dir = Path(outdir) / "schemas"
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
