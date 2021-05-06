import json
import types
import unittest.mock
from typing import List

import avro.schema
import click
from avrogen import write_schema_files


def load_schema_file(schema_file: str) -> str:
    with open(schema_file) as f:
        raw_schema_text = f.read()

    redo_spaces = json.dumps(json.loads(raw_schema_text), indent=2)
    return redo_spaces


def merge_schemas(schemas: List[str]) -> str:
    # Combine schemas.
    schemas_obj = [json.loads(schema) for schema in schemas]
    merged = ["null"] + schemas_obj

    # Deduplicate repeated names.
    def Register(self, schema):
        if schema.fullname in self._names:
            print(f"deduping {schema.fullname}")
        else:
            self._names[schema.fullname] = schema

    with unittest.mock.patch("avro.schema.Names.Register", Register):
        cleaned_schema = avro.schema.SchemaFromJSONData(merged)

    # Convert back to an Avro schema JSON representation.
    class MappingProxyEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, types.MappingProxyType):
                return dict(obj)
            return json.JSONEncoder.default(self, obj)

    out_schema = cleaned_schema.to_json()
    encoded = json.dumps(out_schema, cls=MappingProxyEncoder, indent=2)
    return encoded


def suppress_checks_in_file(filepath: str) -> None:
    """Adds a couple lines to the top of a file to suppress flake8 and black"""

    with open(filepath, "r+") as f:
        contents = f.read()

        f.seek(0, 0)
        f.write("# flake8: noqa\n")
        f.write("# fmt: off\n")
        f.write(contents)
        f.write("# fmt: on\n")


@click.command()
@click.argument("schema_files", type=click.Path(exists=True), nargs=-1, required=True)
@click.argument("outdir", type=click.Path(), required=True)
def generate(schema_files: List[str], outdir: str) -> None:
    schemas = []
    for schema_file in schema_files:
        schema = load_schema_file(schema_file)
        schemas.append(schema)

    merged_schema = merge_schemas(schemas)

    write_schema_files(merged_schema, outdir)
    suppress_checks_in_file(f"{outdir}/schema_classes.py")
    with open(f"{outdir}/__init__.py", "w"):
        # Truncate this file.
        pass


if __name__ == "__main__":
    generate()
