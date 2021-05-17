import json

import click
from avrogen import write_schema_files


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
@click.argument("schema_file", type=click.Path(exists=True))
@click.argument("outdir", type=click.Path())
def generate(schema_file: str, outdir: str) -> None:
    # print(f'using {schema_file}')
    with open(schema_file) as f:
        raw_schema_text = f.read()

    no_spaces_schema = json.dumps(json.loads(raw_schema_text))
    schema_json = no_spaces_schema.replace(
        '{"type": "string", "avro.java.string": "String"}', '"string"'
    )

    redo_spaces = json.dumps(json.loads(schema_json), indent=2)

    write_schema_files(redo_spaces, outdir)
    suppress_checks_in_file(f"{outdir}/schema_classes.py")
    with open(f"{outdir}/__init__.py", "w"):
        # Truncate this file.
        pass


if __name__ == "__main__":
    generate()
