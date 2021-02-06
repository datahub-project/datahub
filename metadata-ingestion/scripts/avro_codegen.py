import json
import sys
from avrogen import write_schema_files

import click


@click.command()
@click.argument("schema_file", type=click.Path(exists=True))
@click.argument("outdir", type=click.Path())
def generate(schema_file: str, outdir: str):
    # print(f'using {schema_file}')
    with open(schema_file) as f:
        raw_schema_text = f.read()

    no_spaces_schema = json.dumps(json.loads(raw_schema_text))
    schema_json = no_spaces_schema.replace('{"type": "string", "avro.java.string": "String"}', '"string"')

    redo_spaces = json.dumps(json.loads(schema_json), indent=2)

    write_schema_files(redo_spaces, outdir)


if __name__ == "__main__":
    generate()
