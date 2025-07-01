from datahub.ingestion.extractor.schema_util import AvroToMceSchemaConverter
from avro.schema import parse as parse_avro, RecordSchema
from datahub.emitter.synchronized_file_emitter import SynchronizedFileEmitter
import datahub.metadata.schema_classes as models
import click
from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
import os
import hashlib
from datahub.ingestion.graph.client import get_default_graph


def get_schema_hash(schema):
    # Convert schema to string if it isn't already
    schema_str = str(schema)

    # Create MD5 hash
    schema_hash = hashlib.md5(schema_str.encode("utf-8")).hexdigest()

    return schema_hash


@click.command(name="avro2datahub")
@click.option("--input-file", "-i", type=click.Path(exists=True), required=True)
@click.option("--platform", type=str, required=True)
@click.option("--output-file", "-o", type=click.Path(), default="metadata.py.json")
@click.option("--to-file", "-f", is_flag=True, default=True)
@click.option("--to-server", "-s", is_flag=True, default=False)
def generate_schema_file_from_avro_schema(
    input_file: str, platform: str, output_file: str, to_file: bool, to_server: bool
):
    avro_schema_file = input_file
    output_file_name = output_file
    platform_urn = make_data_platform_urn(platform)
    converter = AvroToMceSchemaConverter(is_key_schema=False)

    # Delete the output file if it exists
    if os.path.exists(output_file_name):
        os.remove(output_file_name)

    with open(avro_schema_file) as f:
        raw_string = f.read()
        avro_schema = parse_avro(raw_string)
        # Get fingerprint bytes
        canonical_form = avro_schema.canonical_form
        print(
            f"Schema canonical form: Length ({len(canonical_form)}); {canonical_form}"
        )
        md5_bytes = avro_schema.fingerprint("md5")
        # Convert to hex string
        avro_schema_hash = md5_bytes.hex()
        assert isinstance(
            avro_schema, RecordSchema
        ), "This command only works for Avro records"
        dataset_urn = make_dataset_urn(
            platform=platform_urn,
            name=(
                f"{avro_schema.namespace}.{avro_schema.name}"
                if avro_schema.namespace
                else avro_schema.name
            ),
        )
        schema_fields = [
            f for f in converter.to_mce_fields(avro_schema, is_key_schema=False)
        ]
        schema_metadata = models.SchemaMetadataClass(
            schemaName=avro_schema.name,
            platform=platform_urn,
            version=0,
            hash=avro_schema_hash,
            platformSchema=models.OtherSchemaClass(rawSchema=raw_string),
            fields=schema_fields,
        )
        assert schema_metadata.validate()
        if to_file:
            with SynchronizedFileEmitter(output_file_name) as file_emitter:
                file_emitter.emit(
                    MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=schema_metadata
                    )
                )
        if to_server:
            with get_default_graph() as graph:
                graph.emit(
                    MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=schema_metadata
                    )
                )

    print(f"Wrote metadata to {output_file}")


if __name__ == "__main__":
    generate_schema_file_from_avro_schema()
