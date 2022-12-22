import json

import click
import feast

if feast.__version__ <= "0.18.0":
    from feast import Client  # type: ignore
    from feast.data_source import (  # type: ignore
        BigQuerySource,
        FileSource,
        KafkaSource,
        KinesisSource,
    )


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.option("--core_url", required=True, type=str, help="Feast core URL")
@click.option(
    "--output_path",
    required=False,
    default=None,
    type=str,
    help="Path to write output JSON file to",
)
def cli(core_url, output_path):
    client = Client(core_url=core_url)

    tables = client.list_feature_tables()

    # sort tables by name for consistent outputs
    tables = sorted(tables, key=lambda x: x.name)

    parsed_tables = []

    for table in tables:
        # sort entities by name for consistent outputs
        entities = sorted(table.entities)

        batch_source = None
        stream_source = None

        # platform and name for constructing URN later on
        batch_source_platform = "unknown"
        stream_source_platform = "unknown"
        batch_source_name = "unknown"
        stream_source_name = "unknown"

        if isinstance(table.batch_source, BigQuerySource):
            batch_source = "BigQuerySource"
            batch_source_platform = "bigquery"
            batch_source_name = table.batch_source.bigquery_options.table_ref

        if isinstance(table.batch_source, FileSource):
            batch_source = "FileSource"
            batch_source_platform = "file"

            # replace slashes because the react frontend can't parse them correctly
            batch_source_name = table.batch_source.file_options.file_url.replace(
                "/", "."
            )

            # replace redundant file prefix
            if batch_source_name.startswith("file:.."):
                batch_source_name = batch_source_name[7:]

        if isinstance(table.stream_source, KafkaSource):
            stream_source = "KafkaSource"
            stream_source_platform = "kafka"
            stream_source_name = table.stream_source.kafka_options.topic

        if isinstance(table.stream_source, KinesisSource):
            stream_source = "KinesisSource"
            stream_source_platform = "kinesis"
            stream_source_name = f"{table.stream_source.kinesis_options.region}-{table.stream_source.kinesis_options.stream_name}"

        # currently unused in MCE outputs, but useful for debugging
        stream_source_config = table.to_dict()["spec"].get("streamSource")
        batch_source_config = table.to_dict()["spec"]["batchSource"]

        raw_entities = [
            client.get_entity(entity_name) for entity_name in table.entities
        ]
        raw_entities = sorted(raw_entities, key=lambda x: x.name)

        source_info = {
            "batch_source": batch_source,
            "stream_source": stream_source,
            "batch_source_config": batch_source_config,
            "stream_source_config": stream_source_config,
            "batch_source_platform": batch_source_platform,
            "stream_source_platform": stream_source_platform,
            "batch_source_name": batch_source_name,
            "stream_source_name": stream_source_name,
        }

        # sort entities by name for consistent outputs
        entities = sorted(
            [
                {
                    "name": x.name,
                    "type": x.value_type.name,
                    "description": x.description,
                    **source_info,
                }
                for x in raw_entities
            ],
            key=lambda x: x["name"],
        )

        # sort features by name for consistent outputs
        features = sorted(
            [
                {"name": x.name, "type": x.dtype.name, **source_info}
                for x in table.features
            ],
            key=lambda x: x["name"],
        )

        parsed_tables.append(
            {
                "name": table.name,
                "entities": entities,
                "features": features,
            }
        )

    if output_path is not None:
        with open(output_path, "w") as f:
            json.dump(parsed_tables, f)

    else:
        print(parsed_tables)


if __name__ == "__main__":
    cli()
