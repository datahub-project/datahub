import json

import click
from feast import Client
from feast.data_source import BigQuerySource, FileSource, KafkaSource, KinesisSource


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
    features = client.list_features_by_ref().values()
    entities = client.list_entities()

    # sort tables by name for consistent outputs
    tables = sorted(tables, key=lambda x: x.name)
    features = sorted(features, key=lambda x: x.name)
    entities = sorted(entities, key=lambda x: x.name)

    parsed_features = [
        {"name": feature.name, "type": feature.dtype.name} for feature in features
    ]

    parsed_entities = [
        {
            "name": entity.name,
            "type": entity.value_type.name,
            "description": entity.description,
        }
        for entity in entities
    ]

    parsed_tables = []

    for table in tables:

        # sort features by name for consistent outputs
        features = sorted([x.name for x in table.features])
        # sort entities by name for consistent outputs
        entities = sorted(table.entities)

        batch_source = None
        stream_source = None

        if isinstance(table.batch_source, BigQuerySource):

            batch_source = "BigQuerySource"

        if isinstance(table.batch_source, FileSource):

            batch_source = "FileSource"

        if isinstance(table.stream_source, KafkaSource):

            stream_source = "KafkaSource"

        if isinstance(table.stream_source, KinesisSource):

            stream_source = "KinesisSource"

        stream_source_config = table.to_dict()["spec"].get("streamSource")

        if stream_source_config is not None:
            stream_source_config = json.dumps(stream_source_config)

        parsed_tables.append(
            {
                "name": table.name,
                "features": features,
                "entities": entities,
                "batch_source": batch_source,
                "stream_source": stream_source,
                "batch_source_config": json.dumps(
                    table.to_dict()["spec"]["batchSource"]
                ),
                "stream_source_config": stream_source_config,
            }
        )

    output = {
        "features": parsed_features,
        "entities": parsed_entities,
        "tables": parsed_tables,
    }

    if output_path is not None:

        with open(output_path, "w") as f:
            json.dump(output, f)

    else:

        print(output)


if __name__ == "__main__":

    cli()
