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
    "--options",
    required=False,
    default={},
    type=dict,
    help="JSON of additional options to pass to feast.Client",
)
def cli(core_url, options):

    client = Client(core_url=core_url, **options)

    tables = client.list_feature_tables()

    # sort tables by name for consistent outputs
    tables = sorted(tables, key=lambda x: x.name)

    parsed_tables = []

    for table in tables:

        # sort features by name for consistent outputs
        features = sorted(table.features, key=lambda x: x.name)

        # sort entities by name for consistent outputs
        entities = [client.get_entity(entity_name) for entity_name in table.entities]
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

        featureset_stream_config = table.to_dict()["spec"].get("streamSource")

        if featureset_stream_config is not None:
            featureset_stream_config = json.dumps(featureset_stream_config)

        parsed_tables.append(
            {
                "features": parsed_features,
                "entities": parsed_entities,
                "batch_source": batch_source,
                "stream_source": stream_source,
            }
        )

    print(json.dumps(parsed_tables))


if __name__ == "__main__":

    cli()
