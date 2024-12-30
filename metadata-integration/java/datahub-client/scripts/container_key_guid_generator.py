import click
from typing import Dict, Any
import json
from dataclasses import dataclass
from abc import ABC, abstractmethod
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey


class URNGenerator(ABC):
    @abstractmethod
    def generate(self, args: Dict[str, Any]) -> str:
        pass


class DatabaseURNGenerator(URNGenerator):
    def generate(self, args: Dict[str, Any]) -> str:
        required_fields = ["platform", "database"]
        for field in required_fields:
            if field not in args:
                raise ValueError(f"Missing required field: {field}")

        all_fields = required_fields + ["instance"]
        for arg in args:
            if arg not in all_fields:
                raise ValueError(f"Invalid field: {arg}")

        database_key = DatabaseKey(
            platform=args["platform"],
            instance=args.get("instance"),
            database=args["database"],
        )
        return database_key.as_urn()


class SchemaURNGenerator(URNGenerator):
    def generate(self, args: Dict[str, Any]) -> str:
        required_fields = ["platform", "database", "schema"]
        all_fields = required_fields + ["instance", "env"]
        for field in required_fields:
            if field not in args:
                raise ValueError(f"Missing required field: {field}")

        for arg in args:
            if arg not in all_fields:
                raise ValueError(f"Invalid field: {arg}")

        schema_key = SchemaKey(
            platform=args["platform"],
            instance=args.get("instance"),
            env=args.get("env"),
            database=args["database"],
            schema=args["schema"],
        )
        return schema_key.as_urn()


URN_GENERATORS = {
    "database": DatabaseURNGenerator(),
    "schema": SchemaURNGenerator(),
}


def validate_key_value(ctx, param, value):
    if not value:
        return {}

    result = {}
    for item in value:
        try:
            key, val = item.split("=", 1)
            result[key.strip()] = val.strip()
        except ValueError:
            raise click.BadParameter(
                f"Invalid key-value pair: {item}. Format should be key=value"
            )
    return result


@click.command()
@click.option(
    "--container-type",
    type=click.Choice(["database", "schema"]),
    required=True,
    help="The type of container to generate a URN for",
)
@click.option(
    "--param",
    "-p",
    multiple=True,
    callback=validate_key_value,
    help="Parameters in key=value format. Can be used multiple times.",
)
@click.option(
    "--output-format",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Output format for the URN",
)
def generate_urn(container_type: str, param: Dict[str, str], output_format: str):
    """Generate URNs for different types of containers.

    Example usage:
    ./container_urn_generator.py --container-type database -p platform=test-platform -p instance=DEV -p database=test-database
    """
    try:
        generator = URN_GENERATORS[container_type]
        urn = generator.generate(param)

        if output_format == "json":
            result = {"urn": urn, "container_type": container_type, "parameters": param}
            click.echo(json.dumps(result, indent=2))
        else:
            click.echo(urn)

    except KeyError as e:
        raise click.UsageError(f"Unknown container type: {container_type}")
    except ValueError as e:
        raise click.UsageError(str(e))


if __name__ == "__main__":
    generate_urn()
