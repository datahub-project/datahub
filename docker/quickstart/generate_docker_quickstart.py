from io import StringIO
from typing import List
import os
import sys
import pathlib
from collections.abc import Mapping

import click
import yaml
from dotenv import dotenv_values
from yaml import Loader

COMPOSE_SPECS = {
    "docker-compose.quickstart.yml": [
        "../docker-compose.yml",
        "../docker-compose.override.yml",
    ],
    "docker-compose-m1.quickstart.yml": [
        "../docker-compose.yml",
        "../docker-compose.override.yml",
        "../docker-compose.m1.yml",
    ],
    "docker-compose-without-neo4j.quickstart.yml": [
        "../docker-compose-without-neo4j.yml",
        "../docker-compose-without-neo4j.override.yml",
    ],
    "docker-compose-without-neo4j-m1.quickstart.yml": [
        "../docker-compose-without-neo4j.yml",
        "../docker-compose-without-neo4j.override.yml",
        "../docker-compose-without-neo4j.m1.yml",
    ],
    "docker-compose.monitoring.quickstart.yml": [
        "../monitoring/docker-compose.monitoring.yml",
    ],
    "docker-compose.consumers.quickstart.yml": [
        "../docker-compose.consumers.yml",
    ],
    "docker-compose.consumers-without-neo4j.quickstart.yml": [
        "../docker-compose.consumers-without-neo4j.yml",
    ],
    "docker-compose.kafka-setup.quickstart.yml": [
        "../docker-compose.kafka-setup.yml",
    ],
}

omitted_services = [
    "kafka-rest-proxy",
    "kafka-topics-ui",
    "schema-registry-ui",
    "kibana",
]
# Note that these are upper bounds on memory usage. Once exceeded, the container is killed.
# Each service will be configured to use much less Java heap space than allocated here.
mem_limits = {
    "elasticsearch": "1G",
}


def dict_merge(dct, merge_dct):
    for k, v in merge_dct.items():
        if k in dct and isinstance(dct[k], dict) and isinstance(merge_dct[k], Mapping):
            dict_merge(dct[k], merge_dct[k])
        elif k in dct and isinstance(dct[k], list):
            a = set(dct[k])
            b = set(merge_dct[k])
            if a != b:
                dct[k] = sorted(list(a.union(b)))
        else:
            dct[k] = merge_dct[k]


def modify_docker_config(base_path, docker_yaml_config):
    if not docker_yaml_config["services"]:
        docker_yaml_config["services"] = {}
    # 0. Filter out services to be omitted.
    for key in docker_yaml_config["services"]:
        if key in omitted_services:
            del docker_yaml_config["services"][key]

    for name, service in docker_yaml_config["services"].items():
        # 1. Extract the env file pointer
        env_file = service.get("env_file")

        if env_file is not None:
            # 2. Construct full .env path
            env_file_path = os.path.join(base_path, env_file)

            # 3. Resolve the .env values
            env_vars = dotenv_values(env_file_path)

            # 4. Create an "environment" block if it does not exist
            if "environment" not in service:
                service["environment"] = list()

            # 5. Append to an "environment" block to YAML
            for key, value in env_vars.items():
                if value is not None:
                    service["environment"].append(f"{key}={value}")
                else:
                    service["environment"].append(f"{key}")

            # 6. Delete the "env_file" value
            del service["env_file"]

        # 7. Delete build instructions
        if "build" in service:
            del service["build"]

        # 8. Set memory limits
        if name in mem_limits:
            service["deploy"] = {"resources":{"limits":{"memory":mem_limits[name]}}}

        # 9. Correct relative paths for volume mounts
        if "volumes" in service:
            volumes = service["volumes"]
            for i in range(len(volumes)):
                ## Quickstart yaml files are located under quickstart. To get correct paths, need to refer to parent directory
                if volumes[i].startswith("../"):
                    volumes[i] = "../" + volumes[i]
                elif volumes[i].startswith("./"):
                    volumes[i] = "." + volumes[i]


def dedup_env_vars(merged_docker_config):
    for service in merged_docker_config["services"]:
        if "environment" in merged_docker_config["services"][service]:
            lst = merged_docker_config["services"][service]["environment"]
            if lst is not None:
                # use a set to cache duplicates
                caches = set()
                results = {}
                for item in lst:
                    partitions = item.rpartition("=")
                    prefix = partitions[0]
                    suffix = partitions[1]
                    # check whether prefix already exists
                    if prefix not in caches and suffix != "":
                        results[prefix] = item
                        caches.add(prefix)
                if set(lst) != set([v for k, v in results.items()]):
                    sorted_vars = sorted([k for k in results])
                    merged_docker_config["services"][service]["environment"] = [
                        results[var] for var in sorted_vars
                    ]


def merge_files(compose_files: List[str]) -> str:
    """
    Generates a merged docker-compose file with env variables inlined.

    Example Usage: python3 generate_docker_quickstart.py generate-one ../docker-compose.yml ../docker-compose.override.yml ../docker-compose-gen.yml
    """

    # Resolve .env files to inlined vars
    modified_files = []
    for compose_file in compose_files:
        with open(compose_file, "r") as orig_conf:
            docker_config = yaml.load(orig_conf, Loader=Loader)

        base_path = os.path.dirname(compose_file)
        modify_docker_config(base_path, docker_config)
        modified_files.append(docker_config)

    # Merge services, networks, and volumes maps
    merged_docker_config = modified_files[0]
    for modified_file in modified_files:
        dict_merge(merged_docker_config, modified_file)

    # Dedup env vars, last wins
    dedup_env_vars(merged_docker_config)

    # Generate yaml to string.
    out = StringIO()
    yaml.dump(
        merged_docker_config,
        out,
        default_flow_style=False,
        width=1000,
    )
    return out.getvalue()


@click.group()
def main_cmd() -> None:
    pass


@main_cmd.command()
@click.argument(
    "compose-files",
    nargs=-1,
    type=click.Path(
        exists=True,
        dir_okay=False,
    ),
)
@click.argument("output-file", type=click.Path())
def generate_one(compose_files, output_file) -> None:
    """
    Generates a merged docker-compose file with env variables inlined.

    Example Usage: python3 generate_docker_quickstart.py generate-one ../docker-compose.yml ../docker-compose.override.yml ../docker-compose-gen.yml
    """

    merged_contents = merge_files(compose_files)

    # Write output file
    pathlib.Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(output_file).write_text(merged_contents)

    print(f"Successfully generated {output_file}.")


@main_cmd.command()
@click.pass_context
def generate_all(ctx: click.Context) -> None:
    """
    Generates all merged docker-compose files with env variables inlined.
    """

    for output_compose_file, inputs in COMPOSE_SPECS.items():
        ctx.invoke(generate_one, compose_files=inputs, output_file=output_compose_file)


@main_cmd.command()
def check_all() -> None:
    """
    Checks that the generated docker-compose files are up to date.
    """

    for output_compose_file, inputs in COMPOSE_SPECS.items():
        expected = merge_files(inputs)

        # Check that the files match.
        current = pathlib.Path(output_compose_file).read_text()

        if expected != current:
            print(
                f"File {output_compose_file} is out of date. Please run `python3 generate_docker_quickstart.py generate-all`."
            )
            sys.exit(1)


if __name__ == "__main__":
    main_cmd()
