import os
from collections import OrderedDict
from collections.abc import Mapping

import click
import yaml
from dotenv import dotenv_values
from yaml import Loader

# Generates a merged docker-compose file with env variables inlined.
# Usage: python3 docker_compose_cli_gen.py ../docker-compose.yml ../docker-compose.override.yml ../docker-compose-gen.yml

omitted_services = [
    "kafka-rest-proxy",
    "kafka-topics-ui",
    "schema-registry-ui",
    "kibana",
]
# Note that these are upper bounds on memory usage. Once exceeded, the container is killed.
# Each service will be configured to use much less Java heap space than allocated here.
mem_limits = {
    "elasticsearch": "1g",
}


def dict_merge(dct, merge_dct):
    for k, v in merge_dct.items():
        if k in dct and isinstance(dct[k], dict) and isinstance(merge_dct[k], Mapping):
            dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]


def modify_docker_config(base_path, docker_yaml_config):
    # 0. Filter out services to be omitted.
    for key in list(docker_yaml_config["services"]):
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
            service["mem_limit"] = mem_limits[name]

        # 9. Correct relative paths for volume mounts
        if "volumes" in service:
            volumes = service["volumes"]
            for i in range(len(volumes)):
                ## Quickstart yaml files are located under quickstart. To get correct paths, need to refer to parent directory
                if volumes[i].startswith("../"):
                    volumes[i] = "../" + volumes[i]
                elif volumes[i].startswith("./"):
                    volumes[i] = "." + volumes[i]

    # 9. Set docker compose version to 2.
    # We need at least this version, since we use features like start_period for
    # healthchecks and shell-like variable interpolation.
    docker_yaml_config["version"] = "2.3"


@click.command()
@click.argument(
    "compose-files",
    nargs=-1,
    type=click.Path(
        exists=True,
        dir_okay=False,
    ),
)
@click.argument("output-file", type=click.Path())
def generate(compose_files, output_file) -> None:

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

    # Write output file
    output_dir = os.path.dirname(output_file)
    if len(output_dir) and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with open(output_file, "w") as new_conf_file:
        yaml.dump(
            merged_docker_config,
            new_conf_file,
            default_flow_style=False,
        )

    print(f"Successfully generated {output_file}.")


if __name__ == "__main__":
    generate()
