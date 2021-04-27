import click
import os
import yaml
from collections.abc import Mapping
from yaml import Loader
from dotenv import dotenv_values

# Generates a merged docker-compose file with env variables inlined.
# Usage: python3 docker_compose_cli_gen.py ../docker-compose.yml ../docker-compose.override.yml ../docker-compose-gen.yml

omitted_services = [
    "kafka-rest-proxy",
    "kafka-topics-ui",
    "schema-registry-ui",
    "kibana"
]

def dict_merge(dct, merge_dct):
    for k, v in merge_dct.items():
        if (k in dct and isinstance(dct[k], dict)
                and isinstance(merge_dct[k], Mapping)):
            dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]

def rewrite_docker_config(base_path, docker_yaml_config):
    # 0. Filter out services to be omitted.
    keys_to_delete = [key for key in docker_yaml_config["services"] if key in omitted_services]
    for key in keys_to_delete: del docker_yaml_config["services"][key]

    for name,service in docker_yaml_config["services"].items():

        # 1. Parse the env file pointer
        env_file = service.get("env_file")

        if env_file is None:
            continue

        # 2. Construct full .env path
        env_file_path = os.path.join(base_path, env_file)

        # 3. Resolve the .env values
        env_vars = dotenv_values(env_file_path)

        # 4. Add an "environment" block to my YAML
        formatted_env_pairs = map(lambda tuple: "{key}={value}".format(key=tuple[0], value=tuple[1]), env_vars.items())
        service["environment"] = list(formatted_env_pairs)

        # 5. Delete the "env_file" value
        del service["env_file"]


@click.command()
@click.argument("compose-files", nargs=-1, type=click.Path(exists=True, dir_okay=False))
@click.argument("output-file", type=click.Path())
def generate(compose_files, output_file) -> None:

    # Resolve .env files to inlined vars
    modified_files = []
    for compose_file in compose_files:
        with open(compose_file, 'r') as orig_conf:
            docker_config = yaml.load(orig_conf, Loader=Loader)

        base_path = os.path.dirname(compose_file)
        rewrite_docker_config(base_path, docker_config)
        modified_files.append(docker_config)

    # Merge services, networks, and volumes maps
    merged_docker_config = modified_files[0]
    for modified_file in modified_files:
        dict_merge(merged_docker_config, modified_file)

    # Write output file
    output_dir = os.path.dirname(output_file)
    if len(output_dir) and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with open(output_file, 'w') as new_conf_file:
        yaml.dump(merged_docker_config, new_conf_file, default_flow_style=False)

    print("Successfully generated {output_file}.".format(output_file=output_file))


if __name__ == "__main__":
    generate()