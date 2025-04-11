import yaml
import sys


def modify_yaml(file_path):
    # Read the existing file
    with open(file_path, "r") as file:
        content = file.read()

    # Parse the YAML content
    data = yaml.safe_load(content)

    # Modify the datahub-actions section
    if "services" in data and "datahub-actions" in data["services"]:
        datahub_actions = data["services"]["datahub-actions"]
        datahub_actions["env_file"] = ["${ACTIONS_ENV_FILE:-}"]

    # Write the modified content back to the file
    with open(file_path, "w") as file:
        yaml.dump(data, file, sort_keys=False)

    print(f"Successfully added env_file to datahub-actions in {file_path}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <path_to_docker_compose.yml>")
        sys.exit(1)

    file_path = sys.argv[1]
    modify_yaml(file_path)
