# datahub user

The `user` command allows you to interact with the User entity in DataHub. It supports two main operations:

## upsert

Create or update users from a YAML file. For detailed information, please refer to [Creating Users and Groups with DataHub CLI](/docs/api/tutorials/owners.md#upsert-users).

```shell
datahub user upsert -f users.yaml
```

An example of `users.yaml` would look like as in [bar.user.dhub.yaml](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/cli_usage/user/bar.user.dhub.yaml) file for the complete code.

```yaml
- id: bar@acryl.io
  first_name: The
  last_name: Bar
  email: bar@acryl.io
  slack: "@the_bar_raiser"
  description: "I like raising the bar higher"
  groups:
    - foogroup@acryl.io
- id: datahub
  slack: "@datahubproject"
  phone: "1-800-GOT-META"
  description: "The DataHub Project"
  picture_link: "https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/datahub-logo-color-stable.svg"
```

## add

Create a native DataHub user with email/password authentication. This command creates users who can log in directly to DataHub using their email and password.

```shell
# Create a user with a role
datahub user add --email user@example.com --display-name "John Doe" --password --role Admin

# Create a user without a role
datahub user add --email user@example.com --display-name "Jane Smith" --password
```

**Options:**

- `--email` (required): User's email address, which will be used as their login ID
- `--display-name` (required): User's full display name
- `--password` (required): Flag to prompt for password input (password will be hidden during entry)
- `--role` (optional): Role to assign to the user. Valid values are `Admin`, `Editor`, or `Reader` (case-insensitive)

**Notes:**

- The command will check if a user with the specified email already exists and exit if found
- Passwords are entered securely via a hidden prompt and require confirmation
- If role assignment fails, the user will still be created but without the role
- Requires admin permissions to execute
