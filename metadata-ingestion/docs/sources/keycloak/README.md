# Keycloak

<!-- Set Support Status -->

![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)

## Integration Details

The Keycloak source ingests metadata about Users, Groups, and Group Membership from a Keycloak instance. It uses the Keycloak Admin REST API to fetch this information.

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept   | DataHub Concept                                                                                           | Notes                            |
| ---------------- | --------------------------------------------------------------------------------------------------------- | -------------------------------- |
| User             | [CorpUser](https://docs.datahub.com/docs/generated/metamodel/entities/corpuser)                           | Mapped from Keycloak Users.      |
| Group            | [CorpGroup](https://docs.datahub.com/docs/generated/metamodel/entities/corpgroup)                         | Mapped from Keycloak Groups.     |
| Group Membership | [Group Membership](https://docs.datahub.com/docs/generated/metamodel/entities/corpgroup#group-membership) | Relates CorpUsers to CorpGroups. |

### Supported Capabilities

| Capability               | Status | Notes         |
| ------------------------ | :----: | ------------- |
| Platform Instance        |   ❌   | Not supported |
| Extract Users            |   ✅   |               |
| Extract Groups           |   ✅   |               |
| Extract Group Membership |   ✅   |               |

## Metadata Ingestion Quickstart

### Prerequisites

In order to ingest metadata from Keycloak, you will need:

- A Service Account (Client ID and Client Secret) in Keycloak with appropriate permissions (e.g., `view-users`, `query-groups`, `query-users` role mappings).
- The URL of your Keycloak server.

### Install the Plugin(s)

Run the following commands to install the relevant plugin(s):

`pip install 'acryl-datahub[keycloak]'`

### Configure the Ingestion Recipe(s)

Use the following recipe(s) to get started with ingestion.

```yml
source:
  type: keycloak
  config:
    # Keycloak Coordinates
    server_url: "https://keycloak.example.com"
    realm: "myrealm"

    # Credentials
    client_id: "myclient"
    client_secret: "mysecret"

    # Optional Configuration
    # verify_ssl: true
    # ingest_users: true
    # ingest_groups: true
    # ingest_group_membership: true
sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

<details>
  <summary>View All Recipe Configuration Options</summary>

| Field                     | Required | Default | Description                                                                                                 |
| ------------------------- | :------: | :-----: | ----------------------------------------------------------------------------------------------------------- |
| `server_url`              |    ✅    |         | The base URL of the Keycloak server, e.g. https://keycloak.example.com/auth or https://keycloak.example.com |
| `realm`                   |    ✅    |         | The Keycloak realm to ingest from                                                                           |
| `client_id`               |    ✅    |         | Client ID                                                                                                   |
| `client_secret`           |    ✅    |         | Client Secret                                                                                               |
| `verify_ssl`              |    ❌    | `True`  | Whether to verify SSL certificates. If a string is provided, it is treated as a path to a CA bundle to use. |
| `ingest_users`            |    ❌    | `True`  | Whether users should be ingested into DataHub.                                                              |
| `ingest_groups`           |    ❌    | `True`  | Whether groups should be ingested into DataHub.                                                             |
| `ingest_group_membership` |    ❌    | `True`  | Whether group membership should be ingested into DataHub.                                                   |
| `mask_group_id`           |    ❌    | `True`  | Whether workunit ID's for groups should be masked to avoid leaking sensitive information.                   |
| `mask_user_id`            |    ❌    | `True`  | Whether workunit ID's for users should be masked to avoid leaking sensitive information.                    |

</details>

## Troubleshooting

### Connection Issues

If you see connection errors, ensure `server_url` is reachable from the ingestion host and that `verify_ssl` is set appropriately for your environment (though disabling SSL verification is not recommended for production).

### Permission Issues

Ensure the Client ID/Service Account has the necessary Realm Management roles (like `query-users`, `query-groups`, `view-users`) assigned to it in Keycloak.
