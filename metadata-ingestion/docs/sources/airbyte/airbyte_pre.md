## Configuration Notes
1. Airbyte source is available for both Airbyte Cloud and Airbyte Open Source (OSS) users.
2. For Airbyte Cloud user need to provide api_key in recipe to ingest metadata and for Airbyte OSS username and password.
3. Refer Walkthrough demo [here](https://www.loom.com/share/7997a7c67cd642cc8d1c72ef0dfcc4bc) to create a api_key from [Developer Portal](https://portal.airbyte.com/) in case you are using Airbyte Cloud.

## Concept mapping 

| Airbyte              	   | Datahub               |
|--------------------------|-----------------------|
| `Workspace`              | `DataFlow`       	   |
| `Connection`             | `DataJob`             |
| `Sourc`                  | `Dataset`             |
| `Destination`            | `Dataset`             |
| `Connection Job History` | `DataProcessInstance` |

Source and destination gets mapped with Dataset as an Input and Output of Connection.
