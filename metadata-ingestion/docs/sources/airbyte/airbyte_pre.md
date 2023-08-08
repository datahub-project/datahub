## Configuration Notes
1. Airbyte source is available for both Airbyte Cloud and Airbyte Open Source (OSS) users.
2. Airbyte Cloud users need to provide api_key in the recipe for authentication and Airbyte OSS users need to provide username and password.
3. For Airbyte Cloud refer demo [here](https://www.loom.com/share/7997a7c67cd642cc8d1c72ef0dfcc4bc) to create a api_key from [Developer Portal](https://portal.airbyte.com/).

## Concept mapping 

| Airbyte              	   | Datahub               |
|--------------------------|-----------------------|
| `Workspace`              | `DataFlow`       	   |
| `Connection`             | `DataJob`             |
| `Source`                 | `Dataset`             |
| `Destination`            | `Dataset`             |
| `Connection Job History` | `DataProcessInstance` |

Source and destination are mapped to Dataset as an Input and Output of Connection.
