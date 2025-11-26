### Concept Mapping

| Hightouch Object | DataHub Entity                                                                        | Description                                                   |
| ---------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `Source`         | [Dataset](../../docs/generated/metamodel/entities/dataset.md)                         | Source database/warehouse (referenced as input)               |
| `Model`          | [Dataset](../../docs/generated/metamodel/entities/dataset.md)                         | SQL query or transformation (optional, platform: "hightouch") |
| `Sync`           | [DataJob](../../docs/generated/metamodel/entities/datajob.md)                         | Data pipeline that moves data from model to destination       |
| `Destination`    | [Dataset](../../docs/generated/metamodel/entities/dataset.md)                         | Target system (referenced as output)                          |
| `Sync Run`       | [DataProcessInstance](../../docs/generated/metamodel/entities/dataprocessinstance.md) | Execution instance with statistics                            |

### Supported Platforms

This connector supports 30+ data sources and 50+ destinations. For the complete list, see the [Compatibility](#compatibility) section in the setup guide.
