### Concept Mapping

| Hightouch Object | DataHub Entity                                                           | Description                                                   |
| ---------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------- |
| `Source`         | [Dataset](../../metamodel/entities/dataset.md)                           | Source database/warehouse (referenced as input)               |
| `Model`          | [Dataset](../../metamodel/entities/dataset.md)                           | SQL query or transformation (optional, platform: "hightouch") |
| `Sync`           | [Data Job](../../metamodel/entities/dataJob.md)                          | Data pipeline that moves data from model to destination       |
| `Destination`    | [Dataset](../../metamodel/entities/dataset.md)                           | Target system (referenced as output)                          |
| `Sync Run`       | [Data Process Instance](../../metamodel/entities/dataprocessinstance.md) | Execution instance with statistics                            |

### Supported Platforms

This connector supports 30+ data sources and 50+ destinations. For the complete list, see the [Compatibility](#compatibility) section in the setup guide.
