This connector ingests [Hex](https://hex.tech/) assets into DataHub.

### Concept Mapping

| Hex Concept | DataHub Concept                                                                                    | Notes               |
|-------------|----------------------------------------------------------------------------------------------------|---------------------|
| `"hex"`     | [Data Platform](https://datahubproject.io/docs/generated/metamodel/entities/dataplatform/)         |                     |
| Workspace   | [Container](https://datahubproject.io/docs/generated/metamodel/entities/container/)                |  |
| Project     | [Dashboard](https://datahubproject.io/docs/generated/metamodel/entities/dashboard/)                | Subtype `Project`   |
| Component   | [Dashboard](https://datahubproject.io/docs/generated/metamodel/entities/dashboard/)                | Subtype `Component` |
| Collection  | [Tag](https://datahubproject.io/docs/generated/metamodel/entities/Tag/)                            |  |

Other Hex concepts are not mapped to DataHub entities yet.

### Limitations

Currently, the [Hex API](https://learn.hex.tech/docs/api/api-reference) has some limitations that affect the completeness of the extracted metadata:

1. **Projects and Components Relationship**: The API does not support fetching the many-to-many relationship between Projects and their Components.

2. **Metadata Access**: There is no direct method to retrieve metadata for Collections, Status, or Categories. This information is only available indirectly through references within Projects and Components.

Please keep these limitations in mind when working with the Hex connector.