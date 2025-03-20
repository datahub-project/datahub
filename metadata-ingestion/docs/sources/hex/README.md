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