### Concept Mapping

Here's a table for **Concept Mapping** between Dremio and DataHub to provide a clear overview of how entities and concepts in Dremio are mapped to corresponding entities in DataHub:

| Source Concept             | DataHub Concept | Notes                                                                                                                                                            |
| -------------------------- | --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Physical Dataset/Table** | `Dataset`       | Subtype: `Table` Note: `Ownership` information will only be available for the Cloud and Enterprise editions, it will not be available for the Community edition. |
| **Virtual Dataset/Views**  | `Dataset`       | Subtype: Note: `Ownership` information will only be available for the Cloud and Enterprise editions, it will not be available for the Community edition.`View`   |
| **Spaces**                 | `Container`     | Mapped to DataHub’s `Container` aspect. Subtype: `Space`                                                                                                         |
| **Folders**                | `Container`     | Mapped as a `Container` in DataHub. Subtype: `Folder`                                                                                                            |
| **Sources**                | `Container`     | Represented as a `Container` in DataHub. Subtype: `Source`                                                                                                       |
