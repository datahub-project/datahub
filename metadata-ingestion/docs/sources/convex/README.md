## Overview

Convex is a reactive backend platform that pairs a document database with server-side functions, used as the application backend for web and mobile products. Learn more in the [official Convex documentation](https://docs.convex.dev/).

The DataHub integration for Convex extracts one container per deployment and one dataset per table, with schema fields mapped from the JSON Schema that Convex derives from the documents it stores. Row counts are ingested as dataset profiles and can be disabled.

## Concept Mapping

| Source Concept     | DataHub Concept               | Notes                                                                 |
| ------------------ | ----------------------------- | --------------------------------------------------------------------- |
| Deployment         | Container (CONVEX_DEPLOYMENT) | One per entry in the `deployments` list, named by its `name` field.   |
| Table              | Dataset                       | Named `<deployment name>.<table name>`.                               |
| Document field     | SchemaField                   | Typed from the table's JSON Schema, including `anyOf` unions.         |
| Document reference | SchemaField description       | Convex reports these as `Id(<table>)`, which becomes the description. |
| Row count          | Dataset Profile               | Optional, controlled by `include_row_counts`.                         |
