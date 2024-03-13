## Integration Details

This source extracts the following:

- Workspaces and workbooks within that workspaces as Container.
- Sigma Datasets as Datahub Datasets.
- Pages as Datahub dashboards and elements present inside pages as charts.

## Configuration Notes

1. Refer [doc](https://help.sigmacomputing.com/docs/generate-api-client-credentials) to generate an API client credentials.
2. Provide the generated Client ID and Secret in Recipe.

## Concept mapping 

| Sigma                  | Datahub												         | Notes                            |
|------------------------|---------------------------------------------------------------|----------------------------------|
| `Workspace`            | [Container](../../metamodel/entities/container.md)     	     | SubType `"Sigma Workspace"`      |
| `Workbook`             | [Container](../../metamodel/entities/container.md)            | SubType `"Sigma Workbook"`       |
| `Page`                 | [Dashboard](../../metamodel/entities/dashboard.md)            |                                  |
| `Element`              | [Chart](../../metamodel/entities/chart.md)                    |                                  |
| `Dataset`              | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Sigma Dataset"`        |
| `User`                 | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) | Optionally Extracted             |