## Integration Details

This source extracts the following:

- Accessible spaces and apps within that spaces as Container.
- Qlik Datasets as Datahub Datasets with schema metadata.

## Configuration Notes

1. Refer [doc](https://qlik.dev/authenticate/api-key/generate-your-first-api-key/) to generate an API key from the hub.
2. Get tenant hostname from About tab after login to qlik sense account.

## Concept mapping 

| Qlik Cloud		   | Datahub												    |
|--------------------------|--------------------------------------------------------------------------------------------------------|
| `Space`                  | [Container](https://datahubproject.io/docs/generated/metamodel/entities/container)     	              |
| `App`                    | [Container](https://datahubproject.io/docs/generated/metamodel/entities/container)                     |
| `Dataset`                | [Dataset](https://datahubproject.io/docs/generated/metamodel/entities/dataset)                         |