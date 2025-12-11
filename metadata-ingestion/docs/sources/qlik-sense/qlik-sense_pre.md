<!--
  ~ © Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.
  ~
  ~ Licensed under the Open Government Licence v3.0.
-->

## Integration Details

This source extracts the following:

- Accessible spaces and apps within that spaces as Container.
- Qlik Datasets as Datahub Datasets with schema metadata.
- Sheets as Datahub dashboard and charts present inside sheets.

## Configuration Notes

1. Refer [doc](https://qlik.dev/authenticate/api-key/generate-your-first-api-key/) to generate an API key from the hub.
2. Get tenant hostname from About tab after login to qlik sense account.

## Concept mapping

| Qlik Sense | Datahub                                                     | Notes                    |
| ---------- | ----------------------------------------------------------- | ------------------------ |
| `Space`    | [Container](../../metamodel/entities/container.md)          | SubType `"Qlik Space"`   |
| `App`      | [Container](../../metamodel/entities/container.md)          | SubType `"Qlik App"`     |
| `Sheet`    | [Dashboard](../../metamodel/entities/dashboard.md)          |                          |
| `Chart`    | [Chart](../../metamodel/entities/chart.md)                  |                          |
| `Dataset`  | [Dataset](../../metamodel/entities/dataset.md)              | SubType `"Qlik Dataset"` |
| `User`     | [User (aka CorpUser)](../../metamodel/entities/corpuser.md) | Optionally Extracted     |
