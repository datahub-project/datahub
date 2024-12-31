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
| `Workbook`             | [Dashboard](../../metamodel/entities/dashboard.md)            | SubType `"Sigma Workbook"`       |
| `Page`                 | [Dashboard](../../metamodel/entities/dashboard.md)            |                                  |
| `Element`              | [Chart](../../metamodel/entities/chart.md)                    |                                  |
| `Dataset`              | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Sigma Dataset"`        |
| `User`                 | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) | Optionally Extracted             |

## Advanced Configurations

### Chart source platform mapping
If you want to provide platform details(platform name, platform instance and env) for chart's all external upstream data sources, then you can use `chart_sources_platform_mapping` as below:

#### Example - For just one specific chart's external upstream data sources
```yml
    chart_sources_platform_mapping:
      'workspace_name/workbook_name/chart_name_1': 
        data_source_platform: snowflake
        platform_instance: new_instance
        env: PROD

      'workspace_name/folder_name/workbook_name/chart_name_2': 
        data_source_platform: postgres
        platform_instance: cloud_instance
        env: DEV
```

#### Example - For all charts within one specific workbook
```yml
    chart_sources_platform_mapping:
      'workspace_name/workbook_name_1': 
        data_source_platform: snowflake
        platform_instance: new_instance
        env: PROD
      
      'workspace_name/folder_name/workbook_name_2': 
        data_source_platform: snowflake
        platform_instance: new_instance
        env: PROD
```

#### Example - For all workbooks charts within one specific workspace
```yml
    chart_sources_platform_mapping:
      'workspace_name': 
        data_source_platform: snowflake
        platform_instance: new_instance
        env: PROD
```

#### Example - All workbooks use the same connection
```yml
    chart_sources_platform_mapping:
      '*': 
        data_source_platform: snowflake
        platform_instance: new_instance
        env: PROD
```
