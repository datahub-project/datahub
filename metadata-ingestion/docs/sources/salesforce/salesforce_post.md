### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Probe support

`datahub recipe probe` checks a recipe against a Salesforce org without running ingestion. It offers
`objects` (standard sObjects) and `custom_objects` (API names ending in `__c`):

```shell
datahub recipe probe methods --recipe salesforce_recipe.yml
datahub recipe probe run objects --recipe salesforce_recipe.yml
datahub recipe probe filter --recipe salesforce_recipe.yml --kind "Custom Object" --name Property__c
```

The probe signs in the way ingestion does, with the recipe's `auth`, `is_sandbox` and `api_version`,
and lists objects with ingestion's own `EntityDefinition` query, so it lists only the customizable
objects ingestion considers. Each record's `name` is the API name `object_pattern` is matched
against, and `label` is the display label, which the pattern does not see. `object_pattern` judges
both kinds. `profile_pattern` only decides which ingested objects are profiled, so it is not a probe
filter. The probe reads no records, record counts or field definitions.

A failed sign-in or query exits 3 with Salesforce's error code, such as `INVALID_LOGIN`. A recipe
missing a credential its `auth` type needs exits 2 before anything connects.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

- This connector has only been tested with Salesforce Developer Edition.
- This connector only supports table level profiling (Row and Column counts) as of now. Row counts are approximate as returned by [Salesforce RecordCount REST API](https://developer.Salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_record_count.htm).
- This integration does not support ingesting Salesforce [External Objects](https://developer.Salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_external_objects.htm)

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
