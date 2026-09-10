import { FieldType, RecipeField, setListValuesOnRecipe } from '@app/ingest/source/builder/RecipeForm/common';

export const CUBE_API_URL: RecipeField = {
    name: 'api_url',
    label: 'API URL',
    tooltip: 'Base URL of the Cube REST API, including the base path (the part ending in /cubejs-api).',
    type: FieldType.TEXT,
    fieldPath: 'source.config.api_url',
    placeholder: 'https://your-tenant.cubecloud.dev/cubejs-api',
    required: true,
    rules: null,
};

export const CUBE_DEPLOYMENT_TYPE: RecipeField = {
    name: 'deployment_type',
    label: 'Deployment Type',
    tooltip: 'Whether the target is self-hosted Cube Core or Cube Cloud.',
    type: FieldType.SELECT,
    fieldPath: 'source.config.deployment_type',
    rules: null,
    options: [
        { label: 'Cube Core', value: 'CORE' },
        { label: 'Cube Cloud', value: 'CLOUD' },
    ],
};

export const CUBE_API_TOKEN: RecipeField = {
    name: 'api_token',
    label: 'API Token',
    tooltip:
        'API token used to authenticate against Cube. For Cube Core this is a JWT signed with CUBEJS_API_SECRET; for Cube Cloud use a Metadata API token (or leave blank and provide a Cloud API key below).',
    type: FieldType.SECRET,
    fieldPath: 'source.config.api_token',
    placeholder: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...',
    required: true,
    rules: null,
};

export const CUBE_CLOUD_API_KEY: RecipeField = {
    name: 'cloud_api_key',
    label: 'Cloud API Key',
    tooltip:
        'Cube Cloud Control Plane API key (Account → API keys). When set with the Deployment ID and Environment ID, the connector mints a metadata-scoped token automatically and unlocks warehouse lineage, reports, and workbooks.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.cloud_api_key',
    placeholder: 'cube-cloud-api-key',
    required: false,
    rules: null,
};

export const CUBE_DEPLOYMENT_ID: RecipeField = {
    name: 'deployment_id',
    label: 'Deployment ID',
    tooltip: 'Cube Cloud deployment id, used together with the Cloud API key to access the Metadata and Platform APIs.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.deployment_id',
    placeholder: '12345',
    required: false,
    rules: null,
};

export const CUBE_ENVIRONMENT_ID: RecipeField = {
    name: 'environment_id',
    label: 'Environment ID',
    tooltip: 'Cube Cloud environment id, used together with the Cloud API key to mint a Metadata API token.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.environment_id',
    placeholder: 'production',
    required: false,
    rules: null,
};

export const CUBE_WAREHOUSE_PLATFORM: RecipeField = {
    name: 'warehouse_platform',
    label: 'Warehouse Platform',
    tooltip:
        'The DataHub platform of the warehouse Cube reads from (e.g. snowflake, postgres, bigquery). Required to build lineage from Cubes to their upstream warehouse tables.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.warehouse_platform',
    placeholder: 'snowflake',
    required: false,
    rules: null,
};

export const CUBE_WAREHOUSE_DATABASE: RecipeField = {
    name: 'warehouse_database',
    label: 'Warehouse Database',
    tooltip: 'The database of the upstream warehouse, used to fully qualify lineage to warehouse tables.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.warehouse_database',
    placeholder: 'analytics',
    required: false,
    rules: null,
};

export const CUBE_INCLUDE_CUBES: RecipeField = {
    name: 'include_cubes',
    label: 'Include Cubes',
    tooltip: 'Ingest base cubes as datasets.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_cubes',
    rules: null,
};

export const CUBE_INCLUDE_REPORTS: RecipeField = {
    name: 'include_reports',
    label: 'Include Reports',
    tooltip: 'Cube Cloud only. Ingest saved reports as DataHub charts. Requires Platform API access.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_reports',
    rules: null,
};

export const CUBE_INCLUDE_WORKBOOKS: RecipeField = {
    name: 'include_workbooks',
    label: 'Include Workbooks',
    tooltip: 'Cube Cloud only. Ingest workbooks as DataHub dashboards. Requires Platform API access.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_workbooks',
    rules: null,
};

export const CUBE_PARSE_SQL_FOR_LINEAGE: RecipeField = {
    name: 'parse_sql_for_lineage',
    label: 'Parse SQL for Lineage',
    tooltip:
        'Parse each cube\u2019s SQL definition to derive lineage to upstream warehouse tables. Requires the Warehouse Platform to be set. Primarily used for Cube Core.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.parse_sql_for_lineage',
    rules: null,
};

export const CUBE_INCLUDE_COLUMN_LINEAGE: RecipeField = {
    name: 'include_column_lineage',
    label: 'Include Column Lineage',
    tooltip: 'Emit column-level lineage in addition to table-level lineage.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_column_lineage',
    rules: null,
};

export const CUBE_ALLOW: RecipeField = {
    name: 'cube_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Only include Cubes whose names match one of these Regular Expressions (REGEX).',
    placeholder: 'orders.*',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.cube_pattern.allow',
    rules: null,
    section: 'Cubes',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, 'source.config.cube_pattern.allow'),
};

export const CUBE_DENY: RecipeField = {
    name: 'cube_pattern.deny',
    label: 'Deny Patterns',
    tooltip:
        'Exclude Cubes whose names match one of these Regular Expressions (REGEX). Deny takes precedence over allow.',
    placeholder: 'staging_.*',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.cube_pattern.deny',
    rules: null,
    section: 'Cubes',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, 'source.config.cube_pattern.deny'),
};
