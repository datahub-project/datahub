import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';

export const AIRBYTE_DEPLOYMENT_TYPE: RecipeField = {
    name: 'deployment_type',
    label: 'Deployment Type',
    tooltip: 'The type of Airbyte deployment - "oss" for self-hosted or "cloud" for Airbyte Cloud.',
    type: FieldType.SELECT,
    fieldPath: 'source.config.deployment_type',
    options: [
        { label: 'Open Source', value: 'oss' },
        { label: 'Cloud', value: 'cloud' },
    ],
    rules: null,
    required: true,
};

export const AIRBYTE_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host & Port',
    tooltip: 'The URL of your Airbyte instance (e.g. http://localhost:8000).',
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    placeholder: 'http://localhost:8000',
    rules: null,
    required: true,
};

export const AIRBYTE_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'Username for Airbyte basic authentication.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'airbyte',
    rules: null,
    required: false,
};

export const AIRBYTE_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'Password for Airbyte basic authentication.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    rules: null,
    required: false,
};

export const AIRBYTE_API_KEY: RecipeField = {
    name: 'api_key',
    label: 'API Key',
    tooltip: 'API key/token for Airbyte authentication (alternative to username/password).',
    type: FieldType.SECRET,
    fieldPath: 'source.config.api_key',
    placeholder: 'your-api-key',
    rules: null,
    required: false,
};

export const AIRBYTE_OAUTH2_CLIENT_ID: RecipeField = {
    name: 'oauth2_client_id',
    label: 'OAuth2 Client ID',
    tooltip: 'OAuth2 client ID for Airbyte Cloud authentication.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.oauth2_client_id',
    placeholder: 'client-id',
    rules: null,
    required: true,
};

export const AIRBYTE_OAUTH2_CLIENT_SECRET: RecipeField = {
    name: 'oauth2_client_secret',
    label: 'OAuth2 Client Secret',
    tooltip: 'OAuth2 client secret for Airbyte Cloud authentication.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.oauth2_client_secret',
    placeholder: 'client-secret',
    rules: null,
    required: true,
};

export const AIRBYTE_OAUTH2_REFRESH_TOKEN: RecipeField = {
    name: 'oauth2_refresh_token',
    label: 'OAuth2 Refresh Token',
    tooltip: 'OAuth2 refresh token for Airbyte Cloud authentication.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.oauth2_refresh_token',
    placeholder: 'refresh-token',
    rules: null,
    required: true,
};

export const AIRBYTE_CLOUD_WORKSPACE_ID: RecipeField = {
    name: 'cloud_workspace_id',
    label: 'Cloud Workspace ID',
    tooltip: 'The workspace ID for your Airbyte Cloud account.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.cloud_workspace_id',
    placeholder: 'workspace-id',
    rules: null,
    required: true,
};

export const AIRBYTE_VERIFY_SSL: RecipeField = {
    name: 'verify_ssl',
    label: 'Verify SSL',
    tooltip: 'Whether to verify SSL certificates when connecting to Airbyte.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.verify_ssl',
    rules: null,
    required: false,
};

export const AIRBYTE_EXTRACT_COLUMN_LEVEL_LINEAGE: RecipeField = {
    name: 'extract_column_level_lineage',
    label: 'Extract Column-level Lineage',
    tooltip: 'Extract column-level lineage between source and destination datasets.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_column_level_lineage',
    rules: null,
    required: false,
};

export const AIRBYTE_INCLUDE_STATUSES: RecipeField = {
    name: 'include_statuses',
    label: 'Include Job Statuses',
    tooltip: 'Include status information from Airbyte connection jobs.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_statuses',
    rules: null,
    required: false,
};

export const AIRBYTE_EXTRACT_OWNERS: RecipeField = {
    name: 'extract_owners',
    label: 'Extract Owners',
    tooltip: 'Extract ownership information from connection names.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_owners',
    rules: null,
    required: false,
};

export const AIRBYTE_OWNER_EXTRACTION_PATTERN: RecipeField = {
    name: 'owner_extraction_pattern',
    label: 'Owner Extraction Pattern',
    tooltip: 'Regular expression pattern to extract owner from connection names.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.owner_extraction_pattern',
    placeholder: '.*owner:([\\w-]+).*',
    rules: null,
    required: false,
};

export const AIRBYTE_EXTRACT_TAGS: RecipeField = {
    name: 'extract_tags',
    label: 'Extract Tags',
    tooltip: 'Extract tags from Airbyte metadata.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_tags',
    rules: null,
    required: false,
};

export const AIRBYTE_PLATFORM_INSTANCE: RecipeField = {
    name: 'platform_instance',
    label: 'Platform Instance',
    tooltip: 'A unique name to identify this Airbyte instance.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.platform_instance',
    placeholder: 'airbyte-instance',
    rules: null,
    required: false,
};

// Filtering Fields
export const AIRBYTE_SOURCE_PATTERN: RecipeField = {
    name: 'source_pattern',
    label: 'Source Filter Pattern',
    tooltip: 'Regex pattern to filter sources by name.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.source_pattern.allow[0]',
    placeholder: '.*MySQL.*',
    rules: null,
    required: false,
};

export const AIRBYTE_DESTINATION_PATTERN: RecipeField = {
    name: 'destination_pattern',
    label: 'Destination Filter Pattern',
    tooltip: 'Regex pattern to filter destinations by name.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.destination_pattern.allow[0]',
    placeholder: '.*Postgres.*',
    rules: null,
    required: false,
};

export const AIRBYTE_REQUEST_TIMEOUT: RecipeField = {
    name: 'request_timeout',
    label: 'Request Timeout',
    tooltip: 'Timeout for API requests in seconds.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.request_timeout',
    rules: null,
    required: false,
};
