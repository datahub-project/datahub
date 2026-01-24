/**
 * Matillion Recipe Form Fields (V2 Ingestion UI)
 *
 * Note: This file is intentionally duplicated in both V1 (ingest) and V2 (ingestV2) folders
 * to maintain backward compatibility during the UI transition period. Any changes should be
 * applied to both files until V1 is fully deprecated.
 *
 * IMPORTANT: For advanced lineage configuration (namespace_to_platform_instance mapping),
 * users should use the YAML editor mode. This complex nested structure maps OpenLineage
 * namespace URIs to DataHub platform instances and is critical for connecting lineage
 * to existing datasets in DataHub. See the example recipes for configuration details.
 */
import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';

export const MATILLION_CLIENT_ID: RecipeField = {
    name: 'client_id',
    label: 'Client ID',
    tooltip: 'Matillion API Client ID from Settings → API credentials in your Matillion Data Productivity Cloud account.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.api_config.client_id',
    placeholder: 'your-client-id',
    rules: null,
    required: true,
};

export const MATILLION_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    label: 'Client Secret',
    tooltip:
        'Matillion API Client Secret (copy immediately after generation as it will only be shown once).',
    type: FieldType.SECRET,
    fieldPath: 'source.config.api_config.client_secret',
    placeholder: 'your-client-secret',
    rules: null,
    required: true,
};

export const MATILLION_REGION: RecipeField = {
    name: 'region',
    label: 'Region',
    tooltip: 'Matillion Data Productivity Cloud region: EU1 (Europe) or US1 (United States).',
    type: FieldType.SELECT,
    fieldPath: 'source.config.api_config.region',
    options: [
        { label: 'EU1 (Europe)', value: 'EU1' },
        { label: 'US1 (United States)', value: 'US1' },
    ],
    rules: null,
};

export const MATILLION_ENV: RecipeField = {
    name: 'env',
    label: 'Environment',
    tooltip: 'The environment for all emitted metadata (e.g., PROD, DEV, STAGING).',
    type: FieldType.TEXT,
    fieldPath: 'source.config.env',
    placeholder: 'PROD',
    rules: null,
};

export const MATILLION_PLATFORM_INSTANCE: RecipeField = {
    name: 'platform_instance',
    label: 'Platform Instance',
    tooltip: 'Unique identifier for this Matillion instance. Useful when ingesting from multiple Matillion accounts.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.platform_instance',
    placeholder: 'matillion-prod',
    rules: null,
};

export const MATILLION_INCLUDE_EXECUTIONS: RecipeField = {
    name: 'include_pipeline_executions',
    label: 'Include Pipeline Executions',
    tooltip: 'Ingest pipeline execution history as DataProcessInstances with detailed run statistics.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_pipeline_executions',
    rules: null,
};

export const MATILLION_MAX_EXECUTIONS: RecipeField = {
    name: 'max_executions_per_pipeline',
    label: 'Max Executions per Pipeline',
    tooltip: 'Maximum number of historical pipeline executions to ingest per pipeline. Default is 10.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.max_executions_per_pipeline',
    placeholder: '10',
    rules: null,
};

export const MATILLION_EXTRACT_CONTAINERS: RecipeField = {
    name: 'extract_projects_to_containers',
    label: 'Extract Projects as Containers',
    tooltip: 'Create DataHub containers for Matillion projects and environments for hierarchical organization.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_projects_to_containers',
    rules: null,
};

export const MATILLION_PARSE_SQL: RecipeField = {
    name: 'parse_sql_for_lineage',
    label: 'Parse SQL for Lineage',
    tooltip:
        'Parse SQL queries to infer additional column-level lineage. Requires a DataHub graph connection.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.parse_sql_for_lineage',
    rules: null,
    section: 'Lineage',
};

export const MATILLION_LINEAGE_START_DAYS: RecipeField = {
    name: 'lineage_start_days_ago',
    label: 'Lineage Start Days Ago',
    tooltip: 'Extract lineage from OpenLineage events generated in the last N days. Default is 7 days.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.lineage_start_days_ago',
    placeholder: '7',
    rules: null,
    section: 'Lineage',
};

export const MATILLION_INCLUDE_STREAMING: RecipeField = {
    name: 'include_streaming_pipelines',
    label: 'Include Streaming Pipelines',
    tooltip: 'Include streaming pipeline metadata (experimental feature).',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_streaming_pipelines',
    rules: null,
    section: 'Advanced',
};

export const MATILLION_INCLUDE_UNPUBLISHED: RecipeField = {
    name: 'include_unpublished_pipelines',
    label: 'Include Unpublished Pipelines',
    tooltip:
        'Discover and ingest unpublished pipelines from recent execution history. Disable to only ingest published pipelines.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_unpublished_pipelines',
    rules: null,
    section: 'Advanced',
};

export const MATILLION_INCLUDE_UNPUBLISHED: RecipeField = {
    name: 'include_unpublished_pipelines',
    label: 'Include Unpublished Pipelines',
    tooltip:
        'Ingest pipelines discovered from lineage events even if not in published-pipelines list. May include test/dev pipelines.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_unpublished_pipelines',
    rules: null,
    section: 'Pipelines',
};

export const MATILLION_PIPELINE_ALLOW: RecipeField = {
    name: 'pipeline_patterns.allow',
    label: 'Pipeline Allow Patterns',
    tooltip: 'Only include pipelines that match these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.pipeline_patterns.allow',
    rules: null,
    section: 'Pipelines',
    placeholder: '.*',
};

export const MATILLION_PIPELINE_DENY: RecipeField = {
    name: 'pipeline_patterns.deny',
    label: 'Pipeline Deny Patterns',
    tooltip: 'Exclude pipelines that match these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.pipeline_patterns.deny',
    rules: null,
    section: 'Pipelines',
    placeholder: 'test-.*',
};

export const MATILLION_PROJECT_ALLOW: RecipeField = {
    name: 'project_patterns.allow',
    label: 'Project Allow Patterns',
    tooltip: 'Only include projects that match these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.project_patterns.allow',
    rules: null,
    section: 'Projects',
    placeholder: '.*',
};

export const MATILLION_PROJECT_DENY: RecipeField = {
    name: 'project_patterns.deny',
    label: 'Project Deny Patterns',
    tooltip: 'Exclude projects that match these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.project_patterns.deny',
    rules: null,
    section: 'Projects',
    placeholder: 'archive-.*',
};

export const MATILLION_ENVIRONMENT_ALLOW: RecipeField = {
    name: 'environment_patterns.allow',
    label: 'Environment Allow Patterns',
    tooltip: 'Only include environments that match these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.environment_patterns.allow',
    rules: null,
    section: 'Environments',
    placeholder: '.*',
};

export const MATILLION_ENVIRONMENT_DENY: RecipeField = {
    name: 'environment_patterns.deny',
    label: 'Environment Deny Patterns',
    tooltip: 'Exclude environments that match these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.environment_patterns.deny',
    rules: null,
    section: 'Environments',
    placeholder: 'sandbox-.*',
};

export const MATILLION_STREAMING_ALLOW: RecipeField = {
    name: 'streaming_pipeline_patterns.allow',
    label: 'Streaming Pipeline Allow Patterns',
    tooltip: 'Only include streaming pipelines that match these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.streaming_pipeline_patterns.allow',
    rules: null,
    section: 'Streaming Pipelines',
    placeholder: '.*',
};

export const MATILLION_STREAMING_DENY: RecipeField = {
    name: 'streaming_pipeline_patterns.deny',
    label: 'Streaming Pipeline Deny Patterns',
    tooltip: 'Exclude streaming pipelines that match these regex patterns.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: 'source.config.streaming_pipeline_patterns.deny',
    rules: null,
    section: 'Streaming Pipelines',
    placeholder: 'test-.*',
};

export const MATILLION_REQUEST_TIMEOUT: RecipeField = {
    name: 'request_timeout_sec',
    label: 'Request Timeout (seconds)',
    tooltip: 'Timeout for API requests in seconds. Default is 30.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.api_config.request_timeout_sec',
    placeholder: '30',
    rules: null,
    section: 'Advanced',
};

export const MATILLION_STATEFUL_INGESTION: RecipeField = {
    name: 'stateful_ingestion.enabled',
    label: 'Enable Stateful Ingestion',
    tooltip: 'Enable stateful ingestion to track and remove stale metadata.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.stateful_ingestion.enabled',
    rules: null,
    section: 'Advanced',
};

const allFields: RecipeField[] = [
    MATILLION_CLIENT_ID,
    MATILLION_CLIENT_SECRET,
    MATILLION_REGION,
    MATILLION_ENV,
    MATILLION_PLATFORM_INSTANCE,
    MATILLION_INCLUDE_EXECUTIONS,
    MATILLION_PROJECT_ALLOW,
    MATILLION_PROJECT_DENY,
    MATILLION_ENVIRONMENT_ALLOW,
    MATILLION_ENVIRONMENT_DENY,
    MATILLION_PIPELINE_ALLOW,
    MATILLION_PIPELINE_DENY,
    MATILLION_STREAMING_ALLOW,
    MATILLION_STREAMING_DENY,
    MATILLION_STATEFUL_INGESTION,
];

export default allFields;
