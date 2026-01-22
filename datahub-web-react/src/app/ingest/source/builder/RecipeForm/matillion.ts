/**
 * Matillion Recipe Form Fields (V1 Ingestion UI)
 *
 * Note: This file is intentionally duplicated in both V1 (ingest) and V2 (ingestV2) folders
 * to maintain backward compatibility during the UI transition period. Any changes should be
 * applied to both files until V1 is fully deprecated.
 */
import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';

export const MATILLION_API_TOKEN: RecipeField = {
    name: 'api_token',
    label: 'API Token',
    tooltip:
        'Your Matillion API Bearer Token. Generate one from Settings → API Tokens in your Matillion Data Productivity Cloud account.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.api_config.api_token',
    placeholder: 'your_api_token_here',
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
    MATILLION_API_TOKEN,
    MATILLION_REGION,
    MATILLION_ENV,
    MATILLION_PLATFORM_INSTANCE,
    MATILLION_INCLUDE_EXECUTIONS,
    MATILLION_MAX_EXECUTIONS,
    MATILLION_EXTRACT_CONTAINERS,
    MATILLION_PIPELINE_ALLOW,
    MATILLION_PIPELINE_DENY,
    MATILLION_PROJECT_ALLOW,
    MATILLION_PROJECT_DENY,
    MATILLION_REQUEST_TIMEOUT,
    MATILLION_STATEFUL_INGESTION,
];

export default allFields;
