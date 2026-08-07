import { get } from 'lodash';

import { FieldType, RecipeField, setListValuesOnRecipe } from '@app/ingest/source/builder/RecipeForm/common';

export const SQLMESH_PROJECT_PATH: RecipeField = {
    name: 'project_path',
    label: 'Project Path',
    tooltip:
        'Filesystem path to the SQLMesh project directory (the folder containing config.py/config.yaml and the models/ directory). Must be readable by the machine running ingestion.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.project_path',
    placeholder: '/path/to/sqlmesh_project',
    required: true,
    rules: null,
};

export const SQLMESH_ENVIRONMENT: RecipeField = {
    name: 'environment',
    label: 'Environment',
    tooltip: 'The SQLMesh environment to ingest from (e.g. prod, dev). Defaults to prod.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.environment',
    placeholder: 'prod',
    rules: null,
};

export const SQLMESH_GATEWAY: RecipeField = {
    name: 'gateway',
    label: 'Gateway',
    tooltip:
        "SQLMesh gateway name. Defaults to the project's default gateway. Required when reading state from Tobiko Cloud, since the gateway determines which state connection is used.",
    type: FieldType.TEXT,
    fieldPath: 'source.config.gateway',
    placeholder: 'my_gateway',
    rules: null,
};

export const SQLMESH_TARGET_PLATFORM: RecipeField = {
    name: 'target_platform',
    label: 'Target Platform',
    tooltip:
        'The warehouse platform SQLMesh writes to (e.g. snowflake, bigquery, databricks). Auto-detected from the gateway connection when left blank — only set this if auto-detection is wrong. Must match the platform in your warehouse connector recipe so sibling URNs stitch correctly.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.target_platform',
    placeholder: 'snowflake',
    rules: null,
};

export const SQLMESH_TARGET_PLATFORM_INSTANCE: RecipeField = {
    name: 'target_platform_instance',
    label: 'Target Platform Instance',
    tooltip:
        'Platform instance for the target warehouse. Must exactly match the platform_instance configured in your warehouse connector recipe so sibling URNs stitch correctly.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.target_platform_instance',
    placeholder: 'prod_snowflake',
    rules: null,
};

export const SQLMESH_DEFAULT_CATALOG: RecipeField = {
    name: 'default_catalog',
    label: 'Default Catalog',
    tooltip:
        "Default catalog (database) prepended to two-part model names (schema.model) so warehouse sibling URNs are three-part (catalog.schema.table). Set this when your warehouse connector emits three-part URNs but SQLMesh model names omit the catalog. Example: 'analytics' turns 'star.dim_developer' into 'analytics.star.dim_developer'.",
    type: FieldType.TEXT,
    fieldPath: 'source.config.default_catalog',
    placeholder: 'analytics',
    rules: null,
};

export const SQLMESH_TOBIKO_CLOUD_TOKEN: RecipeField = {
    name: 'tobiko_cloud_token',
    label: 'Tobiko Cloud Token',
    tooltip:
        'Tobiko Cloud API token. Set this when the project runs against Tobiko Cloud and DataHub should read from the real cloud state store. Requires Gateway to be set. Leave blank for local SQLMesh projects.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.tobiko_cloud_token',
    placeholder: 'tobiko_cloud_token',
    rules: null,
};

const modelAllowFieldPath = 'source.config.model_name_pattern.allow';
export const SQLMESH_MODEL_ALLOW: RecipeField = {
    name: 'model_name_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Regex patterns for model names to include. Matched against the fully qualified model name.',
    placeholder: 'analytics.star.*',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: modelAllowFieldPath,
    rules: null,
    section: 'Models',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, modelAllowFieldPath),
};

const modelDenyFieldPath = 'source.config.model_name_pattern.deny';
export const SQLMESH_MODEL_DENY: RecipeField = {
    name: 'model_name_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Regex patterns for model names to exclude. Deny patterns take precedence over allow patterns.',
    placeholder: 'analytics.staging.*',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: modelDenyFieldPath,
    rules: null,
    section: 'Models',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, modelDenyFieldPath),
};

export const SQLMESH_ENV: RecipeField = {
    name: 'env',
    label: 'DataHub Environment',
    tooltip: 'The DataHub environment (Fabric) that emitted dataset URNs are stamped with, e.g. PROD, DEV.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.env',
    placeholder: 'PROD',
    rules: null,
};

const includeSchemaPath = 'source.config.include_schema';
export const SQLMESH_INCLUDE_SCHEMA: RecipeField = {
    name: 'include_schema',
    label: 'Include Schema',
    tooltip:
        'Emit column schema metadata for each model. Disable to reduce volume when schema is already captured by a warehouse connector.',
    type: FieldType.BOOLEAN,
    fieldPath: includeSchemaPath,
    getValueFromRecipeOverride: (recipe: any) => {
        const value = get(recipe, includeSchemaPath);
        return value === undefined || value === null ? true : value;
    },
    rules: null,
};

const includeLineagePath = 'source.config.include_lineage';
export const SQLMESH_INCLUDE_LINEAGE: RecipeField = {
    name: 'include_lineage',
    label: 'Include Lineage',
    tooltip:
        'Emit model-to-model lineage derived from SQLMesh DAG dependencies. Disable if lineage is managed by another connector.',
    type: FieldType.BOOLEAN,
    fieldPath: includeLineagePath,
    getValueFromRecipeOverride: (recipe: any) => {
        const value = get(recipe, includeLineagePath);
        return value === undefined || value === null ? true : value;
    },
    rules: null,
};

const includeColumnLineagePath = 'source.config.include_column_lineage';
export const SQLMESH_INCLUDE_COLUMN_LINEAGE: RecipeField = {
    name: 'include_column_lineage',
    label: 'Include Column Lineage',
    tooltip:
        'Emit column-level lineage derived from SQLMesh SQL parsing (via SQLGlot). Disable for very large projects where per-column analysis is too slow.',
    type: FieldType.BOOLEAN,
    fieldPath: includeColumnLineagePath,
    getValueFromRecipeOverride: (recipe: any) => {
        const value = get(recipe, includeColumnLineagePath);
        return value === undefined || value === null ? true : value;
    },
    rules: null,
};

export const SQLMESH_AUDIT_RESULTS_PATH: RecipeField = {
    name: 'audit_results_path',
    label: 'Audit Results Path',
    tooltip:
        'Path to a JSON file containing SQLMesh audit pass/fail results. When set, the connector emits assertion run events so pass/fail status appears on the Data Quality tab. The file must exist at ingestion time.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.audit_results_path',
    placeholder: '/path/to/audit_results.json',
    rules: null,
};

export const SQLMESH_REMOVE_STALE_METADATA: RecipeField = {
    name: 'stateful_ingestion.remove_stale_metadata',
    label: 'Remove Stale Metadata',
    tooltip:
        'When stateful ingestion is enabled, soft-delete SQLMesh entities that are no longer present in the project between runs.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.stateful_ingestion.remove_stale_metadata',
    rules: null,
};
