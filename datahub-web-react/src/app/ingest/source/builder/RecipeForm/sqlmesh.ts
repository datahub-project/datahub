import i18next from 'i18next';
import { get, omit, set } from 'lodash';

import { FieldType, RecipeField, setListValuesOnRecipe } from '@app/ingest/source/builder/RecipeForm/common';

// The include_* toggles default to true in the connector config, so reflect that
// in the form when the recipe hasn't set the field explicitly.
const getBooleanValueWithTrueDefault = (fieldPath: string) => (recipe: any) => {
    const value = get(recipe, fieldPath);
    return value === undefined || value === null ? true : value;
};

export const SQLMESH_PROJECT_PATH: RecipeField = {
    name: 'project_path',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.projectPath.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.projectPath.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.project_path',
    placeholder: '/path/to/sqlmesh_project',
    required: true,
    rules: null,
};

export const SQLMESH_ENVIRONMENT: RecipeField = {
    name: 'environment',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.environment.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.environment.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.environment',
    placeholder: 'prod',
    rules: null,
};

export const SQLMESH_GATEWAY: RecipeField = {
    name: 'gateway',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.gateway.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.gateway.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.gateway',
    placeholder: 'my_gateway',
    rules: null,
};

export const SQLMESH_TARGET_PLATFORM: RecipeField = {
    name: 'target_platform',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.targetPlatform.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.targetPlatform.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.target_platform',
    placeholder: 'snowflake',
    rules: null,
};

export const SQLMESH_TARGET_PLATFORM_INSTANCE: RecipeField = {
    name: 'target_platform_instance',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.targetPlatformInstance.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.targetPlatformInstance.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.target_platform_instance',
    placeholder: 'prod_snowflake',
    rules: null,
};

export const SQLMESH_DEFAULT_CATALOG: RecipeField = {
    name: 'default_catalog',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.defaultCatalog.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.defaultCatalog.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.default_catalog',
    placeholder: 'analytics',
    rules: null,
};

export const SQLMESH_TOBIKO_CLOUD_TOKEN: RecipeField = {
    name: 'tobiko_cloud_token',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.tobikoCloudToken.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.tobikoCloudToken.tooltip');
    },
    type: FieldType.SECRET,
    fieldPath: 'source.config.tobiko_cloud_token',
    placeholder: 'tobiko_cloud_token',
    rules: null,
};

const modelAllowFieldPath = 'source.config.model_name_pattern.allow';
export const SQLMESH_MODEL_ALLOW: RecipeField = {
    name: 'model_name_pattern.allow',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.allowPatterns');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.modelAllow.tooltip');
    },
    placeholder: 'analytics.star.*',
    type: FieldType.LIST,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: modelAllowFieldPath,
    rules: null,
    section: 'Models',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, modelAllowFieldPath),
};

const modelDenyFieldPath = 'source.config.model_name_pattern.deny';
export const SQLMESH_MODEL_DENY: RecipeField = {
    name: 'model_name_pattern.deny',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.denyPatterns');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.modelDeny.tooltip');
    },
    placeholder: 'analytics.staging.*',
    type: FieldType.LIST,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: modelDenyFieldPath,
    rules: null,
    section: 'Models',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, modelDenyFieldPath),
};

export const SQLMESH_ENV: RecipeField = {
    name: 'env',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.env.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.env.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.env',
    placeholder: 'PROD',
    rules: null,
};

const includeSchemaPath = 'source.config.include_schema';
export const SQLMESH_INCLUDE_SCHEMA: RecipeField = {
    name: 'include_schema',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.includeSchema.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.includeSchema.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: includeSchemaPath,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(includeSchemaPath),
    rules: null,
};

const includeLineagePath = 'source.config.include_lineage';
export const SQLMESH_INCLUDE_LINEAGE: RecipeField = {
    name: 'include_lineage',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.includeLineage.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.includeLineage.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: includeLineagePath,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(includeLineagePath),
    rules: null,
};

const includeColumnLineagePath = 'source.config.include_column_lineage';
export const SQLMESH_INCLUDE_COLUMN_LINEAGE: RecipeField = {
    name: 'include_column_lineage',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.includeColumnLineage.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.includeColumnLineage.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: includeColumnLineagePath,
    getValueFromRecipeOverride: getBooleanValueWithTrueDefault(includeColumnLineagePath),
    rules: null,
};

export const SQLMESH_AUDIT_RESULTS_PATH: RecipeField = {
    name: 'audit_results_path',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.auditResultsPath.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.auditResultsPath.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.audit_results_path',
    placeholder: '/path/to/audit_results.json',
    rules: null,
};

const statefulIngestionEnabledPath = 'source.config.stateful_ingestion.enabled';
const removeStaleMetadataPath = 'source.config.stateful_ingestion.remove_stale_metadata';
export const SQLMESH_REMOVE_STALE_METADATA: RecipeField = {
    name: 'stateful_ingestion.remove_stale_metadata',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.removeStaleMetadata.label');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.removeStaleMetadata.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: removeStaleMetadataPath,
    // Mirror the backend default: StatefulStaleMetadataRemovalConfig defaults
    // remove_stale_metadata to true, so a recipe with stateful ingestion on and the
    // key omitted is really deleting stale entities — render it checked. Only an
    // explicit remove_stale_metadata: false (or stateful ingestion off) is unchecked.
    getValueFromRecipeOverride: (recipe: any) =>
        get(recipe, statefulIngestionEnabledPath) === true && get(recipe, removeStaleMetadataPath) !== false,
    // Write flips both keys together: remove_stale_metadata is a no-op unless stateful
    // ingestion is enabled, so turning it on sets enabled too, and turning it off drops
    // remove_stale (leaving enabled untouched isn't possible without a partial state, so
    // we clear enabled as well to avoid an orphaned stateful_ingestion.enabled: false).
    setValueOnRecipeOverride: (recipe: any, value: boolean) => {
        if (value) {
            return set(set({ ...recipe }, statefulIngestionEnabledPath, true), removeStaleMetadataPath, true);
        }
        return omit(set({ ...recipe }, statefulIngestionEnabledPath, false), removeStaleMetadataPath);
    },
    rules: null,
};
