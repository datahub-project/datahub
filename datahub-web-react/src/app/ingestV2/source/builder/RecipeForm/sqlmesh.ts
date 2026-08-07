import i18next from 'i18next';
import { get } from 'lodash';

import { FieldType, FilterRecipeField, FilterRule, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

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
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.projectPath.helper');
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
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.environment.helper');
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
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.gateway.helper');
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
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.targetPlatform.helper');
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
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.targetPlatformInstance.helper');
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
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.defaultCatalog.helper');
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
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.tobikoCloudToken.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.tobikoCloudToken.tooltip');
    },
    type: FieldType.SECRET,
    fieldPath: 'source.config.tobiko_cloud_token',
    placeholder: 'tobiko_cloud_token',
    rules: null,
};

export const SQLMESH_MODEL_ALLOW: FilterRecipeField = {
    name: 'model_name_pattern.allow',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.allowPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.modelAllow.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.modelAllow.tooltip');
    },
    placeholder: 'analytics.star.*',
    type: FieldType.LIST,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: 'source.config.model_name_pattern.allow',
    rule: FilterRule.INCLUDE,
    section: 'Models',
    filteringResource: 'Model',
    rules: null,
};

export const SQLMESH_MODEL_DENY: FilterRecipeField = {
    name: 'model_name_pattern.deny',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.denyPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.modelDeny.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.modelDeny.tooltip');
    },
    placeholder: 'analytics.staging.*',
    type: FieldType.LIST,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: 'source.config.model_name_pattern.deny',
    rule: FilterRule.EXCLUDE,
    section: 'Models',
    filteringResource: 'Model',
    rules: null,
};

export const SQLMESH_ENV: RecipeField = {
    name: 'env',
    get label() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.env.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.env.helper');
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
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.includeSchema.helper');
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
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.includeLineage.helper');
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
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.includeColumnLineage.helper');
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
    get helper() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.auditResultsPath.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:sqlmesh.auditResultsPath.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.audit_results_path',
    placeholder: '/path/to/audit_results.json',
    rules: null,
};
