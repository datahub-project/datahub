import i18next from 'i18next';
import get from 'lodash/get';

import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

export const AAS_SERVER: RecipeField = {
    name: 'server',
    get label() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.server.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.server.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.server.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.server',
    placeholder: 'asazure://westus.asazure.windows.net/myserver',
    required: true,
    rules: null,
};

export const AAS_AUTH_TYPE: RecipeField = {
    name: 'auth_type',
    get label() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.authType.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.authType.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.authType.tooltip');
    },
    type: FieldType.SELECT,
    fieldPath: 'source.config.auth_type',
    get options() {
        return [
            {
                label: i18next.t('ingestion.sourceBuilder:azureAnalysisServices.authType.servicePrincipal'),
                value: 'service_principal',
            },
            {
                label: i18next.t('ingestion.sourceBuilder:azureAnalysisServices.authType.deviceCode'),
                value: 'device_code',
            },
            {
                label: i18next.t('ingestion.sourceBuilder:azureAnalysisServices.authType.interactive'),
                value: 'interactive',
            },
            {
                label: i18next.t('ingestion.sourceBuilder:azureAnalysisServices.authType.usernamePassword'),
                value: 'username_password',
            },
        ];
    },
    placeholder: 'service_principal',
    rules: null,
};

export const AAS_TENANT_ID: RecipeField = {
    name: 'tenant_id',
    get label() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.tenantId.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.tenantId.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.tenantId.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.tenant_id',
    placeholder: 'a949d688-67c0-4bf1-a344-e939411c6c0a',
    rules: null,
};

export const AAS_CLIENT_ID: RecipeField = {
    name: 'client_id',
    get label() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.clientId.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.clientId.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.clientId.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.client_id',
    placeholder: 'client id',
    rules: null,
};

export const AAS_CLIENT_SECRET: RecipeField = {
    name: 'client_secret',
    get label() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.clientSecret.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.clientSecret.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.clientSecret.tooltip');
    },
    type: FieldType.SECRET,
    fieldPath: 'source.config.client_secret',
    placeholder: 'client secret',
    rules: null,
};

export const AAS_USERNAME: RecipeField = {
    name: 'username',
    get label() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.username.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.username.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.username.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'user@contoso.com',
    rules: null,
};

export const AAS_PASSWORD: RecipeField = {
    name: 'password',
    get label() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.password.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.password.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.password.tooltip');
    },
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    rules: null,
};

const extractLineagePath = 'source.config.extract_lineage';
export const AAS_EXTRACT_LINEAGE: RecipeField = {
    name: 'extract_lineage',
    get label() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.extractLineage.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.extractLineage.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.extractLineage.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: extractLineagePath,
    getValueFromRecipeOverride: (recipe: any) => {
        const value = get(recipe, extractLineagePath);
        if (value !== undefined && value !== null) {
            return value;
        }
        return true;
    },
    rules: null,
};

const extractColumnLineagePath = 'source.config.extract_column_level_lineage';
export const AAS_EXTRACT_COLUMN_LINEAGE: RecipeField = {
    name: 'extract_column_level_lineage',
    get label() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.extractColumnLineage.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.extractColumnLineage.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.extractColumnLineage.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: extractColumnLineagePath,
    getValueFromRecipeOverride: (recipe: any) => {
        const value = get(recipe, extractColumnLineagePath);
        if (value !== undefined && value !== null) {
            return value;
        }
        return true;
    },
    rules: null,
};

const emitSemanticModelPath = 'source.config.emit_semantic_model.enabled';
export const AAS_EMIT_SEMANTIC_MODEL: RecipeField = {
    name: 'emit_semantic_model.enabled',
    get label() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.emitSemanticModel.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.emitSemanticModel.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.emitSemanticModel.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: emitSemanticModelPath,
    getValueFromRecipeOverride: (recipe: any) => {
        const value = get(recipe, emitSemanticModelPath);
        if (value !== undefined && value !== null) {
            return value;
        }
        return false;
    },
    rules: null,
};

const ingestHiddenMetricsPath = 'source.config.emit_semantic_model.ingest_hidden_metrics.enabled';
export const AAS_INGEST_HIDDEN_METRICS: RecipeField = {
    name: 'emit_semantic_model.ingest_hidden_metrics.enabled',
    get label() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.ingestHiddenMetrics.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.ingestHiddenMetrics.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.ingestHiddenMetrics.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: ingestHiddenMetricsPath,
    getValueFromRecipeOverride: (recipe: any) => {
        const value = get(recipe, ingestHiddenMetricsPath);
        if (value !== undefined && value !== null) {
            return value;
        }
        return false;
    },
    rules: null,
};

const tagHiddenMetricsPath = 'source.config.emit_semantic_model.ingest_hidden_metrics.tag_hidden_metrics';
export const AAS_TAG_HIDDEN_METRICS: RecipeField = {
    name: 'emit_semantic_model.ingest_hidden_metrics.tag_hidden_metrics',
    get label() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.tagHiddenMetrics.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.tagHiddenMetrics.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.tagHiddenMetrics.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: tagHiddenMetricsPath,
    getValueFromRecipeOverride: (recipe: any) => {
        const value = get(recipe, tagHiddenMetricsPath);
        if (value !== undefined && value !== null) {
            return value;
        }
        return false;
    },
    rules: null,
};

const hiddenMetricTagPath = 'source.config.emit_semantic_model.ingest_hidden_metrics.hidden_metric_tag';
export const AAS_HIDDEN_METRIC_TAG: RecipeField = {
    name: 'emit_semantic_model.ingest_hidden_metrics.hidden_metric_tag',
    get label() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.hiddenMetricTag.label');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.hiddenMetricTag.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.hiddenMetricTag.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: hiddenMetricTagPath,
    placeholder: 'hidden_metric',
    rules: null,
};

const databaseAllowFieldPath = 'source.config.database_pattern.allow';
export const AAS_DATABASE_ALLOW: FilterRecipeField = {
    name: 'database_pattern.allow',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.allowPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.databaseAllow.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.databaseAllow.tooltip');
    },
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: databaseAllowFieldPath,
    rules: null,
    section: 'Models',
    filteringResource: 'Model',
    placeholder: 'SalesModel',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, databaseAllowFieldPath),
};

const databaseDenyFieldPath = 'source.config.database_pattern.deny';
export const AAS_DATABASE_DENY: FilterRecipeField = {
    name: 'database_pattern.deny',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.denyPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.databaseDeny.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.databaseDeny.tooltip');
    },
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: databaseDenyFieldPath,
    rules: null,
    section: 'Models',
    filteringResource: 'Model',
    placeholder: 'SalesModel',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, databaseDenyFieldPath),
};

const tableAllowFieldPath = 'source.config.table_pattern.allow';
export const AAS_TABLE_ALLOW: FilterRecipeField = {
    name: 'table_pattern.allow',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.allowPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.tableAllow.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.tableAllow.tooltip');
    },
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: tableAllowFieldPath,
    rules: null,
    section: 'Tables',
    filteringResource: 'Table',
    placeholder: 'Sales',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableAllowFieldPath),
};

const tableDenyFieldPath = 'source.config.table_pattern.deny';
export const AAS_TABLE_DENY: FilterRecipeField = {
    name: 'table_pattern.deny',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.denyPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.tableDeny.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:azureAnalysisServices.tableDeny.tooltip');
    },
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: tableDenyFieldPath,
    rules: null,
    section: 'Tables',
    filteringResource: 'Table',
    placeholder: 'Sales',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableDenyFieldPath),
};
