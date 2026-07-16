import i18next from 'i18next';
import { get, omit, set } from 'lodash';
import React from 'react';

import dayjs from '@utils/dayjs';
import type { Dayjs } from '@utils/dayjs';

export enum FieldType {
    TEXT,
    BOOLEAN,
    LIST,
    SELECT,
    SECRET,
    DICT,
    TEXTAREA,
    DATE,
}

interface Option {
    label: string;
    value: string;
}

export type FieldsValues = Record<string, any>;

export interface RecipeField {
    name: string;
    label: string;
    dynamicLabel?: (values: FieldsValues) => string;
    tooltip: string | React.ReactNode;
    helper?: string | React.ReactNode;
    type: FieldType;
    fieldPath: string | string[];
    rules: any[] | null;
    required?: boolean;
    dynamicRequired?: (values: FieldsValues) => boolean;
    hidden?: boolean;
    dynamicHidden?: (values: FieldsValues) => boolean;
    disabled?: boolean;
    dynamicDisabled?: (values: FieldsValues) => boolean;
    options?: Option[];
    buttonLabel?: string;
    keyField?: RecipeField;
    fields?: RecipeField[];
    getValueFromRecipeOverride?: (recipe: any) => any;
    setValueOnRecipeOverride?: (recipe: any, value: any) => any;
    placeholder?: string;
}

export enum FilterRule {
    INCLUDE = 'include',
    EXCLUDE = 'exclude',
}

export interface FilterRecipeField extends RecipeField {
    rule: FilterRule;
    section: string;
    filteringResource: string;
}

function clearFieldAndParents(recipe: any, fieldPath: string | string[]) {
    set(recipe, fieldPath, undefined);

    // remove last item from fieldPath
    const updatedFieldPath = Array.isArray(fieldPath)
        ? fieldPath.slice(0, -1).join('.')
        : fieldPath.split('.').slice(0, -1).join('.');

    if (updatedFieldPath) {
        const parentKeys = Object.keys(get(recipe, updatedFieldPath));

        // only child left is what we just set as undefined
        if (parentKeys.length === 1) {
            clearFieldAndParents(recipe, updatedFieldPath);
        }
    }
    return recipe;
}
export function setFieldValueOnRecipe(recipe: any, value: any, fieldPath: string | string[]) {
    const updatedRecipe = { ...recipe };
    if (value === null || value === '' || value === undefined) {
        clearFieldAndParents(updatedRecipe, fieldPath);
        return updatedRecipe;
    }
    set(updatedRecipe, fieldPath, value);
    return updatedRecipe;
}

export function setListValuesOnRecipe(recipe: any, values: string[] | undefined, fieldPath: string) {
    const updatedRecipe = { ...recipe };
    if (values !== undefined) {
        const filteredValues: string[] | undefined = values.filter((v) => !!v);
        return filteredValues.length
            ? setFieldValueOnRecipe(updatedRecipe, filteredValues, fieldPath)
            : setFieldValueOnRecipe(updatedRecipe, null, fieldPath);
    }
    return updatedRecipe;
}

const NUM_CHARACTERS_TO_REMOVE_FROM_DATE = 5;

function setDateValueOnRecipe(recipe: any, value: Dayjs | undefined, fieldPath: string) {
    const updatedRecipe = { ...recipe };
    if (value !== undefined) {
        if (!value) {
            return setFieldValueOnRecipe(updatedRecipe, null, fieldPath);
        }
        const isoDateString = value.toISOString();
        const formattedDateString = isoDateString
            .substring(0, isoDateString.length - NUM_CHARACTERS_TO_REMOVE_FROM_DATE)
            .concat('Z');
        return setFieldValueOnRecipe(updatedRecipe, formattedDateString, fieldPath);
    }
    return updatedRecipe;
}

/* ---------------------------------------------------- Filter Section ---------------------------------------------------- */
const databaseAllowFieldPath = 'source.config.database_pattern.allow';
export const DATABASE_ALLOW: FilterRecipeField = {
    name: 'database_pattern.allow',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.allowPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.databasePatternAllow.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.databasePatternAllow.tooltip');
    },
    placeholder: 'database_name',
    type: FieldType.LIST,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: databaseAllowFieldPath,
    rules: null,
    section: 'Databases',
    filteringResource: 'Database',
    rule: FilterRule.INCLUDE,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, databaseAllowFieldPath),
};

const databaseDenyFieldPath = 'source.config.database_pattern.deny';
export const DATABASE_DENY: FilterRecipeField = {
    name: 'database_pattern.deny',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.denyPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.databasePatternDeny.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.databasePatternDeny.tooltip');
    },
    placeholder: 'database_name',
    type: FieldType.LIST,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: databaseDenyFieldPath,
    rules: null,
    section: 'Databases',
    filteringResource: 'Database',
    rule: FilterRule.EXCLUDE,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, databaseDenyFieldPath),
};

const schemaAllowFieldPath = 'source.config.schema_pattern.allow';
export const SCHEMA_ALLOW: FilterRecipeField = {
    name: 'schema_pattern.allow',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.allowPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.schemaPatternAllow.helper');
    },
    // TODO: Change this to FULLY qualified names once the allow / deny consistency track is completed.
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.schemaPatternAllow.tooltip');
    },
    placeholder: 'company_schema',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: schemaAllowFieldPath,
    rules: null,
    section: 'Schemas',
    filteringResource: 'Schema',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaAllowFieldPath),
};

const schemaDenyFieldPath = 'source.config.schema_pattern.deny';
export const SCHEMA_DENY: FilterRecipeField = {
    name: 'schema_pattern.deny',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.denyPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.schemaPatternDeny.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.schemaPatternDeny.tooltip');
    },
    placeholder: 'company_schema',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: schemaDenyFieldPath,
    rules: null,
    section: 'Schemas',
    filteringResource: 'Schema',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaDenyFieldPath),
};

const tableAllowFieldPath = 'source.config.table_pattern.allow';
export const TABLE_ALLOW: FilterRecipeField = {
    name: 'table_pattern.allow',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.allowPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.tablePatternAllow.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.tablePatternAllow.tooltip');
    },
    placeholder: 'database_name.company_schema.table_name',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: tableAllowFieldPath,
    rules: null,
    section: 'Tables',
    filteringResource: 'Table',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableAllowFieldPath),
};

const tableDenyFieldPath = 'source.config.table_pattern.deny';
export const TABLE_DENY: FilterRecipeField = {
    name: 'table_pattern.deny',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.denyPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.tablePatternDeny.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.tablePatternDeny.tooltip');
    },
    placeholder: 'database_name.company_schema.table_name',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: tableDenyFieldPath,
    rules: null,
    section: 'Tables',
    filteringResource: 'Table',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableDenyFieldPath),
};

const viewAllowFieldPath = 'source.config.view_pattern.allow';
export const VIEW_ALLOW: FilterRecipeField = {
    name: 'view_pattern.allow',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.allowPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.viewPatternAllow.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.viewPatternAllow.tooltip');
    },
    placeholder: 'database_name.company_schema.view_name',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: viewAllowFieldPath,
    rules: null,
    section: 'Views',
    filteringResource: 'View',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, viewAllowFieldPath),
};

const viewDenyFieldPath = 'source.config.view_pattern.deny';
export const VIEW_DENY: FilterRecipeField = {
    name: 'view_pattern.deny',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.denyPatterns');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.viewPatternDeny.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.viewPatternDeny.tooltip');
    },
    placeholder: 'database_name.company_schema.view_name',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    get buttonLabel() {
        return i18next.t('ingestion.sourceBuilder:buttons.addPattern');
    },
    fieldPath: viewDenyFieldPath,
    rules: null,
    section: 'Views',
    filteringResource: 'View',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, viewDenyFieldPath),
};

/* ---------------------------------------------------- Advance Section ---------------------------------------------------- */
const includeLineageFieldPathA = 'source.config.include_table_lineage';
const includeLineageFieldPathB = 'source.config.include_view_lineage';
export const INCLUDE_LINEAGE: RecipeField = {
    name: 'include_lineage',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.includeLineage');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.includeLineage.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.includeLineage.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: includeLineageFieldPathA,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) =>
        get(recipe, includeLineageFieldPathA) && get(recipe, includeLineageFieldPathB),
    setValueOnRecipeOverride: (recipe: any, value: boolean) => {
        let updatedRecipe = setFieldValueOnRecipe(recipe, value, includeLineageFieldPathA);
        updatedRecipe = setFieldValueOnRecipe(updatedRecipe, value, includeLineageFieldPathB);
        return updatedRecipe;
    },
};

export const INCLUDE_TABLE_LINEAGE: RecipeField = {
    name: 'include_table_lineage',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.includeTableLineage');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.includeTableLineage.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.includeTableLineage.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_table_lineage',
    rules: null,
};

const isProfilingEnabledFieldPath = 'source.config.profiling.enabled';
export const TABLE_PROFILING_ENABLED: RecipeField = {
    name: 'profiling.enabled',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.enableTableProfiling');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.enableTableProfiling.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.enableTableProfiling.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: isProfilingEnabledFieldPath,
    rules: null,
};

const isTableProfilingOnlyFieldPath = 'source.config.profiling.profile_table_level_only';
export const COLUMN_PROFILING_ENABLED: RecipeField = {
    name: 'column_profiling.enabled',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.enableColumnProfiling');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.enableColumnProfiling.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.enableColumnProfiling.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: isTableProfilingOnlyFieldPath,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        return get(recipe, isProfilingEnabledFieldPath) && !get(recipe, isTableProfilingOnlyFieldPath);
    },
    setValueOnRecipeOverride: (recipe: any, value: boolean) => {
        return setFieldValueOnRecipe(recipe, !value, isTableProfilingOnlyFieldPath);
    },
};

export const STATEFUL_INGESTION_ENABLED: RecipeField = {
    name: 'stateful_ingestion.enabled',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.enableStatefulIngestion');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.enableStatefulIngestion.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.enableStatefulIngestion.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.stateful_ingestion.enabled',
    rules: null,
};

export const TABLE_LINEAGE_MODE: RecipeField = {
    name: 'table_lineage_mode',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.tableLineageMode');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.tableLineageMode.helper');
    },
    tooltip: (
        <div>
            <p>
                Which table lineage collector mode to use. Check out{' '}
                <a
                    href="https://docs.datahub.com/docs/generated/ingestion/sources/redshift/#config-details"
                    target="_blank"
                    rel="noreferrer"
                >
                    the documentation
                </a>{' '}
                explaining the difference between the three available modes.
            </p>
        </div>
    ),
    type: FieldType.SELECT,
    fieldPath: 'source.config.table_lineage_mode',
    rules: null,
    options: [
        {
            get label() {
                return i18next.t('ingestion.sourceBuilder:fields.tableLineageMode.stlScanBased');
            },
            value: 'stl_scan_based',
        },
        {
            get label() {
                return i18next.t('ingestion.sourceBuilder:fields.tableLineageMode.sqlBased');
            },
            value: 'sql_based',
        },
        {
            get label() {
                return i18next.t('ingestion.sourceBuilder:fields.tableLineageMode.mixed');
            },
            value: 'mixed',
        },
    ],
};

export const INGEST_TAGS: RecipeField = {
    name: 'ingest_tags',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.ingestTags');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.ingestTags.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.ingestTags.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_tags',
    rules: null,
};

export const INGEST_OWNER: RecipeField = {
    name: 'ingest_owner',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.ingestOwner');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.ingestOwner.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.ingestOwner.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_owner',
    rules: null,
};

const includeTablesPath = 'source.config.include_tables';
export const INCLUDE_TABLES: RecipeField = {
    name: 'include_tables',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.includeTables');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.includeTables.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.includeTables.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: includeTablesPath,
    // Always set include views indicator to true by default.
    // This is in accordance with what the ingestion sources do.
    getValueFromRecipeOverride: (recipe: any) => {
        const includeTables = get(recipe, includeTablesPath);
        if (includeTables !== undefined && includeTables !== null) {
            return includeTables;
        }
        return true;
    },
    rules: null,
};

const includeViewsPath = 'source.config.include_views';
export const INCLUDE_VIEWS: RecipeField = {
    name: 'include_views',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.includeViews');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.includeViews.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.includeViews.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: includeViewsPath,
    // Always set include views indicator to true by default.
    // This is in accordance with what the ingestion sources do.
    getValueFromRecipeOverride: (recipe: any) => {
        const includeViews = get(recipe, includeViewsPath);
        if (includeViews !== undefined && includeViews !== null) {
            return includeViews;
        }
        return true;
    },
    rules: null,
};

export const GIT_INFO_REPO: RecipeField = {
    name: 'git_info.repo',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.gitInfoRepo');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.gitInfoRepo.helper');
    },
    tooltip: (
        <div>
            <p>URL or name of your Git repository. Supports GitHub, GitLab, and other Git platforms. Examples:</p>
            <ul>
                <li>GitHub: datahub-project/datahub or https://github.com/datahub-project/datahub</li>
                <li>GitLab: https://gitlab.com/gitlab-org/gitlab</li>
                <li>Other platforms: https://your-git-server.com/org/repo</li>
            </ul>
        </div>
    ),
    type: FieldType.TEXT,
    fieldPath: 'source.config.git_info.repo',
    rules: null,
};

export const EXTRACT_USAGE_HISTORY: RecipeField = {
    name: 'extract_usage_history',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.extractUsageHistory');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.extractUsageHistory.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.extractUsageHistory.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_usage_history',
    rules: null,
};

export const EXTRACT_OWNERS: RecipeField = {
    name: 'extract_owners',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.extractOwners');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.extractOwners.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.extractOwners.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_owners',
    rules: null,
};

export const SKIP_PERSONAL_FOLDERS: RecipeField = {
    name: 'skip_personal_folders',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.skipPersonalFolders');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.skipPersonalFolders.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.skipPersonalFolders.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.skip_personal_folders',
    rules: null,
};

const startTimeFieldPath = 'source.config.start_time';
export const START_TIME: RecipeField = {
    name: 'start_time',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.startTime');
    },
    get helper() {
        return i18next.t('ingestion.sourceBuilder:fields.startTime.helper');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.startTime.tooltip');
    },
    placeholder: 'Select date and time',
    type: FieldType.DATE,
    fieldPath: startTimeFieldPath,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const isoDateString = get(recipe, startTimeFieldPath);
        if (isoDateString) {
            return dayjs(isoDateString);
        }
        return isoDateString;
    },
    setValueOnRecipeOverride: (recipe: any, value?: Dayjs) => setDateValueOnRecipe(recipe, value, startTimeFieldPath),
};

const envFieldPath = 'source.config.env';
export const ENV: RecipeField = {
    name: 'env',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.env');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.env.tooltip');
    },
    type: FieldType.TEXT,
    fieldPath: envFieldPath,
    placeholder: 'PROD',
    rules: null,
};

export const profilingEnabledFieldPath = 'source.config.profiling.enabled';
export const profilingTableLevelOnlyFieldPath = 'source.config.profiling.profile_table_level_only';

export function updateProfilingFields(
    recipe: any,
    isTableProfilingEnabled: boolean,
    isColumnProfilingEnabled: boolean,
): any {
    let updatedRecipe = { ...recipe };

    if (isTableProfilingEnabled && !isColumnProfilingEnabled) {
        updatedRecipe = set(updatedRecipe, profilingEnabledFieldPath, true);
        updatedRecipe = set(updatedRecipe, profilingTableLevelOnlyFieldPath, true);
        return updatedRecipe;
    }

    if (isTableProfilingEnabled && isColumnProfilingEnabled) {
        updatedRecipe = set(updatedRecipe, profilingEnabledFieldPath, true);
        updatedRecipe = set(updatedRecipe, profilingTableLevelOnlyFieldPath, false);
        return updatedRecipe;
    }

    updatedRecipe = omit(updatedRecipe, profilingTableLevelOnlyFieldPath);
    updatedRecipe = set(updatedRecipe, profilingEnabledFieldPath, false);
    return updatedRecipe;
}

export function getColumnProfilingCheckboxValue(recipe: any) {
    // FYI: we need the value of checkbox so the value from recipe is reverted
    const columnProfilingRecipeValue = get(recipe, profilingTableLevelOnlyFieldPath);
    const isColumnProfilingEnabled = columnProfilingRecipeValue === undefined ? false : !columnProfilingRecipeValue;
    return isColumnProfilingEnabled;
}

const profilingEnabledFieldName = 'profiling_enabled';
export const PROFILING_ENABLED: RecipeField = {
    name: profilingEnabledFieldName,
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.tableProfiling');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.tableProfiling.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: profilingEnabledFieldPath,
    rules: null,
    setValueOnRecipeOverride: (recipe, value) => {
        const isTableProfilingEnabled = value;
        const isColumnProfilingEnabled = getColumnProfilingCheckboxValue(recipe);
        return updateProfilingFields(recipe, isTableProfilingEnabled, isColumnProfilingEnabled);
    },
};

export const PROFILING_TABLE_LEVEL_ONLY: RecipeField = {
    name: 'profile_table_level_only',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.columnProfiling');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.columnProfiling.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: profilingTableLevelOnlyFieldPath,
    dynamicDisabled: (values) => !!get(values, profilingEnabledFieldName) !== true,
    setValueOnRecipeOverride: (recipe, value) => {
        const isTableProfilingEnabled = !!get(recipe, profilingEnabledFieldPath);
        const isColumnProfilingEnabled = value;
        return updateProfilingFields(recipe, isTableProfilingEnabled, isColumnProfilingEnabled);
    },
    getValueFromRecipeOverride: (recipe) => getColumnProfilingCheckboxValue(recipe),
    rules: null,
};

export const removeStaleMetadataEnabledFieldPath = 'source.config.stateful_ingestion.remove_stale_metadata';
export const statefulIngestionEnabledFieldPath = 'source.config.stateful_ingestion.enabled';

export function setRemoveStaleMetadataOnRecipe(recipe: any, value: boolean): any {
    let updatedRecipe = { ...recipe };
    if (value) {
        updatedRecipe = set(updatedRecipe, statefulIngestionEnabledFieldPath, value);
        updatedRecipe = set(updatedRecipe, removeStaleMetadataEnabledFieldPath, value);
    } else {
        updatedRecipe = set(updatedRecipe, statefulIngestionEnabledFieldPath, value);
        updatedRecipe = omit(updatedRecipe, removeStaleMetadataEnabledFieldPath);
    }
    return updatedRecipe;
}

export const REMOVE_STALE_METADATA_ENABLED: RecipeField = {
    name: 'remove_stale_metadata',
    get label() {
        return i18next.t('ingestion.sourceBuilder:fields.removeStaleMetadata');
    },
    get tooltip() {
        return i18next.t('ingestion.sourceBuilder:fields.removeStaleMetadata.tooltip');
    },
    type: FieldType.BOOLEAN,
    fieldPath: statefulIngestionEnabledFieldPath,
    rules: null,
    setValueOnRecipeOverride: setRemoveStaleMetadataOnRecipe,
};
