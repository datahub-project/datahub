import { get, omit, set } from 'lodash';
import moment, { Moment } from 'moment-timezone';
import React from 'react';

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

export function setDateValueOnRecipe(recipe: any, value: Moment | undefined, fieldPath: string) {
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
    label: 'Allow Patterns',
    helper: 'Regex patterns to include specific databases (e.g. prod_.*).',
    tooltip:
        'Only include specific Databases by providing the name of a Database, or a Regular Expression (REGEX). If not provided, all Databases will be included.',
    placeholder: 'database_name',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
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
    label: 'Deny Patterns',
    helper: 'Regex patterns to exclude databases. Deny overrides allow.',
    tooltip:
        'Exclude specific Databases by providing the name of a Database, or a Regular Expression (REGEX). If not provided, all Databases will be included. Deny patterns always take precedence over Allow patterns.',
    placeholder: 'database_name',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: databaseDenyFieldPath,
    rules: null,
    section: 'Databases',
    filteringResource: 'Database',
    rule: FilterRule.EXCLUDE,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, databaseDenyFieldPath),
};

const dashboardAllowFieldPath = 'source.config.dashboard_pattern.allow';
export const DASHBOARD_ALLOW: FilterRecipeField = {
    name: 'dashboard_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific Dashboards',
    tooltip:
        'Only include specific Dashboards by providing the name of a Dashboard, or a Regular Expression (REGEX). If not provided, all Dashboards will be included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: dashboardAllowFieldPath,
    rules: null,
    section: 'Dashboards',
    filteringResource: 'Dashboard',
    placeholder: 'my_dashboard',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, dashboardAllowFieldPath),
};

const dashboardDenyFieldPath = 'source.config.dashboard_pattern.deny';
export const DASHBOARD_DENY: FilterRecipeField = {
    name: 'dashboard_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific Dashboards',
    tooltip:
        'Exclude specific Dashboards by providing the name of a Dashboard, or a Regular Expression (REGEX). If not provided, all Dashboards will be included. Deny patterns always take precendence over Allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: dashboardDenyFieldPath,
    rules: null,
    section: 'Dashboards',
    filteringResource: 'Dashboard',
    placeholder: 'my_dashboard',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, dashboardDenyFieldPath),
};

const schemaAllowFieldPath = 'source.config.schema_pattern.allow';
export const SCHEMA_ALLOW: FilterRecipeField = {
    name: 'schema_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific Schemas',
    // TODO: Change this to FULLY qualified names once the allow / deny consistency track is completed.
    tooltip:
        'Only include specific Schemas by providing the name of a Schema, or a Regular Expression (REGEX) to include specific Schemas. If not provided, all Schemas inside allowed Databases will be included.',
    placeholder: 'company_schema',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
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
    label: 'Deny Patterns',
    helper: 'Exclude specific Schemas',
    tooltip:
        'Exclude specific Schemas by providing the name of a Schema, or a Regular Expression (REGEX). If not provided, all Schemas inside allowed Databases will be included. Deny patterns always take precedence over Allow patterns.',
    placeholder: 'company_schema',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
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
    label: 'Allow Patterns',
    helper: 'Include Tables with names',
    tooltip:
        'Only include Tables with particular names by providing the fully qualified name of a Table, or a Regular Expression (REGEX). If not provided, all Tables inside allowed Databases and Schemas will be included in ingestion.',
    placeholder: 'database_name.company_schema.table_name',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
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
    label: 'Deny Patterns',
    helper: 'Exclude Tables with names',
    tooltip:
        'Exclude Tables with particular names by providing the fully qualified name of a Table, or a Regular Expression (REGEX). If not provided, all Tables inside allowed Databases and Schemas will be included in ingestion. Deny patterns always take precedence over Allow patterns.',
    placeholder: 'database_name.company_schema.table_name',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
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
    label: 'Allow Patterns',
    helper: 'Include Views with names',
    tooltip:
        'Only include Views with particular names by providing the fully qualified name of a View, or a Regular Expression (REGEX). If not provided, all Views inside allowed Databases and Schemas will be included in ingestion.',
    placeholder: 'database_name.company_schema.view_name',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
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
    label: 'Deny Patterns',
    helper: 'Exclude Views with names',
    tooltip:
        'Exclude Views with particular names by providing the fully qualified name of a View, or a Regular Expression (REGEX). If not provided, all Views inside allowed Databases and Schemas will be included in ingestion. Deny patterns always take precedence over Allow patterns.',
    placeholder: 'database_name.company_schema.view_name',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
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
    label: 'Include Lineage',
    helper: 'Include Table and View lineage',
    tooltip: 'Include Table and View lineage in your ingestion.',
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
    label: 'Include Table Lineage',
    helper: 'Extract Table-Level lineage',
    tooltip: 'Extract Table-Level lineage metadata. Enabling this may increase the duration of the sync.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_table_lineage',
    rules: null,
};

const isProfilingEnabledFieldPath = 'source.config.profiling.enabled';
export const TABLE_PROFILING_ENABLED: RecipeField = {
    name: 'profiling.enabled',
    label: 'Enable Table Profiling',
    helper: 'Generate Data Profiles for Tables',
    tooltip: 'Generate Data Profiles for extracted Tables. Enabling this may increase the duration of the sync.',
    type: FieldType.BOOLEAN,
    fieldPath: isProfilingEnabledFieldPath,
    rules: null,
};

const isTableProfilingOnlyFieldPath = 'source.config.profiling.profile_table_level_only';
export const COLUMN_PROFILING_ENABLED: RecipeField = {
    name: 'column_profiling.enabled',
    label: 'Enable Column Profiling',
    helper: 'Generate Column Data Profiles',
    tooltip:
        'Generate Data Profiles for the Columns in extracted Tables. Enabling this may increase the duration of the sync.',
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
    label: 'Enable Stateful Ingestion',
    helper: 'Remove stale assets from DataHub',
    tooltip: 'Remove stale assets from DataHub once they have been deleted in the ingestion source.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.stateful_ingestion.enabled',
    rules: null,
};

export const UPSTREAM_LINEAGE_IN_REPORT: RecipeField = {
    name: 'upstream_lineage_in_report',
    label: 'Include Upstream Lineage In Report.',
    helper: 'Debug lineage information',
    tooltip: 'Useful for debugging lineage information. Set to True to see the raw lineage created internally.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.upstream_lineage_in_report',
    rules: null,
};

export const TABLE_LINEAGE_MODE: RecipeField = {
    name: 'table_lineage_mode',
    label: 'Table Lineage Mode',
    helper: 'Select table lineage collector mode',
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
        { label: 'stl_scan_based', value: 'stl_scan_based' },
        { label: 'sql_based', value: 'sql_based' },
        { label: 'mixed', value: 'mixed' },
    ],
};

export const INGEST_TAGS: RecipeField = {
    name: 'ingest_tags',
    label: 'Ingest Tags',
    helper: 'Ingest Tags from source',
    tooltip: 'Ingest Tags from the source. Be careful: This can override Tags entered by users of DataHub.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_tags',
    rules: null,
};

export const INGEST_OWNER: RecipeField = {
    name: 'ingest_owner',
    label: 'Ingest Owner',
    helper: 'Ingest Owner from source',
    tooltip: 'Ingest Owner from source. Be careful: This cah override Owners added by users of DataHub.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_owner',
    rules: null,
};

const includeTablesPath = 'source.config.include_tables';
export const INCLUDE_TABLES: RecipeField = {
    name: 'include_tables',
    label: 'Include Tables',
    helper: 'Extract Tables from source',
    tooltip: 'Extract Tables from source.',
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
    label: 'Include Views',
    helper: 'Extract Views from source',
    tooltip: 'Extract Views from source.',
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
    label: 'Git Repository',
    helper: 'URL or name of Git repository',
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
    label: 'Extract Usage History',
    helper: 'Ingest usage statistics for dashboards',
    tooltip:
        'Experimental (Subject to breaking change) -- Whether to ingest usage statistics for dashboards. Setting this to True will query looker system activity explores to fetch historical dashboard usage.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_usage_history',
    rules: null,
};

export const EXTRACT_OWNERS: RecipeField = {
    name: 'extract_owners',
    label: 'Extract Owners',
    helper: 'Extracts ownership from Looker',
    tooltip:
        'When enabled, extracts ownership from Looker directly. When disabled, ownership is left empty for dashboards and charts.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_owners',
    rules: null,
};

export const SKIP_PERSONAL_FOLDERS: RecipeField = {
    name: 'skip_personal_folders',
    label: 'Skip Personal Folders',
    helper: 'Skip dashboards in personal folders',
    tooltip:
        'Whether to skip ingestion of dashboards in personal folders. Setting this to True will only ingest dashboards in the Shared folder space.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.skip_personal_folders',
    rules: null,
};

const startTimeFieldPath = 'source.config.start_time';
export const START_TIME: RecipeField = {
    name: 'start_time',
    label: 'Start Time',
    helper: 'Earliest date for processing logs',
    tooltip:
        'Earliest date used when processing audit logs for lineage, usage, and more. Default: Last full day in UTC or last time DataHub ingested usage (if stateful ingestion is enabled). Tip: Set this to an older date (e.g. 1 month ago) to bootstrap your first ingestion run, and then reduce for subsequent runs. Changing this may increase the duration of the sync.',
    placeholder: 'Select date and time',
    type: FieldType.DATE,
    fieldPath: startTimeFieldPath,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const isoDateString = get(recipe, startTimeFieldPath);
        if (isoDateString) {
            return moment(isoDateString);
        }
        return isoDateString;
    },
    setValueOnRecipeOverride: (recipe: any, value?: Moment) => setDateValueOnRecipe(recipe, value, startTimeFieldPath),
};

const envFieldPath = 'source.config.env';
export const ENV: RecipeField = {
    name: 'env',
    label: 'Environment',
    tooltip: 'Optional environment label (e.g. PROD/DEV/STG). Defaults to PROD.',
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
    label: 'Table Profiling',
    tooltip: 'Run profiling queries for table statistics.',
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
    label: 'Column Profiling',
    tooltip:
        'Run column-level profiling in addition to table stats. Substantially increases time and cost. Requires Table Profiling enabled.',
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
    label: 'Remove Stale Metadata',
    tooltip:
        'Automatically remove deleted tables from DataHub. Maintains catalog accuracy when Glue tables are dropped.',
    type: FieldType.BOOLEAN,
    fieldPath: statefulIngestionEnabledFieldPath,
    rules: null,
    setValueOnRecipeOverride: setRemoveStaleMetadataOnRecipe,
};
