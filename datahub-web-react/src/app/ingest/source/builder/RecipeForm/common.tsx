import React from 'react';
import { set, get } from 'lodash';
import moment, { Moment } from 'moment-timezone';

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

export interface RecipeField {
    name: string;
    label: string;
    tooltip: string | React.ReactNode;
    type: FieldType;
    fieldPath: string | string[];
    rules: any[] | null;
    section?: string;
    options?: Option[];
    buttonLabel?: string;
    keyField?: RecipeField;
    fields?: RecipeField[];
    getValueFromRecipeOverride?: (recipe: any) => any;
    setValueOnRecipeOverride?: (recipe: any, value: any) => any;
    placeholder?: string;
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
    if (value !== undefined) {
        if (value === null || value === '') {
            clearFieldAndParents(updatedRecipe, fieldPath);
            return updatedRecipe;
        }
        set(updatedRecipe, fieldPath, value);
    }
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
export const DATABASE_ALLOW: RecipeField = {
    name: 'database_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_db$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: databaseAllowFieldPath,
    rules: null,
    section: 'Databases',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, databaseAllowFieldPath),
};

const databaseDenyFieldPath = 'source.config.database_pattern.deny';
export const DATABASE_DENY: RecipeField = {
    name: 'database_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_db$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: databaseDenyFieldPath,
    rules: null,
    section: 'Databases',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, databaseDenyFieldPath),
};

const dashboardAllowFieldPath = 'source.config.dashboard_pattern.allow';
export const DASHBOARD_ALLOW: RecipeField = {
    name: 'dashboard_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: dashboardAllowFieldPath,
    rules: null,
    section: 'Dashboards',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, dashboardAllowFieldPath),
};

const dashboardDenyFieldPath = 'source.config.dashboard_pattern.deny';
export const DASHBOARD_DENY: RecipeField = {
    name: 'dashboard_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: dashboardDenyFieldPath,
    rules: null,
    section: 'Dashboards',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, dashboardDenyFieldPath),
};

const schemaAllowFieldPath = 'source.config.schema_pattern.allow';
export const SCHEMA_ALLOW: RecipeField = {
    name: 'schema_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaAllowFieldPath,
    rules: null,
    section: 'Schemas',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaAllowFieldPath),
};

const schemaDenyFieldPath = 'source.config.schema_pattern.deny';
export const SCHEMA_DENY: RecipeField = {
    name: 'schema_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaDenyFieldPath,
    rules: null,
    section: 'Schemas',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaDenyFieldPath),
};

const viewAllowFieldPath = 'source.config.view_pattern.allow';
export const VIEW_ALLOW: RecipeField = {
    name: 'view_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: viewAllowFieldPath,
    rules: null,
    section: 'Views',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, viewAllowFieldPath),
};

const viewDenyFieldPath = 'source.config.view_pattern.deny';
export const VIEW_DENY: RecipeField = {
    name: 'view_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: viewDenyFieldPath,
    rules: null,
    section: 'Views',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, viewDenyFieldPath),
};

const tableAllowFieldPath = 'source.config.table_pattern.allow';
export const TABLE_ALLOW: RecipeField = {
    name: 'table_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: tableAllowFieldPath,
    rules: null,
    section: 'Tables',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableAllowFieldPath),
};

const tableDenyFieldPath = 'source.config.table_pattern.deny';
export const TABLE_DENY: RecipeField = {
    name: 'table_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: tableDenyFieldPath,
    rules: null,
    section: 'Tables',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableDenyFieldPath),
};

/* ---------------------------------------------------- Advance Section ---------------------------------------------------- */
const includeLineageFieldPathA = 'source.config.include_table_lineage';
const includeLineageFieldPathB = 'source.config.include_view_lineage';
export const INCLUDE_LINEAGE: RecipeField = {
    name: 'include_lineage',
    label: 'Include Lineage',
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
    tooltip: 'Whether or not table lineage should be ingested.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_table_lineage',
    rules: null,
};

export const PROFILING_ENABLED: RecipeField = {
    name: 'profiling.enabled',
    label: 'Enable Profiling',
    tooltip: 'Whether profiling should be done.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.profiling.enabled',
    rules: null,
};

export const STATEFUL_INGESTION_ENABLED: RecipeField = {
    name: 'stateful_ingestion.enabled',
    label: 'Enable Stateful Ingestion',
    tooltip: 'Remove stale datasets from datahub once they have been deleted in the source.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.stateful_ingestion.enabled',
    rules: null,
};

export const UPSTREAM_LINEAGE_IN_REPORT: RecipeField = {
    name: 'upstream_lineage_in_report',
    label: 'Include Upstream Lineage In Report.',
    tooltip: 'Useful for debugging lineage information. Set to True to see the raw lineage created internally.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.upstream_lineage_in_report',
    rules: null,
};

export const TABLE_LINEAGE_MODE: RecipeField = {
    name: 'table_lineage_mode',
    label: 'Table Lineage Mode',
    tooltip: (
        <div>
            <p>
                Which table lineage collector mode to use. Check out{' '}
                <a
                    href="https://datahubproject.io/docs/generated/ingestion/sources/redshift/#config-details"
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
    tooltip: 'Ingest Tags from source. This will override Tags entered from UI',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_tags',
    rules: null,
};

export const INGEST_OWNER: RecipeField = {
    name: 'ingest_owner',
    label: 'Ingest Owner',
    tooltip: 'Ingest Owner from source. This will override Owner info entered from UI',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.ingest_owner',
    rules: null,
};

export const GITHUB_INFO_REPO: RecipeField = {
    name: 'github_info.repo',
    label: 'GitHub Repo',
    tooltip: (
        <div>
            <p>
                Name of your github repo. e.g. repo for{' '}
                <a href="https://github.com/datahub-project/datahub" target="_blank" rel="noreferrer">
                    https://github.com/datahub-project/datahub
                </a>{' '}
                is datahub-project/datahub.
            </p>
        </div>
    ),
    type: FieldType.TEXT,
    fieldPath: 'source.config.github_info.repo',
    rules: null,
};

export const EXTRACT_USAGE_HISTORY: RecipeField = {
    name: 'extract_usage_history',
    label: 'Extract Usage History',
    tooltip:
        'Experimental (Subject to breaking change) -- Whether to ingest usage statistics for dashboards. Setting this to True will query looker system activity explores to fetch historical dashboard usage.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_usage_history',
    rules: null,
};

export const EXTRACT_OWNERS: RecipeField = {
    name: 'extract_owners',
    label: 'Extract Owners',
    tooltip:
        'When enabled, extracts ownership from Looker directly. When disabled, ownership is left empty for dashboards and charts.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.extract_owners',
    rules: null,
};

export const SKIP_PERSONAL_FOLDERS: RecipeField = {
    name: 'skip_personal_folders',
    label: 'Skip Personal Folders',
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
    tooltip:
        'Earliest date of audit logs to process for usage, lineage etc. Default: Last full day in UTC or last time DataHub ingested usage (if stateful ingestion is enabled). Tip: Set this to an older date (e.g. 1 month ago) for your first ingestion run, and then uncheck it for future runs.',
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
