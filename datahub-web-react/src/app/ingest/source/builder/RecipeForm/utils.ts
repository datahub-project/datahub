import { set, get } from 'lodash';
import { SNOWFLAKE } from '../../conf/snowflake/snowflake';

export enum FieldType {
    TEXT,
    BOOLEAN,
    LIST,
}

export interface RecipeField {
    name: string;
    label: string;
    tooltip: string;
    type: FieldType;
    rules: any[] | null;
    section?: string;
    fieldPath?: string;
    getValueFromRecipe: (recipe: any) => any;
    setValueOnRecipe: (recipe: any, value: any) => any;
}

function clearFieldAndParents(recipe: any, fieldPath: string) {
    set(recipe, fieldPath, undefined);

    const updatedFieldPath = fieldPath.split('.').slice(0, -1).join('.'); // remove last item from fieldPath
    if (updatedFieldPath) {
        const parentKeys = Object.keys(get(recipe, updatedFieldPath));

        // only child left is what we just set as undefined
        if (parentKeys.length === 1) {
            clearFieldAndParents(recipe, updatedFieldPath);
        }
    }
    return recipe;
}

export function setFieldValueOnRecipe(recipe: any, value: any, fieldPath: string) {
    const updatedRecipe = { ...recipe };
    if (value !== undefined) {
        if (value === null) {
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

const accountIdFieldPath = 'source.config.account_id';
export const ACCOUNT_ID: RecipeField = {
    name: 'account_id',
    label: 'Account ID',
    tooltip: 'Snowflake account. e.g. abc48144',
    type: FieldType.TEXT,
    rules: null,
    fieldPath: accountIdFieldPath,
    getValueFromRecipe: (recipe: any) => get(recipe, accountIdFieldPath),
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, accountIdFieldPath),
};

const warehouseFieldPath = 'source.config.warehouse';
export const WAREHOUSE: RecipeField = {
    name: 'warehouse',
    label: 'Warehouse',
    tooltip: 'Snowflake warehouse.',
    type: FieldType.TEXT,
    rules: null,
    getValueFromRecipe: (recipe: any) => get(recipe, warehouseFieldPath),
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, warehouseFieldPath),
};

const usernameFieldPath = 'source.config.username';
export const USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'Snowflake username.',
    type: FieldType.TEXT,
    rules: null,
    getValueFromRecipe: (recipe: any) => get(recipe, usernameFieldPath),
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, usernameFieldPath),
};

const passwordFieldPath = 'source.config.password';
export const PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'Snowflake password.',
    type: FieldType.TEXT,
    rules: null,
    getValueFromRecipe: (recipe: any) => get(recipe, passwordFieldPath),
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, passwordFieldPath),
};

const roleFieldPath = 'source.config.role';
export const ROLE: RecipeField = {
    name: 'role',
    label: 'Role',
    tooltip: 'Snowflake role.',
    type: FieldType.TEXT,
    rules: null,
    getValueFromRecipe: (recipe: any) => get(recipe, roleFieldPath),
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, roleFieldPath),
};

const includeLineageFieldPathA = 'source.config.include_table_lineage';
const includeLineageFieldPathB = 'source.config.include_view_lineage';
export const INCLUDE_LINEAGE: RecipeField = {
    name: 'include_lineage',
    label: 'Include Lineage',
    tooltip: 'Include Table and View lineage in your ingestion.',
    type: FieldType.BOOLEAN,
    rules: null,
    getValueFromRecipe: (recipe: any) => get(recipe, includeLineageFieldPathA) && get(recipe, includeLineageFieldPathB),
    setValueOnRecipe: (recipe: any, value: boolean) => {
        let updatedRecipe = setFieldValueOnRecipe(recipe, value, includeLineageFieldPathA);
        updatedRecipe = setFieldValueOnRecipe(updatedRecipe, value, includeLineageFieldPathB);
        return updatedRecipe;
    },
};

const ignoreStartTimeLineageFieldPath = 'source.config.ignore_start_time_lineage';
export const IGNORE_START_TIME_LINEAGE: RecipeField = {
    name: 'ignore_start_time_lineage',
    label: 'Ignore Start Time Lineage',
    tooltip: 'Get all lineage by ignoring the start_time field. It is suggested to set to true initially.',
    type: FieldType.BOOLEAN,
    rules: null,
    getValueFromRecipe: (recipe: any) => get(recipe, ignoreStartTimeLineageFieldPath),
    setValueOnRecipe: (recipe: any, value: string) =>
        setFieldValueOnRecipe(recipe, value, ignoreStartTimeLineageFieldPath),
};

const checkRoleGrantsFieldPath = 'source.config.check_role_grants';
export const CHECK_ROLE_GRANTS: RecipeField = {
    name: 'check_role_grants',
    label: 'Check Role Grants',
    tooltip:
        'If set to True then checks role grants at the beginning of the ingestion run. To be used for debugging purposes. If you think everything is working fine then set it to False. In some cases this can take long depending on how many roles you might have.',
    type: FieldType.BOOLEAN,
    rules: null,
    getValueFromRecipe: (recipe: any) => get(recipe, checkRoleGrantsFieldPath),
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, checkRoleGrantsFieldPath),
};

const profilingEnabledFieldPath = 'source.config.profiling.enabled';
export const PROFILING_ENABLED: RecipeField = {
    name: 'profiling.enabled',
    label: 'Enable Profiling',
    tooltip: 'Whether profiling should be done.',
    type: FieldType.BOOLEAN,
    rules: null,
    getValueFromRecipe: (recipe: any) => get(recipe, profilingEnabledFieldPath),
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, profilingEnabledFieldPath),
};

const statefulIngestionEnabledFieldPath = 'source.config.stateful_ingestion.enabled';
export const STATEFUL_INGESTION_ENABLED: RecipeField = {
    name: 'stateful_ingestion.enabled',
    label: 'Enable Stateful Ingestion',
    tooltip: 'Enable the type of the ingestion state provider registered with datahub.',
    type: FieldType.BOOLEAN,
    rules: null,
    getValueFromRecipe: (recipe: any) => get(recipe, statefulIngestionEnabledFieldPath),
    setValueOnRecipe: (recipe: any, value: string) =>
        setFieldValueOnRecipe(recipe, value, statefulIngestionEnabledFieldPath),
};

const databaseAllowFieldPath = 'source.config.database_pattern.allow';
export const DATABASE_ALLOW: RecipeField = {
    name: 'database_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Databases',
    getValueFromRecipe: (recipe: any) => get(recipe, databaseAllowFieldPath),
    setValueOnRecipe: (recipe: any, values: string[]) => setListValuesOnRecipe(recipe, values, databaseAllowFieldPath),
};

const databaseDenyFieldPath = 'source.config.database_pattern.deny';
export const DATABASE_DENY: RecipeField = {
    name: 'database_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Databases',
    getValueFromRecipe: (recipe: any) => get(recipe, databaseDenyFieldPath),
    setValueOnRecipe: (recipe: any, values: string[]) => setListValuesOnRecipe(recipe, values, databaseDenyFieldPath),
};

const schemaAllowFieldPath = 'source.config.schema_pattern.allow';
export const SCHEMA_ALLOW: RecipeField = {
    name: 'schema_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Schemas',
    getValueFromRecipe: (recipe: any) => get(recipe, schemaAllowFieldPath),
    setValueOnRecipe: (recipe: any, values: string[]) => setListValuesOnRecipe(recipe, values, schemaAllowFieldPath),
};

const schemaDenyFieldPath = 'source.config.schema_pattern.deny';
export const SCHEMA_DENY: RecipeField = {
    name: 'schema_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Schemas',
    getValueFromRecipe: (recipe: any) => get(recipe, schemaDenyFieldPath),
    setValueOnRecipe: (recipe: any, values: string[]) => setListValuesOnRecipe(recipe, values, schemaDenyFieldPath),
};

const viewAllowFieldPath = 'source.config.view_pattern.allow';
export const VIEW_ALLOW: RecipeField = {
    name: 'view_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Views',
    getValueFromRecipe: (recipe: any) => get(recipe, viewAllowFieldPath),
    setValueOnRecipe: (recipe: any, values: string[]) => setListValuesOnRecipe(recipe, values, viewAllowFieldPath),
};

const viewDenyFieldPath = 'source.config.view_pattern.deny';
export const VIEW_DENY: RecipeField = {
    name: 'view_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Views',
    getValueFromRecipe: (recipe: any) => get(recipe, viewDenyFieldPath),
    setValueOnRecipe: (recipe: any, values: string[]) => setListValuesOnRecipe(recipe, values, viewDenyFieldPath),
};

const tableAllowFieldPath = 'source.config.table_pattern.allow';
export const TABLE_ALLOW: RecipeField = {
    name: 'table_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Tables',
    getValueFromRecipe: (recipe: any) => get(recipe, tableAllowFieldPath),
    setValueOnRecipe: (recipe: any, values: string[]) => setListValuesOnRecipe(recipe, values, tableAllowFieldPath),
};

const tableDenyFieldPath = 'source.config.table_pattern.deny';
export const TABLE_DENY: RecipeField = {
    name: 'table_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Tables',
    getValueFromRecipe: (recipe: any) => get(recipe, tableDenyFieldPath),
    setValueOnRecipe: (recipe: any, values: string[]) => setListValuesOnRecipe(recipe, values, tableDenyFieldPath),
};

export const RECIPE_FIELDS = {
    [SNOWFLAKE]: {
        fields: [ACCOUNT_ID, WAREHOUSE, USERNAME, PASSWORD, ROLE],
        advancedFields: [
            INCLUDE_LINEAGE,
            IGNORE_START_TIME_LINEAGE,
            CHECK_ROLE_GRANTS,
            PROFILING_ENABLED,
            STATEFUL_INGESTION_ENABLED,
        ],
        filterFields: [
            TABLE_ALLOW,
            TABLE_DENY,
            DATABASE_ALLOW,
            DATABASE_DENY,
            SCHEMA_ALLOW,
            SCHEMA_DENY,
            VIEW_ALLOW,
            VIEW_DENY,
        ],
    },
};

export const CONNECTORS_WITH_FORM = new Set(Object.keys(RECIPE_FIELDS));
