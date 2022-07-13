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
    getValueFromRecipe: (recipe: any) => any;
    setValueOnRecipe: (recipe: any, value: any) => any;
}

function updateParentAndField(recipe: any, field: string, parentField: string) {
    const updatedRecipe = { ...recipe };
    // remove this field from the recipe if it's empty
    if (updatedRecipe.source.config?.[parentField]?.[field]) {
        updatedRecipe.source.config[parentField][field] = undefined;

        // remove its parent if there are no other fields underneath it
        const parentKeys = Object.keys(updatedRecipe.source.config[parentField]);
        if (parentKeys.length === 1 && parentKeys[0] === field) {
            updatedRecipe.source.config[parentField] = undefined;
        }
    }
    return updatedRecipe;
}

export function setFieldValueOnRecipe(
    recipe: any,
    value: string | boolean | null | undefined,
    field: string,
    parentField?: string,
) {
    if (value !== undefined) {
        const recipeValue = value === null ? undefined : value; // clear out fields with null values
        const updatedRecipe = { ...recipe };
        if (!updatedRecipe.source.config) updatedRecipe.source.config = {};

        if (recipeValue === undefined && parentField) {
            // clearing the field and possibly its parent
            const finalRecipe = updateParentAndField(updatedRecipe, field, parentField);
            return finalRecipe;
        }

        if (parentField) {
            if (!updatedRecipe.source.config[parentField]) updatedRecipe.source.config[parentField] = {};
            updatedRecipe.source.config[parentField][field] = recipeValue;
        } else {
            updatedRecipe.source.config[field] = recipeValue;
        }

        return updatedRecipe;
    }
    return recipe;
}

export function setListValuesOnRecipe(recipe: any, values: string[], field: string, parentField: string) {
    if (values !== undefined) {
        const updatedRecipe = { ...recipe };
        if (!updatedRecipe.source.config) updatedRecipe.source.config = {};

        const filteredValues: string[] | undefined = values.filter((v) => !!v);
        if (!filteredValues.length) {
            // clearing the field and possibly its parent
            const finalRecipe = updateParentAndField(updatedRecipe, field, parentField);
            return finalRecipe;
        }

        if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
        if (!updatedRecipe.source.config[parentField]) updatedRecipe.source.config[parentField] = {};
        updatedRecipe.source.config[parentField][field] = filteredValues;
        return updatedRecipe;
    }
    return recipe;
}

export const ACCOUNT_ID: RecipeField = {
    name: 'account_id',
    label: 'Account ID',
    tooltip: 'Snowflake account. e.g. abc48144',
    type: FieldType.TEXT,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.account_id,
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, 'account_id'),
};

export const WAREHOUSE: RecipeField = {
    name: 'warehouse',
    label: 'Warehouse',
    tooltip: 'Snowflake warehouse.',
    type: FieldType.TEXT,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.warehouse,
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, 'warehouse'),
};

export const USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'Snowflake username.',
    type: FieldType.TEXT,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.username,
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, 'username'),
};

export const PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'Snowflake password.',
    type: FieldType.TEXT,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.password,
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, 'password'),
};

export const ROLE: RecipeField = {
    name: 'role',
    label: 'Role',
    tooltip: 'Snowflake role.',
    type: FieldType.TEXT,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.role,
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, 'role'),
};

export const INCLUDE_LINEAGE: RecipeField = {
    name: 'include_lineage',
    label: 'Include Lineage',
    tooltip: 'Include Table and View lineage in your ingestion.',
    type: FieldType.BOOLEAN,
    rules: null,
    getValueFromRecipe: (recipe: any) =>
        recipe.source.config?.include_table_lineage && recipe.source.config?.include_view_lineage,
    setValueOnRecipe: (recipe: any, value: boolean) => {
        if (value !== undefined) {
            const updatedRecipe = { ...recipe };
            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            updatedRecipe.source.config.include_table_lineage = value;
            updatedRecipe.source.config.include_view_lineage = value;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const IGNORE_START_TIME_LINEAGE: RecipeField = {
    name: 'ignore_start_time_lineage',
    label: 'Ignore Start Time Lineage',
    tooltip: 'Get all lineage by ignoring the start_time field. It is suggested to set to true initially.',
    type: FieldType.BOOLEAN,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.ignore_start_time_lineage,
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, 'ignore_start_time_lineage'),
};

export const CHECK_ROLE_GRANTS: RecipeField = {
    name: 'check_role_grants',
    label: 'Check Role Grants',
    tooltip:
        'If set to True then checks role grants at the beginning of the ingestion run. To be used for debugging purposes. If you think everything is working fine then set it to False. In some cases this can take long depending on how many roles you might have.',
    type: FieldType.BOOLEAN,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.check_role_grants,
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, 'check_role_grants'),
};

export const PROFILING_ENABLED: RecipeField = {
    name: 'profiling.enabled',
    label: 'Enable Profiling',
    tooltip: 'Whether profiling should be done.',
    type: FieldType.BOOLEAN,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.profiling?.enabled,
    setValueOnRecipe: (recipe: any, value: string) => setFieldValueOnRecipe(recipe, value, 'enabled', 'profiling'),
};

export const STATEFUL_INGESTION_ENABLED: RecipeField = {
    name: 'stateful_ingestion.enabled',
    label: 'Enable Stateful Ingestion',
    tooltip: 'Enable the type of the ingestion state provider registered with datahub.',
    type: FieldType.BOOLEAN,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.stateful_ingestion?.enabled,
    setValueOnRecipe: (recipe: any, value: string) =>
        setFieldValueOnRecipe(recipe, value, 'enabled', 'stateful_ingestion'),
};

export const DATABASE_ALLOW: RecipeField = {
    name: 'database_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Databases',
    getValueFromRecipe: (recipe: any) => recipe.source.config?.database_pattern?.allow,
    setValueOnRecipe: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, 'allow', 'database_pattern'),
};

export const DATABASE_DENY: RecipeField = {
    name: 'database_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Databases',
    getValueFromRecipe: (recipe: any) => recipe.source.config?.database_pattern?.deny,
    setValueOnRecipe: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, 'deny', 'database_pattern'),
};

export const SCHEMA_ALLOW: RecipeField = {
    name: 'schema_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Schemas',
    getValueFromRecipe: (recipe: any) => recipe.source.config?.schema_pattern?.allow,
    setValueOnRecipe: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, 'allow', 'schema_pattern'),
};

export const SCHEMA_DENY: RecipeField = {
    name: 'schema_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Schemas',
    getValueFromRecipe: (recipe: any) => recipe.source.config?.schema_pattern?.deny,
    setValueOnRecipe: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, 'deny', 'schema_pattern'),
};

export const VIEW_ALLOW: RecipeField = {
    name: 'view_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Views',
    getValueFromRecipe: (recipe: any) => recipe.source.config?.view_pattern?.allow,
    setValueOnRecipe: (recipe: any, values: string[]) => setListValuesOnRecipe(recipe, values, 'allow', 'view_pattern'),
};

export const VIEW_DENY: RecipeField = {
    name: 'view_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Views',
    getValueFromRecipe: (recipe: any) => recipe.source.config?.view_pattern?.deny,
    setValueOnRecipe: (recipe: any, values: string[]) => setListValuesOnRecipe(recipe, values, 'deny', 'view_pattern'),
};

export const TABLE_ALLOW: RecipeField = {
    name: 'table_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Tables',
    getValueFromRecipe: (recipe: any) => recipe.source.config?.table_pattern?.allow,
    setValueOnRecipe: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, 'allow', 'table_pattern'),
};

export const TABLE_DENY: RecipeField = {
    name: 'table_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    type: FieldType.LIST,
    rules: null,
    section: 'Tables',
    getValueFromRecipe: (recipe: any) => recipe.source.config?.table_pattern?.deny,
    setValueOnRecipe: (recipe: any, values: string[]) => setListValuesOnRecipe(recipe, values, 'deny', 'table_pattern'),
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
