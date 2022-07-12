import { SNOWFLAKE } from '../conf/snowflake/snowflake';

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
    getValueFromRecipe: (recipe: any) => any;
    setValueOnRecipe: (recipe: any, value: any) => any;
}

export const ACCOUNT_ID: RecipeField = {
    name: 'account_id',
    label: 'Account ID',
    tooltip: 'Snowflake account. e.g. abc48144',
    type: FieldType.TEXT,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.account_id,
    setValueOnRecipe: (recipe: any, value: string) => {
        if (value !== undefined) {
            const updatedRecipe = { ...recipe };
            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            updatedRecipe.source.config.account_id = value;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const WAREHOUSE = {
    name: 'warehouse',
    label: 'Warehouse',
    tooltip: 'Snowflake warehouse.',
    type: FieldType.TEXT,
    rules: [
        {
            required: true,
            message: 'Warehouse is required',
        },
    ],
    getValueFromRecipe: (recipe: any) => recipe.source.config?.warehouse,
    setValueOnRecipe: (recipe: any, value: string) => {
        if (value !== undefined) {
            const updatedRecipe = { ...recipe };
            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            updatedRecipe.source.config.warehouse = value;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const USERNAME = {
    name: 'username',
    label: 'Username',
    tooltip: 'Snowflake username.',
    type: FieldType.TEXT,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.username,
    setValueOnRecipe: (recipe: any, value: string) => {
        if (value !== undefined) {
            const updatedRecipe = { ...recipe };
            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            updatedRecipe.source.config.username = value;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const PASSWORD = {
    name: 'password',
    label: 'Password',
    tooltip: 'Snowflake password.',
    type: FieldType.TEXT,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.password,
    setValueOnRecipe: (recipe: any, value: string) => {
        if (value !== undefined) {
            const updatedRecipe = { ...recipe };
            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            updatedRecipe.source.config.password = value;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const ROLE = {
    name: 'role',
    label: 'Role',
    tooltip: 'Snowflake role.',
    type: FieldType.TEXT,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.role,
    setValueOnRecipe: (recipe: any, value: string) => {
        if (value !== undefined) {
            const updatedRecipe = { ...recipe };
            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            updatedRecipe.source.config.role = value;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const INCLUDE_LINEAGE = {
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

export const DATABASE_ALLOW = {
    name: 'database_pattern.allow',
    label: 'Allow Patterns for Databases',
    tooltip: 'Use Regex here.',
    type: FieldType.LIST,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.table_pattern?.allow,
    setValueOnRecipe: (recipe: any, values: string[]) => {
        if (values !== undefined) {
            const updatedRecipe = { ...recipe };

            const filteredValues = values.filter((v) => !!v);
            if (!filteredValues.length) return { ...recipe };

            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            if (!updatedRecipe.source.config.table_pattern) updatedRecipe.source.config.table_pattern = {};
            updatedRecipe.source.config.table_pattern.allow = filteredValues;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const RECIPE_FIELDS = {
    [SNOWFLAKE]: {
        fields: [ACCOUNT_ID, WAREHOUSE, USERNAME, PASSWORD, ROLE],
        advancedFields: [INCLUDE_LINEAGE],
        filterFields: [DATABASE_ALLOW],
    },
};
