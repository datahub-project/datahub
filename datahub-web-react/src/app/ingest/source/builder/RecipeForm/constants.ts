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

export const WAREHOUSE: RecipeField = {
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

export const USERNAME: RecipeField = {
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

export const PASSWORD: RecipeField = {
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

export const ROLE: RecipeField = {
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
    setValueOnRecipe: (recipe: any, value: boolean) => {
        if (value !== undefined) {
            const updatedRecipe = { ...recipe };
            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            updatedRecipe.source.config.ignore_start_time_lineage = value;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const CHECK_ROLE_GRANTS: RecipeField = {
    name: 'check_role_grants',
    label: 'Check Role Grants',
    tooltip:
        'If set to True then checks role grants at the beginning of the ingestion run. To be used for debugging purposes. If you think everything is working fine then set it to False. In some cases this can take long depending on how many roles you might have.',
    type: FieldType.BOOLEAN,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.check_role_grants,
    setValueOnRecipe: (recipe: any, value: boolean) => {
        if (value !== undefined) {
            const updatedRecipe = { ...recipe };
            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            updatedRecipe.source.config.check_role_grants = value;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const PROFILING_ENABLED: RecipeField = {
    name: 'profiling.enabled',
    label: 'Enable Profiling',
    tooltip: 'Whether profiling should be done.',
    type: FieldType.BOOLEAN,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.profiling?.enabled,
    setValueOnRecipe: (recipe: any, value: boolean) => {
        if (value !== undefined) {
            const updatedRecipe = { ...recipe };
            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            if (!updatedRecipe.source.config.profiling) updatedRecipe.source.config.profiling = {};
            updatedRecipe.source.config.profiling.enabled = value;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const STATEFUL_INGESTION_ENABLED: RecipeField = {
    name: 'stateful_ingestion.enabled',
    label: 'Enable Stateful Ingestion',
    tooltip: 'Enable the type of the ingestion state provider registered with datahub.',
    type: FieldType.BOOLEAN,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.stateful_ingestion?.enabled,
    setValueOnRecipe: (recipe: any, value: boolean) => {
        if (value !== undefined) {
            const updatedRecipe = { ...recipe };
            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            if (!updatedRecipe.source.config.stateful_ingestion) updatedRecipe.source.config.stateful_ingestion = {};
            updatedRecipe.source.config.stateful_ingestion.enabled = value;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const DATABASE_ALLOW: RecipeField = {
    name: 'database_pattern.allow',
    label: 'Allow Patterns for Databases',
    tooltip: 'Use Regex here.',
    type: FieldType.LIST,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.database_pattern?.allow,
    setValueOnRecipe: (recipe: any, values: string[]) => {
        if (values !== undefined) {
            const updatedRecipe = { ...recipe };

            const filteredValues = values.filter((v) => !!v);
            if (!filteredValues.length) return { ...recipe };

            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            if (!updatedRecipe.source.config.database_pattern) updatedRecipe.source.config.database_pattern = {};
            updatedRecipe.source.config.database_pattern.allow = filteredValues;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const DATABASE_DENY: RecipeField = {
    name: 'database_pattern.deny',
    label: 'Deny Patterns for Databases',
    tooltip: 'Use Regex here.',
    type: FieldType.LIST,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.database_pattern?.deny,
    setValueOnRecipe: (recipe: any, values: string[]) => {
        if (values !== undefined) {
            const updatedRecipe = { ...recipe };

            const filteredValues = values.filter((v) => !!v);
            if (!filteredValues.length) return { ...recipe };

            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            if (!updatedRecipe.source.config.database_pattern) updatedRecipe.source.config.database_pattern = {};
            updatedRecipe.source.config.database_pattern.deny = filteredValues;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const SCHEMA_ALLOW: RecipeField = {
    name: 'schema_pattern.allow',
    label: 'Allow Patterns for Schemas',
    tooltip: 'Use Regex here.',
    type: FieldType.LIST,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.schema_pattern?.allow,
    setValueOnRecipe: (recipe: any, values: string[]) => {
        if (values !== undefined) {
            const updatedRecipe = { ...recipe };

            const filteredValues = values.filter((v) => !!v);
            if (!filteredValues.length) return { ...recipe };

            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            if (!updatedRecipe.source.config.schema_pattern) updatedRecipe.source.config.schema_pattern = {};
            updatedRecipe.source.config.schema_pattern.allow = filteredValues;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const SCHEMA_DENY: RecipeField = {
    name: 'schema_pattern.deny',
    label: 'Deny Patterns for Schemas',
    tooltip: 'Use Regex here.',
    type: FieldType.LIST,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.schema_pattern?.deny,
    setValueOnRecipe: (recipe: any, values: string[]) => {
        if (values !== undefined) {
            const updatedRecipe = { ...recipe };

            const filteredValues = values.filter((v) => !!v);
            if (!filteredValues.length) return { ...recipe };

            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            if (!updatedRecipe.source.config.schema_pattern) updatedRecipe.source.config.schema_pattern = {};
            updatedRecipe.source.config.schema_pattern.deny = filteredValues;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const VIEW_ALLOW: RecipeField = {
    name: 'view_pattern.allow',
    label: 'Allow Patterns for Views',
    tooltip: 'Use Regex here.',
    type: FieldType.LIST,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.view_pattern?.allow,
    setValueOnRecipe: (recipe: any, values: string[]) => {
        if (values !== undefined) {
            const updatedRecipe = { ...recipe };

            const filteredValues = values.filter((v) => !!v);
            if (!filteredValues.length) return { ...recipe };

            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            if (!updatedRecipe.source.config.view_pattern) updatedRecipe.source.config.view_pattern = {};
            updatedRecipe.source.config.view_pattern.allow = filteredValues;
            return updatedRecipe;
        }
        return recipe;
    },
};

export const VIEW_DENY: RecipeField = {
    name: 'view_pattern.deny',
    label: 'Deny Patterns for Views',
    tooltip: 'Use Regex here.',
    type: FieldType.LIST,
    rules: null,
    getValueFromRecipe: (recipe: any) => recipe.source.config?.view_pattern?.deny,
    setValueOnRecipe: (recipe: any, values: string[]) => {
        if (values !== undefined) {
            const updatedRecipe = { ...recipe };

            const filteredValues = values.filter((v) => !!v);
            if (!filteredValues.length) return { ...recipe };

            if (!updatedRecipe.source.config) updatedRecipe.source.config = {};
            if (!updatedRecipe.source.config.view_pattern) updatedRecipe.source.config.view_pattern = {};
            updatedRecipe.source.config.view_pattern.deny = filteredValues;
            return updatedRecipe;
        }
        return recipe;
    },
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
        filterFields: [DATABASE_ALLOW, DATABASE_DENY, SCHEMA_ALLOW, SCHEMA_DENY, VIEW_ALLOW, VIEW_DENY],
    },
};
