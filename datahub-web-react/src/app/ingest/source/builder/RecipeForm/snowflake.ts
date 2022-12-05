import { FieldType, RecipeField, setListValuesOnRecipe } from './common';

export const SNOWFLAKE_ACCOUNT_ID: RecipeField = {
    name: 'account_id',
    label: 'Account ID',
    tooltip: 'Snowflake account. e.g. abc48144',
    type: FieldType.TEXT,
    fieldPath: 'source.config.account_id',
    rules: null,
};

export const SNOWFLAKE_WAREHOUSE: RecipeField = {
    name: 'warehouse',
    label: 'Warehouse',
    tooltip: 'Snowflake warehouse.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.warehouse',
    rules: null,
};

export const SNOWFLAKE_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'Snowflake username.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.username',
    rules: null,
};

export const SNOWFLAKE_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'Snowflake password.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    rules: null,
};

export const SNOWFLAKE_ROLE: RecipeField = {
    name: 'role',
    label: 'Role',
    tooltip: 'Snowflake role.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.role',
    rules: null,
};

const schemaAllowFieldPath = 'source.config.schema_pattern.allow';
export const SNOWFLAKE_SCHEMA_ALLOW: RecipeField = {
    name: 'schema_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_schema$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaAllowFieldPath,
    rules: null,
    section: 'Schemas',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaAllowFieldPath),
};

const schemaDenyFieldPath = 'source.config.schema_pattern.deny';
export const SNOWFLAKE_SCHEMA_DENY: RecipeField = {
    name: 'schema_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_schema$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: schemaDenyFieldPath,
    rules: null,
    section: 'Schemas',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, schemaDenyFieldPath),
};

const tableAllowFieldPath = 'source.config.table_pattern.allow';
export const SNOWFLAKE_TABLE_ALLOW: RecipeField = {
    name: 'table_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_db\\.my_schema\\.table_name$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: tableAllowFieldPath,
    rules: null,
    section: 'Tables',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableAllowFieldPath),
};

const tableDenyFieldPath = 'source.config.table_pattern.deny';
export const SNOWFLAKE_TABLE_DENY: RecipeField = {
    name: 'table_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_db\\.my_schema\\.table_name$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: tableDenyFieldPath,
    rules: null,
    section: 'Tables',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableDenyFieldPath),
};

const viewAllowFieldPath = 'source.config.view_pattern.allow';
export const SNOWFLAKE_VIEW_ALLOW: RecipeField = {
    name: 'view_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_db\\.my_schema\\.view_name$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: viewAllowFieldPath,
    rules: null,
    section: 'Views',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, viewAllowFieldPath),
};

const viewDenyFieldPath = 'source.config.view_pattern.deny';
export const SNOWFLAKE_VIEW_DENY: RecipeField = {
    name: 'view_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_db\\.my_schema\\.view_name$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: viewDenyFieldPath,
    rules: null,
    section: 'Views',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, viewDenyFieldPath),
};
