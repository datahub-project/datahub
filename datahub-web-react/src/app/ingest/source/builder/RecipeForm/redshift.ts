import { RecipeField, FieldType, setListValuesOnRecipe } from './common';

export const REDSHIFT_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host Port',
    tooltip: 'Host URL.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    rules: null,
};

export const REDSHIFT_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    tooltip: 'Database (catalog).',
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    rules: null,
};

export const REDSHIFT_USERNAME: RecipeField = {
    name: 'redshift.username',
    label: 'Redshift username',
    tooltip: 'Username',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    rules: null,
};

export const REDSHIFT_PASSWORD: RecipeField = {
    name: 'redshift.password',
    label: 'Redshift password',
    tooltip: 'Password',
    type: FieldType.TEXT,
    fieldPath: 'source.config.password',
    rules: null,
};

const schemaAllowFieldPath = 'source.config.schema_pattern.allow';
export const REDSHIFT_SCHEMA_ALLOW: RecipeField = {
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
export const REDSHIFT_SCHEMA_DENY: RecipeField = {
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
export const REDSHIFT_TABLE_ALLOW: RecipeField = {
    name: 'table_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_schema\\.table_name$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: tableAllowFieldPath,
    rules: null,
    section: 'Tables',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableAllowFieldPath),
};

const tableDenyFieldPath = 'source.config.table_pattern.deny';
export const REDSHIFT_TABLE_DENY: RecipeField = {
    name: 'table_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_schema\\.table_name$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: tableDenyFieldPath,
    rules: null,
    section: 'Tables',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableDenyFieldPath),
};

const viewAllowFieldPath = 'source.config.view_pattern.allow';
export const REDSHIFT_VIEW_ALLOW: RecipeField = {
    name: 'view_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_schema\\.view_name$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: viewAllowFieldPath,
    rules: null,
    section: 'Views',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, viewAllowFieldPath),
};

const viewDenyFieldPath = 'source.config.view_pattern.deny';
export const REDSHIFT_VIEW_DENY: RecipeField = {
    name: 'view_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_schema\\.view_name$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: viewDenyFieldPath,
    rules: null,
    section: 'Views',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, viewDenyFieldPath),
};
