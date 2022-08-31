import { RecipeField, FieldType, setListValuesOnRecipe } from './common';

export const TABLEAU_CONNECTION_URI: RecipeField = {
    name: 'connect_uri',
    label: 'Connection URI',
    tooltip: 'Tableau host URL.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.connect_uri',
    rules: null,
};

const tableauProjectFieldPath = 'source.config.projects';
export const TABLEAU_PROJECT: RecipeField = {
    name: 'projects',
    label: 'Projects',
    tooltip: 'List of projects',
    type: FieldType.LIST,
    buttonLabel: 'Add project',
    fieldPath: tableauProjectFieldPath,
    rules: null,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, tableauProjectFieldPath),
};

export const TABLEAU_SITE: RecipeField = {
    name: 'site',
    label: 'Tableau Site',
    tooltip:
        'Tableau Site. Always required for Tableau Online. Use empty string to connect with Default site on Tableau Server.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.site',
    rules: null,
};

export const TABLEAU_USERNAME: RecipeField = {
    name: 'tableau.username',
    label: 'Username',
    tooltip: 'Tableau username, must be set if authenticating using username/password.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    rules: null,
};

export const TABLEAU_PASSWORD: RecipeField = {
    name: 'tableau.password',
    label: 'Password',
    tooltip: 'Tableau password, must be set if authenticating using username/password.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    rules: null,
};
