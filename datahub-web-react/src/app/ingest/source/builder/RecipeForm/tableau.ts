import { RecipeField, FieldType, setListValuesOnRecipe } from './common';

export const TABLEAU_CONNECTION_URI: RecipeField = {
    name: 'connect_uri',
    label: 'Host URL',
    tooltip: 'The URL where the Tableau instance is hosted.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.connect_uri',
    placeholder: 'https://prod-ca-a.online.tableau.com',
    required: true,
    rules: null,
};

const tableauProjectFieldPath = 'source.config.projects';
export const TABLEAU_PROJECT: RecipeField = {
    name: 'projects',
    label: 'Projects',
    tooltip: 'The list of Projects to extract metadata for.',
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
        'The Tableau Site. Required for Tableau Online. Leave this blank to extract from the default site on Tableau Server.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.site',
    placeholder: 'datahub',
    rules: null,
};

export const TABLEAU_TOKEN_NAME: RecipeField = {
    name: 'tableau.token_name',
    label: 'Token Name',
    tooltip:
        'The name of the Personal Access Token used to extract metadata. Required if authenticating using a Personal Access Token.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'access_token_name',
    rules: null,
};

export const TABLEAU_TOKEN_VALUE: RecipeField = {
    name: 'tableau.token_value',
    label: 'Token Value',
    tooltip:
        'The value of the Personal Access Token used to extract metadata. Required if authenticating using a Personal Access Token.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'access_token_value',
    rules: null,
};

export const TABLEAU_USERNAME: RecipeField = {
    name: 'tableau.username',
    label: 'Username',
    tooltip: 'Tableau username. Only required if Token is not provided.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'tableau',
    rules: null,
};

export const TABLEAU_PASSWORD: RecipeField = {
    name: 'tableau.password',
    label: 'Password',
    tooltip: 'Tableau password. Only required if Token is not provided.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    rules: null,
};
