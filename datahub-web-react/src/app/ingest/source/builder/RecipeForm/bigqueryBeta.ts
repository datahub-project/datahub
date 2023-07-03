import { FieldType, RecipeField, setListValuesOnRecipe } from './common';

export const BIGQUERY_BETA_PROJECT_ID: RecipeField = {
    name: 'credential.project_id',
    label: 'Project ID',
    tooltip: "The Project ID, which can be found in your service account's JSON Key (project_id)",
    placeholder: 'my-project-123',
    type: FieldType.TEXT,
    fieldPath: 'source.config.credential.project_id',
    rules: null,
    required: true,
};

const projectIdAllowFieldPath = 'source.config.project_id_pattern.allow';
export const PROJECT_ALLOW: RecipeField = {
    name: 'project_id_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here to filter for project IDs.',
    placeholder: '^my_db$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: projectIdAllowFieldPath,
    rules: null,
    section: 'Projects',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, projectIdAllowFieldPath),
};

const projectIdDenyFieldPath = 'source.config.project_id_pattern.deny';
export const PROJECT_DENY: RecipeField = {
    name: 'project_id_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here to filter for project IDs.',
    placeholder: '^my_db$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: projectIdDenyFieldPath,
    rules: null,
    section: 'Projects',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, projectIdDenyFieldPath),
};

const datasetAllowFieldPath = 'source.config.dataset_pattern.allow';
export const DATASET_ALLOW: RecipeField = {
    name: 'dataset_pattern.allow',
    label: 'Allow Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_db$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: datasetAllowFieldPath,
    rules: null,
    section: 'Datasets',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, datasetAllowFieldPath),
};

const datasetDenyFieldPath = 'source.config.dataset_pattern.deny';
export const DATASET_DENY: RecipeField = {
    name: 'dataset_pattern.deny',
    label: 'Deny Patterns',
    tooltip: 'Use regex here.',
    placeholder: '^my_db$',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: datasetDenyFieldPath,
    rules: null,
    section: 'Datasets',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, datasetDenyFieldPath),
};
