import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

export const BIGQUERY_BETA_PROJECT_ID: RecipeField = {
    name: 'credential.project_id',
    label: 'Project ID',
    helper: 'Project ID from service account',
    tooltip: "The Project ID, which can be found in your service account's JSON Key (project_id)",
    placeholder: 'my-project-123',
    type: FieldType.TEXT,
    fieldPath: 'source.config.credential.project_id',
    rules: null,
    required: true,
};

const projectIdAllowFieldPath = 'source.config.project_id_pattern.allow';
export const PROJECT_ALLOW: FilterRecipeField = {
    name: 'project_id_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Filter for project IDs',
    tooltip: 'Use regex here to filter for project IDs.',
    placeholder: '^my_db$',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: projectIdAllowFieldPath,
    rules: null,
    section: 'Projects',
    filteringResource: 'Project',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, projectIdAllowFieldPath),
};

const projectIdDenyFieldPath = 'source.config.project_id_pattern.deny';
export const PROJECT_DENY: FilterRecipeField = {
    name: 'project_id_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Filter out project IDs',
    tooltip: 'Use regex here to filter for project IDs.',
    placeholder: '^my_db$',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: projectIdDenyFieldPath,
    rules: null,
    section: 'Projects',
    filteringResource: 'Project',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, projectIdDenyFieldPath),
};

const datasetAllowFieldPath = 'source.config.dataset_pattern.allow';
export const DATASET_ALLOW: FilterRecipeField = {
    name: 'dataset_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Allow specific datasets',
    tooltip: 'Use regex here.',
    placeholder: '^my_db$',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: datasetAllowFieldPath,
    rules: null,
    section: 'Datasets',
    filteringResource: 'Dataset',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, datasetAllowFieldPath),
};

const datasetDenyFieldPath = 'source.config.dataset_pattern.deny';
export const DATASET_DENY: FilterRecipeField = {
    name: 'dataset_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Deny specific datasets',
    tooltip: 'Use regex here.',
    placeholder: '^my_db$',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: datasetDenyFieldPath,
    rules: null,
    section: 'Datasets',
    filteringResource: 'Dataset',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, datasetDenyFieldPath),
};
