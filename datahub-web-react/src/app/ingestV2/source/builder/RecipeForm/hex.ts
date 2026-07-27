import {
    FieldType,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

export const HEX_WORKSPACE_NAME: RecipeField = {
    name: 'workspace_name',
    label: 'Workspace Name',
    helper: 'Your Hex workspace name, visible in your Hex home page URL: https://app.hex.tech/<workspace_name>',
    tooltip: 'The Hex workspace name or workspace ID to ingest from.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.workspace_name',
    placeholder: 'my-workspace',
    required: true,
    rules: null,
};

export const HEX_TOKEN: RecipeField = {
    name: 'token',
    label: 'API Token',
    helper: 'Workspace Token or Personal Access Token from your Hex workspace settings.',
    tooltip: 'A Hex Workspace Token (recommended) or Personal Access Token with read access.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.token',
    placeholder: 'hxtp_...',
    required: true,
    rules: null,
};

const projectAllowFieldPath = 'source.config.project_title_pattern.allow';
export const HEX_PROJECT_ALLOW: FilterRecipeField = {
    name: 'project_title_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific Hex Projects by title',
    tooltip:
        'Only include Hex Projects whose title matches one of the provided regular expressions. If not provided, all Projects are included.',
    type: FieldType.LIST,
    rule: FilterRule.INCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: projectAllowFieldPath,
    rules: null,
    section: 'Projects',
    filteringResource: 'Project',
    placeholder: 'My Dashboard.*',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, projectAllowFieldPath),
};

const projectDenyFieldPath = 'source.config.project_title_pattern.deny';
export const HEX_PROJECT_DENY: FilterRecipeField = {
    name: 'project_title_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific Hex Projects by title',
    tooltip:
        'Exclude Hex Projects whose title matches one of the provided regular expressions. Deny patterns take precedence over Allow patterns.',
    type: FieldType.LIST,
    rule: FilterRule.EXCLUDE,
    buttonLabel: 'Add pattern',
    fieldPath: projectDenyFieldPath,
    rules: null,
    section: 'Projects',
    filteringResource: 'Project',
    placeholder: 'Scratch.*',
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, projectDenyFieldPath),
};
