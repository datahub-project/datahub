import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

export const GITHUB_DOCUMENTS_TOKEN: RecipeField = {
    name: 'github_token',
    label: 'GitHub token',
    tooltip: 'Personal access token or GitHub App installation token with repository read access.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.github_token',
    placeholder: 'ghp_xxxxxxxxxxxx',
    rules: null,
    required: true,
};

export const GITHUB_DOCUMENTS_REPOSITORY: RecipeField = {
    name: 'repository',
    label: 'Repository',
    tooltip: "Repository to ingest, as 'owner/repo' or a full GitHub URL.",
    type: FieldType.TEXT,
    fieldPath: 'source.config.repository',
    placeholder: 'org/repo',
    rules: null,
    required: true,
};

export const GITHUB_DOCUMENTS_BRANCH: RecipeField = {
    name: 'branch',
    label: 'Branch',
    type: FieldType.TEXT,
    fieldPath: 'source.config.branch',
    placeholder: 'main',
    rules: null,
    required: true,
};

export const GITHUB_DOCUMENTS_PATH_PREFIX: RecipeField = {
    name: 'path_prefix',
    label: 'Path prefix',
    tooltip: 'Only ingest files under this path (for example, docs).',
    type: FieldType.TEXT,
    fieldPath: 'source.config.path_prefix',
    placeholder: 'docs',
    rules: null,
};

export const GITHUB_DOCUMENTS_FILE_EXTENSIONS: RecipeField = {
    name: 'file_extensions',
    label: 'File extensions',
    tooltip: 'Include the leading dot (for example, .md).',
    type: FieldType.LIST,
    fieldPath: 'source.config.file_extensions',
    placeholder: '.md',
    buttonLabel: 'Add extension',
    rules: null,
};

export const GITHUB_DOCUMENTS_PARENT_DOCUMENT_URN: RecipeField = {
    name: 'parent_document_urn',
    label: 'Parent document URN',
    tooltip:
        'Optional parent under which imported items are nested. When unset, a repository root folder document is created automatically.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.parent_document_urn',
    rules: null,
};

export const GITHUB_DOCUMENTS_SHOW_IN_GLOBAL_CONTEXT: RecipeField = {
    name: 'show_in_global_context',
    label: 'Show in global context',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.show_in_global_context',
    rules: null,
};
