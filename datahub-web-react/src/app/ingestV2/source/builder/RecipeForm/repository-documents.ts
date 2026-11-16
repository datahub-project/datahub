import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

export const REPOSITORY_DOCUMENTS_BRANCH: RecipeField = {
    name: 'branch',
    label: 'Branch',
    tooltip: 'Git branch to read files from (defaults to main).',
    type: FieldType.TEXT,
    fieldPath: 'source.config.branch',
    placeholder: 'main',
    rules: null,
    required: true,
};

export const REPOSITORY_DOCUMENTS_PATH_PREFIX: RecipeField = {
    name: 'path_prefix',
    label: 'Path prefix',
    tooltip: 'Only ingest files under this path (for example, docs).',
    type: FieldType.TEXT,
    fieldPath: 'source.config.path_prefix',
    placeholder: 'docs',
    rules: null,
};

export const REPOSITORY_DOCUMENTS_FILE_EXTENSIONS: RecipeField = {
    name: 'file_extensions',
    label: 'File extensions',
    tooltip: 'Include the leading dot (for example, .md).',
    type: FieldType.LIST,
    fieldPath: 'source.config.file_extensions',
    placeholder: '.md',
    buttonLabel: 'Add extension',
    rules: null,
};
