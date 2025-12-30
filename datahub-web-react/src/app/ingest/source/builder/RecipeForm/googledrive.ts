import { FieldType, RecipeField } from '@src/app/ingest/source/builder/RecipeForm/common';

export const GOOGLE_DRIVE_CREDENTIALS_PATH: RecipeField = {
    name: 'credentials_path',
    label: 'Credentials Path',
    tooltip: 'Path to the Google service account credentials JSON file',
    type: FieldType.TEXT,
    fieldPath: 'source.config.credentials_path',
    placeholder: '/path/to/credentials.json',
    required: true,
    rules: null,
};

export const ROOT_FOLDER_ID: RecipeField = {
    name: 'root_folder_id',
    label: 'Root Folder ID',
    tooltip: 'Google Drive folder ID to start ingestion from. If not provided, will start from root.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.root_folder_id',
    placeholder: '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms',
    required: false,
    rules: null,
};

export const FILE_SIZE_LIMIT: RecipeField = {
    name: 'file_size_limit',
    label: 'File Size Limit (MB)',
    tooltip: 'Maximum file size in MB to process for metadata extraction',
    type: FieldType.TEXT,
    fieldPath: 'source.config.file_size_limit',
    placeholder: '100',
    required: false,
    rules: null,
};

export const INCLUDE_SHARED_DRIVES: RecipeField = {
    name: 'include_shared_drives',
    label: 'Include Shared Drives',
    tooltip: 'Whether to include shared drives in the ingestion',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_shared_drives',
    rules: null,
};

export const INCLUDE_TRASHED: RecipeField = {
    name: 'include_trashed',
    label: 'Include Trashed Files',
    tooltip: 'Whether to include trashed files in the ingestion',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.include_trashed',
    rules: null,
};

export const FILE_PATTERN_ALLOW: RecipeField = {
    name: 'file_pattern_allow',
    label: 'File Pattern Allow',
    tooltip: 'Regex pattern for files to include based on file name',
    type: FieldType.LIST,
    fieldPath: 'source.config.file_pattern_allow',
    placeholder: '.*\\.xlsx?$',
    required: false,
    rules: null,
};

export const FILE_PATTERN_DENY: RecipeField = {
    name: 'file_pattern_deny',
    label: 'File Pattern Deny',
    tooltip: 'Regex pattern for files to exclude based on file name',
    type: FieldType.LIST,
    fieldPath: 'source.config.file_pattern_deny',
    placeholder: '.*\\.tmp$',
    required: false,
    rules: null,
};

export const FOLDER_PATTERN_ALLOW: RecipeField = {
    name: 'folder_pattern_allow',
    label: 'Folder Pattern Allow',
    tooltip: 'Regex pattern for folders to include based on folder name',
    type: FieldType.LIST,
    fieldPath: 'source.config.folder_pattern_allow',
    placeholder: 'data.*',
    required: false,
    rules: null,
};

export const FOLDER_PATTERN_DENY: RecipeField = {
    name: 'folder_pattern_deny',
    label: 'Folder Pattern Deny',
    tooltip: 'Regex pattern for folders to exclude based on folder name',
    type: FieldType.LIST,
    fieldPath: 'source.config.folder_pattern_deny',
    placeholder: '.*temp.*',
    required: false,
    rules: null,
};

export const GOOGLE_DRIVE_FIELDS = [GOOGLE_DRIVE_CREDENTIALS_PATH, ROOT_FOLDER_ID];
