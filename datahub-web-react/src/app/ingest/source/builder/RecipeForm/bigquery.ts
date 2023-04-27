import { RecipeField, FieldType } from './common';

export const BIGQUERY_PROJECT_ID: RecipeField = {
    name: 'project_id',
    label: 'Project ID',
    tooltip: 'Project ID where you have rights to run queries and create tables.',
    placeholder: 'my-project-123',
    type: FieldType.TEXT,
    fieldPath: 'source.config.project_id',
    rules: null,
    required: true,
};

export const BIGQUERY_CREDENTIAL_PROJECT_ID: RecipeField = {
    name: 'credential.project_id',
    label: 'Credentials Project ID',
    tooltip: "The Project ID, which can be found in your service account's JSON Key (project_id)",
    placeholder: 'my-project-123',
    type: FieldType.TEXT,
    fieldPath: 'source.config.credential.project_id',
    rules: null,
};

export const BIGQUERY_PRIVATE_KEY_ID: RecipeField = {
    name: 'credential.private_key_id',
    label: 'Private Key Id',
    tooltip: "The Private Key id, which can be found in your service account's JSON Key (private_key_id)",
    type: FieldType.SECRET,
    fieldPath: 'source.config.credential.private_key_id',
    placeholder: 'd0121d0000882411234e11166c6aaa23ed5d74e0',
    rules: null,
    required: true,
};

export const BIGQUERY_PRIVATE_KEY: RecipeField = {
    name: 'credential.private_key',
    label: 'Private Key',
    tooltip: "The Private key, which can be found in your service account's JSON Key (private_key).",
    placeholder: '-----BEGIN PRIVATE KEY-----....\n-----END PRIVATE KEY-----',
    type: FieldType.SECRET,
    fieldPath: 'source.config.credential.private_key',
    rules: null,
    required: true,
};

export const BIGQUERY_CLIENT_EMAIL: RecipeField = {
    name: 'credential.client_email',
    label: 'Client Email',
    tooltip: "The Client Email, which can be found in your service account's JSON Key (client_email).",
    placeholder: 'client_email@gmail.com',
    type: FieldType.TEXT,
    fieldPath: 'source.config.credential.client_email',
    rules: null,
    required: true,
};

export const BIGQUERY_CLIENT_ID: RecipeField = {
    name: 'credential.client_id',
    label: 'Client ID',
    tooltip: "The Client ID, which can be found in your service account's JSON Key (client_id).",
    placeholder: '123456789098765432101',
    type: FieldType.TEXT,
    fieldPath: 'source.config.credential.client_id',
    rules: null,
    required: true,
};
