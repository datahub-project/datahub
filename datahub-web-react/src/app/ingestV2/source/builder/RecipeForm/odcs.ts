import { get, omit } from 'lodash';

import { FieldType, RecipeField, setFieldValueOnRecipe } from '@app/ingestV2/source/builder/RecipeForm/common';

export const ODCS_LOCATION_LOCAL = 'local';
export const ODCS_LOCATION_S3 = 's3';
export const ODCS_LOCATION_GCS = 'gcs';
export const ODCS_LOCATION_HTTP = 'http';
export const ODCS_LOCATION_GIT = 'git';

const sourceLocationFieldPath = 'source.config.source_location';
const awsConnectionFieldPath = 'source.config.aws_connection';
const gcsConnectionFieldPath = 'source.config.gcs_connection';
const gitInfoFieldPath = 'source.config.git_info';
const httpConnectionFieldPath = 'source.config.http_connection';
const pathFieldPath = 'source.config.path';

// `source_location` is form-only (the connector rejects unknown keys): it only
// controls which credential fields the form shows. We never write it, and we
// drop the credentials of every location other than the selected one.
function setOdcsSourceLocationOnRecipe(recipe: any, value: string | undefined): any {
    let updatedRecipe = omit({ ...recipe }, [sourceLocationFieldPath]);
    if (value !== ODCS_LOCATION_S3) updatedRecipe = omit(updatedRecipe, [awsConnectionFieldPath]);
    if (value !== ODCS_LOCATION_GCS) updatedRecipe = omit(updatedRecipe, [gcsConnectionFieldPath]);
    if (value !== ODCS_LOCATION_GIT) updatedRecipe = omit(updatedRecipe, [gitInfoFieldPath]);
    if (value !== ODCS_LOCATION_HTTP) updatedRecipe = omit(updatedRecipe, [httpConnectionFieldPath]);
    return updatedRecipe;
}

// Infers the source location for a YAML-authored recipe so the form opens on the
// right fields: git if a repo is set, otherwise the scheme of the first path.
function getOdcsSourceLocationFromRecipe(recipe: any): string | undefined {
    if (get(recipe, gitInfoFieldPath)) return ODCS_LOCATION_GIT;
    const rawPath = get(recipe, pathFieldPath);
    const paths: string[] = Array.isArray(rawPath) ? rawPath : [rawPath];
    const first = paths.find((p) => typeof p === 'string' && p.trim() !== '');
    if (typeof first !== 'string') return undefined;
    const p = first.trim().toLowerCase();
    if (p.startsWith('s3://')) return ODCS_LOCATION_S3;
    if (p.startsWith('gs://')) return ODCS_LOCATION_GCS;
    if (p.startsWith('http://') || p.startsWith('https://')) return ODCS_LOCATION_HTTP;
    return ODCS_LOCATION_LOCAL;
}

function createLocationRequiredValidator(requiredLocation: string, fieldLabel: string, locationLabel: string) {
    return ({ getFieldValue }) => ({
        validator(_, value) {
            if (getFieldValue('source_location') === requiredLocation && !value) {
                return Promise.reject(new Error(`${fieldLabel} is required for ${locationLabel}`));
            }
            return Promise.resolve();
        },
    });
}

export const ODCS_PATH: RecipeField = {
    name: 'path',
    label: 'Path',
    helper: 'Local path, object-store URI, or HTTP URL',
    tooltip:
        'Location of ODCS YAML: a local file, directory, or glob pattern; an s3:// or gs:// object-store URI (single file or glob); or an http(s):// URL to a single file. To pass multiple paths, switch to the YAML editor. When a Git repository is configured below, non-URI paths are resolved relative to the checkout.',
    type: FieldType.TEXT,
    fieldPath: pathFieldPath,
    placeholder: '/path/to/contracts or s3://bucket/contracts/*.odcs.yaml',
    rules: null,
    required: true,
};

export const ODCS_SOURCE_LOCATION: RecipeField = {
    name: 'source_location',
    label: 'Source Location',
    helper: 'Where the ODCS contract files live',
    tooltip:
        'Where your ODCS YAML lives. Amazon S3 and Google Cloud Storage reveal their object-store credentials, and Git reveals the repository fields; Local and HTTP(S) need no credentials. This selector only controls which fields are shown — the connector infers the location from the Path scheme and Git settings. To read from more than one location at once, use the YAML editor.',
    type: FieldType.SELECT,
    fieldPath: sourceLocationFieldPath,
    options: [
        { label: 'Local file / directory', value: ODCS_LOCATION_LOCAL },
        { label: 'Amazon S3', value: ODCS_LOCATION_S3 },
        { label: 'Google Cloud Storage', value: ODCS_LOCATION_GCS },
        { label: 'HTTP(S) URL', value: ODCS_LOCATION_HTTP },
        { label: 'Git repository', value: ODCS_LOCATION_GIT },
    ],
    placeholder: 'Select a location',
    rules: null,
    required: false,
    setValueOnRecipeOverride: setOdcsSourceLocationOnRecipe,
    getValueFromRecipeOverride: getOdcsSourceLocationFromRecipe,
};

export const ODCS_STRICT_VALIDATION: RecipeField = {
    name: 'strict_validation',
    label: 'Strict Validation',
    tooltip:
        'Skip ODCS files that fail JSON-Schema validation. When off, schema violations are reported as warnings but the contract is still ingested.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.strict_validation',
    rules: null,
};

export const ODCS_EMIT_ASSERTIONS: RecipeField = {
    name: 'emit_assertions',
    label: 'Emit Quality Assertions',
    tooltip: 'Emit Assertion entities derived from the ODCS quality[] rules, targeting the logical dataset.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.emit_assertions',
    rules: null,
};

export const ODCS_EMIT_SCHEMA_ASSERTION: RecipeField = {
    name: 'emit_schema_assertion',
    label: 'Emit Schema Assertion',
    tooltip:
        "Emit one schema assertion per schema[] entry, pinning the contract's declared schema so schema drift is evaluable as a contract violation.",
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.emit_schema_assertion',
    rules: null,
};

export const ODCS_EMIT_LOGICAL_PARENT: RecipeField = {
    name: 'emit_logical_parent',
    label: 'Emit Logical Parent Link',
    tooltip:
        'Emit a logicalParent link from each resolved physical dataset to its logical ODCS dataset. Disable to keep ODCS from writing any aspect onto physical datasets.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.emit_logical_parent',
    rules: null,
};

// Remote sourcing: pull contracts from a Git repository. Non-URI `path` entries
// are resolved relative to the checkout. Uses deploy_key (SSH key content) rather
// than a key file so it works in managed ingestion, matching the LookML source.
export const ODCS_GIT_INFO_REPO: RecipeField = {
    name: 'git_info.repo',
    label: 'Git Repository',
    helper: 'Git repo to clone and scan for contracts',
    tooltip:
        'Git repository to shallow-clone and scan for ODCS files. Accepts a GitHub shorthand (org/repo) or a full Git URL. Non-URI Path entries are resolved relative to the checkout.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.git_info.repo',
    placeholder: 'my-org/data-contracts',
    rules: [createLocationRequiredValidator(ODCS_LOCATION_GIT, 'Git Repository', 'a Git repository source')],
    dynamicHidden: (values) => values?.source_location !== ODCS_LOCATION_GIT,
    dynamicRequired: (values) => values?.source_location === ODCS_LOCATION_GIT,
};

export const ODCS_GIT_INFO_BRANCH: RecipeField = {
    name: 'git_info.branch',
    label: 'Git Branch',
    helper: 'Branch to check out (defaults to main)',
    tooltip: 'Branch, tag, or commit to check out from the Git repository. Defaults to the repository default branch.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.git_info.branch',
    placeholder: 'main',
    rules: null,
    dynamicHidden: (values) => values?.source_location !== ODCS_LOCATION_GIT,
};

const odcsDeployKeyFieldPath = 'source.config.git_info.deploy_key';
export const ODCS_GIT_INFO_DEPLOY_KEY: RecipeField = {
    name: 'git_info.deploy_key',
    label: 'Git Deploy Key',
    helper: 'SSH private key for repo access',
    tooltip:
        'An SSH private key provisioned for read access to the Git repository. Leave blank for public repositories or HTTP(S) URLs that need no authentication.',
    type: FieldType.SECRET,
    fieldPath: odcsDeployKeyFieldPath,
    placeholder: '-----BEGIN OPENSSH PRIVATE KEY-----\n...',
    rules: null,
    dynamicHidden: (values) => values?.source_location !== ODCS_LOCATION_GIT,
    setValueOnRecipeOverride: (recipe: any, value: string) => {
        const valueWithNewLine = value ? `${value}\n` : value;
        return setFieldValueOnRecipe(recipe, valueWithNewLine, odcsDeployKeyFieldPath);
    },
};

// Remote sourcing: object-store credentials for s3:// URIs in Path. Leave the
// keys blank to fall back to ambient AWS credentials (instance role, profile,
// or environment); for roles or profiles, complete aws_connection in the YAML.
export const ODCS_AWS_ACCESS_KEY_ID: RecipeField = {
    name: 'aws_connection.aws_access_key_id',
    label: 'AWS Access Key ID',
    helper: 'Leave blank to use ambient AWS credentials',
    tooltip:
        'AWS access key ID for reading ODCS files from s3:// URIs in Path. Leave blank to use an instance role, profile, or environment credentials instead.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.aws_connection.aws_access_key_id',
    placeholder: 'AKIA...',
    rules: null,
    dynamicHidden: (values) => values?.source_location !== ODCS_LOCATION_S3,
};

export const ODCS_AWS_SECRET_ACCESS_KEY: RecipeField = {
    name: 'aws_connection.aws_secret_access_key',
    label: 'AWS Secret Access Key',
    helper: 'Required when an Access Key ID is set',
    tooltip: 'AWS secret access key paired with the access key ID for reading s3:// URIs in Path.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.aws_connection.aws_secret_access_key',
    placeholder: 'aws-secret-access-key',
    rules: null,
    dynamicHidden: (values) => values?.source_location !== ODCS_LOCATION_S3,
};

export const ODCS_AWS_REGION: RecipeField = {
    name: 'aws_connection.aws_region',
    label: 'AWS Region',
    helper: 'Required for s3:// paths',
    tooltip: 'AWS region for reading ODCS files from s3:// URIs in Path. Required when any Path entry is an S3 URI.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.aws_connection.aws_region',
    placeholder: 'us-east-1',
    rules: [createLocationRequiredValidator(ODCS_LOCATION_S3, 'AWS Region', 'an Amazon S3 source')],
    dynamicHidden: (values) => values?.source_location !== ODCS_LOCATION_S3,
    dynamicRequired: (values) => values?.source_location === ODCS_LOCATION_S3,
};

export const ODCS_GCS_HMAC_KEY_ID: RecipeField = {
    name: 'gcs_connection.credential.hmac_access_id',
    label: 'GCS HMAC Key ID',
    helper: 'Required for gs:// paths',
    tooltip:
        'GCS HMAC key ID (S3-compatible access) for reading ODCS files from gs:// URIs in Path. Required together with the HMAC secret when any Path entry is a GCS URI.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.gcs_connection.credential.hmac_access_id',
    placeholder: 'GOOG1E...',
    rules: [createLocationRequiredValidator(ODCS_LOCATION_GCS, 'GCS HMAC Key ID', 'a Google Cloud Storage source')],
    dynamicHidden: (values) => values?.source_location !== ODCS_LOCATION_GCS,
    dynamicRequired: (values) => values?.source_location === ODCS_LOCATION_GCS,
};

export const ODCS_GCS_HMAC_KEY_SECRET: RecipeField = {
    name: 'gcs_connection.credential.hmac_access_secret',
    label: 'GCS HMAC Key Secret',
    helper: 'Required for gs:// paths',
    tooltip: 'GCS HMAC key secret paired with the HMAC key ID for reading gs:// URIs in Path.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.gcs_connection.credential.hmac_access_secret',
    placeholder: 'hmac-secret',
    rules: [createLocationRequiredValidator(ODCS_LOCATION_GCS, 'GCS HMAC Key Secret', 'a Google Cloud Storage source')],
    dynamicHidden: (values) => values?.source_location !== ODCS_LOCATION_GCS,
    dynamicRequired: (values) => values?.source_location === ODCS_LOCATION_GCS,
};

// Remote sourcing: authentication for http(s):// URLs in Path. Bearer token and
// HTTP basic auth are mutually exclusive; leave both blank for public URLs.
export const ODCS_HTTP_TOKEN: RecipeField = {
    name: 'http_connection.token',
    label: 'HTTP Bearer Token',
    helper: 'Leave blank for public URLs or basic auth',
    tooltip:
        'Bearer token for authenticating http(s):// requests, sent as an "Authorization: Bearer <token>" header. Mutually exclusive with the username/password below.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.http_connection.token',
    placeholder: 'my-bearer-token',
    rules: null,
    dynamicHidden: (values) => values?.source_location !== ODCS_LOCATION_HTTP,
};

export const ODCS_HTTP_USERNAME: RecipeField = {
    name: 'http_connection.username',
    label: 'HTTP Username',
    helper: 'Basic auth; pair with a password',
    tooltip:
        'Username for HTTP basic authentication of http(s):// requests. Requires a password and is mutually exclusive with the bearer token.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.http_connection.username',
    placeholder: 'username',
    rules: null,
    dynamicHidden: (values) => values?.source_location !== ODCS_LOCATION_HTTP,
};

export const ODCS_HTTP_PASSWORD: RecipeField = {
    name: 'http_connection.password',
    label: 'HTTP Password',
    helper: 'Required when a username is set',
    tooltip: 'Password for HTTP basic authentication, paired with the username above.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.http_connection.password',
    placeholder: 'password',
    rules: null,
    dynamicHidden: (values) => values?.source_location !== ODCS_LOCATION_HTTP,
};

// Checkbox is inverted from the backend field: checked means verify_ssl=false.
// Unchecked drops the key so the connector keeps its safe default (verify on).
const odcsVerifySslFieldPath = 'source.config.http_connection.verify_ssl';
export const ODCS_HTTP_VERIFY_SSL: RecipeField = {
    name: 'http_connection.disable_ssl_verification',
    label: 'Disable TLS Verification',
    helper: 'Skip certificate checks (trusted hosts only)',
    tooltip:
        'Disable verification of the server TLS certificate when fetching ODCS files over https://. Only enable for trusted hosts with self-signed certificates.',
    type: FieldType.BOOLEAN,
    fieldPath: odcsVerifySslFieldPath,
    rules: null,
    dynamicHidden: (values) => values?.source_location !== ODCS_LOCATION_HTTP,
    getValueFromRecipeOverride: (recipe: any) => get(recipe, odcsVerifySslFieldPath) === false,
    setValueOnRecipeOverride: (recipe: any, value: boolean) => {
        if (value) return setFieldValueOnRecipe(recipe, false, odcsVerifySslFieldPath);
        return omit({ ...recipe }, [odcsVerifySslFieldPath]);
    },
};
