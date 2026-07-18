import { FieldType, RecipeField, setFieldValueOnRecipe } from '@app/ingestV2/source/builder/RecipeForm/common';

export const ODCS_PATH: RecipeField = {
    name: 'path',
    label: 'Path',
    helper: 'Local path, object-store URI, or HTTP URL',
    tooltip:
        'Location of ODCS YAML: a local file, directory, or glob pattern; an s3:// or gs:// object-store URI (single file or glob); or an http(s):// URL to a single file. To pass multiple paths, switch to the YAML editor. When a Git repository is configured below, non-URI paths are resolved relative to the checkout.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.path',
    placeholder: '/path/to/contracts or s3://bucket/contracts/*.odcs.yaml',
    rules: null,
    required: true,
};

export const ODCS_TAG_PREFIX: RecipeField = {
    name: 'tag_prefix',
    label: 'Tag Prefix',
    tooltip: 'Optional prefix prepended to every tag emitted from ODCS tags (e.g. "odcs.").',
    type: FieldType.TEXT,
    fieldPath: 'source.config.tag_prefix',
    placeholder: 'odcs.',
    rules: null,
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

export const ODCS_SCHEMA_ASSERTION_COMPATIBILITY: RecipeField = {
    name: 'schema_assertion_compatibility',
    label: 'Schema Assertion Compatibility',
    tooltip:
        'Compatibility mode for the schema assertion: SUPERSET (an instance must contain at least the contract fields; extras allowed), EXACT_MATCH, or SUBSET.',
    type: FieldType.SELECT,
    options: [
        { label: 'SUPERSET', value: 'SUPERSET' },
        { label: 'EXACT_MATCH', value: 'EXACT_MATCH' },
        { label: 'SUBSET', value: 'SUBSET' },
    ],
    fieldPath: 'source.config.schema_assertion_compatibility',
    placeholder: 'SUPERSET',
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

export const ODCS_VERIFY_PHYSICAL_URNS_EXIST: RecipeField = {
    name: 'verify_physical_urns_exist',
    label: 'Verify Physical URNs Exist',
    tooltip:
        'When a DataHub graph is available, verify each derived physical dataset URN exists before emitting its logicalParent link; URNs not found are left unbound with a warning instead of creating stub datasets.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.verify_physical_urns_exist',
    rules: null,
};

export const ODCS_REPLICATE_CONTRACT_METADATA: RecipeField = {
    name: 'replicate_contract_metadata',
    label: 'Replicate Contract Metadata',
    tooltip:
        'Write contract-level ownership and tags to the logical dataset on every run. Disable to let manual UI edits to those aspects survive subsequent ingest runs.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.replicate_contract_metadata',
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
        'Optional Git repository to shallow-clone and scan for ODCS files. Accepts a GitHub shorthand (org/repo) or a full Git URL. When set, non-URI Path entries are resolved relative to the checkout.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.git_info.repo',
    placeholder: 'my-org/data-contracts',
    rules: null,
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
    setValueOnRecipeOverride: (recipe: any, value: string) => {
        const valueWithNewLine = value ? `${value}\n` : value;
        return setFieldValueOnRecipe(recipe, valueWithNewLine, odcsDeployKeyFieldPath);
    },
};

// Remote sourcing: object-store credentials for s3:// and gs:// URIs in Path.
export const ODCS_AWS_REGION: RecipeField = {
    name: 'aws_connection.aws_region',
    label: 'AWS Region',
    helper: 'Required for s3:// paths',
    tooltip:
        'AWS region for reading ODCS files from s3:// URIs in Path. Required when any Path entry is an S3 URI. For access keys, roles, or profiles, use the YAML editor to complete the aws_connection block.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.aws_connection.aws_region',
    placeholder: 'us-east-1',
    rules: null,
};

export const ODCS_GCS_HMAC_KEY_ID: RecipeField = {
    name: 'gcs_connection.hmac_key_id',
    label: 'GCS HMAC Key ID',
    helper: 'Required for gs:// paths',
    tooltip:
        'GCS HMAC key ID (S3-compatible access) for reading ODCS files from gs:// URIs in Path. Required together with the HMAC secret when any Path entry is a GCS URI.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.gcs_connection.hmac_key_id',
    placeholder: 'GOOG1E...',
    rules: null,
};

export const ODCS_GCS_HMAC_KEY_SECRET: RecipeField = {
    name: 'gcs_connection.hmac_key_secret',
    label: 'GCS HMAC Key Secret',
    helper: 'Required for gs:// paths',
    tooltip: 'GCS HMAC key secret paired with the HMAC key ID for reading gs:// URIs in Path.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.gcs_connection.hmac_key_secret',
    placeholder: 'hmac-secret',
    rules: null,
};
