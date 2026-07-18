import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

export const ODCS_PATH: RecipeField = {
    name: 'path',
    label: 'Path',
    tooltip:
        'Path to an ODCS YAML file, a directory containing ODCS YAML files, or a glob pattern. To pass multiple paths, switch to the YAML editor.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.path',
    placeholder: '/path/to/contracts',
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
