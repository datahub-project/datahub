import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

export const RDF_SOURCE: RecipeField = {
    name: 'source',
    label: 'Source',
    helper: 'RDF source: file, folder, URL, or comma-separated files',
    tooltip:
        'Source to process: file path, folder path, server URL, or comma-separated files. Examples: /path/to/file.ttl, /path/to/folder, https://example.com/data.ttl, file1.ttl,file2.ttl',
    type: FieldType.TEXT,
    fieldPath: 'source.config.source',
    placeholder: '/path/to/file.ttl or /path/to/folder or https://example.com/data.ttl',
    required: true,
    rules: null,
};

export const RDF_FORMAT: RecipeField = {
    name: 'format',
    label: 'RDF Format',
    helper: 'RDF format (auto-detected if not specified)',
    tooltip: 'RDF format (auto-detected if not specified). Examples: turtle, xml, n3, nt, json-ld',
    type: FieldType.SELECT,
    fieldPath: 'source.config.format',
    placeholder: 'Auto-detect',
    options: [
        { label: 'Auto-detect', value: '' },
        { label: 'Turtle', value: 'turtle' },
        { label: 'RDF/XML', value: 'xml' },
        { label: 'N3', value: 'n3' },
        { label: 'N-Triples', value: 'nt' },
        { label: 'JSON-LD', value: 'json-ld' },
    ],
    rules: null,
};

export const RDF_EXTENSIONS: RecipeField = {
    name: 'extensions',
    label: 'File Extensions',
    helper: 'File extensions to process when source is a folder',
    tooltip: 'File extensions to process when source is a folder. Default: .ttl, .rdf, .owl, .n3, .nt',
    type: FieldType.LIST,
    fieldPath: 'source.config.extensions',
    placeholder: '.ttl',
    buttonLabel: 'Add extension',
    rules: null,
};

export const RDF_RECURSIVE: RecipeField = {
    name: 'recursive',
    label: 'Recursive Folder Processing',
    helper: 'Enable recursive folder processing when source is a folder',
    tooltip: 'Enable recursive folder processing when source is a folder (default: true)',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.recursive',
    rules: null,
};

export const RDF_ENVIRONMENT: RecipeField = {
    name: 'environment',
    label: 'DataHub Environment',
    helper: 'DataHub environment (PROD, DEV, TEST, etc.)',
    tooltip: 'DataHub environment (PROD, DEV, TEST, etc.)',
    type: FieldType.SELECT,
    fieldPath: 'source.config.environment',
    placeholder: 'PROD',
    options: [
        { label: 'PROD', value: 'PROD' },
        { label: 'DEV', value: 'DEV' },
        { label: 'TEST', value: 'TEST' },
        { label: 'UAT', value: 'UAT' },
    ],
    rules: null,
};

export const RDF_DIALECT: RecipeField = {
    name: 'dialect',
    label: 'RDF Dialect',
    helper: 'Force a specific RDF dialect (default: auto-detect)',
    tooltip: 'Force a specific RDF dialect (default: auto-detect). Options: default, fibo, generic',
    type: FieldType.SELECT,
    fieldPath: 'source.config.dialect',
    placeholder: 'Auto-detect',
    options: [
        { label: 'Auto-detect', value: '' },
        { label: 'Default', value: 'default' },
        { label: 'FIBO', value: 'fibo' },
        { label: 'Generic', value: 'generic' },
    ],
    rules: null,
};
