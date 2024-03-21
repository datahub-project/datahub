import { validateURL } from '../../utils';
import { RecipeField, FieldType } from './common';

export const CSV_FILE_URL: RecipeField = {
    name: 'filename',
    label: 'File URL',
    tooltip: 'File URL of the CSV file to ingest.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.filename',
    placeholder: 'File URL',
    required: true,
    rules: [() => validateURL('File URL')],
};

export const CSV_ARRAY_DELIMITER: RecipeField = {
    name: 'array_delimiter',
    label: 'Array delimiter',
    tooltip: 'Delimiter to use when parsing array fields (tags, terms and owners)',
    type: FieldType.TEXT,
    fieldPath: 'source.config.array_delimiter',
    placeholder: 'Array delimiter',
    rules: null,
};

export const CSV_DELIMITER: RecipeField = {
    name: 'delimiter',
    label: 'Delimiter',
    tooltip: 'Delimiter to use when parsing CSV',
    type: FieldType.TEXT,
    fieldPath: 'source.config.delimiter',
    placeholder: 'Delimiter',
    rules: null,
};

export const CSV_WRITE_SEMANTICS: RecipeField = {
    name: 'write_semantics',
    label: 'Write Semantics',
    tooltip:
        'Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be "PATCH" or "OVERRIDE"',
    type: FieldType.SELECT,
    options: [
        { label: 'PATCH', value: 'PATCH' },
        { label: 'OVERRIDE', value: 'OVERRIDE' },
    ],
    fieldPath: 'source.config.write_semantics',
    placeholder: 'Write Semantics',
    rules: null,
};
