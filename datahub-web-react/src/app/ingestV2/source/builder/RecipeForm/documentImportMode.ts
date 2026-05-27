import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

export const DOCUMENT_IMPORT_MODE: RecipeField = {
    name: 'document_import_mode',
    label: 'Document import mode',
    tooltip:
        'Native documents are editable in DataHub. External documents are read-only and link back to the source system.',
    type: FieldType.SELECT,
    fieldPath: 'source.config.document_import_mode',
    rules: null,
    options: [
        { label: 'Native (editable in DataHub)', value: 'NATIVE' },
        { label: 'External (read-only, links to source)', value: 'EXTERNAL' },
    ],
};
