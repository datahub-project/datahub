import { get } from 'lodash';

import { FieldType, RecipeField, setFieldValueOnRecipe } from '@app/ingestV2/source/builder/RecipeForm/common';

export type DocumentImportModeValue = 'NATIVE' | 'EXTERNAL';

const DOCUMENT_IMPORT_MODE_FIELD_PATH = 'source.config.document_import_mode';

const DOCUMENT_IMPORT_MODE_TOOLTIP =
    'Native documents are editable in DataHub. External documents are read-only, link back to the source system, and cannot be edited in DataHub.';

const DOCUMENT_IMPORT_MODE_HELPER = 'Native = editable in DataHub. External = read-only with external links.';

const DOCUMENT_IMPORT_MODE_OPTIONS = [
    { label: 'Native (editable in DataHub)', value: 'NATIVE' },
    { label: 'External (read-only, links to source)', value: 'EXTERNAL' },
];

export function createDocumentImportModeField(defaultMode: DocumentImportModeValue): RecipeField {
    return {
        name: 'document_import_mode',
        label: 'Document import mode',
        tooltip: DOCUMENT_IMPORT_MODE_TOOLTIP,
        helper: DOCUMENT_IMPORT_MODE_HELPER,
        type: FieldType.SELECT,
        fieldPath: DOCUMENT_IMPORT_MODE_FIELD_PATH,
        rules: null,
        options: DOCUMENT_IMPORT_MODE_OPTIONS,
        getValueFromRecipeOverride: (recipe) => get(recipe, DOCUMENT_IMPORT_MODE_FIELD_PATH) ?? defaultMode,
        setValueOnRecipeOverride: (recipe, value) =>
            setFieldValueOnRecipe(recipe, value ?? defaultMode, DOCUMENT_IMPORT_MODE_FIELD_PATH),
    };
}

export const GITHUB_DOCUMENTS_IMPORT_MODE = createDocumentImportModeField('NATIVE');
export const NOTION_DOCUMENTS_IMPORT_MODE = createDocumentImportModeField('EXTERNAL');
export const CONFLUENCE_DOCUMENTS_IMPORT_MODE = createDocumentImportModeField('EXTERNAL');

/** @deprecated Use source-specific import mode fields instead. */
export const DOCUMENT_IMPORT_MODE = GITHUB_DOCUMENTS_IMPORT_MODE;
