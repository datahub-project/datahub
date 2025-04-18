import { RecipeField, FieldType } from '@app/ingest/source/builder/RecipeForm/common';

const CONNECTION_NAME: RecipeField = {
    name: 'name',
    label: 'Connection Name',
    tooltip: 'An internal name to idenity this connection in DataHub.',
    placeholder: 'My Connection',
    type: FieldType.TEXT,
    fieldPath: 'name',
    rules: null,
    required: true,
};

export const commonFields = [CONNECTION_NAME];
