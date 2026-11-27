import { canEditDisplayedStructuredProperties } from '@app/entityV2/shared/tabs/Properties/structuredPropertyPrivileges';

import { EntityType } from '@types';

describe('canEditDisplayedStructuredProperties', () => {
    const dataset = {
        type: EntityType.Dataset,
        privileges: { canEditProperties: true, canEditSchemaFieldProperties: false },
    };
    const columnEditor = {
        type: EntityType.Dataset,
        privileges: { canEditProperties: false, canEditSchemaFieldProperties: true },
    };
    const schemaField = {
        type: EntityType.SchemaField,
        parent: { privileges: { canEditProperties: false, canEditSchemaFieldProperties: true } },
    };

    it('uses entity properties on a dataset and column properties for a field', () => {
        expect(canEditDisplayedStructuredProperties(dataset, false)).toBe(true);
        expect(canEditDisplayedStructuredProperties(dataset, true)).toBe(false);
        expect(canEditDisplayedStructuredProperties(columnEditor, false)).toBe(false);
        expect(canEditDisplayedStructuredProperties(columnEditor, true)).toBe(true);
    });

    it('reads column properties from the dataset parent on a schema field', () => {
        expect(canEditDisplayedStructuredProperties(schemaField, false)).toBe(true);
        expect(canEditDisplayedStructuredProperties(schemaField, true)).toBe(true);
    });
});
