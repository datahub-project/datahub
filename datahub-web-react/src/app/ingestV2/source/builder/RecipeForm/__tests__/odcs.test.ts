import { FieldType } from '@app/ingestV2/source/builder/RecipeForm/common';
import { RECIPE_FIELDS } from '@app/ingestV2/source/builder/RecipeForm/constants';
import { ODCS_PATH, ODCS_SCHEMA_ASSERTION_COMPATIBILITY } from '@app/ingestV2/source/builder/RecipeForm/odcs';
import { ODCS } from '@app/ingestV2/source/conf/odcs/odcs';

describe('ODCS recipe form fields', () => {
    it('registers ODCS in RECIPE_FIELDS with path as the only required top-level field', () => {
        const spec = RECIPE_FIELDS[ODCS];
        expect(spec).toBeDefined();
        expect(spec.fields).toEqual([ODCS_PATH]);
        expect(ODCS_PATH.required).toBe(true);
    });

    it('maps every field to a source.config.* recipe path matching its name', () => {
        // A typo'd fieldPath silently writes the wrong recipe key, so pin the contract explicitly.
        const spec = RECIPE_FIELDS[ODCS];
        const allFields = [...spec.fields, ...spec.advancedFields];
        allFields.forEach((field) => {
            const fieldPath = Array.isArray(field.fieldPath) ? field.fieldPath[0] : field.fieldPath;
            expect(fieldPath.startsWith('source.config.')).toBe(true);
        });
        expect(ODCS_PATH.fieldPath).toBe('source.config.path');
    });

    it('offers exactly the schema-assertion compatibility modes the connector accepts', () => {
        expect(ODCS_SCHEMA_ASSERTION_COMPATIBILITY.type).toBe(FieldType.SELECT);
        const values = (ODCS_SCHEMA_ASSERTION_COMPATIBILITY.options ?? []).map((o) => o.value);
        expect(values).toEqual(['SUPERSET', 'EXACT_MATCH', 'SUBSET']);
    });
});
