import { describe, expect, it } from 'vitest';

import { SCHEMA_FORMS_BY_CONNECTOR } from '@app/ingestV2/source/builder/useSchemaForm';

describe('SCHEMA_FORMS_BY_CONNECTOR', () => {
    it('indexes the redshift form with a connection section first', () => {
        expect(SCHEMA_FORMS_BY_CONNECTOR.redshift.sections[0].key).toBe('connection');
    });

    it('includes feature sections such as lineage', () => {
        const sectionKeys = SCHEMA_FORMS_BY_CONNECTOR.redshift.sections.map((section) => section.key);
        expect(sectionKeys).toContain('lineage');
    });

    it('returns undefined for an unknown connector', () => {
        expect(SCHEMA_FORMS_BY_CONNECTOR.nope_xyz).toBeUndefined();
    });

    it('gives every section an array of fields', () => {
        Object.values(SCHEMA_FORMS_BY_CONNECTOR).forEach((form) => {
            form.sections.forEach((section) => {
                expect(Array.isArray(section.fields)).toBe(true);
            });
        });
    });
});
