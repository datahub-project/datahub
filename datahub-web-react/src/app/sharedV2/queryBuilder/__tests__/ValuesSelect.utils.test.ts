import i18next from 'i18next';
import { describe, expect, it } from 'vitest';

import { getValuesSelectLabel } from '@app/sharedV2/queryBuilder/ValuesSelect.utils';

describe('getValuesSelectLabel', () => {
    const t = i18next.getFixedT(null, 'shared.query-builder');

    it('returns Documents for parentDocument', () => {
        expect(getValuesSelectLabel('parentDocument', t)).toBe('Documents');
    });

    it('returns known overrides for common properties', () => {
        expect(getValuesSelectLabel('urn', t)).toBe('Assets');
        expect(getValuesSelectLabel('glossaryTerms', t)).toBe('Terms');
        expect(getValuesSelectLabel('_entityType', t)).toBe('Types');
        expect(getValuesSelectLabel('typeNames', t)).toBe('Sub Types');
        expect(getValuesSelectLabel('fieldPaths', t)).toBe('Columns');
        expect(getValuesSelectLabel('platformInstance', t)).toBe('Instances');
        expect(getValuesSelectLabel('owners', t)).toBe('Owners');
    });

    it('falls back to capitalizing the property id', () => {
        expect(getValuesSelectLabel('domains', t)).toBe('Domains');
        expect(getValuesSelectLabel('container', t)).toBe('Container');
        expect(getValuesSelectLabel('tags', t)).toBe('Tags');
    });

    it('returns undefined when property is missing', () => {
        expect(getValuesSelectLabel(undefined, t)).toBeUndefined();
    });

    it('prefers the display name over the raw field id for structured properties', () => {
        expect(getValuesSelectLabel('structuredProperties.8f473633-abc', t, 'ListSP')).toBe('ListSP');
    });

    it('keeps built-in overrides even when a display name is supplied', () => {
        expect(getValuesSelectLabel('urn', t, 'Something Else')).toBe('Assets');
    });
});
