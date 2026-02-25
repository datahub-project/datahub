import { getCategoryOptions, ALL_CATEGORY_OPTIONS } from '../HistorySidebar.utils';
import { ChangeCategoryType, EntityType } from '@src/types.generated';

describe('getCategoryOptions', () => {
    it('returns all 7 categories for datasets', () => {
        const options = getCategoryOptions(EntityType.Dataset);
        expect(options).toEqual(ALL_CATEGORY_OPTIONS);
    });

    it('returns only Documentation, Owners, Domains, Properties for glossary terms', () => {
        const options = getCategoryOptions(EntityType.GlossaryTerm);
        const values = options.map((o) => o.value);

        expect(values).toEqual([
            ChangeCategoryType.Documentation,
            ChangeCategoryType.Ownership,
            ChangeCategoryType.Domain,
            ChangeCategoryType.StructuredProperty,
        ]);
        expect(values).not.toContain(ChangeCategoryType.TechnicalSchema);
        expect(values).not.toContain(ChangeCategoryType.Tag);
        expect(values).not.toContain(ChangeCategoryType.GlossaryTerm);
    });

    it('returns only Documentation, Owners, Properties for domains', () => {
        const options = getCategoryOptions(EntityType.Domain);
        const values = options.map((o) => o.value);

        expect(values).toEqual([
            ChangeCategoryType.Documentation,
            ChangeCategoryType.Ownership,
            ChangeCategoryType.StructuredProperty,
        ]);
        expect(values).not.toContain(ChangeCategoryType.Domain);
        expect(values).not.toContain(ChangeCategoryType.TechnicalSchema);
    });

    it('falls back to all categories for entity types without a mapping', () => {
        const options = getCategoryOptions(EntityType.Chart);
        expect(options).toEqual(ALL_CATEGORY_OPTIONS);
    });

    it('falls back to all categories when entityType is undefined', () => {
        const options = getCategoryOptions(undefined);
        expect(options).toEqual(ALL_CATEGORY_OPTIONS);
    });

    it('preserves display order from ALL_CATEGORY_OPTIONS', () => {
        // Ensure filtered results keep the same relative order as the master list,
        // so the dropdown doesn't reorder items unexpectedly.
        const allValues = ALL_CATEGORY_OPTIONS.map((o) => o.value);
        const glossaryValues = getCategoryOptions(EntityType.GlossaryTerm).map((o) => o.value);

        const expectedOrder = allValues.filter((v) => glossaryValues.includes(v));
        expect(glossaryValues).toEqual(expectedOrder);
    });
});
