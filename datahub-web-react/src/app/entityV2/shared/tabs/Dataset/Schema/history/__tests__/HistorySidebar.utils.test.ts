import {
    ALL_CATEGORY_OPTIONS,
    CATEGORY_APPLICATION,
    CATEGORY_ASSET_MEMBERSHIP,
    CATEGORY_DOMAIN,
    CATEGORY_STRUCTURED_PROPERTY,
    filterChangeEntries,
    getCategoryOptions,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar.utils';
import { ChangeCategoryType, EntityType } from '@src/types.generated';

describe('getCategoryOptions', () => {
    it('returns all 8 categories for datasets', () => {
        const options = getCategoryOptions(EntityType.Dataset);
        const values = options.map((o) => o.value);

        expect(values).toEqual([
            ChangeCategoryType.TechnicalSchema,
            ChangeCategoryType.Documentation,
            ChangeCategoryType.Tag,
            ChangeCategoryType.GlossaryTerm,
            ChangeCategoryType.Ownership,
            CATEGORY_DOMAIN,
            CATEGORY_STRUCTURED_PROPERTY,
            CATEGORY_APPLICATION,
        ]);
    });

    it('returns Documentation, Terms, Owners, Domains, Properties, Applications for glossary terms', () => {
        const options = getCategoryOptions(EntityType.GlossaryTerm);
        const values = options.map((o) => o.value);

        expect(values).toEqual([
            ChangeCategoryType.Documentation,
            ChangeCategoryType.GlossaryTerm,
            ChangeCategoryType.Ownership,
            CATEGORY_DOMAIN,
            CATEGORY_STRUCTURED_PROPERTY,
            CATEGORY_APPLICATION,
        ]);
        expect(values).not.toContain(ChangeCategoryType.TechnicalSchema);
        expect(values).not.toContain(ChangeCategoryType.Tag);
    });

    it('returns only Documentation, Owners, Properties for domains', () => {
        const options = getCategoryOptions(EntityType.Domain);
        const values = options.map((o) => o.value);

        expect(values).toEqual([
            ChangeCategoryType.Documentation,
            ChangeCategoryType.Ownership,
            CATEGORY_STRUCTURED_PROPERTY,
        ]);
        expect(values).not.toContain(CATEGORY_DOMAIN);
        expect(values).not.toContain(ChangeCategoryType.TechnicalSchema);
        expect(values).not.toContain(CATEGORY_APPLICATION);
    });

    it('returns all supported categories including Assets for data products', () => {
        const options = getCategoryOptions(EntityType.DataProduct);
        const values = options.map((o) => o.value);

        expect(values).toEqual([
            ChangeCategoryType.Documentation,
            ChangeCategoryType.Tag,
            ChangeCategoryType.GlossaryTerm,
            ChangeCategoryType.Ownership,
            CATEGORY_DOMAIN,
            CATEGORY_STRUCTURED_PROPERTY,
            CATEGORY_APPLICATION,
            CATEGORY_ASSET_MEMBERSHIP,
        ]);
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

describe('filterChangeEntries', () => {
    const ALL_CATEGORIES = ['TAG', 'DOCUMENTATION', 'OWNERSHIP'];

    const entries = [
        {
            transaction: {
                changes: [
                    { category: 'TAG', description: 'Tag added: PII' },
                    { category: 'TAG', description: 'Tag removed: Deprecated' },
                ],
            },
        },
        {
            transaction: {
                changes: [{ category: 'DOCUMENTATION', description: 'Description changed from old to new' }],
            },
        },
        {
            transaction: {
                changes: [{ category: 'OWNERSHIP', description: 'Owner added: alice@datahub.com' }],
            },
        },
    ];

    it('returns all entries when no filters are active', () => {
        const result = filterChangeEntries(entries, ALL_CATEGORIES, ALL_CATEGORIES, '');
        expect(result).toHaveLength(3);
    });

    it('filters by category', () => {
        const result = filterChangeEntries(entries, ['TAG'], ALL_CATEGORIES, '');
        expect(result).toHaveLength(1);
        expect(result[0].transaction.changes?.[0]?.category).toBe('TAG');
    });

    it('filters by search text (case-insensitive)', () => {
        const result = filterChangeEntries(entries, ALL_CATEGORIES, ALL_CATEGORIES, 'pii');
        expect(result).toHaveLength(1);
        expect(result[0].transaction.changes?.[0]?.description).toContain('PII');
    });

    it('applies both category and search filters together', () => {
        const result = filterChangeEntries(entries, ['TAG', 'DOCUMENTATION'], ALL_CATEGORIES, 'deprecated');
        expect(result).toHaveLength(1);
        expect(result[0].transaction.changes?.[1]?.description).toContain('Deprecated');
    });

    it('returns empty array when search matches no descriptions', () => {
        const result = filterChangeEntries(entries, ALL_CATEGORIES, ALL_CATEGORIES, 'nonexistent');
        expect(result).toHaveLength(0);
    });

    it('trims whitespace from search text', () => {
        const result = filterChangeEntries(entries, ALL_CATEGORIES, ALL_CATEGORIES, '  alice  ');
        expect(result).toHaveLength(1);
        expect(result[0].transaction.changes?.[0]?.description).toContain('alice');
    });

    it('searches against displayTexts when provided', () => {
        // Raw descriptions contain URNs, but displayTexts have resolved names
        const displayTexts = [
            'Added tag "PII" Tag removed: Deprecated',
            'Description changed from old to new',
            'Owner added: "Alice Smith"',
        ];
        const result = filterChangeEntries(entries, ALL_CATEGORIES, ALL_CATEGORIES, 'Alice Smith', displayTexts);
        expect(result).toHaveLength(1);
        expect(result[0].transaction.changes?.[0]?.description).toContain('alice@datahub.com');
    });

    it('falls back to raw descriptions when displayTexts is undefined', () => {
        const result = filterChangeEntries(entries, ALL_CATEGORIES, ALL_CATEGORIES, 'alice@datahub.com', undefined);
        expect(result).toHaveLength(1);
    });

    it('does not match raw description when displayTexts is provided', () => {
        // "alice@datahub.com" is in the raw description but not in the display text
        const displayTexts = ['tag stuff', 'doc stuff', 'Owner added: "Alice Smith"'];
        const result = filterChangeEntries(entries, ALL_CATEGORIES, ALL_CATEGORIES, 'alice@datahub.com', displayTexts);
        expect(result).toHaveLength(0);
    });
});
