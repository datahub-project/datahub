import {
    CATEGORY_APPLICATION,
    CATEGORY_ASSET_MEMBERSHIP,
    CATEGORY_DEPRECATION,
    CATEGORY_DOMAIN,
    CATEGORY_LIFECYCLE,
    CATEGORY_PARENT,
    CATEGORY_STRUCTURED_PROPERTY,
    CATEGORY_VERSIONING,
    CategoryOption,
    filterChangeEntries,
    findDescriptionChange,
    getCategoryOptions,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar.utils';
import timelineSupportedCategories from '@app/entityV2/shared/tabs/Dataset/Schema/history/timelineSupportedCategories.json';
import { ChangeCategoryType, ChangeOperationType, EntityType } from '@src/types.generated';

const ALL_CATEGORY_OPTIONS: CategoryOption[] = [
    { value: ChangeCategoryType.TechnicalSchema, label: 'Schema' },
    { value: ChangeCategoryType.Documentation, label: 'Documentation' },
    { value: ChangeCategoryType.Tag, label: 'Tags' },
    { value: ChangeCategoryType.GlossaryTerm, label: 'Terms' },
    { value: ChangeCategoryType.Ownership, label: 'Owners' },
    { value: CATEGORY_DOMAIN, label: 'Domains' },
    { value: CATEGORY_STRUCTURED_PROPERTY, label: 'Properties' },
    { value: CATEGORY_APPLICATION, label: 'Applications' },
    { value: CATEGORY_ASSET_MEMBERSHIP, label: 'Assets' },
    { value: CATEGORY_VERSIONING, label: 'Versions' },
    { value: CATEGORY_DEPRECATION, label: 'Deprecation' },
    { value: CATEGORY_PARENT, label: 'Hierarchy' },
    { value: CATEGORY_LIFECYCLE, label: 'Lifecycle stage' },
];

describe('getCategoryOptions', () => {
    it('offers Deprecation and Hierarchy for glossary terms', () => {
        const values = getCategoryOptions(EntityType.GlossaryTerm).map((o) => o.value);
        expect(values).toContain(CATEGORY_DEPRECATION);
        expect(values).toContain(CATEGORY_PARENT);
        expect(values).not.toContain(ChangeCategoryType.TechnicalSchema);
    });

    it('offers Lifecycle for glossary terms', () => {
        expect(getCategoryOptions(EntityType.GlossaryTerm).map((o) => o.value)).toContain(CATEGORY_LIFECYCLE);
    });

    it('omits Deprecation for glossary nodes', () => {
        const values = getCategoryOptions(EntityType.GlossaryNode).map((o) => o.value);
        expect(values).toContain(CATEGORY_PARENT);
        expect(values).not.toContain(CATEGORY_DEPRECATION);
    });

    it('offers Schema only where the backend registers it', () => {
        expect(getCategoryOptions(EntityType.Dataset).map((o) => o.value)).toContain(
            ChangeCategoryType.TechnicalSchema,
        );
        expect(getCategoryOptions(EntityType.Domain).map((o) => o.value)).not.toContain(
            ChangeCategoryType.TechnicalSchema,
        );
    });

    it('falls back to all options for an unknown entity type', () => {
        const options = getCategoryOptions(EntityType.CorpUser);
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

describe('timelineSupportedCategories.json', () => {
    // Categories the backend serves that this sidebar deliberately has no filter for.
    const NO_SIDEBAR_OPTION: Record<string, string> = {
        RELATED_ENTITIES: 'document-only; documents show their history in DocumentChangeHistoryDrawer',
    };

    it('has a sidebar filter option for every category it lists', () => {
        const optionValues = new Set(getCategoryOptions(undefined).map((o) => o.value));
        const missing = Object.entries(timelineSupportedCategories as Record<string, string[]>).flatMap(
            ([entityType, categories]) =>
                categories
                    .filter((category) => !optionValues.has(category) && !(category in NO_SIDEBAR_OPTION))
                    .map((category) => `${entityType}.${category}`),
        );
        expect(missing).toEqual([]);
    });
});

describe('findDescriptionChange', () => {
    it('picks the documentation change that carries a description, not the first one', () => {
        const nameChange = {
            category: ChangeCategoryType.Documentation,
            operation: ChangeOperationType.Modify,
            parameters: [{ key: 'field', value: 'name' }],
        };
        const definitionChange = {
            category: ChangeCategoryType.Documentation,
            operation: ChangeOperationType.Modify,
            parameters: [{ key: 'description', value: 'New definition' }],
        };
        expect(findDescriptionChange([nameChange, definitionChange])).toBe(definitionChange);
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
