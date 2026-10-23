import {
    CATEGORY_APPLICATION,
    CATEGORY_ASSET_MEMBERSHIP,
    CATEGORY_DOMAIN,
    CATEGORY_STRUCTURED_PROPERTY,
    CATEGORY_VERSIONING,
    CategoryOption,
    filterChangeEntries,
    getCategoryOptions,
    resolveActorName,
} from '@app/entityV2/shared/tabs/Dataset/Schema/history/HistorySidebar.utils';
import { ChangeCategoryType, EntityType } from '@src/types.generated';

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
];

describe('getCategoryOptions', () => {
    it('returns all 9 categories for datasets', () => {
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
            CATEGORY_VERSIONING,
        ]);
    });

    it('returns Documentation, Terms, Owners, Domains, Properties, Applications, Versions for glossary terms', () => {
        const options = getCategoryOptions(EntityType.GlossaryTerm);
        const values = options.map((o) => o.value);

        expect(values).toEqual([
            ChangeCategoryType.Documentation,
            ChangeCategoryType.GlossaryTerm,
            ChangeCategoryType.Ownership,
            CATEGORY_DOMAIN,
            CATEGORY_STRUCTURED_PROPERTY,
            CATEGORY_APPLICATION,
            CATEGORY_VERSIONING,
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

describe('resolveActorName', () => {
    const SERVICE_ACCOUNT_URN = 'urn:li:corpuser:service_00000000-0000-4000-8000-000000000001';

    it('returns null when actorUrn is missing', () => {
        expect(resolveActorName(undefined, new Map())).toBeNull();
        expect(resolveActorName(null, new Map())).toBeNull();
    });

    it('prefers the resolved display name from nameMap over the raw username', () => {
        const nameMap = new Map([[SERVICE_ACCOUNT_URN, 'example_ingestion_service']]);
        expect(resolveActorName(SERVICE_ACCOUNT_URN, nameMap)).toBe('example_ingestion_service');
    });

    it('falls back to the raw URN trailing segment when nameMap has no entry for the actor', () => {
        expect(resolveActorName(SERVICE_ACCOUNT_URN, new Map())).toBe('service_00000000-0000-4000-8000-000000000001');
    });

    it('falls back to the raw URN trailing segment when nameMap is undefined', () => {
        expect(resolveActorName(SERVICE_ACCOUNT_URN, undefined)).toBe('service_00000000-0000-4000-8000-000000000001');
    });

    it('produces the same string as before for a human user whose username is their email', () => {
        // Regression guard: this is the case that "looked correct" pre-fix purely by
        // convention (username == email), not because it was actually resolved.
        const humanUrn = 'urn:li:corpuser:jane.doe@example.com';
        expect(resolveActorName(humanUrn, new Map())).toBe('jane.doe@example.com');
        expect(resolveActorName(humanUrn, new Map([[humanUrn, 'jane.doe@example.com']]))).toBe('jane.doe@example.com');
    });

    it('resolves the system actor to its display name when present in nameMap', () => {
        const systemUrn = 'urn:li:corpuser:__datahub_system';
        expect(resolveActorName(systemUrn, new Map([[systemUrn, 'Datahub System']]))).toBe('Datahub System');
    });
});
