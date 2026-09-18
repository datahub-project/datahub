import {
    canBeAssetBadge,
    getBadgeUrnToReplace,
    getFilteredSortedStructuredProperties,
    getNewAllowedPlatforms,
    haveAllowedValuesChanged,
    matchesAllowedPlatforms,
    replaceAssetBadge,
    toAllowedValueInputs,
} from '@app/govern/structuredProperties/utils';
import { EntityType, StructuredPropertyEntity } from '@src/types.generated';

function makePlatform(urn: string) {
    return { urn, type: EntityType.DataPlatform };
}

function makeProperty(allowedPlatformUrns?: string[]): StructuredPropertyEntity {
    return {
        urn: 'urn:li:structuredProperty:test',
        type: EntityType.StructuredProperty,
        definition: {
            allowedPlatforms: allowedPlatformUrns?.map(makePlatform),
        },
    } as unknown as StructuredPropertyEntity;
}

describe('matchesAllowedPlatforms', () => {
    it('returns true when property has no allowedPlatforms restriction', () => {
        expect(matchesAllowedPlatforms(makeProperty(), 'urn:li:dataPlatform:bigquery')).toBe(true);
    });

    it('returns true when allowedPlatforms is empty', () => {
        expect(matchesAllowedPlatforms(makeProperty([]), 'urn:li:dataPlatform:bigquery')).toBe(true);
    });

    it('returns true when platformUrn matches an allowed platform', () => {
        const property = makeProperty(['urn:li:dataPlatform:bigquery', 'urn:li:dataPlatform:snowflake']);
        expect(matchesAllowedPlatforms(property, 'urn:li:dataPlatform:bigquery')).toBe(true);
    });

    it('returns false when platformUrn does not match any allowed platform', () => {
        const property = makeProperty(['urn:li:dataPlatform:snowflake']);
        expect(matchesAllowedPlatforms(property, 'urn:li:dataPlatform:bigquery')).toBe(false);
    });

    it('returns false when property has allowedPlatforms but platformUrn is null', () => {
        const property = makeProperty(['urn:li:dataPlatform:snowflake']);
        expect(matchesAllowedPlatforms(property, null)).toBe(false);
    });

    it('returns false when property has allowedPlatforms but platformUrn is undefined', () => {
        const property = makeProperty(['urn:li:dataPlatform:snowflake']);
        expect(matchesAllowedPlatforms(property, undefined)).toBe(false);
    });
});

describe('getNewAllowedPlatforms', () => {
    it('returns only platforms not already on the property', () => {
        const entity = makeProperty(['urn:li:dataPlatform:bigquery']);
        const result = getNewAllowedPlatforms(entity, {
            allowedPlatforms: ['urn:li:dataPlatform:bigquery', 'urn:li:dataPlatform:snowflake'],
        });
        expect(result).toEqual(['urn:li:dataPlatform:snowflake']);
    });

    it('returns undefined when all submitted platforms are already present', () => {
        const entity = makeProperty(['urn:li:dataPlatform:bigquery']);
        const result = getNewAllowedPlatforms(entity, {
            allowedPlatforms: ['urn:li:dataPlatform:bigquery'],
        });
        expect(result).toBeUndefined();
    });

    it('returns undefined when allowedPlatforms is not set in values', () => {
        const entity = makeProperty(['urn:li:dataPlatform:bigquery']);
        const result = getNewAllowedPlatforms(entity, {});
        expect(result).toBeUndefined();
    });

    it('returns all platforms when the property has none yet', () => {
        const entity = makeProperty([]);
        const result = getNewAllowedPlatforms(entity, {
            allowedPlatforms: ['urn:li:dataPlatform:bigquery', 'urn:li:dataPlatform:snowflake'],
        });
        expect(result).toEqual(['urn:li:dataPlatform:bigquery', 'urn:li:dataPlatform:snowflake']);
    });
});

describe('toAllowedValueInputs', () => {
    it('drops blank rows and preserves valid string and zero values', () => {
        expect(
            toAllowedValueInputs([
                { rowId: 'empty' },
                { rowId: 'string', stringValue: 'Gold' },
                { rowId: 'whitespace', stringValue: '  ' },
                { rowId: 'zero', numberValue: 0 },
            ]),
        ).toEqual([
            { stringValue: 'Gold', description: undefined },
            { numberValue: 0, description: undefined },
        ]);
    });

    it('rejects non-finite numbers', () => {
        expect(toAllowedValueInputs([{ rowId: 'infinite', numberValue: 'Infinity' }])).toEqual([]);
    });
});

describe('haveAllowedValuesChanged', () => {
    const gold = { rowId: 'gold', stringValue: 'Gold', isPersisted: true };
    const silver = { rowId: 'silver', stringValue: 'Silver', isPersisted: true };

    it('ignores client-only row metadata', () => {
        expect(haveAllowedValuesChanged([gold], [{ ...gold, rowId: 'different-id' }])).toBe(false);
    });

    it('detects an order change', () => {
        expect(haveAllowedValuesChanged([gold, silver], [silver, gold])).toBe(true);
    });
});

describe('getBadgeUrnToReplace', () => {
    it('returns a different active badge when the saved property enables badges', () => {
        expect(getBadgeUrnToReplace('urn:old', 'urn:new', true)).toBe('urn:old');
    });

    it('does not replace the property currently being edited', () => {
        expect(getBadgeUrnToReplace('urn:same', 'urn:same', true)).toBeUndefined();
    });
});

describe('replaceAssetBadge', () => {
    it('disables the previous badge', async () => {
        const updateBadge = vi.fn().mockResolvedValue(undefined);

        await replaceAssetBadge({
            existingBadgeUrn: 'urn:old',
            savedPropertyUrn: 'urn:new',
            enableBadge: true,
            updateBadge,
        });

        expect(updateBadge).toHaveBeenCalledWith('urn:old', false);
    });

    it('rolls back the newly saved badge when disabling the previous badge fails', async () => {
        const updateBadge = vi.fn().mockRejectedValueOnce(new Error('failed')).mockResolvedValueOnce(undefined);

        await expect(
            replaceAssetBadge({
                existingBadgeUrn: 'urn:old',
                savedPropertyUrn: 'urn:new',
                enableBadge: true,
                updateBadge,
            }),
        ).rejects.toThrow('failed');

        expect(updateBadge).toHaveBeenNthCalledWith(2, 'urn:new', false);
    });
});

describe('canBeAssetBadge', () => {
    it('does not treat an empty allowed-value input as a bounded value set', () => {
        expect(canBeAssetBadge('string', [{}])).toBe(false);
    });

    it('accepts zero as a bounded numeric value', () => {
        expect(canBeAssetBadge('number', [{ numberValue: 0 }])).toBe(true);
    });
});

function makeSp(opts: { displayName?: string; qualifiedName?: string; time?: number }): StructuredPropertyEntity {
    return {
        urn: `urn:li:structuredProperty:${opts.qualifiedName ?? opts.displayName ?? 'x'}`,
        type: EntityType.StructuredProperty,
        definition: {
            displayName: opts.displayName,
            qualifiedName: opts.qualifiedName ?? '',
            created: opts.time !== undefined ? { time: opts.time } : undefined,
        },
    } as unknown as StructuredPropertyEntity;
}

describe('getFilteredSortedStructuredProperties', () => {
    it('matches on displayName, case-insensitively', () => {
        const props = [makeSp({ displayName: 'Data Quality Score' })];
        expect(getFilteredSortedStructuredProperties(props, 'quality')).toHaveLength(1);
    });

    it('falls back to qualifiedName when displayName is missing', () => {
        const props = [makeSp({ qualifiedName: 'io.acryl.privacy.enumProperty14' })];
        expect(getFilteredSortedStructuredProperties(props, 'enumProperty14')).toHaveLength(1);
    });

    it('excludes properties whose displayed name does not contain the query', () => {
        const props = [makeSp({ displayName: 'Certification Status' })];
        expect(getFilteredSortedStructuredProperties(props, 'quality')).toHaveLength(0);
    });

    it('sorts matches by creation time, newest first', () => {
        const older = makeSp({ displayName: 'test one', time: 1 });
        const newer = makeSp({ displayName: 'test two', time: 2 });
        const result = getFilteredSortedStructuredProperties([older, newer], 'test');
        expect(result.map((p) => p.definition.created?.time)).toEqual([2, 1]);
    });
});
