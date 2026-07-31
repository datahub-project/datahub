import {
    getFilteredSortedStructuredProperties,
    getNewAllowedPlatforms,
    matchesAllowedPlatforms,
} from '@app/govern/structuredProperties/utils';
import { StructuredPropertyEntity } from '@src/types.generated';

function makePlatform(urn: string) {
    return { urn, type: 'DATA_PLATFORM' as any };
}

function makeProperty(allowedPlatformUrns?: string[]): StructuredPropertyEntity {
    return {
        urn: 'urn:li:structuredProperty:test',
        type: 'STRUCTURED_PROPERTY' as any,
        definition: {
            allowedPlatforms: allowedPlatformUrns?.map(makePlatform),
        },
    } as any;
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

function makeSp(opts: { displayName?: string; qualifiedName?: string; time?: number }): StructuredPropertyEntity {
    return {
        urn: `urn:li:structuredProperty:${opts.qualifiedName ?? opts.displayName ?? 'x'}`,
        type: 'STRUCTURED_PROPERTY' as any,
        definition: {
            displayName: opts.displayName,
            qualifiedName: opts.qualifiedName ?? '',
            created: opts.time !== undefined ? { time: opts.time } : undefined,
        },
    } as any;
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
