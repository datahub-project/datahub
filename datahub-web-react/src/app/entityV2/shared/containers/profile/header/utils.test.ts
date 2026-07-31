import { filterForAssetBadge } from '@app/entityV2/shared/containers/profile/header/utils';

import { StructuredPropertiesEntry } from '@types';

function makeEntry(
    showAsAssetBadge: boolean,
    isHidden: boolean,
    allowedPlatformUrns?: string[],
): StructuredPropertiesEntry {
    return {
        structuredProperty: {
            urn: 'urn:li:structuredProperty:test',
            type: 'STRUCTURED_PROPERTY' as any,
            settings: { showAsAssetBadge, isHidden },
            definition: {
                allowedPlatforms: allowedPlatformUrns?.map((urn) => ({ urn, type: 'DATA_PLATFORM' as any })),
            },
        },
    } as any;
}

describe('filterForAssetBadge', () => {
    it('returns false when showAsAssetBadge is false', () => {
        expect(filterForAssetBadge(makeEntry(false, false), 'urn:li:dataPlatform:bigquery')).toBe(false);
    });

    it('returns false when isHidden is true', () => {
        expect(filterForAssetBadge(makeEntry(true, true), 'urn:li:dataPlatform:bigquery')).toBe(false);
    });

    it('returns true when badge is visible and there are no platform restrictions', () => {
        expect(filterForAssetBadge(makeEntry(true, false), 'urn:li:dataPlatform:bigquery')).toBe(true);
    });

    it('returns true when badge is visible and allowedPlatforms is empty', () => {
        expect(filterForAssetBadge(makeEntry(true, false, []), 'urn:li:dataPlatform:bigquery')).toBe(true);
    });

    it('returns true when platformUrn matches an allowed platform', () => {
        const entry = makeEntry(true, false, ['urn:li:dataPlatform:bigquery', 'urn:li:dataPlatform:snowflake']);
        expect(filterForAssetBadge(entry, 'urn:li:dataPlatform:bigquery')).toBe(true);
    });

    it('returns false when platformUrn does not match any allowed platform', () => {
        const entry = makeEntry(true, false, ['urn:li:dataPlatform:snowflake']);
        expect(filterForAssetBadge(entry, 'urn:li:dataPlatform:bigquery')).toBe(false);
    });

    it('returns false when property has platform restrictions but platformUrn is not provided', () => {
        const entry = makeEntry(true, false, ['urn:li:dataPlatform:bigquery']);
        expect(filterForAssetBadge(entry, null)).toBe(false);
        expect(filterForAssetBadge(entry, undefined)).toBe(false);
    });
});
