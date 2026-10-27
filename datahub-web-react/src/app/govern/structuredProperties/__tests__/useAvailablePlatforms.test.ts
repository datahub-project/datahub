import { renderHook } from '@testing-library/react-hooks';
import { Mock, vi } from 'vitest';

import useAvailablePlatforms from '@app/govern/structuredProperties/useAvailablePlatforms';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';

vi.mock('@app/useEntityRegistry');
vi.mock('@src/graphql/search.generated');

function makePlatformResult(urn: string, displayName: string, logoUrl?: string) {
    return {
        entity: {
            urn,
            type: EntityType.DataPlatform,
            properties: { displayName, logoUrl: logoUrl ?? null },
        },
    };
}

describe('useAvailablePlatforms', () => {
    beforeEach(() => {
        (useEntityRegistry as Mock).mockReturnValue({
            getDisplayName: vi.fn((_, platform) => platform.properties?.displayName ?? platform.urn),
        });
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('returns an empty array when there are no results', () => {
        (useGetSearchResultsForMultipleQuery as Mock).mockReturnValue({ data: null });

        const { result } = renderHook(() => useAvailablePlatforms());
        expect(result.current).toEqual([]);
    });

    it('maps search results to PlatformOptions with label, value, and platform', () => {
        (useGetSearchResultsForMultipleQuery as Mock).mockReturnValue({
            data: {
                searchAcrossEntities: {
                    searchResults: [makePlatformResult('urn:li:dataPlatform:bigquery', 'BigQuery')],
                },
            },
        });

        const { result } = renderHook(() => useAvailablePlatforms());
        expect(result.current).toHaveLength(1);
        expect(result.current[0]).toMatchObject({
            label: 'BigQuery',
            value: 'urn:li:dataPlatform:bigquery',
        });
        expect(result.current[0].platform.urn).toBe('urn:li:dataPlatform:bigquery');
    });

    it('sorts platforms alphabetically by label', () => {
        (useGetSearchResultsForMultipleQuery as Mock).mockReturnValue({
            data: {
                searchAcrossEntities: {
                    searchResults: [
                        makePlatformResult('urn:li:dataPlatform:snowflake', 'Snowflake'),
                        makePlatformResult('urn:li:dataPlatform:bigquery', 'BigQuery'),
                        makePlatformResult('urn:li:dataPlatform:redshift', 'Redshift'),
                    ],
                },
            },
        });

        const { result } = renderHook(() => useAvailablePlatforms());
        expect(result.current.map((o) => o.label)).toEqual(['BigQuery', 'Redshift', 'Snowflake']);
    });
});
