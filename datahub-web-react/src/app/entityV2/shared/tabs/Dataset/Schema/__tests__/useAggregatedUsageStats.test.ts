import { renderHook } from '@testing-library/react-hooks';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { useAggregatedUsageStats } from '@app/entityV2/shared/tabs/Dataset/Schema/useAggregatedUsageStats';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';

vi.mock('@app/entity/shared/EntityContext');
vi.mock('@app/entityV2/shared/useIsSeparateSiblingsMode');

describe('useAggregatedUsageStats', () => {
    const mockUsageStats = {
        __typename: 'UsageQueryResult',
        aggregations: {
            __typename: 'UsageQueryResultAggregations',
            uniqueUserCount: 25,
            totalSqlQueries: 1300,
            fields: [
                {
                    __typename: 'FieldUsageCounts',
                    fieldName: 'id',
                    count: 300,
                },
                {
                    __typename: 'FieldUsageCounts',
                    fieldName: 'name',
                    count: 50,
                },
            ],
            users: [
                {
                    __typename: 'UserUsageCounts',
                    user: { urn: 'urn:li:corpuser:user1' },
                    count: 300,
                },
            ],
        },
        buckets: [
            {
                __typename: 'UsageAggregation',
                bucket: 1640995200000,
                metrics: { totalSqlQueries: 300 }, // This would be merged from 100 + 200 duplicate buckets
            },
            {
                __typename: 'UsageAggregation',
                bucket: 1640995300000,
                metrics: { totalSqlQueries: 150 },
            },
        ],
    };

    beforeEach(() => {
        vi.resetAllMocks();
        (useIsSeparateSiblingsMode as any).mockReturnValue(false);
        (useBaseEntity as any).mockReturnValue({
            dataset: {
                urn: 'urn:li:dataset:test',
                usageStats: mockUsageStats,
            },
        });
    });

    it('should return usage stats from entity data', () => {
        const { result } = renderHook(() => useAggregatedUsageStats());

        expect(result.current).toEqual(mockUsageStats);
    });

    it('should return null when no usage data is available', () => {
        (useBaseEntity as any).mockReturnValue({ dataset: {} });

        const { result } = renderHook(() => useAggregatedUsageStats());

        expect(result.current).toBeNull();
    });

    it('should return null when in separate siblings mode', () => {
        (useIsSeparateSiblingsMode as any).mockReturnValue(true);

        const { result } = renderHook(() => useAggregatedUsageStats());

        expect(result.current).toBeNull();
    });

    it('should return null when entity data is not available', () => {
        (useBaseEntity as any).mockReturnValue(null);

        const { result } = renderHook(() => useAggregatedUsageStats());

        expect(result.current).toBeNull();
    });
});
