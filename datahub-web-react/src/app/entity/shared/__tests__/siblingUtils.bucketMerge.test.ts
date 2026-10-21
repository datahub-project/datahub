import { combineEntityDataWithSiblings } from '@app/entity/shared/siblingUtils';

// Wraps the primary entity and its sibling in the shape entity queries return, so the merge runs
// through the public combineEntityDataWithSiblings API.
function combineWithSibling(primaryEntity: object, siblingEntity: object) {
    const combined = combineEntityDataWithSiblings({
        dataset: {
            urn: 'urn:li:dataset:primary',
            siblings: { isPrimary: true },
            siblingsSearch: {
                count: 1,
                searchResults: [{ entity: { urn: 'urn:li:dataset:sibling', ...siblingEntity } }],
            },
            ...primaryEntity,
        },
    });
    return combined.dataset as any;
}

describe('Usage Stats Bucket Merging', () => {
    it('should merge usage stats buckets from different time windows', () => {
        const primaryEntity = {
            usageStats: {
                __typename: 'UsageQueryResult',
                aggregations: {
                    __typename: 'UsageQueryResultAggregations',
                    uniqueUserCount: 10,
                    totalSqlQueries: 500,
                    fields: [],
                    users: [],
                },
                buckets: [
                    {
                        __typename: 'UsageAggregation',
                        bucket: 1640995200000,
                        metrics: { totalSqlQueries: 100 },
                    },
                    {
                        __typename: 'UsageAggregation',
                        bucket: 1640995300000,
                        metrics: { totalSqlQueries: 150 },
                    },
                ],
            },
        };

        const siblingEntity = {
            usageStats: {
                __typename: 'UsageQueryResult',
                aggregations: {
                    __typename: 'UsageQueryResultAggregations',
                    uniqueUserCount: 15,
                    totalSqlQueries: 800,
                    fields: [],
                    users: [],
                },
                buckets: [
                    {
                        __typename: 'UsageAggregation',
                        bucket: 1640995400000, // Different time window
                        metrics: { totalSqlQueries: 75 },
                    },
                ],
            },
        };

        const result = combineWithSibling(primaryEntity, siblingEntity);

        expect(result.usageStats.buckets).toEqual([
            {
                __typename: 'UsageAggregation',
                bucket: 1640995200000,
                metrics: { totalSqlQueries: 100 },
            },
            {
                __typename: 'UsageAggregation',
                bucket: 1640995300000,
                metrics: { totalSqlQueries: 150 },
            },
            {
                __typename: 'UsageAggregation',
                bucket: 1640995400000,
                metrics: { totalSqlQueries: 75 },
            },
        ]);
    });

    it('should merge metrics for duplicate time windows', () => {
        const primaryEntity = {
            usageStats: {
                __typename: 'UsageQueryResult',
                aggregations: {
                    __typename: 'UsageQueryResultAggregations',
                    uniqueUserCount: 10,
                    totalSqlQueries: 500,
                    fields: [],
                    users: [],
                },
                buckets: [
                    {
                        __typename: 'UsageAggregation',
                        bucket: 1640995200000,
                        metrics: { totalSqlQueries: 100 },
                    },
                ],
            },
        };

        const siblingEntity = {
            usageStats: {
                __typename: 'UsageQueryResult',
                aggregations: {
                    __typename: 'UsageQueryResultAggregations',
                    uniqueUserCount: 15,
                    totalSqlQueries: 800,
                    fields: [],
                    users: [],
                },
                buckets: [
                    {
                        __typename: 'UsageAggregation',
                        bucket: 1640995200000, // Same time window
                        metrics: { totalSqlQueries: 200 },
                    },
                ],
            },
        };

        const result = combineWithSibling(primaryEntity, siblingEntity);

        expect(result.usageStats.buckets).toEqual([
            {
                __typename: 'UsageAggregation',
                bucket: 1640995200000,
                metrics: { totalSqlQueries: 300 }, // 100 + 200 merged
            },
        ]);
    });

    it('should sort buckets by time window', () => {
        const primaryEntity = {
            usageStats: {
                __typename: 'UsageQueryResult',
                aggregations: {
                    __typename: 'UsageQueryResultAggregations',
                    uniqueUserCount: 10,
                    totalSqlQueries: 500,
                    fields: [],
                    users: [],
                },
                buckets: [
                    {
                        __typename: 'UsageAggregation',
                        bucket: 1640995300000, // Later time
                        metrics: { totalSqlQueries: 150 },
                    },
                ],
            },
        };

        const siblingEntity = {
            usageStats: {
                __typename: 'UsageQueryResult',
                aggregations: {
                    __typename: 'UsageQueryResultAggregations',
                    uniqueUserCount: 15,
                    totalSqlQueries: 800,
                    fields: [],
                    users: [],
                },
                buckets: [
                    {
                        __typename: 'UsageAggregation',
                        bucket: 1640995200000, // Earlier time
                        metrics: { totalSqlQueries: 100 },
                    },
                ],
            },
        };

        const result = combineWithSibling(primaryEntity, siblingEntity);

        expect(result.usageStats.buckets).toEqual([
            {
                __typename: 'UsageAggregation',
                bucket: 1640995200000, // Should be first after sorting
                metrics: { totalSqlQueries: 100 },
            },
            {
                __typename: 'UsageAggregation',
                bucket: 1640995300000,
                metrics: { totalSqlQueries: 150 },
            },
        ]);
    });
});
