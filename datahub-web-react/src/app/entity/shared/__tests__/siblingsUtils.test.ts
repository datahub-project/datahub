import { dataset3WithLineage, dataset4WithLineage } from '../../../../Mocks';
import {
    omitEmpty,
    getPrimarySiblingFromEntity,
    getUpstreamsAndDownstreamsFromEntityAndSiblings,
    combineEntityDataWithSiblings,
} from '../siblingUtils';

const usageStats = {
    buckets: [
        {
            bucket: 1650412800000,
            duration: 'DAY',
            resource: 'urn:li:dataset:4',
            metrics: {
                uniqueUserCount: 1,
                totalSqlQueries: 37,
                topSqlQueries: ['some sql query'],
                __typename: 'UsageAggregationMetrics',
            },
            __typename: 'UsageAggregation',
        },
    ],
    aggregations: {
        uniqueUserCount: 2,
        totalSqlQueries: 39,
        users: [
            {
                user: {
                    urn: 'urn:li:corpuser:user',
                    username: 'user',
                    __typename: 'CorpUser',
                },
                count: 2,
                userEmail: 'user@datahubproject.io',
                __typename: 'UserUsageCounts',
            },
        ],
        fields: [
            {
                fieldName: 'field',
                count: 7,
                __typename: 'FieldUsageCounts',
            },
        ],
        __typename: 'UsageQueryResultAggregations',
    },
    __typename: 'UsageQueryResult',
};

const datasetPrimary = { ...dataset3WithLineage };
const datasetUnprimary = { ...dataset4WithLineage, usageStats };

datasetPrimary.siblings = {
    isPrimary: true,
    siblings: [datasetUnprimary],
};

datasetUnprimary.siblings = {
    isPrimary: false,
    siblings: [datasetPrimary],
};

describe('siblingUtisl', () => {
    describe('omitEmpty', () => {
        it('will not overwrite nulls from a sibling onto an entity', () => {
            expect(omitEmpty({ a: null, b: 3, c: [], d: { buckets: [] } }, false)).toMatchObject({
                b: 3,
            });
        });

        it('will not include sibling container', () => {
            expect(
                omitEmpty(
                    { a: null, b: 3, c: [], d: { buckets: [] }, container: { urn: 'urn:li:container:123' } },
                    true,
                ),
            ).toMatchObject({
                b: 3,
            });
        });
    });

    describe('getPrimarySiblingsFromEntity', () => {
        it('will get the primary sibling if it is that', () => {
            expect(getPrimarySiblingFromEntity(datasetPrimary)).toEqual(datasetPrimary);
        });

        it('will get the primary sibling if it is not that', () => {
            expect(getPrimarySiblingFromEntity(datasetUnprimary)).toEqual(datasetPrimary);
        });
    });

    describe('getUpstreamsAndDownstreamsFromEntityAndSiblings', () => {
        it('will merge its siblings lineage into itself if primary', () => {
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            const { allUpstreams, allDownstreams } = getUpstreamsAndDownstreamsFromEntityAndSiblings(datasetPrimary);

            // dataset4, the sibling, should be missing
            expect(allUpstreams.map((entity) => entity?.urn)).toEqual([
                // from sibling
                'urn:li:dataset:6',
                'urn:li:dataset:5',
                // from self
                'urn:li:dataset:7',
            ]);

            expect(allDownstreams.map((entity) => entity?.urn)).toEqual([]);
        });

        it('will not merge its siblings lineage into itself if not primary', () => {
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            const { allUpstreams, allDownstreams } = getUpstreamsAndDownstreamsFromEntityAndSiblings(datasetUnprimary);

            expect(allUpstreams.map((entity) => entity?.urn)).toEqual([
                // from self
                'urn:li:dataset:6',
                'urn:li:dataset:5',
            ]);

            expect(allDownstreams.map((entity) => entity?.urn)).toEqual(['urn:li:dataset:3']);
        });
    });

    describe('combineEntityDataWithSiblings', () => {
        it('combines my metadata with my siblings', () => {
            const baseEntity = { dataset: datasetPrimary };
            expect(baseEntity.dataset.usageStats).toBeNull();
            expect(combineEntityDataWithSiblings(baseEntity).dataset.usageStats).toEqual(usageStats);
        });
    });
});
