import { dataset3WithLineage, dataset4WithLineage } from '../../../../Mocks';
import { EntityType } from '../../../../types.generated';
import { combineEntityDataWithSiblings } from '../siblingUtils';

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

const datasetPrimary = {
    ...dataset3WithLineage,
    properties: {
        ...dataset3WithLineage.properties,
        description: 'primary description',
    },
    editableProperties: {
        description: '',
    },
    globalTags: {
        tags: [
            {
                tag: {
                    type: EntityType.Tag,
                    urn: 'urn:li:tag:primary-tag',
                    name: 'primary-tag',
                    description: 'primary tag',
                    properties: {
                        name: 'primary-tag',
                        description: 'primary tag',
                        colorHex: 'primary tag color',
                    },
                },
            },
        ],
    },
};
const datasetUnprimary = {
    ...dataset4WithLineage,
    usageStats,
    properties: {
        ...dataset4WithLineage.properties,
        description: 'unprimary description',
    },
    editableProperties: {
        description: 'secondary description',
    },
    globalTags: {
        tags: [
            {
                tag: {
                    type: EntityType.Tag,
                    urn: 'urn:li:tag:unprimary-tag',
                    name: 'unprimary-tag',
                    description: 'unprimary tag',
                    properties: {
                        name: 'unprimary-tag',
                        description: 'unprimary tag',
                        colorHex: 'unprimary tag color',
                    },
                },
            },
        ],
    },
};

const datasetPrimaryWithSiblings = {
    ...datasetPrimary,
    siblings: {
        isPrimary: true,
        siblings: [datasetUnprimary],
    },
};

// const datasetUnprimaryWithSiblings = {
//     ...datasetUnprimary,
//     siblings: {
//         isPrimary: true,
//         siblings: [datasetPrimary],
//     },
// };

describe('siblingUtils', () => {
    describe('combineEntityDataWithSiblings', () => {
        it('combines my metadata with my siblings', () => {
            const baseEntity = { dataset: datasetPrimaryWithSiblings };
            expect(baseEntity.dataset.usageStats).toBeNull();
            const combinedData = combineEntityDataWithSiblings(baseEntity);
            // will merge properties only one entity has
            expect(combinedData.dataset.usageStats).toEqual(usageStats);

            // will merge arrays
            expect(combinedData.dataset.globalTags.tags).toHaveLength(2);
            expect(combinedData.dataset.globalTags.tags[0].tag.urn).toEqual('urn:li:tag:unprimary-tag');
            expect(combinedData.dataset.globalTags.tags[1].tag.urn).toEqual('urn:li:tag:primary-tag');

            // will overwrite string properties w/ primary
            expect(combinedData.dataset.editableProperties.description).toEqual('secondary description');

            // will take secondary string properties in the case of empty string
            expect(combinedData.dataset.properties.description).toEqual('primary description');
        });
    });
});
