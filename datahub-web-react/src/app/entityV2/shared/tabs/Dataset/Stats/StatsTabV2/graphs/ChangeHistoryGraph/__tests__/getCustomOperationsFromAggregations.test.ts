import { getCustomOperationsFromAggregations } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/utils';
import { OperationsAggregationsResult } from '@src/types.generated';

const EXAMPLE_AGGREGATION_WITH_CUSTOM_TYPES: OperationsAggregationsResult = {
    customOperationsMap: [
        {
            key: 'custom_type_1',
            value: 0,
        },
        {
            key: 'custom_type_2',
            value: 0,
        },
        {
            key: 'custom_type_3',
            value: 0,
        },
    ],
};

const EXAMPLE_AGGREGATION_WITHOUT_CUSTOM_TYPES: OperationsAggregationsResult = {
    customOperationsMap: null,
};

describe('getCustomOperationsFromAggregations', () => {
    it('should return types when aggregation has custom types', () => {
        const response = getCustomOperationsFromAggregations(EXAMPLE_AGGREGATION_WITH_CUSTOM_TYPES);

        expect(response).toStrictEqual(['custom_type_1', 'custom_type_2', 'custom_type_3']);
    });
    it('should return types when aggregation has custom types and defaults provided', () => {
        const defaultCustomOperationTypes = ['custom_type_4'];

        const response = getCustomOperationsFromAggregations(
            EXAMPLE_AGGREGATION_WITH_CUSTOM_TYPES,
            defaultCustomOperationTypes,
        );

        expect(response).toStrictEqual(['custom_type_1', 'custom_type_2', 'custom_type_3', 'custom_type_4']);
    });

    it('should not return types when aggregation has no custom types', () => {
        const response = getCustomOperationsFromAggregations(EXAMPLE_AGGREGATION_WITHOUT_CUSTOM_TYPES);

        expect(response).toStrictEqual([]);
    });
    it('should return types when aggregation has no custom types and defaupts provided', () => {
        const response = getCustomOperationsFromAggregations(EXAMPLE_AGGREGATION_WITHOUT_CUSTOM_TYPES, [
            'custom_type_1',
        ]);

        expect(response).toStrictEqual(['custom_type_1']);
    });

    it('should not return types when aggregation is undefined and defaults not provided', () => {
        const response = getCustomOperationsFromAggregations(undefined);

        expect(response).toStrictEqual([]);
    });

    it('should return types when aggregation is undefined and defaults provided', () => {
        const response = getCustomOperationsFromAggregations(undefined, ['custom_type_1']);

        expect(response).toStrictEqual(['custom_type_1']);
    });
});
