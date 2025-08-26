import { AggregationGroup } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { convertAggregationsToOperationsData } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/utils';
import { OperationType, OperationsAggregationsResult } from '@src/types.generated';

const EXAMPLE_AGGREGATION_WITH_CUSTOM_TYPES: OperationsAggregationsResult = {
    totalInserts: 1,
    totalUpdates: 2,
    totalDeletes: 3,
    totalAlters: 4,
    totalCreates: 5,
    totalDrops: 6,
    totalCustoms: 3,
    customOperationsMap: [
        { key: 'custom_type_1', value: 1 },
        { key: 'custom_type_2', value: 2 },
    ],
};

const EXAMPLE_AGGREGATION_WITHOUT_CUSTOM_TYPES: OperationsAggregationsResult = {
    totalInserts: 1,
    totalUpdates: 2,
    totalDeletes: 3,
    totalAlters: 4,
    totalCreates: 5,
    totalDrops: 6,
    totalCustoms: 0,
    customOperationsMap: null,
};

describe('convertAggregationsToOperationsData', () => {
    it('should return correct value when aggregation has custom types and defaults provided', () => {
        const defaultTypes = ['custom_type_3'];

        const response = convertAggregationsToOperationsData(EXAMPLE_AGGREGATION_WITH_CUSTOM_TYPES, defaultTypes);

        expect(response?.summary.totalCustomOperations).toBe(3);
        expect(response?.summary.totalOperations).toBe(24);
        expect(response?.operations).toStrictEqual({
            inserts: {
                value: 1,
                key: 'INSERT',
                group: AggregationGroup.Purple,
                type: OperationType.Insert,
                name: 'Insert',
            },
            updates: {
                value: 2,
                key: 'UPDATE',
                group: AggregationGroup.Purple,
                type: OperationType.Update,
                name: 'Update',
            },
            deletes: {
                value: 3,
                key: 'DELETE',
                group: AggregationGroup.Red,
                type: OperationType.Delete,
                name: 'Delete',
            },
            alters: {
                value: 4,
                key: 'ALTER',
                group: AggregationGroup.Purple,
                type: OperationType.Alter,
                name: 'Alter',
            },
            creates: {
                value: 5,
                key: 'CREATE',
                group: AggregationGroup.Purple,
                type: OperationType.Create,
                name: 'Create',
            },
            drops: {
                value: 6,
                key: 'DROP',
                group: AggregationGroup.Red,
                type: OperationType.Drop,
                name: 'Drop',
            },
            custom_custom_type_1: {
                value: 1,
                group: AggregationGroup.Purple,
                type: OperationType.Custom,
                customType: 'custom_type_1',
                name: 'custom_type_1',
                key: 'custom_custom_type_1',
            },
            custom_custom_type_2: {
                value: 2,
                group: AggregationGroup.Purple,
                type: OperationType.Custom,
                customType: 'custom_type_2',
                name: 'custom_type_2',
                key: 'custom_custom_type_2',
            },
            custom_custom_type_3: {
                group: AggregationGroup.Purple,
                key: 'custom_custom_type_3',
                customType: 'custom_type_3',
                name: 'custom_type_3',
                type: OperationType.Custom,
                value: 0,
            },
        });
    });

    it('should return correct value when aggregation has custom types and defaults not provided', () => {
        const response = convertAggregationsToOperationsData(EXAMPLE_AGGREGATION_WITH_CUSTOM_TYPES);

        expect(response?.summary.totalCustomOperations).toBe(3);
        expect(response?.summary.totalOperations).toBe(24);
        expect(response?.operations).toStrictEqual({
            inserts: {
                value: 1,
                key: 'INSERT',
                group: AggregationGroup.Purple,
                type: OperationType.Insert,
                name: 'Insert',
            },
            updates: {
                value: 2,
                key: 'UPDATE',
                group: AggregationGroup.Purple,
                type: OperationType.Update,
                name: 'Update',
            },
            deletes: {
                value: 3,
                key: 'DELETE',
                group: AggregationGroup.Red,
                type: OperationType.Delete,
                name: 'Delete',
            },
            alters: {
                value: 4,
                key: 'ALTER',
                group: AggregationGroup.Purple,
                type: OperationType.Alter,
                name: 'Alter',
            },
            creates: {
                value: 5,
                key: 'CREATE',
                group: AggregationGroup.Purple,
                type: OperationType.Create,
                name: 'Create',
            },
            drops: {
                value: 6,
                key: 'DROP',
                group: AggregationGroup.Red,
                type: OperationType.Drop,
                name: 'Drop',
            },
            custom_custom_type_1: {
                value: 1,
                group: AggregationGroup.Purple,
                type: OperationType.Custom,
                customType: 'custom_type_1',
                name: 'custom_type_1',
                key: 'custom_custom_type_1',
            },
            custom_custom_type_2: {
                value: 2,
                group: AggregationGroup.Purple,
                type: OperationType.Custom,
                customType: 'custom_type_2',
                name: 'custom_type_2',
                key: 'custom_custom_type_2',
            },
        });
    });

    it('should return correct value when aggregation has no custom types and defaults provided', () => {
        const defaultTypes = ['custom_type_3'];

        const response = convertAggregationsToOperationsData(EXAMPLE_AGGREGATION_WITHOUT_CUSTOM_TYPES, defaultTypes);

        expect(response?.summary.totalCustomOperations).toBe(0);
        expect(response?.summary.totalOperations).toBe(21);
        expect(response?.operations).toStrictEqual({
            inserts: {
                value: 1,
                key: 'INSERT',
                group: AggregationGroup.Purple,
                type: OperationType.Insert,
                name: 'Insert',
            },
            updates: {
                value: 2,
                key: 'UPDATE',
                group: AggregationGroup.Purple,
                type: OperationType.Update,
                name: 'Update',
            },
            deletes: {
                value: 3,
                key: 'DELETE',
                group: AggregationGroup.Red,
                type: OperationType.Delete,
                name: 'Delete',
            },
            alters: {
                value: 4,
                key: 'ALTER',
                group: AggregationGroup.Purple,
                type: OperationType.Alter,
                name: 'Alter',
            },
            creates: {
                value: 5,
                key: 'CREATE',
                group: AggregationGroup.Purple,
                type: OperationType.Create,
                name: 'Create',
            },
            drops: {
                value: 6,
                key: 'DROP',
                group: AggregationGroup.Red,
                type: OperationType.Drop,
                name: 'Drop',
            },
            custom_custom_type_3: {
                value: 0,
                group: AggregationGroup.Purple,
                type: OperationType.Custom,
                customType: 'custom_type_3',
                name: 'custom_type_3',
                key: 'custom_custom_type_3',
            },
        });
    });

    it('should return correct value when aggregation has no custom types and defaults not provided', () => {
        const response = convertAggregationsToOperationsData(EXAMPLE_AGGREGATION_WITHOUT_CUSTOM_TYPES);

        expect(response?.summary.totalCustomOperations).toBe(0);
        expect(response?.summary.totalOperations).toBe(21);
        expect(response?.operations).toStrictEqual({
            inserts: {
                value: 1,
                key: 'INSERT',
                group: AggregationGroup.Purple,
                type: OperationType.Insert,
                name: 'Insert',
            },
            updates: {
                value: 2,
                key: 'UPDATE',
                group: AggregationGroup.Purple,
                type: OperationType.Update,
                name: 'Update',
            },
            deletes: {
                value: 3,
                key: 'DELETE',
                group: AggregationGroup.Red,
                type: OperationType.Delete,
                name: 'Delete',
            },
            alters: {
                value: 4,
                key: 'ALTER',
                group: AggregationGroup.Purple,
                type: OperationType.Alter,
                name: 'Alter',
            },
            creates: {
                value: 5,
                key: 'CREATE',
                group: AggregationGroup.Purple,
                type: OperationType.Create,
                name: 'Create',
            },
            drops: {
                value: 6,
                key: 'DROP',
                group: AggregationGroup.Red,
                type: OperationType.Drop,
                name: 'Drop',
            },
        });
    });

    it('should return correct value when aggregation is undefined and defaults provided', () => {
        const defaultTypes = ['custom_type_3'];

        const response = convertAggregationsToOperationsData(undefined, defaultTypes);

        expect(response?.summary.totalCustomOperations).toBe(0);
        expect(response?.summary.totalOperations).toBe(0);
        expect(response?.operations).toStrictEqual({
            inserts: {
                value: 0,
                key: 'INSERT',
                group: AggregationGroup.Purple,
                type: OperationType.Insert,
                name: 'Insert',
            },
            updates: {
                value: 0,
                key: 'UPDATE',
                group: AggregationGroup.Purple,
                type: OperationType.Update,
                name: 'Update',
            },
            deletes: {
                value: 0,
                key: 'DELETE',
                group: AggregationGroup.Red,
                type: OperationType.Delete,
                name: 'Delete',
            },
            alters: {
                value: 0,
                key: 'ALTER',
                group: AggregationGroup.Purple,
                type: OperationType.Alter,
                name: 'Alter',
            },
            creates: {
                value: 0,
                key: 'CREATE',
                group: AggregationGroup.Purple,
                type: OperationType.Create,
                name: 'Create',
            },
            drops: {
                value: 0,
                key: 'DROP',
                group: AggregationGroup.Red,
                type: OperationType.Drop,
                name: 'Drop',
            },
            custom_custom_type_3: {
                value: 0,
                group: AggregationGroup.Purple,
                type: OperationType.Custom,
                customType: 'custom_type_3',
                name: 'custom_type_3',
                key: 'custom_custom_type_3',
            },
        });
    });

    it('should return correct value when aggregation is undefined and defaults not provided', () => {
        const response = convertAggregationsToOperationsData(undefined);

        expect(response?.summary.totalCustomOperations).toBe(0);
        expect(response?.summary.totalOperations).toBe(0);
        expect(response?.operations).toStrictEqual({
            inserts: {
                value: 0,
                key: 'INSERT',
                group: AggregationGroup.Purple,
                type: OperationType.Insert,
                name: 'Insert',
            },
            updates: {
                value: 0,
                key: 'UPDATE',
                group: AggregationGroup.Purple,
                type: OperationType.Update,
                name: 'Update',
            },
            deletes: {
                value: 0,
                key: 'DELETE',
                group: AggregationGroup.Red,
                type: OperationType.Delete,
                name: 'Delete',
            },
            alters: {
                value: 0,
                key: 'ALTER',
                group: AggregationGroup.Purple,
                type: OperationType.Alter,
                name: 'Alter',
            },
            creates: {
                value: 0,
                key: 'CREATE',
                group: AggregationGroup.Purple,
                type: OperationType.Create,
                name: 'Create',
            },
            drops: {
                value: 0,
                key: 'DROP',
                group: AggregationGroup.Red,
                type: OperationType.Drop,
                name: 'Drop',
            },
        });
    });
});
