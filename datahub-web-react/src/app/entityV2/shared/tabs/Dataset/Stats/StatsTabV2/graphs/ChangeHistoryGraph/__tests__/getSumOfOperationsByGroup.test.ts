import { DEFAULT_OPERATION_TYPES } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/constants';
import {
    AggregationGroup,
    OperationsData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { getSumOfOperationsByAggregationGroup } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/utils';
import { OperationType } from '@src/types.generated';

const OPERATIONS_DATA_SAMPLE: OperationsData = {
    summary: {
        mom: null,
        totalOperations: 23,
        totalCustomOperations: 1,
    },
    operations: {
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
    },
};

const OPERATIONS_DATA_WITHOUT_CUSTOM_TYPES_SAMPLE: OperationsData = {
    summary: {
        mom: null,
        totalOperations: 22,
        totalCustomOperations: 0,
    },
    operations: {
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
    },
};

describe('getSumOfOperationsByGroup', () => {
    it('should correctly summarize values by purple group', () => {
        const response = getSumOfOperationsByAggregationGroup(OPERATIONS_DATA_SAMPLE, AggregationGroup.Purple, [
            ...DEFAULT_OPERATION_TYPES,
            'custom_custom_type_1',
        ]);

        expect(response).toBe(13);
    });

    it('should correctly summarize values by red group', () => {
        const response = getSumOfOperationsByAggregationGroup(OPERATIONS_DATA_SAMPLE, AggregationGroup.Red, [
            ...DEFAULT_OPERATION_TYPES,
            'custom_custom_type_1',
        ]);

        expect(response).toBe(9);
    });

    it('should correctly summarize values by purple group when data has no custom operations', () => {
        const response = getSumOfOperationsByAggregationGroup(
            OPERATIONS_DATA_WITHOUT_CUSTOM_TYPES_SAMPLE,
            AggregationGroup.Purple,
            [...DEFAULT_OPERATION_TYPES, 'custom_custom_type_1'],
        );

        expect(response).toBe(12);
    });

    it('should correctly summarize values by red group when data has no custom operations', () => {
        const response = getSumOfOperationsByAggregationGroup(
            OPERATIONS_DATA_WITHOUT_CUSTOM_TYPES_SAMPLE,
            AggregationGroup.Red,
            [...DEFAULT_OPERATION_TYPES, 'custom_custom_type_1'],
        );

        expect(response).toBe(9);
    });

    it('should correctly summarize values by provided operation types', () => {
        const response = getSumOfOperationsByAggregationGroup(OPERATIONS_DATA_SAMPLE, AggregationGroup.Purple, [
            OperationType.Create,
        ]);

        expect(response).toBe(5);
    });
});
